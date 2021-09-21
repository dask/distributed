from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Generator
from typing import TYPE_CHECKING, Optional

from tornado.ioloop import PeriodicCallback

import dask
from dask.utils import parse_timedelta

from .utils import import_term

if TYPE_CHECKING:
    from .scheduler import SchedulerState, TaskState, WorkerState


class ActiveMemoryManagerExtension:
    """Scheduler extension that optimizes memory usage across the cluster.
    It can be either triggered by hand or automatically every few seconds; at every
    iteration it performs one or both of the following:

    - create new replicas of in-memory tasks
    - destroy replicas of in-memory tasks; this never destroys the last available copy.

    There are no 'move' operations. A move is performed in two passes: first you create
    a copy and, in the next iteration, you delete the original (if the copy succeeded).

    This extension is configured by the dask config section
    ``distributed.scheduler.active-memory-manager``.
    """

    scheduler: SchedulerState
    policies: set[ActiveMemoryManagerPolicy]
    interval: float

    # These attributes only exist within the scope of self.run()
    # Current memory (in bytes) allocated on each worker, plus/minus pending actions
    workers_memory: dict[WorkerState, int]
    # Pending replications and deletions for each task
    pending: defaultdict[TaskState, tuple[set[WorkerState], set[WorkerState]]]

    def __init__(
        self,
        scheduler: SchedulerState,
        # The following parameters are exposed so that one may create, run, and throw
        # away on the fly a specialized manager, separate from the main one.
        policies: Optional[set[ActiveMemoryManagerPolicy]] = None,
        register: bool = True,
        start: Optional[bool] = None,
        interval: Optional[float] = None,
    ):
        self.scheduler = scheduler

        if policies is None:
            policies = set()
            for kwargs in dask.config.get(
                "distributed.scheduler.active-memory-manager.policies"
            ):
                kwargs = kwargs.copy()
                cls = import_term(kwargs.pop("class"))
                if not issubclass(cls, ActiveMemoryManagerPolicy):
                    raise TypeError(
                        f"{cls}: Expected ActiveMemoryManagerPolicy; got {type(cls)}"
                    )
                policies.add(cls(**kwargs))

        for policy in policies:
            policy.manager = self
        self.policies = policies

        if register:
            scheduler.extensions["amm"] = self
            scheduler.handlers.update(
                {
                    "amm_run_once": self.run_once,
                    "amm_start": self.start,
                    "amm_stop": self.stop,
                }
            )

        if interval is None:
            interval = parse_timedelta(
                dask.config.get("distributed.scheduler.active-memory-manager.interval")
            )
        self.interval = interval
        if start is None:
            start = dask.config.get("distributed.scheduler.active-memory-manager.start")
        if start:
            self.start()

    def start(self, comm=None) -> None:
        """Start executing every ``self.interval`` seconds until scheduler shutdown"""
        pc = PeriodicCallback(self.run_once, self.interval * 1000.0)
        self.scheduler.periodic_callbacks["amm"] = pc
        pc.start()

    def stop(self, comm=None) -> None:
        """Stop periodic execution"""
        pc = self.scheduler.periodic_callbacks.pop("amm", None)
        if pc:
            pc.stop()

    def run_once(self, comm=None) -> None:
        """Run all policies once and asynchronously (fire and forget) enact their
        recommendations to replicate/drop keys
        """
        # This should never fail since this is a synchronous method
        assert not hasattr(self, "pending")

        self.pending = defaultdict(lambda: (set(), set()))
        self.workers_memory = {
            w: w.memory.optimistic for w in self.scheduler.workers.values()
        }
        try:
            # populate self.pending
            self._run_policies()

            drop_by_worker = defaultdict(set)
            repl_by_worker = defaultdict(dict)
            for ts, (pending_repl, pending_drop) in self.pending.items():
                if not ts.who_has:
                    continue
                who_has = [ws_snd.address for ws_snd in ts.who_has - pending_drop]
                assert who_has  # Never drop the last replica
                for ws_rec in pending_repl:
                    assert ws_rec not in ts.who_has
                    repl_by_worker[ws_rec.address][ts.key] = who_has
                for ws in pending_drop:
                    assert ws in ts.who_has
                    drop_by_worker[ws.address].add(ts.key)

            # Fire-and-forget enact recommendations from policies
            # This is temporary code, waiting for
            # https://github.com/dask/distributed/pull/5046
            for addr, who_has in repl_by_worker.items():
                asyncio.create_task(self.scheduler.gather_on_worker(addr, who_has))
            for addr, keys in drop_by_worker.items():
                asyncio.create_task(self.scheduler.delete_worker_data(addr, keys))
            # End temporary code

        finally:
            del self.workers_memory
            del self.pending

    def _run_policies(self) -> None:
        """Sequentially run ActiveMemoryManagerPolicy.run() for all registered policies,
        obtain replicate/drop suggestions, and use them to populate self.pending.
        """
        candidates: Optional[set[WorkerState]]
        cmd: str
        ws: Optional[WorkerState]
        ts: TaskState
        nreplicas: int

        for policy in list(self.policies):  # a policy may remove itself
            policy_gen = policy.run()
            ws = None
            while True:
                try:
                    cmd, ts, candidates = policy_gen.send(ws)
                except StopIteration:
                    break  # next policy

                pending_repl, pending_drop = self.pending[ts]

                if cmd == "replicate":
                    ws = self._find_recipient(ts, candidates, pending_repl)
                    if ws:
                        pending_repl.add(ws)
                        self.workers_memory[ws] += ts.nbytes

                elif cmd == "drop":
                    ws = self._find_dropper(ts, candidates, pending_drop)
                    if ws:
                        pending_drop.add(ws)
                        self.workers_memory[ws] = max(
                            0, self.workers_memory[ws] - ts.nbytes
                        )

                else:
                    raise ValueError(f"Unknown command: {cmd}")  # pragma: nocover

    def _find_recipient(
        self,
        ts: TaskState,
        candidates: Optional[set[WorkerState]],
        pending_repl: set[WorkerState],
    ) -> Optional[WorkerState]:
        """Choose a worker to acquire a new replica of an in-memory task among a set of
        candidates. If candidates is None, default to all workers in the cluster that do
        not hold a replica yet. The worker with the lowest memory usage (downstream of
        pending replications and drops) will be returned.
        """
        if ts.state != "memory":
            return None
        if candidates is None:
            candidates = set(self.scheduler.workers.values())
        candidates -= ts.who_has
        candidates -= pending_repl
        if not candidates:
            return None
        return min(candidates, key=self.workers_memory.get)

    def _find_dropper(
        self,
        ts: TaskState,
        candidates: Optional[set[WorkerState]],
        pending_drop: set[WorkerState],
    ) -> Optional[WorkerState]:
        """Choose a worker to drop its replica of an in-memory task among a set of
        candidates. If candidates is None, default to all workers in the cluster that
        hold a replica. The worker with the highest memory usage (downstream of pending
        replications and drops) will be returned.
        """
        if len(ts.who_has) - len(pending_drop) < 2:
            return None
        if candidates is None:
            candidates = ts.who_has.copy()
        else:
            candidates &= ts.who_has
        candidates -= pending_drop
        candidates -= {waiter_ts.processing_on for waiter_ts in ts.waiters}
        if not candidates:
            return None
        return max(candidates, key=self.workers_memory.get)


class ActiveMemoryManagerPolicy:
    """Abstract parent class"""

    manager: ActiveMemoryManagerExtension

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def run(
        self,
    ) -> Generator[
        tuple[str, TaskState, Optional[set[WorkerState]]],
        Optional[WorkerState],
        None,
    ]:
        """This method is invoked by the ActiveMemoryManager every few seconds, or
        whenever the user invokes scheduler.amm_run_once().
        It is an iterator that must emit any of the following:

        - "replicate", <TaskState>, None
        - "replicate", <TaskState>, {subset of potential workers to replicate to}
        - "drop", <TaskState>, None
        - "drop", <TaskState>, {subset of potential workers to drop from}

        Each element yielded indicates the desire to create or destroy a single replica
        of a key. If a subset of workers is not provided, it defaults to all workers on
        the cluster. Either the ActiveMemoryManager or the Worker may later decide to
        disregard the request, e.g. because it would delete the last copy of a key or
        because the key is currently needed on that worker.

        You may optionally retrieve which worker it was decided the key will be
        replicated to or dropped from, as follows:

        ```python
        choice = yield "replicate", ts, None
        ```

        ``choice`` is either a WorkerState or None; the latter is returned if the
        ActiveMemoryManager chose to disregard the request.

        The current pending (accepted) commands can be inspected on
        ``self.manager.pending``; this includes the commands previously yielded by this
        same method.

        The current memory usage on each worker, *downstream of all pending commands*,
        can be inspected on ``self.manager.workers_memory``.
        """
        raise NotImplementedError("Virtual method")


class ReduceReplicas(ActiveMemoryManagerPolicy):
    """Make sure that in-memory tasks are not replicated on more workers than desired;
    drop the excess replicas.
    """

    def run(self):
        # TODO this is O(n) to the total number of in-memory tasks on the cluster; it
        #      could be made faster by automatically attaching it to a TaskState when it
        #      goes above one replica and detaching it when it drops below two.
        for ts in self.manager.scheduler.tasks.values():
            if len(ts.who_has) < 2:
                continue

            desired_replicas = 1  # TODO have a marker on TaskState

            # If a dependent task has not been assigned to a worker yet, err on the side
            # of caution and preserve an additional replica for it.
            # However, if two dependent tasks have been already assigned to the same
            # worker, don't double count them.
            nwaiters = len({waiter.processing_on or waiter for waiter in ts.waiters})

            ndrop = len(ts.who_has) - max(desired_replicas, nwaiters)
            if ts in self.manager.pending:
                pending_repl, pending_drop = self.manager.pending[ts]
                ndrop += len(pending_repl) - len(pending_drop)

            # ndrop could be negative, which for range() is the same as 0.
            for _ in range(ndrop):
                yield "drop", ts, None
