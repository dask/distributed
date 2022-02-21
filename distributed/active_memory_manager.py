from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Generator
from typing import TYPE_CHECKING

from tornado.ioloop import PeriodicCallback

import dask
from dask.utils import parse_timedelta

from .core import Status
from .metrics import time
from .utils import import_term, log_errors

if TYPE_CHECKING:  # pragma: nocover
    from .client import Client
    from .scheduler import Scheduler, TaskState, WorkerState

# Main logger. This is reasonably terse also at DEBUG level.
logger = logging.getLogger(__name__)
# Per-task logging. Exceptionally verbose at DEBUG level.
task_logger = logging.getLogger(__name__ + ".tasks")


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

    scheduler: Scheduler
    policies: set[ActiveMemoryManagerPolicy]
    interval: float

    # These attributes only exist within the scope of self.run()
    # Current memory (in bytes) allocated on each worker, plus/minus pending actions
    workers_memory: dict[WorkerState, int]
    # Pending replications and deletions for each task
    pending: dict[TaskState, tuple[set[WorkerState], set[WorkerState]]]

    def __init__(
        self,
        scheduler: Scheduler,
        # The following parameters are exposed so that one may create, run, and throw
        # away on the fly a specialized manager, separate from the main one.
        policies: set[ActiveMemoryManagerPolicy] | None = None,
        *,
        register: bool = True,
        start: bool | None = None,
        interval: float | None = None,
    ):
        self.scheduler = scheduler
        self.policies = set()

        if policies is None:
            # Initialize policies from config
            policies = set()
            for kwargs in dask.config.get(
                "distributed.scheduler.active-memory-manager.policies"
            ):
                kwargs = kwargs.copy()
                cls = import_term(kwargs.pop("class"))
                policies.add(cls(**kwargs))

        for policy in policies:
            self.add_policy(policy)

        if register:
            scheduler.extensions["amm"] = self
            scheduler.handlers["amm_handler"] = self.amm_handler

        if interval is None:
            interval = parse_timedelta(
                dask.config.get("distributed.scheduler.active-memory-manager.interval")
            )
        self.interval = interval
        if start is None:
            start = dask.config.get("distributed.scheduler.active-memory-manager.start")
        if start:
            self.start()

    def amm_handler(self, comm, method: str):
        """Scheduler handler, invoked from the Client by
        :class:`~distributed.active_memory_manager.AMMClientProxy`
        """
        assert method in {"start", "stop", "run_once", "running"}
        out = getattr(self, method)
        return out() if callable(out) else out

    def start(self) -> None:
        """Start executing every ``self.interval`` seconds until scheduler shutdown"""
        if self.running:
            return
        pc = PeriodicCallback(self.run_once, self.interval * 1000.0)
        self.scheduler.periodic_callbacks[f"amm-{id(self)}"] = pc
        pc.start()

    def stop(self) -> None:
        """Stop periodic execution"""
        pc = self.scheduler.periodic_callbacks.pop(f"amm-{id(self)}", None)
        if pc:
            pc.stop()

    @property
    def running(self) -> bool:
        """Return True if the AMM is being triggered periodically; False otherwise"""
        return f"amm-{id(self)}" in self.scheduler.periodic_callbacks

    def add_policy(self, policy: ActiveMemoryManagerPolicy) -> None:
        if not isinstance(policy, ActiveMemoryManagerPolicy):
            raise TypeError(f"Expected ActiveMemoryManagerPolicy; got {policy!r}")
        self.policies.add(policy)
        policy.manager = self

    def run_once(self) -> None:
        """Run all policies once and asynchronously (fire and forget) enact their
        recommendations to replicate/drop tasks
        """
        with log_errors():
            ts_start = time()
            # This should never fail since this is a synchronous method
            assert not hasattr(self, "pending")

            self.pending = {}
            self.workers_memory = {
                w: w.memory.optimistic for w in self.scheduler.workers.values()
            }
            try:
                # populate self.pending
                self._run_policies()

                if self.pending:
                    self._enact_suggestions()
            finally:
                del self.workers_memory
                del self.pending
            ts_stop = time()
            logger.debug(
                "Active Memory Manager run in %.0fms", (ts_stop - ts_start) * 1000
            )

    def _run_policies(self) -> None:
        """Sequentially run ActiveMemoryManagerPolicy.run() for all registered policies,
        obtain replicate/drop suggestions, and use them to populate self.pending.
        """
        candidates: set[WorkerState] | None
        cmd: str
        ws: WorkerState | None
        ts: TaskState
        nreplicas: int

        for policy in list(self.policies):  # a policy may remove itself
            logger.debug("Running policy: %s", policy)
            policy_gen = policy.run()
            ws = None
            while True:
                try:
                    cmd, ts, candidates = policy_gen.send(ws)
                except StopIteration:
                    break  # next policy

                try:
                    pending_repl, pending_drop = self.pending[ts]
                except KeyError:
                    pending_repl = set()
                    pending_drop = set()
                    self.pending[ts] = pending_repl, pending_drop

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
        candidates: set[WorkerState] | None,
        pending_repl: set[WorkerState],
    ) -> WorkerState | None:
        """Choose a worker to acquire a new replica of an in-memory task among a set of
        candidates. If candidates is None, default to all workers in the cluster.
        Regardless, workers that either already hold a replica or are scheduled to
        receive one at the end of this AMM iteration are not considered.

        Returns
        -------
        The worker with the lowest memory usage (downstream of pending replications and
        drops), or None if no eligible candidates are available.
        """
        orig_candidates = candidates

        def log_reject(msg: str) -> None:
            task_logger.debug(
                "(replicate, %s, %s) rejected: %s", ts, orig_candidates, msg
            )

        if ts.state != "memory":
            log_reject(f"ts.state = {ts.state}")
            return None

        if candidates is None:
            candidates = self.scheduler.running.copy()
        else:
            # Don't modify orig_candidates
            candidates = candidates & self.scheduler.running
        if not candidates:
            log_reject("no running candidates")
            return None

        candidates -= ts.who_has
        if not candidates:
            log_reject("all candidates already own a replica")
            return None

        candidates -= pending_repl
        if not candidates:
            log_reject("already pending replication on all candidates")
            return None

        # Select candidate with the lowest memory usage
        choice = min(candidates, key=self.workers_memory.__getitem__)
        task_logger.debug(
            "(replicate, %s, %s): replicating to %s", ts, orig_candidates, choice
        )
        return choice

    def _find_dropper(
        self,
        ts: TaskState,
        candidates: set[WorkerState] | None,
        pending_drop: set[WorkerState],
    ) -> WorkerState | None:
        """Choose a worker to drop its replica of an in-memory task among a set of
        candidates. If candidates is None, default to all workers in the cluster.
        Regardless, workers that either do not hold a replica or are already scheduled
        to drop theirs at the end of this AMM iteration are not considered.
        This method also ensures that a key will not lose its last replica.

        Returns
        -------
        The worker with the highest memory usage (downstream of pending replications and
        drops), or None if no eligible candidates are available.
        """
        orig_candidates = candidates

        def log_reject(msg: str) -> None:
            task_logger.debug("(drop, %s, %s) rejected: %s", ts, orig_candidates, msg)

        if len(ts.who_has) - len(pending_drop) < 2:
            log_reject("less than 2 replicas exist")
            return None

        if candidates is None:
            candidates = ts.who_has.copy()
        else:
            # Don't modify orig_candidates
            candidates = candidates & ts.who_has
            if not candidates:
                log_reject("no candidates suggested by the policy own a replica")
                return None

        candidates -= pending_drop
        if not candidates:
            log_reject("already pending drop on all candidates")
            return None

        # The `candidates &` bit could seem redundant with `candidates -=` immediately
        # below on first look, but beware of the second use of this variable later on!
        candidates_with_dependents_processing = candidates & {
            waiter_ts.processing_on for waiter_ts in ts.waiters
        }

        candidates -= candidates_with_dependents_processing
        if not candidates:
            log_reject("all candidates have dependent tasks queued or running on them")
            return None

        # Select candidate with the highest memory usage.
        # Drop from workers with status paused or closing_gracefully first.
        choice = max(
            candidates,
            key=lambda ws: (ws.status != Status.running, self.workers_memory[ws]),
        )

        # IF there is only one candidate that could drop the key
        # AND the candidate has status=running
        # AND there were candidates with status=paused or closing_gracefully, but we
        # discarded them above because they have dependent tasks running on them,
        # THEN temporarily keep the extra replica on the candidate with status=running.
        #
        # This prevents a ping-pong effect between ReduceReplicas (or any other policy
        # that yields drop commands with multiple candidates) and RetireWorker:
        # 1. RetireWorker replicates in-memory tasks from worker A (very busy and being
        #    retired) to worker B (idle)
        # 2. on the next AMM iteration 2 seconds later, ReduceReplicas drops the same
        #    tasks from B (because the replicas on A have dependants on the same worker)
        # 3. on the third AMM iteration 2 seconds later, goto 1 in an infinite loop
        #    which will last for as long as any tasks with dependencies are running on A
        if (
            len(candidates) == 1
            and choice.status == Status.running
            and candidates_with_dependents_processing
            and all(
                ws.status != Status.running
                for ws in candidates_with_dependents_processing
            )
        ):
            log_reject(
                "there is only one replica on workers that aren't paused or retiring"
            )
            return None

        task_logger.debug(
            "(drop, %s, %s): dropping from %s", ts, orig_candidates, choice
        )
        return choice

    def _enact_suggestions(self) -> None:
        """Iterate through self.pending, which was filled by self._run_policies(), and
        push the suggestions to the workers through bulk comms. Return immediately.
        """
        logger.debug("Enacting suggestions for %d tasks:", len(self.pending))

        validate = self.scheduler.validate
        drop_by_worker: (defaultdict[WorkerState, list[str]]) = defaultdict(list)
        repl_by_worker: (defaultdict[WorkerState, list[str]]) = defaultdict(list)

        for ts, (pending_repl, pending_drop) in self.pending.items():
            if not ts.who_has:
                continue
            if validate:
                # Never drop the last replica
                assert ts.who_has - pending_drop

            for ws in pending_repl:
                if validate:
                    assert ws not in ts.who_has
                repl_by_worker[ws].append(ts.key)
            for ws in pending_drop:
                if validate:
                    assert ws in ts.who_has
                drop_by_worker[ws].append(ts.key)

        stimulus_id = f"active_memory_manager-{time()}"
        for ws, keys in repl_by_worker.items():
            logger.debug("- %s to acquire %d replicas", ws, len(keys))
            self.scheduler.request_acquire_replicas(
                ws.address, keys, stimulus_id=stimulus_id
            )
        for ws, keys in drop_by_worker.items():
            logger.debug("- %s to drop %d replicas", ws, len(keys))
            self.scheduler.request_remove_replicas(
                ws.address, keys, stimulus_id=stimulus_id
            )


class ActiveMemoryManagerPolicy:
    """Abstract parent class"""

    manager: ActiveMemoryManagerExtension

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def run(
        self,
    ) -> Generator[
        tuple[str, TaskState, set[WorkerState] | None],
        WorkerState | None,
        None,
    ]:
        """This method is invoked by the ActiveMemoryManager every few seconds, or
        whenever the user invokes ``client.amm.run_once``.
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

        .. code-block:: python

           choice = (yield "replicate", ts, None)

        ``choice`` is either a WorkerState or None; the latter is returned if the
        ActiveMemoryManager chose to disregard the request.

        The current pending (accepted) commands can be inspected on
        ``self.manager.pending``; this includes the commands previously yielded by this
        same method.

        The current memory usage on each worker, *downstream of all pending commands*,
        can be inspected on ``self.manager.workers_memory``.
        """
        raise NotImplementedError("Virtual method")  # pragma: nocover


class AMMClientProxy:
    """Convenience accessors to operate the AMM from the dask client

    Usage: ``client.amm.start()`` etc.

    All methods are asynchronous if the client is asynchronous and synchronous if the
    client is synchronous.
    """

    _client: Client

    def __init__(self, client: Client):
        self._client = client

    def _run(self, method: str):
        """Remotely invoke ActiveMemoryManagerExtension.amm_handler"""
        return self._client.sync(self._client.scheduler.amm_handler, method=method)

    def start(self):
        return self._run("start")

    def stop(self):
        return self._run("stop")

    def run_once(self):
        return self._run("run_once")

    def running(self):
        return self._run("running")


class ReduceReplicas(ActiveMemoryManagerPolicy):
    """Make sure that in-memory tasks are not replicated on more workers than desired;
    drop the excess replicas.
    """

    def run(self):
        nkeys = 0
        ndrop = 0

        for ts in self.manager.scheduler.replicated_tasks:
            desired_replicas = 1  # TODO have a marker on TaskState

            # If a dependent task has not been assigned to a worker yet, err on the side
            # of caution and preserve an additional replica for it.
            # However, if two dependent tasks have been already assigned to the same
            # worker, don't double count them.
            nwaiters = len({waiter.processing_on or waiter for waiter in ts.waiters})

            ndrop_key = len(ts.who_has) - max(desired_replicas, nwaiters)
            if ts in self.manager.pending:
                pending_repl, pending_drop = self.manager.pending[ts]
                ndrop_key += len(pending_repl) - len(pending_drop)

            if ndrop_key > 0:
                nkeys += 1
                ndrop += ndrop_key
                for _ in range(ndrop_key):
                    yield "drop", ts, None

        if ndrop:
            logger.debug(
                "ReduceReplicas: Dropping %d superfluous replicas of %d tasks",
                ndrop,
                nkeys,
            )


class RetireWorker(ActiveMemoryManagerPolicy):
    """Replicate somewhere else all unique in-memory tasks on a worker, preparing for
    its shutdown.

    At any given time, the AMM may have registered multiple instances of this policy,
    one for each worker currently being retired - meaning that most of the time no
    instances will be registered at all. For this reason, this policy doesn't figure in
    the dask config (:file:`distributed.yaml`). Instances are added by
    :meth:`distributed.Scheduler.retire_workers` and automatically remove themselves
    once the worker has been retired. If the AMM is disabled in the dask config,
    :meth:`~distributed.Scheduler.retire_workers` will start a temporary ad-hoc one.

    **Failure condition**

    There may not be any suitable workers to receive the tasks from the retiring worker.
    This happens in two use cases:

    1. This is the only worker in the cluster, or
    2. All workers are either paused or being retired at the same time

    In either case, this policy will fail to move out all keys and set the
    ``no_recipients`` boolean to True. :meth:`~distributed.Scheduler.retire_workers`
    will abort the retirement.

    There is a third use case, where a task fails to be replicated away for whatever
    reason, e.g. because its recipient is unresponsive but the Scheduler doesn't know
    yet. In this case we'll just wait for the next AMM iteration and try again (possibly
    with a different receiving worker, e.g. if the receiving worker was hung but not yet
    declared dead).

    **Retiring a worker with spilled tasks**

    On its very first iteration, this policy suggests that other workers should fetch
    all unique in-memory tasks of the retiring worker. Frequently, this means that in
    the next few moments the retiring worker will be bombarded by
    :meth:`distributed.worker.Worker.get_data` calls from the rest of the cluster. This
    can be a problem if most of the managed memory of the worker has been spilled out,
    as it could send the worker above its terminate threshold. Two measures are in place
    in order to prevent this:

    - At every iteration, this policy drops all tasks on the retiring worker that have
      already been replicated somewhere else. This makes room for further tasks to be
      moved out of the spill file in order to be replicated onto another worker.
    - Once the worker passes the ``pause`` threshold,
      :meth:`~distributed.worker.Worker.get_data` throttles the number of outgoing
      connections to 1.

    Parameters
    ==========
    address: str
        URI of the worker to be retired
    """

    address: str
    no_recipients: bool

    def __init__(self, address: str):
        self.address = address
        self.no_recipients = False

    def __repr__(self) -> str:
        return f"RetireWorker({self.address!r})"

    def run(self):
        """"""
        ws = self.manager.scheduler.workers.get(self.address)
        if ws is None:
            logger.debug("Removing policy %s: Worker no longer in cluster", self)
            self.manager.policies.remove(self)
            return

        nrepl = 0
        nno_rec = 0

        logger.debug("Retiring %s", ws)
        for ts in ws.has_what:
            if len(ts.who_has) > 1:
                # There are already replicas of this key on other workers.
                # Suggest dropping the replica from this worker.
                # Use cases:
                # 1. The suggestion is accepted by the AMM and by the Worker.
                #    The replica on this worker is dropped.
                # 2. The suggestion is accepted by the AMM, but rejected by the Worker.
                #    We'll try again at the next AMM iteration.
                # 3. The suggestion is rejected by the AMM, because another policy
                #    (e.g. ReduceReplicas) already suggested the same for this worker
                # 4. The suggestion is rejected by the AMM, because the task has
                #    dependents queued or running on the same worker.
                #    We'll try again at the next AMM iteration.
                # 5. The suggestion is rejected by the AMM, because all replicas of the
                #    key are on workers being retired and the other RetireWorker
                #    instances already made the same suggestion. We need to deal with
                #    this case and create a replica elsewhere.
                drop_ws = (yield "drop", ts, {ws})
                if drop_ws:
                    continue  # Use case 1 or 2
                if ts.who_has & self.manager.scheduler.running:
                    continue  # Use case 3 or 4
                # Use case 5

            # Either the worker holds the only replica or all replicas are being held
            # by workers that are being retired
            nrepl += 1
            # Don't create an unnecessary additional replica if another policy already
            # asked for one
            try:
                has_pending_repl = bool(self.manager.pending[ts][0])
            except KeyError:
                has_pending_repl = False

            if not has_pending_repl:
                rec_ws = (yield "replicate", ts, None)
                if not rec_ws:
                    # replication was rejected by the AMM (see _find_recipient)
                    nno_rec += 1

        if nno_rec:
            # All workers are paused or closing_gracefully.
            # Scheduler.retire_workers will read this flag and exit immediately.
            # TODO after we implement the automatic transition of workers from paused
            #      to closing_gracefully after a timeout expires, we should revisit this
            #      code to wait for paused workers and only exit immediately if all
            #      workers are in closing_gracefully status.
            self.no_recipients = True
            logger.warning(
                f"Tried retiring worker {self.address}, but {nno_rec} tasks could not "
                "be moved as there are no suitable workers to receive them. "
                "The worker will not be retired."
            )
            self.manager.policies.remove(self)
        elif nrepl:
            logger.info(
                f"Retiring worker {self.address}; {nrepl} keys are being moved away.",
            )
        else:
            logger.info(
                f"Retiring worker {self.address}; no unique keys need to be moved away."
            )
            self.manager.policies.remove(self)

    def done(self) -> bool:
        """Return True if it is safe to close the worker down; False otherwise"""
        ws = self.manager.scheduler.workers.get(self.address)
        if ws is None:
            return True
        return all(len(ts.who_has) > 1 for ts in ws.has_what)
