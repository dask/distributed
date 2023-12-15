from __future__ import annotations

import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from dask.typing import Key

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.protocol.serialize import ToPickle
from distributed.shuffle._core import (
    SchedulerShuffleState,
    ShuffleId,
    ShuffleRunSpec,
    ShuffleSpec,
    barrier_key,
    id_from_key,
)
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin
from distributed.utils import log_errors

if TYPE_CHECKING:
    from distributed.scheduler import (
        Recs,
        Scheduler,
        TaskState,
        TaskStateState,
        WorkerState,
    )

logger = logging.getLogger(__name__)


class ShuffleSchedulerPlugin(SchedulerPlugin):
    """
    Shuffle plugin for the scheduler
    This coordinates the individual worker plugins to ensure correctness
    and collects heartbeat messages for the dashboard.
    See Also
    --------
    ShuffleWorkerPlugin
    """

    scheduler: Scheduler
    active_shuffles: dict[ShuffleId, SchedulerShuffleState]
    heartbeats: defaultdict[ShuffleId, dict]
    _shuffles: defaultdict[ShuffleId, set[SchedulerShuffleState]]
    _archived_by_stimulus: defaultdict[str, set[SchedulerShuffleState]]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers.update(
            {
                "shuffle_barrier": self.barrier,
                "shuffle_get": self.get,
                "shuffle_get_or_create": self.get_or_create,
                "shuffle_restrict_task": self.restrict_task,
            }
        )
        self.heartbeats = defaultdict(lambda: defaultdict(dict))
        self.active_shuffles = {}
        self.scheduler.add_plugin(self, name="shuffle")
        self._shuffles = defaultdict(set)
        self._archived_by_stimulus = defaultdict(set)

    async def start(self, scheduler: Scheduler) -> None:
        worker_plugin = ShuffleWorkerPlugin()
        await self.scheduler.register_worker_plugin(
            None, dumps(worker_plugin), name="shuffle", idempotent=False
        )

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.active_shuffles)

    async def barrier(self, id: ShuffleId, run_id: int, consistent: bool) -> None:
        shuffle = self.active_shuffles[id]
        if shuffle.run_id != run_id:
            raise ValueError(f"{run_id=} does not match {shuffle}")
        if not consistent:
            logger.warning(
                "Shuffle %s restarted due to data inconsistency during barrier",
                shuffle.id,
            )
            return self._restart_shuffle(
                shuffle.id,
                self.scheduler,
                stimulus_id=f"p2p-barrier-inconsistent-{time()}",
            )
        msg = {"op": "shuffle_inputs_done", "shuffle_id": id, "run_id": run_id}
        await self.scheduler.broadcast(
            msg=msg,
            workers=list(shuffle.participating_workers),
        )

    def restrict_task(self, id: ShuffleId, run_id: int, key: Key, worker: str) -> dict:
        shuffle = self.active_shuffles[id]
        if shuffle.run_id > run_id:
            return {
                "status": "error",
                "message": f"Request stale, expected {run_id=} for {shuffle}",
            }
        elif shuffle.run_id < run_id:
            return {
                "status": "error",
                "message": f"Request invalid, expected {run_id=} for {shuffle}",
            }
        ts = self.scheduler.tasks[key]
        self._set_restriction(ts, worker)
        return {"status": "OK"}

    def heartbeat(self, ws: WorkerState, data: dict) -> None:
        for shuffle_id, d in data.items():
            if shuffle_id in self.shuffle_ids():
                self.heartbeats[shuffle_id][ws.address].update(d)

    def get(self, id: ShuffleId, worker: str) -> ToPickle[ShuffleRunSpec]:
        if worker not in self.scheduler.workers:
            # This should never happen
            raise RuntimeError(
                f"Scheduler is unaware of this worker {worker!r}"
            )  # pragma: nocover
        state = self.active_shuffles[id]
        state.participating_workers.add(worker)
        return ToPickle(state.run_spec)

    def get_or_create(
        self,
        # FIXME: This should never be ToPickle[ShuffleSpec]
        spec: ShuffleSpec | ToPickle[ShuffleSpec],
        key: Key,
        worker: str,
    ) -> ToPickle[ShuffleRunSpec]:
        # FIXME: Sometimes, this doesn't actually get pickled
        if isinstance(spec, ToPickle):
            spec = spec.data
        try:
            return self.get(spec.id, worker)
        except KeyError:
            # FIXME: The current implementation relies on the barrier task to be
            # known by its name. If the name has been mangled, we cannot guarantee
            # that the shuffle works as intended and should fail instead.
            self._raise_if_barrier_unknown(spec.id)
            self._raise_if_task_not_processing(key)
            worker_for = self._calculate_worker_for(spec)
            self._ensure_output_tasks_are_non_rootish(spec)
            state = spec.create_new_run(worker_for)
            self.active_shuffles[spec.id] = state
            self._shuffles[spec.id].add(state)
            state.participating_workers.add(worker)
            return ToPickle(state.run_spec)

    def _raise_if_barrier_unknown(self, id: ShuffleId) -> None:
        key = barrier_key(id)
        try:
            self.scheduler.tasks[key]
        except KeyError:
            raise RuntimeError(
                f"Barrier task with key {key!r} does not exist. This may be caused by "
                "task fusion during graph generation. Please let us know that you ran "
                "into this by leaving a comment at distributed#7816."
            )

    def _raise_if_task_not_processing(self, key: Key) -> None:
        task = self.scheduler.tasks[key]
        if task.state != "processing":
            raise RuntimeError(f"Expected {task} to be processing, is {task.state}.")

    def _calculate_worker_for(self, spec: ShuffleSpec) -> dict[Any, str]:
        """Pin the outputs of a P2P shuffle to specific workers.

        The P2P implementation of a hash join combines the loading of shuffled output
        partitions for the left and right side with the actual merge operation into a
        single output task. As a consequence, we need to make sure that shuffles with
        shared output tasks align on the output mapping.

        Parameters
        ----------
        id: ID of the shuffle to pin
        output_partitions: Output partition IDs to pin
        pick: Function that picks a worker given a partition ID and sequence of worker

        .. note:
            This function assumes that the barrier task and the output tasks share
            the same worker restrictions.
        """
        existing: dict[Any, str] = {}
        shuffle_id = spec.id
        barrier = self.scheduler.tasks[barrier_key(shuffle_id)]

        if barrier.worker_restrictions:
            workers = list(barrier.worker_restrictions)
        else:
            workers = list(self.scheduler.workers)

        # Check if this shuffle shares output tasks with a different shuffle that has
        # already been initialized and needs to be taken into account when
        # mapping output partitions to workers.
        # Naively, you could delete this whole paragraph and just call
        # spec.pick_worker; it would return two identical sets of results on both calls
        # of this method... until the set of available workers changes between the two
        # calls, which would cause misaligned shuffle outputs and a deadlock.
        seen = {barrier}
        for dependent in barrier.dependents:
            for possible_barrier in dependent.dependencies:
                if possible_barrier in seen:
                    continue
                seen.add(possible_barrier)
                if not (other_barrier_key := id_from_key(possible_barrier.key)):
                    continue
                if not (shuffle := self.active_shuffles.get(other_barrier_key)):
                    continue
                current_worker_for = shuffle.run_spec.worker_for
                # This is a fail-safe for future three-ways merges. At the moment there
                # should only ever be at most one other shuffle that shares output
                # tasks, so existing will always be empty.
                if existing:  # pragma: nocover
                    for shared_key in existing.keys() & current_worker_for.keys():
                        if existing[shared_key] != current_worker_for[shared_key]:
                            raise RuntimeError(
                                f"Failed to initialize shuffle {spec.id} because "
                                "it cannot align output partition mappings between "
                                f"existing shuffles {seen}. "
                                f"Mismatch encountered for output partition {shared_key!r}: "
                                f"{existing[shared_key]} != {current_worker_for[shared_key]}."
                            )
                existing.update(current_worker_for)

        worker_for = {}
        for partition in spec.output_partitions:
            if (worker := existing.get(partition, None)) is None:
                worker = spec.pick_worker(partition, workers)
            worker_for[partition] = worker
        return worker_for

    def _ensure_output_tasks_are_non_rootish(self, spec: ShuffleSpec) -> None:
        """Output tasks are created without worker restrictions and run once with the
        only purpose of setting the worker restriction and then raising Reschedule, and
        then running again properly on the correct worker. It would be non-trivial to
        set the worker restriction before they're first run due to potential task
        fusion.

        Most times, this lack of initial restrictions would cause output tasks to be
        labelled as rootish on their first (very fast) run, which in turn would break
        the design assumption that the worker-side queue of rootish tasks will last long
        enough to cover the round-trip to the scheduler to receive more tasks, which in
        turn would cause a measurable slowdown on the overall runtime of the shuffle
        operation.

        This method ensures that, given M output tasks and N workers, each worker-side
        queue is pre-loaded with M/N output tasks which can be flushed very fast as
        they all raise Reschedule() in quick succession.

        See Also
        --------
        ShuffleRun._ensure_output_worker
        """
        barrier = self.scheduler.tasks[barrier_key(spec.id)]
        for dependent in barrier.dependents:
            dependent._rootish = False

    @log_errors()
    def _set_restriction(self, ts: TaskState, worker: str) -> None:
        if ts.annotations and "shuffle_original_restrictions" in ts.annotations:
            # This may occur if multiple barriers share the same output task,
            # e.g. in a hash join.
            return
        if ts.annotations is None:
            ts.annotations = dict()
        ts.annotations["shuffle_original_restrictions"] = (
            ts.worker_restrictions.copy()
            if ts.worker_restrictions is not None
            else None
        )
        self.scheduler.set_restrictions({ts.key: {worker}})

    @log_errors()
    def _unset_restriction(self, ts: TaskState) -> None:
        # shuffle_original_restrictions is only set if the task was first scheduled
        # on the wrong worker
        if (
            ts.annotations is None
            or "shuffle_original_restrictions" not in ts.annotations
        ):
            return
        original_restrictions = ts.annotations.pop("shuffle_original_restrictions")
        self.scheduler.set_restrictions({ts.key: original_restrictions})

    def _restart_recommendations(self, id: ShuffleId) -> Recs:
        barrier_task = self.scheduler.tasks[barrier_key(id)]
        recs: Recs = {}

        for dt in barrier_task.dependents:
            if dt.state == "erred":
                return {}
            recs.update({dt.key: "released"})

        if barrier_task.state == "erred":
            # This should never happen, a dependent of the barrier should already
            # be `erred`
            raise RuntimeError(
                f"Expected dependents of {barrier_task=} to be 'erred' if "
                "the barrier is."
            )  # pragma: no cover
        recs.update({barrier_task.key: "released"})

        for dt in barrier_task.dependencies:
            if dt.state == "erred":
                # This should never happen, a dependent of the barrier should already
                # be `erred`
                raise RuntimeError(
                    f"Expected barrier and its dependents to be "
                    f"'erred' if the barrier's dependency {dt} is."
                )  # pragma: no cover
            recs.update({dt.key: "released"})
        return recs

    def _restart_shuffle(
        self, id: ShuffleId, scheduler: Scheduler, *, stimulus_id: str
    ) -> None:
        recs = self._restart_recommendations(id)
        self.scheduler.transitions(recs, stimulus_id=stimulus_id)
        self.scheduler.stimulus_queue_slots_maybe_opened(stimulus_id=stimulus_id)
        logger.warning("Shuffle %s restarted due to stimulus '%s", id, stimulus_id)

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None:
        """Restart all active shuffles when a participating worker leaves the cluster.

        .. note::
            Due to the order of operations in :meth:`~Scheduler.remove_worker`, the
            shuffle may have already been archived by
            :meth:`~ShuffleSchedulerPlugin.transition`. In this case, the
            ``stimulus_id`` is used as a transaction identifier and all archived shuffles
            with a matching `stimulus_id` are restarted.
        """

        # If processing the transactions causes a task to get released, this
        # removes the shuffle from self.active_shuffles. Therefore, we must iterate
        # over a copy.
        for shuffle_id, shuffle in self.active_shuffles.copy().items():
            if worker not in shuffle.participating_workers:
                continue
            logger.debug(
                "Worker %s removed during active shuffle %s due to stimulus '%s'",
                worker,
                shuffle_id,
                stimulus_id,
            )
            exception = RuntimeError(f"Worker {worker} left during active {shuffle}")
            self._fail_on_workers(shuffle, str(exception))
            self._clean_on_scheduler(shuffle_id, stimulus_id)

        for shuffle in self._archived_by_stimulus.get(stimulus_id, set()):
            self._restart_shuffle(shuffle.id, scheduler, stimulus_id=stimulus_id)

    def transition(
        self,
        key: Key,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        """Clean up scheduler and worker state once a shuffle becomes inactive."""
        if finish not in ("released", "forgotten"):
            return
        shuffle_id = id_from_key(key)
        if not shuffle_id:
            return

        if shuffle := self.active_shuffles.get(shuffle_id):
            self._fail_on_workers(shuffle, message=f"{shuffle} forgotten")
            self._clean_on_scheduler(shuffle_id, stimulus_id=stimulus_id)
            logger.debug(
                "Shuffle %s forgotten because task '%s' transitioned to %s due to "
                "stimulus '%s'",
                shuffle_id,
                key,
                finish,
                stimulus_id,
            )

        if finish == "forgotten":
            shuffles = self._shuffles.pop(shuffle_id, set())
            for shuffle in shuffles:
                if shuffle._archived_by:
                    archived = self._archived_by_stimulus[shuffle._archived_by]
                    archived.remove(shuffle)
                    if not archived:
                        del self._archived_by_stimulus[shuffle._archived_by]

    def _fail_on_workers(self, shuffle: SchedulerShuffleState, message: str) -> None:
        worker_msgs = {
            worker: [
                {
                    "op": "shuffle-fail",
                    "shuffle_id": shuffle.id,
                    "run_id": shuffle.run_id,
                    "message": message,
                }
            ]
            for worker in shuffle.participating_workers
        }
        self.scheduler.send_all({}, worker_msgs)

    def _clean_on_scheduler(self, id: ShuffleId, stimulus_id: str | None) -> None:
        shuffle = self.active_shuffles.pop(id)
        if not shuffle._archived_by and stimulus_id:
            shuffle._archived_by = stimulus_id
            self._archived_by_stimulus[stimulus_id].add(shuffle)

        with contextlib.suppress(KeyError):
            del self.heartbeats[id]

        barrier_task = self.scheduler.tasks[barrier_key(id)]
        for dt in barrier_task.dependents:
            self._unset_restriction(dt)

    def restart(self, scheduler: Scheduler) -> None:
        self.active_shuffles.clear()
        self.heartbeats.clear()
        self._shuffles.clear()
        self._archived_by_stimulus.clear()
