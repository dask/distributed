from __future__ import annotations

import contextlib
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any

from distributed.diagnostics.plugin import SchedulerPlugin
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
            None, dumps(worker_plugin), name="shuffle"
        )

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.active_shuffles)

    async def barrier(self, id: ShuffleId, run_id: int) -> None:
        shuffle = self.active_shuffles[id]
        if shuffle.run_id != run_id:
            raise ValueError(f"{run_id=} does not match {shuffle}")
        msg = {"op": "shuffle_inputs_done", "shuffle_id": id, "run_id": run_id}
        await self.scheduler.broadcast(
            msg=msg,
            workers=list(shuffle.participating_workers),
        )

    def restrict_task(self, id: ShuffleId, run_id: int, key: str, worker: str) -> dict:
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
        key: str,
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
            state = spec.create_new_run(self)
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

    def _raise_if_task_not_processing(self, key: str) -> None:
        task = self.scheduler.tasks[key]
        if task.state != "processing":
            raise RuntimeError(f"Expected {task} to be processing, is {task.state}.")

    def _pin_output_workers(
        self,
        id: ShuffleId,
        output_partitions: Iterable[Any],
        pick: Callable[[Any, Sequence[str]], str],
    ) -> dict[Any, str]:
        """Pin the outputs of a P2P shuffle to specific workers.

        Parameters
        ----------
        id: ID of the shuffle to pin
        output_partitions: Output partition IDs to pin
        pick: Function that picks a worker given a partition ID and sequence of worker

        .. note:
            This function assumes that the barrier task and the output tasks share
            the same worker restrictions.
        """
        mapping = {}
        barrier = self.scheduler.tasks[barrier_key(id)]

        if barrier.worker_restrictions:
            workers = list(barrier.worker_restrictions)
        else:
            workers = list(self.scheduler.workers)

        for partition in output_partitions:
            worker = pick(partition, workers)
            mapping[partition] = worker

        for dt in barrier.dependents:
            try:
                partition = dt.annotations["shuffle"]
            except KeyError:
                continue

            if dt.worker_restrictions:
                worker = pick(partition, list(dt.worker_restrictions))
                mapping[partition] = worker
            else:
                worker = mapping[partition]

            self._set_restriction(dt, worker)

        return mapping

    def _set_restriction(self, ts: TaskState, worker: str) -> None:
        if "shuffle_original_restrictions" in ts.annotations:
            # This may occur if multiple barriers share the same output task,
            # e.g. in a hash join.
            return
        ts.annotations["shuffle_original_restrictions"] = ts.worker_restrictions.copy()
        self.scheduler.set_restrictions({ts.key: {worker}})

    def _unset_restriction(self, ts: TaskState) -> None:
        # shuffle_original_restrictions is only set if the task was first scheduled
        # on the wrong worker
        if "shuffle_original_restrictions" not in ts.annotations:
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
            exception = RuntimeError(f"Worker {worker} left during active {shuffle}")
            self._fail_on_workers(shuffle, str(exception))
            self._clean_on_scheduler(shuffle_id, stimulus_id)

        for shuffle in self._archived_by_stimulus.get(stimulus_id, set()):
            self._restart_shuffle(shuffle.id, scheduler, stimulus_id=stimulus_id)

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        """Clean up scheduler and worker state once a shuffle becomes inactive."""
        if finish not in ("released", "forgotten"):
            return
        if not isinstance(key, str) or not key.startswith("shuffle-barrier-"):
            return
        shuffle_id = id_from_key(key)

        if shuffle := self.active_shuffles.get(shuffle_id):
            self._fail_on_workers(shuffle, message=f"{shuffle} forgotten")
            self._clean_on_scheduler(shuffle_id, stimulus_id=stimulus_id)

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
