from __future__ import annotations

import abc
import contextlib
import itertools
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from itertools import product
from typing import TYPE_CHECKING, Any, ClassVar, cast

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.protocol.pickle import dumps
from distributed.shuffle._rechunk import ChunkedAxes, NDIndex
from distributed.shuffle._shuffle import (
    ShuffleId,
    ShuffleType,
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


@dataclass(eq=False)
class ShuffleState(abc.ABC):
    _run_id_iterator: ClassVar[itertools.count] = itertools.count(1)

    id: ShuffleId
    run_id: int
    output_workers: set[str]
    participating_workers: set[str]
    _archived_by: str | None

    @abc.abstractmethod
    def to_msg(self) -> dict[str, Any]:
        """Transform the shuffle state into a JSON-serializable message"""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.id}[{self.run_id}]>"

    def __eq__(self, other: object) -> bool:
        if type(other) != type(self):
            return False
        return cast(ShuffleState, other).run_id == self.run_id

    def __hash__(self) -> int:
        return hash(self.run_id)


@dataclass(eq=False)
class DataFrameShuffleState(ShuffleState):
    type: ClassVar[ShuffleType] = ShuffleType.DATAFRAME
    worker_for: dict[int, str]
    column: str

    def to_msg(self) -> dict[str, Any]:
        return {
            "status": "OK",
            "type": DataFrameShuffleState.type,
            "run_id": self.run_id,
            "worker_for": self.worker_for,
            "column": self.column,
            "output_workers": self.output_workers,
        }


@dataclass(eq=False)
class ArrayRechunkState(ShuffleState):
    type: ClassVar[ShuffleType] = ShuffleType.ARRAY_RECHUNK
    worker_for: dict[NDIndex, str]
    old: ChunkedAxes
    new: ChunkedAxes

    def to_msg(self) -> dict[str, Any]:
        return {
            "status": "OK",
            "type": ArrayRechunkState.type,
            "run_id": self.run_id,
            "worker_for": self.worker_for,
            "old": self.old,
            "new": self.new,
            "output_workers": self.output_workers,
        }


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
    active_shuffles: dict[ShuffleId, ShuffleState]
    heartbeats: defaultdict[ShuffleId, dict]
    _shuffles: defaultdict[ShuffleId, set[ShuffleState]]
    _archived_by_stimulus: defaultdict[str, set[ShuffleState]]

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
        assert shuffle.run_id == run_id, f"{run_id=} does not match {shuffle}"
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

    def get(self, id: ShuffleId, worker: str) -> dict[str, Any]:
        assert worker in self.scheduler.workers
        state = self.active_shuffles[id]
        state.participating_workers.add(worker)
        return state.to_msg()

    def get_or_create(
        self,
        id: ShuffleId,
        key: str,
        type: str,
        worker: str,
        spec: dict[str, Any],
    ) -> dict:
        assert worker in self.scheduler.workers
        try:
            return self.get(id, worker)
        except KeyError:
            # FIXME: The current implementation relies on the barrier task to be
            # known by its name. If the name has been mangled, we cannot guarantee
            # that the shuffle works as intended and should fail instead.
            self._raise_if_barrier_unknown(id)
            self._raise_if_task_not_processing(key)

            state: ShuffleState
            if type == ShuffleType.DATAFRAME:
                state = self._create_dataframe_shuffle_state(id, spec)
            elif type == ShuffleType.ARRAY_RECHUNK:
                state = self._create_array_rechunk_state(id, spec)
            else:  # pragma: no cover
                raise TypeError(type)
            self.active_shuffles[id] = state
            self._shuffles[id].add(state)
            state.participating_workers.add(worker)
            return state.to_msg()

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

    def _create_dataframe_shuffle_state(
        self, id: ShuffleId, spec: dict[str, Any]
    ) -> DataFrameShuffleState:
        column = spec["column"]
        npartitions = spec["npartitions"]
        parts_out = spec["parts_out"]
        assert column is not None
        assert npartitions is not None
        assert parts_out is not None

        pick_worker = partial(get_worker_for_range_sharding, npartitions)

        mapping = self._pin_output_workers(id, parts_out, pick_worker)
        output_workers = set(mapping.values())

        return DataFrameShuffleState(
            id=id,
            run_id=next(ShuffleState._run_id_iterator),
            worker_for=mapping,
            column=column,
            output_workers=output_workers,
            participating_workers=output_workers.copy(),
            _archived_by=None,
        )

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

    def _create_array_rechunk_state(
        self, id: ShuffleId, spec: dict[str, Any]
    ) -> ArrayRechunkState:
        old = spec["old"]
        new = spec["new"]
        assert old is not None
        assert new is not None

        parts_out = product(*(range(len(c)) for c in new))
        mapping = self._pin_output_workers(id, parts_out, get_worker_for_hash_sharding)
        output_workers = set(mapping.values())

        return ArrayRechunkState(
            id=id,
            run_id=next(ShuffleState._run_id_iterator),
            worker_for=mapping,
            output_workers=output_workers,
            old=old,
            new=new,
            participating_workers=output_workers.copy(),
            _archived_by=None,
        )

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
            return {}
        recs.update({barrier_task.key: "released"})

        for dt in barrier_task.dependencies:
            if dt.state == "erred":
                return {}
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
        for shuffle in self._archived_by_stimulus.get(stimulus_id, set()):
            if worker not in shuffle.participating_workers:
                continue

            self._restart_shuffle(shuffle.id, scheduler, stimulus_id=stimulus_id)

        # If processing the transactions causes a task to get released, this
        # removes the shuffle from self.states. Therefore, we must iterate
        # over a copy.
        for shuffle_id, shuffle in self.active_shuffles.copy().items():
            if worker not in shuffle.participating_workers:
                continue
            exception = RuntimeError(f"Worker {worker} left during active {shuffle}")
            self._fail_on_workers(shuffle, str(exception))
            self._clean_on_scheduler(shuffle_id, stimulus_id)
            self._restart_shuffle(shuffle_id, scheduler, stimulus_id=stimulus_id)

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        if finish not in ("released", "forgotten"):
            return
        if not key.startswith("shuffle-barrier-"):
            return
        shuffle_id = id_from_key(key)
        try:
            shuffle = self.active_shuffles[shuffle_id]
        except KeyError:
            pass
        else:
            self._fail_on_workers(shuffle, message=f"{shuffle} forgotten")
            self._clean_on_scheduler(shuffle_id, stimulus_id=stimulus_id)

        if finish == "forgotten":
            shuffles = self._shuffles.pop(shuffle_id)
            for shuffle in shuffles:
                if shuffle._archived_by:
                    archived = self._archived_by_stimulus[shuffle._archived_by]
                    archived.remove(shuffle)
                    if not archived:
                        del self._archived_by_stimulus[shuffle._archived_by]

    def _fail_on_workers(self, shuffle: ShuffleState, message: str) -> None:
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


def get_worker_for_range_sharding(
    npartitions: int, output_partition: int, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using range sharding"""
    i = len(workers) * output_partition // npartitions
    return workers[i]


def get_worker_for_hash_sharding(
    output_partition: NDIndex, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using hash sharding"""
    i = hash(output_partition) % len(workers)
    return workers[i]
