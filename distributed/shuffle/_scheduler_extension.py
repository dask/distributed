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
from typing import TYPE_CHECKING, Any, ClassVar

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.shuffle._rechunk import ChunkedAxes, NIndex
from distributed.shuffle._shuffle import (
    ShuffleId,
    ShuffleType,
    barrier_key,
    id_from_key,
)

if TYPE_CHECKING:
    from distributed.scheduler import (
        Recs,
        Scheduler,
        TaskState,
        TaskStateState,
        WorkerState,
    )

logger = logging.getLogger(__name__)


@dataclass
class ShuffleState(abc.ABC):
    _run_id_iterator: ClassVar[itertools.count] = itertools.count()

    id: ShuffleId
    run_id: int
    output_workers: set[str]
    participating_workers: set[str]

    @abc.abstractmethod
    def to_msg(self) -> dict[str, Any]:
        """Transform the shuffle state into a JSON-serializable message"""


@dataclass
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


@dataclass
class ArrayRechunkState(ShuffleState):
    type: ClassVar[ShuffleType] = ShuffleType.ARRAY_RECHUNK
    worker_for: dict[NIndex, str]
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


class ShuffleSchedulerExtension(SchedulerPlugin):
    """
    Shuffle extension for the scheduler

    Today this mostly just collects heartbeat messages for the dashboard,
    but in the future it may be responsible for more

    See Also
    --------
    ShuffleWorkerExtension
    """

    scheduler: Scheduler
    states: dict[ShuffleId, ShuffleState]
    heartbeats: defaultdict[ShuffleId, dict]
    erred_shuffles: dict[ShuffleId, Exception]

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
        self.states = {}
        self.erred_shuffles = {}
        self.scheduler.add_plugin(self)

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.states)

    async def barrier(self, id: ShuffleId, run_id: int) -> None:
        shuffle = self.states[id]
        msg = {"op": "shuffle_inputs_done", "shuffle_id": id, "run_id": run_id}
        await self.scheduler.broadcast(
            msg=msg, workers=list(shuffle.participating_workers)
        )

    def restrict_task(self, id: ShuffleId, run_id: int, key: str, worker: str) -> dict:
        shuffle = self.states[id]
        if shuffle.run_id != run_id:
            return {"status": "error", "message": "Stale shuffle"}
        ts = self.scheduler.tasks[key]
        self._set_restriction(ts, worker)
        return {"status": "OK"}

    def heartbeat(self, ws: WorkerState, data: dict) -> None:
        for shuffle_id, d in data.items():
            if shuffle_id in self.shuffle_ids():
                self.heartbeats[shuffle_id][ws.address].update(d)

    def get(self, id: ShuffleId, worker: str) -> dict[str, Any]:
        if exception := self.erred_shuffles.get(id):
            return {"status": "error", "message": str(exception)}
        state = self.states[id]
        state.participating_workers.add(worker)
        return state.to_msg()

    def get_or_create(
        self,
        id: ShuffleId,
        type: str,
        worker: str,
        spec: dict[str, Any],
    ) -> dict:
        try:
            return self.get(id, worker)
        except KeyError:
            # FIXME: The current implementation relies on the barrier task to be
            # known by its name. If the name has been mangled, we cannot guarantee
            # that the shuffle works as intended and should fail instead.
            self._raise_if_barrier_unknown(id)

            state: ShuffleState
            if type == ShuffleType.DATAFRAME:
                state = self._create_dataframe_shuffle_state(id, spec)
            elif type == ShuffleType.ARRAY_RECHUNK:
                state = self._create_array_rechunk_state(id, spec)
            else:  # pragma: no cover
                raise TypeError(type)
            self.states[id] = state
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

    def remove_worker(self, scheduler: Scheduler, worker: str) -> None:
        from time import time

        stimulus_id = f"shuffle-failed-worker-left-{time()}"

        for shuffle_id, shuffle in self.states.items():
            if worker not in shuffle.participating_workers:
                continue
            exception = RuntimeError(
                f"Worker {worker} left during active shuffle {shuffle_id}"
            )
            self.erred_shuffles[shuffle_id] = exception
            self._fail_on_workers(shuffle, str(exception))

            barrier_task = self.scheduler.tasks[barrier_key(shuffle_id)]
            recs: Recs = {}
            if barrier_task.state == "memory":
                for dt in barrier_task.dependents:
                    if worker not in dt.worker_restrictions:
                        continue
                    self._unset_restriction(dt)
                    recs.update({dt.key: "waiting"})
                # TODO: Do we need to handle other states?

            self.scheduler.transitions(recs, stimulus_id=stimulus_id)

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if finish not in ("released", "forgotten"):
            return
        if not key.startswith("shuffle-barrier-"):
            return
        shuffle_id = id_from_key(key)
        try:
            shuffle = self.states[shuffle_id]
        except KeyError:
            return
        self._fail_on_workers(shuffle, message=f"Shuffle {shuffle_id} forgotten")
        self._clean_on_scheduler(shuffle_id)

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

    def _clean_on_scheduler(self, id: ShuffleId) -> None:
        del self.states[id]
        self.erred_shuffles.pop(id, None)
        with contextlib.suppress(KeyError):
            del self.heartbeats[id]

        barrier_task = self.scheduler.tasks[barrier_key(id)]
        for dt in barrier_task.dependents:
            self._unset_restriction(dt)

    def restart(self, scheduler: Scheduler) -> None:
        self.states.clear()
        self.heartbeats.clear()
        self.erred_shuffles.clear()


def get_worker_for_range_sharding(
    npartitions: int, output_partition: int, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using range sharding"""
    i = len(workers) * output_partition // npartitions
    return workers[i]


def get_worker_for_hash_sharding(
    output_partition: NIndex, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using hash sharding"""
    i = hash(output_partition) % len(workers)
    return workers[i]
