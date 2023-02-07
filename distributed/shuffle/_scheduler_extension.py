from __future__ import annotations

import abc
import contextlib
import itertools
import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.shuffle._shuffle import ShuffleId, barrier_key, id_from_key

if TYPE_CHECKING:
    from distributed.scheduler import Recs, Scheduler, TaskStateState, WorkerState

logger = logging.getLogger(__name__)


@dataclass
class ShuffleState(abc.ABC):
    _run_id_iterator: ClassVar[itertools.count] = itertools.count()

    id: ShuffleId
    run_id: int
    output_workers: set[str]
    completed_workers: set[str]
    participating_workers: set[str]

    @abc.abstractmethod
    def to_msg(self) -> dict[str, Any]:
        ...


@dataclass
class DataFrameShuffleState(ShuffleState):
    type: ClassVar[str] = "DataFrameShuffle"
    worker_for: dict[int, str]
    schema: bytes
    column: str

    def to_msg(self) -> dict[str, Any]:
        return {
            "status": "OK",
            "type": DataFrameShuffleState.type,
            "run_id": self.run_id,
            "worker_for": self.worker_for,
            "column": self.column,
            "schema": self.schema,
            "output_workers": self.output_workers,
        }


@dataclass
class ArrayRechunkState(ShuffleState):
    type: ClassVar[str] = "ArrayRechunk"
    worker_for: dict[tuple[int, ...], str]
    old: tuple[tuple[int, ...], ...]
    new: tuple[tuple[int, ...], ...]

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
                "shuffle_get": self.get,
                "shuffle_get_or_create": self.get_or_create,
                "shuffle_get_participating_workers": self.get_participating_workers,
            }
        )
        self.heartbeats = defaultdict(lambda: defaultdict(dict))
        self.states = {}
        self.erred_shuffles = {}
        self.scheduler.add_plugin(self)

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.states)

    def heartbeat(self, ws: WorkerState, data: dict) -> None:
        for shuffle_id, d in data.items():
            if shuffle_id in self.shuffle_ids():
                self.heartbeats[shuffle_id][ws.address].update(d)

    def get(self, id: ShuffleId, worker: str) -> dict[str, Any]:
        if exception := self.erred_shuffles.get(id):
            return {"status": "ERROR", "message": str(exception)}
        state = self.states[id]
        state.participating_workers.add(worker)
        return state.to_msg()

    def get_or_create(
        self,
        id: ShuffleId,
        type: str,
        worker: str,
        spec,
    ) -> dict:
        try:
            return self.get(id, worker)
        except KeyError:
            if type == "DataFrameShuffle":
                state = self._create_dataframe_shuffle_state(id, spec)
            elif type == "ArrayRechunk":
                state = self._create_array_rechunk_state(id, spec)
            else:
                raise NotImplementedError
            self.states[id] = state
            state.participating_workers.add(worker)
            return state.to_msg()

    def _create_dataframe_shuffle_state(
        self, id: ShuffleId, spec: dict[str, Any]
    ) -> DataFrameShuffleState:
        schema = spec["schema"]
        column = spec["column"]
        npartitions = spec["npartitions"]
        assert schema is not None
        assert column is not None
        assert npartitions is not None

        workers = list(self.scheduler.workers)
        output_workers = set()

        name = barrier_key(id)
        mapping = {}

        for ts in self.scheduler.tasks[name].dependents:
            part = ts.annotations["shuffle"]
            if ts.worker_restrictions:
                output_worker = list(ts.worker_restrictions)[0]
            else:
                output_worker = get_worker_for(part, workers, npartitions)
            mapping[part] = output_worker
            output_workers.add(output_worker)
            self.scheduler.set_restrictions({ts.key: {output_worker}})

        return DataFrameShuffleState(
            id=id,
            run_id=next(ShuffleState._run_id_iterator),
            worker_for=mapping,
            schema=schema,
            column=column,
            output_workers=output_workers,
            completed_workers=set(),
            participating_workers=output_workers.copy(),
        )

    def _create_array_rechunk_state(
        self, id: ShuffleId, spec: dict[str, Any]
    ) -> ArrayRechunkState:
        old = spec["old"]
        new = spec["new"]
        assert old is not None
        assert new is not None

        workers = list(self.scheduler.workers)
        output_workers = set()

        name = barrier_key(id)
        mapping = {}

        shape = (len(dim) for dim in new)

        for ts in self.scheduler.tasks[name].dependents:
            part = ts.annotations["shuffle"]  # TODO Improve this
            if ts.worker_restrictions:
                output_worker = list(ts.worker_restrictions)[0]
            else:
                output_worker = get_worker_for_chunk(part, workers, shape)
            mapping[part] = output_worker
            output_workers.add(output_worker)
            self.scheduler.set_restrictions({ts.key: {output_worker}})

        return ArrayRechunkState(
            id=id,
            run_id=next(ShuffleState._run_id_iterator),
            worker_for=mapping,
            output_workers=output_workers,
            old=old,
            new=new,
            completed_workers=set(),
            participating_workers=output_workers.copy(),
        )

    def get_participating_workers(self, id: ShuffleId) -> list[str]:
        return list(self.states[id].participating_workers)

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
                    dt.worker_restrictions.clear()
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

    def restart(self, scheduler: Scheduler) -> None:
        self.states.clear()
        self.heartbeats.clear()
        self.erred_shuffles.clear()


def get_worker_for(output_partition: int, workers: list[str], npartitions: int) -> str:
    "Get the address of the worker which should hold this output partition number"
    i = len(workers) * output_partition // npartitions
    return workers[i]


def get_worker_for_chunk(chunk: tuple[int, ...], workers: list[str], shape) -> str:
    nchunks = math.prod(shape)
    multiplier = 1
    flat_index = 0

    for i, n in zip(chunk, shape):
        flat_index += i * multiplier
        flat_index *= n
    i = len(workers) * flat_index // nchunks
    return workers[i]
