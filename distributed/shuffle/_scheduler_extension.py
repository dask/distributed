from __future__ import annotations

import contextlib
import itertools
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.shuffle._shuffle import ShuffleId, barrier_key, id_from_key

if TYPE_CHECKING:
    from distributed.scheduler import Recs, Scheduler, TaskStateState, WorkerState

logger = logging.getLogger(__name__)


@dataclass
class ShuffleState:
    _run_id_iterator: ClassVar[itertools.count] = itertools.count()

    id: ShuffleId
    run_id: int
    worker_for: dict[int, str]
    schema: bytes
    column: str
    output_workers: set[str]
    participating_workers: set[str]


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

    def heartbeat(self, ws: WorkerState, data: dict) -> None:
        for shuffle_id, d in data.items():
            if shuffle_id in self.shuffle_ids():
                self.heartbeats[shuffle_id][ws.address].update(d)

    def get(
        self,
        id: ShuffleId,
        schema: bytes | None,
        column: str | None,
        npartitions: int | None,
        worker: str,
    ) -> dict:
        if exception := self.erred_shuffles.get(id):
            return {"status": "ERROR", "message": str(exception)}

        if id not in self.states:
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

            state = ShuffleState(
                id=id,
                run_id=next(ShuffleState._run_id_iterator),
                worker_for=mapping,
                schema=schema,
                column=column,
                output_workers=output_workers,
                participating_workers=output_workers.copy(),
            )
            self.states[id] = state

        state = self.states[id]
        state.participating_workers.add(worker)
        return {
            "status": "OK",
            "run_id": state.run_id,
            "worker_for": state.worker_for,
            "column": state.column,
            "schema": state.schema,
            "output_workers": state.output_workers,
        }

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
