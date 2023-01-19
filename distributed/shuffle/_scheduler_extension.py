from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.shuffle._shuffle import ShuffleId, barrier_key, id_from_key

if TYPE_CHECKING:
    from distributed.scheduler import Recs, Scheduler, TaskStateState, WorkerState

logger = logging.getLogger(__name__)


@dataclass
class ShuffleState:
    id: ShuffleId
    worker_for: dict[int, str]
    schema: bytes
    column: str
    output_workers: set[str]
    completed_workers: set[str]
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
    tombstones: set[ShuffleId]
    erred_shuffles: dict[ShuffleId, Exception]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers.update(
            {
                "shuffle_get": self.get,
                "shuffle_get_participating_workers": self.get_participating_workers,
                "shuffle_register_complete": self.register_complete,
            }
        )
        self.heartbeats = defaultdict(lambda: defaultdict(dict))
        self.states = {}
        self.tombstones = set()
        self.erred_shuffles = {}
        self.scheduler.add_plugin(self)

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.states)

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

        if id in self.tombstones:
            return {
                "status": "ERROR",
                "message": f"Shuffle {id} has already been forgotten",
            }
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
                worker_for=mapping,
                schema=schema,
                column=column,
                output_workers=output_workers,
                completed_workers=set(),
                participating_workers=output_workers.copy(),
            )
            self.states[id] = state

        state = self.states[id]
        state.participating_workers.add(worker)
        return {
            "status": "OK",
            "worker_for": state.worker_for,
            "column": state.column,
            "schema": state.schema,
            "output_workers": state.output_workers,
        }

    def get_participating_workers(self, id: ShuffleId) -> list[str]:
        return list(self.states[id].participating_workers)

    async def remove_worker(self, scheduler: Scheduler, worker: str) -> None:
        affected_shuffles = set()
        broadcasts = []
        from time import time

        recs: Recs = {}
        stimulus_id = f"shuffle-failed-worker-left-{time()}"
        barriers = []
        for shuffle_id, state in self.states.items():
            if worker not in state.participating_workers:
                continue
            exception = RuntimeError(
                f"Worker {worker} left during active shuffle {shuffle_id}"
            )
            self.erred_shuffles[shuffle_id] = exception
            contact_workers = state.participating_workers.copy()
            contact_workers.discard(worker)
            affected_shuffles.add(shuffle_id)
            name = barrier_key(shuffle_id)
            barrier_task = self.scheduler.tasks.get(name)
            if barrier_task:
                barriers.append(barrier_task)
                broadcasts.append(
                    scheduler.broadcast(
                        msg={
                            "op": "shuffle_fail",
                            "message": str(exception),
                            "shuffle_id": shuffle_id,
                        },
                        workers=list(contact_workers),
                    )
                )

        results = await asyncio.gather(*broadcasts, return_exceptions=True)
        for barrier_task in barriers:
            if barrier_task.state == "memory":
                for dt in barrier_task.dependents:
                    if worker not in dt.worker_restrictions:
                        continue
                    dt.worker_restrictions.clear()
                    recs.update({dt.key: "waiting"})
            # TODO: Do we need to handle other states?
        self.scheduler.transitions(recs, stimulus_id=stimulus_id)

        # Assumption: No new shuffle tasks scheduled on the worker
        # + no existing tasks anymore
        # All task-finished/task-errer are queued up in batched stream

        exceptions = [result for result in results if isinstance(result, Exception)]
        if exceptions:
            # TODO: Do we need to handle errors here?
            raise RuntimeError(exceptions)

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if finish != "forgotten":
            return
        if not key.startswith("shuffle-barrier-"):
            return
        shuffle_id = id_from_key(key)
        if shuffle_id not in self.states:
            return
        participating_workers = self.states[shuffle_id].participating_workers
        worker_msgs = {
            worker: [
                {
                    "op": "shuffle-fail",
                    "shuffle_id": shuffle_id,
                    "message": f"Shuffle {shuffle_id} forgotten",
                }
            ]
            for worker in participating_workers
        }
        self._clean_on_scheduler(shuffle_id)
        self.scheduler.send_all({}, worker_msgs)

    def register_complete(self, id: ShuffleId, worker: str) -> None:
        """Learn from a worker that it has completed all reads of a shuffle"""
        if exception := self.erred_shuffles.get(id):
            raise exception
        if id not in self.states:
            logger.info("Worker shuffle reported complete after shuffle was removed")
            return
        self.states[id].completed_workers.add(worker)

    def _clean_on_scheduler(self, id: ShuffleId) -> None:
        self.tombstones.add(id)
        del self.states[id]
        self.erred_shuffles.pop(id, None)
        with contextlib.suppress(KeyError):
            del self.heartbeats[id]

    def restart(self, scheduler: Scheduler) -> None:
        self.states.clear()
        self.heartbeats.clear()
        self.tombstones.clear()
        self.erred_shuffles.clear()


def get_worker_for(output_partition: int, workers: list[str], npartitions: int) -> str:
    "Get the address of the worker which should hold this output partition number"
    i = len(workers) * output_partition // npartitions
    return workers[i]
