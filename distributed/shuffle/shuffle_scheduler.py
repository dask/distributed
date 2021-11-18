from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from distributed.diagnostics import SchedulerPlugin
from distributed.utils import key_split_group

from .common import ShuffleId, worker_for

if TYPE_CHECKING:
    from distributed import Scheduler
    from distributed.scheduler import TaskState


TASK_PREFIX = "shuffle"


@dataclass
class ShuffleState:
    workers: list[str]
    out_tasks_left: int
    barrier_reached: bool = False


class ShuffleSchedulerPlugin(SchedulerPlugin):
    output_keys: dict[str, ShuffleId]
    shuffles: dict[ShuffleId, ShuffleState]
    scheduler: Scheduler

    def __init__(self) -> None:
        super().__init__()
        self.shuffles = {}
        self.output_keys = {}

    async def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def transfer(self, id: ShuffleId, key: str) -> None:
        state = self.shuffles.get(id, None)
        if state:
            assert (
                not state.barrier_reached
            ), f"Duplicate shuffle: {key} running after barrier already reached"
            # TODO allow plugins to return recommendations, so we can error this task in some way
            return

        addrs = list(self.scheduler.workers)
        # TODO handle resource/worker restrictions

        # Check how many output tasks there actually are, purely for validation right now.
        # This lets us catch the "error" of culling shuffle output tasks.
        # In the future, we may use it to actually handle culled shuffles properly.
        ts: TaskState = self.scheduler.tasks[key]
        assert (
            len(ts.dependents) == 1
        ), f"{key} should have exactly one dependency (the barrier), not {ts.dependents}"
        barrier = next(iter(ts.dependents))
        nout = len(barrier.dependents)

        self.shuffles[id] = ShuffleState(addrs, nout)

        # TODO allow plugins to return worker messages (though hopefully these will get batched anyway)
        msg = [{"op": "shuffle_init", "id": id, "workers": addrs, "n_out_tasks": nout}]
        self.scheduler.send_all(
            {},
            {addr: msg for addr in addrs},
        )

    def barrier(self, id: ShuffleId, key: str) -> None:
        state = self.shuffles[id]
        assert (
            not state.barrier_reached
        ), f"Duplicate barrier: {key} running but barrier already reached"
        state.barrier_reached = True

        # Identify output tasks
        ts: TaskState = self.scheduler.tasks[key]

        # Set worker restrictions on output tasks, and register their keys for us to watch in transitions
        for dts in ts.dependents:
            assert (
                len(dts.dependencies) == 1
            ), f"Output task {dts} (of shuffle {id}) should have 1 dependency, not {dts.dependencies}"

            assert (
                not dts.worker_restrictions
            ), f"Output task {dts.key} (of shuffle {id}) already has worker restrictions {dts.worker_restrictions}"

            try:
                dts._worker_restrictions = {
                    self.worker_for_key(dts.key, state.out_tasks_left, state.workers)
                }
            except (RuntimeError, IndexError, ValueError) as e:
                raise type(e)(
                    f"Could not pick worker to run dependent {dts.key} of {key}: {e}"
                ) from None

            self.output_keys[dts.key] = id

    def unpack(self, id: ShuffleId, key: str) -> None:
        # Check if all output keys are done

        # NOTE: we don't actually need this `unpack` step or tracking output keys;
        # we could just delete the state in `barrier`.
        # But we do it so we can detect duplicate shuffles, where a `transfer` task
        # tries to reuse a shuffle ID that we're unpacking.
        # (It does also allow us to clean up worker restrictions on error)
        state = self.shuffles[id]
        assert (
            state.barrier_reached
        ), f"Output {key} complete but barrier for shuffle {id} not yet reached"
        assert (
            state.out_tasks_left > 0
        ), f"Output {key} complete; nout_left = {state.out_tasks_left} for shuffle {id}"

        state.out_tasks_left -= 1

        if not state.out_tasks_left:
            # Shuffle is done. Yay!
            del self.shuffles[id]

    def erred(self, id: ShuffleId, key: str) -> None:
        try:
            state = self.shuffles.pop(id)
        except KeyError:
            return

        if state.barrier_reached:
            # Remove worker restrictions for output tasks, in case the shuffle is re-submitted
            for k, id_ in list(self.output_keys.items()):
                if id_ == id:
                    ts: TaskState = self.scheduler.tasks[k]
                    ts._worker_restrictions.clear()
                    del self.output_keys[k]

    def transition(self, key: str, start: str, finish: str, *args, **kwargs):
        parts = parse_key(key)
        if parts and len(parts) == 3:
            prefix, group, id = parts

            if prefix == TASK_PREFIX:
                if start == "waiting" and finish in ("processing", "memory"):
                    # transfer/barrier starting to run
                    if group == "transfer":
                        return self.transfer(ShuffleId(id), key)
                    if group == "barrier":
                        return self.barrier(ShuffleId(id), key)

                # transfer/barrier task erred
                elif finish == "erred":
                    return self.erred(ShuffleId(id), key)

        # Task completed
        if start in ("waiting", "processing") and finish in (
            "memory",
            "released",
            "erred",
        ):
            try:
                id = self.output_keys[key]
            except KeyError:
                return
            # Known unpack task completed or erred
            if finish == "erred":
                return self.erred(id, key)
            return self.unpack(id, key)

    def worker_for_key(self, key: str, npartitions: int, workers: list[str]) -> str:
        "Worker address this task should be assigned to"
        # Infer which output partition number this task is fetching by parsing its key
        # FIXME this is so brittle.
        # For example, after `df.set_index(...).to_delayed()`, you could create
        # keys that don't have indices in them, and get fused (because they should!).
        m = re.match(r"\(.+, (\d+)\)$", key)
        if not m:
            raise RuntimeError(f"{key} does not look like a DataFrame key")

        idx = int(m.group(1))
        addr = worker_for(idx, npartitions, workers)
        if addr not in self.scheduler.workers:
            raise RuntimeError(
                f"Worker {addr} for output partition {idx} no longer known"
            )
        return addr


def parse_key(key: str) -> list[str] | None:
    if TASK_PREFIX in key[: len(TASK_PREFIX) + 2]:
        if key[0] == "(":
            key = key_split_group(key)
        return key.split("-")
    return None
