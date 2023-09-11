from __future__ import annotations

import itertools
import random
from typing import Any

from distributed.client import Client
from distributed.core import PooledRPCCall
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.scheduler import Scheduler, TaskStateState
from distributed.shuffle._core import ShuffleId, ShuffleRun


class PooledRPCShuffle(PooledRPCCall):
    def __init__(self, shuffle: ShuffleRun):
        self.shuffle = shuffle

    def __getattr__(self, key):
        async def _(**kwargs):
            from distributed.protocol.serialize import nested_deserialize

            method_name = key.replace("shuffle_", "")
            kwargs.pop("shuffle_id", None)
            kwargs.pop("run_id", None)
            # TODO: This is a bit awkward. At some point the arguments are
            # already getting wrapped with a `Serialize`. We only want to unwrap
            # here.
            kwargs = nested_deserialize(kwargs)
            meth = getattr(self.shuffle, method_name)
            return await meth(**kwargs)

        return _


class AbstractShuffleTestPool:
    _shuffle_run_id_iterator = itertools.count()

    def __init__(self, *args, **kwargs):
        self.shuffles = {}

    def __call__(self, addr: str, *args: Any, **kwargs: Any) -> PooledRPCShuffle:
        return PooledRPCShuffle(self.shuffles[addr])

    async def shuffle_barrier(self, id: ShuffleId, run_id: int) -> dict[str, None]:
        out = {}
        for addr, s in self.shuffles.items():
            out[addr] = await s.inputs_done()
        return out


class ShuffleAnnotationChaosPlugin(SchedulerPlugin):
    #: Rate at which the plugin randomly drops shuffle annotations
    rate: float
    scheduler: Scheduler | None
    seen: set

    def __init__(self, rate: float):
        self.rate = rate
        self.scheduler = None
        self.seen = set()

    async def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        assert self.scheduler
        if finish != "waiting":
            return
        if not isinstance(key, str) or not key.startswith("shuffle-barrier-"):
            return
        if key in self.seen:
            return

        self.seen.add(key)

        barrier = self.scheduler.tasks[key]

        if self._flip():
            barrier.annotations.pop("shuffle", None)
        for dt in barrier.dependents:
            if self._flip():
                dt.annotations.pop("shuffle", None)

    def _flip(self) -> bool:
        return random.random() < self.rate


async def invoke_annotation_chaos(rate: float, client: Client) -> None:
    if not rate:
        return
    plugin = ShuffleAnnotationChaosPlugin(rate)
    await client.register_plugin(plugin)
