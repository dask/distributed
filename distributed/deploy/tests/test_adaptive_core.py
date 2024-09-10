from __future__ import annotations

import asyncio
import logging

from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.metrics import time
from distributed.utils_test import captured_logger, gen_test


class MyAdaptive(AdaptiveCore):
    def __init__(self, *args, interval=None, **kwargs):
        super().__init__(*args, interval=interval, **kwargs)
        self._target = 0
        self._log = []

    async def target(self):
        return self._target

    async def scale_up(self, n=0):
        self.plan = self.requested = set(range(n))

    async def scale_down(self, workers=()):
        for collection in [self.plan, self.requested, self.observed]:
            for w in workers:
                collection.discard(w)


@gen_test()
async def test_safe_target():
    adapt = MyAdaptive(minimum=1, maximum=4)
    assert await adapt.safe_target() == 1
    adapt._target = 10
    assert await adapt.safe_target() == 4


@gen_test()
async def test_scale_up():
    adapt = MyAdaptive(minimum=1, maximum=4)
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 1}
    assert adapt.plan == {0}

    adapt._target = 10
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 4}
    assert adapt.plan == {0, 1, 2, 3}


@gen_test()
async def test_scale_down():
    adapt = MyAdaptive(minimum=1, maximum=4, wait_count=2)
    adapt._target = 10
    await adapt.adapt()
    assert len(adapt.log) == 1

    adapt.observed = {0, 1, 3}  # all but 2 have arrived

    adapt._target = 2
    await adapt.adapt()
    assert len(adapt.log) == 1  # no change after only one call
    await adapt.adapt()
    assert len(adapt.log) == 2  # no change after only one call
    assert adapt.log[-1][1]["status"] == "down"
    assert 2 in adapt.log[-1][1]["workers"]
    assert len(adapt.log[-1][1]["workers"]) == 2

    old = list(adapt.log)
    await adapt.adapt()
    await adapt.adapt()
    await adapt.adapt()
    await adapt.adapt()
    assert list(adapt.log) == old


@gen_test()
async def test_interval():
    adapt = MyAdaptive(interval="5 ms")
    assert not adapt.plan

    for i in [0, 3, 1]:
        start = time()
        adapt._target = i
        while len(adapt.plan) != i:
            await asyncio.sleep(0.001)
            assert time() < start + 2

    adapt.stop()
    await asyncio.sleep(0.05)

    adapt._target = 10
    await asyncio.sleep(0.02)
    assert len(adapt.plan) == 1  # last value from before, unchanged


@gen_test()
async def test_adapt_logs_error_in_safe_target():
    class BadAdaptive(MyAdaptive):
        """AdaptiveCore subclass which raises an OSError when attempting to adapt

        We use this to check that error handling works properly
        """

        def safe_target(self):
            raise OSError()

    with captured_logger(
        "distributed.deploy.adaptive_core", level=logging.WARNING
    ) as log:
        adapt = BadAdaptive(minimum=1, maximum=4, interval="10ms")
        while "encountered an error" not in log.getvalue():
            await asyncio.sleep(0.01)
    assert "stop" not in log.getvalue()
    assert adapt.state == "running"
    assert adapt.periodic_callback
    assert adapt.periodic_callback.is_running()


@gen_test()
async def test_adapt_logs_errors():
    class BadAdaptive(MyAdaptive):
        async def scale_down(self, workers=None):
            raise OSError()

    adapt = BadAdaptive(minimum=1, maximum=4, wait_count=0, interval="10ms")
    adapt._target = 2
    while adapt.state != "running":
        await asyncio.sleep(0.01)
    assert adapt.periodic_callback.is_running()
    await adapt.adapt()
    assert len(adapt.plan) == 2
    assert len(adapt.requested) == 2
    with captured_logger(
        "distributed.deploy.adaptive_core", level=logging.WARNING
    ) as log:
        adapt._target = 0
        await adapt.adapt()
    text = log.getvalue()
    assert "encountered an error" in text
    assert not adapt._adapting
    assert adapt.periodic_callback
    assert adapt.periodic_callback.is_running()
    adapt.stop()


@gen_test()
async def test_adaptive_logs_stopping_once():
    with captured_logger("distributed.deploy.adaptive_core") as log:
        adapt = MyAdaptive(interval="100ms")
        while adapt.state != "running":
            await asyncio.sleep(0.01)
        assert adapt.periodic_callback
        assert adapt.periodic_callback.is_running()
        pc = adapt.periodic_callback

        adapt.stop()
        adapt.stop()
    assert adapt.state == "stopped"
    assert not adapt.periodic_callback
    assert not pc.is_running()
    lines = log.getvalue().splitlines()
    assert sum("Adaptive scaling stopped" in line for line in lines) == 1


@gen_test()
async def test_adapt_stop_del():
    adapt = MyAdaptive(interval="100ms")
    pc = adapt.periodic_callback
    while adapt.state != "running":
        await asyncio.sleep(0.01)

    del adapt
    while pc.is_running():
        await asyncio.sleep(0.01)
