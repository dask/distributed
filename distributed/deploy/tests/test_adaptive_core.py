from __future__ import annotations

import asyncio

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
async def test_adapt_oserror_safe_target():
    class BadAdaptive(MyAdaptive):
        """AdaptiveCore subclass which raises an OSError when attempting to adapt

        We use this to check that error handling works properly
        """

        def safe_target(self):
            raise OSError()

    with captured_logger("distributed.deploy.adaptive_core") as log:
        adapt = BadAdaptive(minimum=1, maximum=4)
        await adapt.adapt()
    text = log.getvalue()
    assert "Adaptive stopping due to error" in text
    assert "Adaptive stop" in text
    assert not adapt._adapting
    assert not adapt.periodic_callback


@gen_test()
async def test_adapt_oserror_scale():
    """
    FIXME:
    If we encounter an OSError during scale down, we continue as before. It is
    not entirely clear if this is the correct behaviour but defines the current
    state.
    This was probably introduced to protect against comm failures during
    shutdown but the scale down command should be robust call to the scheduler
    which is never scaled down.
    """

    class BadAdaptive(MyAdaptive):
        async def scale_down(self, workers=None):
            raise OSError()

    adapt = BadAdaptive(minimum=1, maximum=4, wait_count=0, interval="10ms")
    adapt._target = 2
    while not adapt.periodic_callback.is_running():
        await asyncio.sleep(0.01)
    await adapt.adapt()
    assert len(adapt.plan) == 2
    assert len(adapt.requested) == 2
    with captured_logger("distributed.deploy.adaptive_core") as log:
        adapt._target = 0
        await adapt.adapt()
    text = log.getvalue()
    assert "Error during adaptive downscaling" in text
    assert not adapt._adapting
    assert adapt.periodic_callback
    assert adapt.periodic_callback.is_running()
    adapt.stop()


@gen_test()
async def test_adapt_stop_del():
    adapt = MyAdaptive(interval="100ms")
    pc = adapt.periodic_callback
    while not adapt.periodic_callback.is_running():
        await asyncio.sleep(0.01)

    del adapt
    while pc.is_running():
        await asyncio.sleep(0.01)
