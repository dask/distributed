from __future__ import annotations

from distributed.deploy.adaptive_core import AdaptiveCore
from distributed.utils_test import gen_test


class MyAdaptiveCore(AdaptiveCore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._observed = set()
        self._plan = set()
        self._requested = set()
        self._target = 0
        self._log = []

    @property
    def observed(self):
        return self._observed

    @property
    def plan(self):
        return self._plan

    @property
    def requested(self):
        return self._requested

    async def target(self):
        return self._target

    async def scale_up(self, n=0):
        self._plan = self._requested = set(range(n))

    async def scale_down(self, workers=()):
        for collection in [self.plan, self.requested, self.observed]:
            for w in workers:
                collection.discard(w)


@gen_test()
async def test_safe_target():
    adapt = MyAdaptiveCore(minimum=1, maximum=4)
    assert await adapt.safe_target() == 1
    adapt._target = 10
    assert await adapt.safe_target() == 4


@gen_test()
async def test_scale_up():
    adapt = MyAdaptiveCore(minimum=1, maximum=4)
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 1}
    assert adapt.plan == {0}

    adapt._target = 10
    await adapt.adapt()
    assert adapt.log[-1][1] == {"status": "up", "n": 4}
    assert adapt.plan == {0, 1, 2, 3}


@gen_test()
async def test_scale_down():
    adapt = MyAdaptiveCore(minimum=1, maximum=4, wait_count=2)
    adapt._target = 10
    await adapt.adapt()
    assert len(adapt.log) == 1

    adapt._observed = {0, 1, 3}  # all but 2 have arrived

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
