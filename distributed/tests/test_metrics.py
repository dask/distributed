from __future__ import annotations

import math
import pickle
import time

import pytest

from distributed import metrics
from distributed.compatibility import WINDOWS


@pytest.mark.parametrize("name", ["time", "monotonic"])
def test_wall_clock(name):
    for _ in range(3):
        time.sleep(0.01)
        t = getattr(time, name)()
        samples = [getattr(metrics, name)() for _ in range(100)]
        # Resolution
        deltas = [sj - si for si, sj in zip(samples[:-1], samples[1:])]
        assert min(deltas) >= 0.0, deltas
        assert max(deltas) <= 0.005, deltas
        assert any(0.0 < d < 0.0001 for d in deltas), deltas
        # Close to time.time() / time.monotonic()
        assert t - 0.5 < samples[0] < t + 0.5


@pytest.mark.slow
@pytest.mark.skipif(
    not WINDOWS, reason="WindowsTime doesn't work with high accuracy base timer"
)
def test_monotonic():
    t = metrics._WindowsTime(time.monotonic, is_monotonic=True, resync_every=0.1).time
    prev = float("-inf")
    t_end = time.perf_counter() + 3
    while time.perf_counter() < t_end:
        sample = t()
        assert sample > prev
        prev = sample


def test_meter():
    it = iter([123, 124])
    with metrics.meter(lambda: next(it)) as m:
        assert m.start == 123
        assert math.isnan(m.stop)
        assert math.isnan(m.delta)
    assert m.start == 123
    assert m.stop == 124
    assert m.delta == 1


def test_meter_raise():
    it = iter([123, 124])
    with pytest.raises(ValueError), metrics.meter(lambda: next(it)) as m:
        raise ValueError()
    assert m.start == 123
    assert m.stop == 124
    assert m.delta == 1


@pytest.mark.parametrize(
    "kwargs,delta",
    [
        ({}, 0),
        ({"floor": 0.1}, 0.1),
        ({"floor": False}, -1),
    ],
)
def test_meter_floor(kwargs, delta):
    it = iter([124, 123])
    with metrics.meter(lambda: next(it), **kwargs) as m:
        pass
    assert m.start == 124
    assert m.stop == 123
    assert m.delta == delta


def test_context_meter():
    it = iter([123, 124])
    cbs = []

    with metrics.context_meter.add_callback(lambda l, v, u: cbs.append((l, v, u))):
        with metrics.context_meter.meter("m1", func=lambda: next(it)):
            pass
        metrics.context_meter.digest_metric("m1", 2, "seconds")
        metrics.context_meter.digest_metric("m1", 1, "foo")

    # Not recorded - out of context
    metrics.context_meter.digest_metric("m1", 123, "foo")

    assert cbs == [
        ("m1", 1, "seconds"),
        ("m1", 2, "seconds"),
        ("m1", 1, "foo"),
    ]


def test_context_meter_raise():
    it = iter([123, 124])
    cbs = []

    with pytest.raises(ValueError):
        with metrics.context_meter.add_callback(lambda l, v, u: cbs.append((l, v, u))):
            with metrics.context_meter.meter("m1", func=lambda: next(it)):
                raise ValueError()

    # Not recorded - out of context
    metrics.context_meter.digest_metric("m1", 123, "foo")

    assert cbs == [("m1", 1, "seconds")]


def test_context_meter_nested():
    it1 = iter([123, 126])
    it2 = iter([123, 124])
    cbs1 = []
    cbs2 = []

    with metrics.context_meter.add_callback(lambda l, v, u: cbs1.append((l, v, u))):
        with metrics.context_meter.add_callback(lambda l, v, u: cbs2.append((l, v, u))):
            with metrics.context_meter.meter("m1", func=lambda: next(it1)):
                with metrics.context_meter.meter("m2", func=lambda: next(it2)):
                    pass

    assert cbs1 == cbs2 == [("m2", 1, "seconds"), ("m1", 2, "seconds")]


def test_context_meter_decorator():
    it = iter([123, 124, 130, 135, 150, 160])
    cbs = []

    @metrics.context_meter.meter_function("m1", func=lambda: next(it))
    def f():
        pass

    with metrics.context_meter.add_callback(lambda l, v, u: cbs.append((l, v, u))):
        f()
        f()

    f()  # Not metered

    assert cbs == [("m1", 1, "seconds"), ("m1", 5, "seconds")]


def test_context_meter_nested_floor():
    """Subtracting calls from nested context_meter.meter() calls can cause the outermost
    calls to drop below the floor
    """
    it1 = iter([123, 125])
    it2 = iter([124, 128])
    cbs = []

    with metrics.context_meter.add_callback(lambda l, v, u: cbs.append((l, v, u))):
        with metrics.context_meter.meter("m1", func=lambda: next(it1), floor=0.1):
            with metrics.context_meter.meter("m2", func=lambda: next(it2)):
                pass

    assert cbs == [("m2", 4, "seconds"), ("m1", 0.1, "seconds")]


def test_context_meter_pickle():
    assert pickle.loads(pickle.dumps(metrics.context_meter)) is metrics.context_meter
