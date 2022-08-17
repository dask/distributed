from __future__ import annotations

import time

import pytest

from distributed import metrics


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
