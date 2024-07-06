from __future__ import annotations

import asyncio
import os

import pytest

from distributed.utils_test import gen_cluster

pytestmark = [
    pytest.mark.gpu,
    pytest.mark.skipif(
        os.environ.get("CUDF_SPILL", "off") != "on"
        or os.environ.get("CUDF_SPILL_STATS", "0") != "1"
        or os.environ.get("DASK_DISTRIBUTED__DIAGNOSTICS__CUDF", "0") != "1",
        reason="cuDF spill stats monitoring must be enabled manually",
    ),
]

cudf = pytest.importorskip("cudf")


def force_spill():
    from cudf.core.buffer.spill_manager import get_global_manager

    manager = get_global_manager()

    # Allocate a new dataframe and trigger spilling by setting a 1 byte limit
    df = cudf.DataFrame({"a": [1, 2, 3]})
    manager.spill_to_device_limit(1)

    # Get bytes spilled from GPU to CPU
    spill_totals, _ = get_global_manager().statistics.spill_totals[("gpu", "cpu")]
    return spill_totals


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
)
async def test_cudf_metrics(c, s, *workers):
    w = list(s.workers.values())[0]
    assert "cudf" in w.metrics
    assert w.metrics["cudf"]["cudf-spilled"] == 0

    spill_totals = (await c.run(force_spill, workers=[w.address]))[w.address]
    assert spill_totals > 0
    # We have to wait for the worker's metrics to update.
    # TODO: avoid sleep, is it possible to wait on the next update of metrics?
    await asyncio.sleep(1)
    assert w.metrics["cudf"]["cudf-spilled"] == spill_totals
