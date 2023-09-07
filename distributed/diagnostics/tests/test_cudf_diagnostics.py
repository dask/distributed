from __future__ import annotations

import pytest

from distributed.utils_test import gen_cluster

pytestmark = pytest.mark.gpu

cudf = pytest.importorskip("cudf")
dask_cuda = pytest.importorskip("dask_cuda")


def force_spill():
    from cudf.core.buffer.spill_manager import get_global_manager

    manager = get_global_manager()

    # 24 bytes
    df = cudf.DataFrame({"a": [1, 2, 3]})

    return manager.spill_to_device_limit(1)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    Worker=dask_cuda.CUDAWorker,
)
async def test_cudf_metrics(c, s, *workers):
    w = list(s.workers.values())[0]
    assert "cudf" in w.metrics
    assert w.metrics["cudf"]["cudf-spilled"] == 0

    try:
        await c.run(force_spill)
    except AttributeError:
        pytest.xfail("cuDF spilling & spilling statistics must be enabled")

    assert w.metrics["cudf"]["cudf-spilled"] == 24
