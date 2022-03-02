import asyncio

import pytest

from dask import delayed
from distributed.utils_test  import gen_cluster


pytestmark = pytest.mark.gpu

dask_cuda = pytest.importorskip("dask_cuda")
rmm = pytest.importorskip("rmm")
pynvml = pytest.importorskip("pynvml")

@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    Worker=dask_cuda.CUDAWorker,
    worker_kwargs={"rmm_track_allocations": True}
)
async def test_rmm_metrics(c, s, *workers):
    w = list(s.workers.values())[0]
    assert "rmm" in w.metrics
    assert w.metrics["rmm"]["rmm-used"] == 0
    result = delayed(rmm.DeviceBuffer)(size=10)
    result = result.persist()
    await asyncio.sleep(1)
    assert w.metrics["rmm"]["rmm-used"] != 0
