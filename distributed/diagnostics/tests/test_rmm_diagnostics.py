from __future__ import annotations

import asyncio

import pytest

from dask import delayed
from dask.utils import parse_bytes

from distributed.utils_test import gen_cluster

pytestmark = pytest.mark.gpu

dask_cuda = pytest.importorskip("dask_cuda")
rmm = pytest.importorskip("rmm")
pynvml = pytest.importorskip("pynvml")


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    Worker=dask_cuda.CUDAWorker,
    worker_kwargs={
        "rmm_pool_size": parse_bytes("10MiB"),
        "rmm_track_allocations": True,
    },
)
async def test_rmm_metrics(c, s, *workers):
    w = list(s.workers.values())[0]
    assert "rmm" in w.metrics
    assert w.metrics["rmm"]["rmm-used"] == 0
    assert w.metrics["rmm"]["rmm-total"] == parse_bytes("10MiB")
    result = delayed(rmm.DeviceBuffer)(size=10)
    result = result.persist()
    await asyncio.sleep(1)
    assert w.metrics["rmm"]["rmm-used"] != 0
    assert w.metrics["rmm"]["rmm-total"] == parse_bytes("10MiB")
