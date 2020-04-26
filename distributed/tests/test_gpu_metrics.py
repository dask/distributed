import pytest
from distributed.utils_test import gen_cluster

pytest.importorskip("pynvml")


@gen_cluster()
async def test_gpu_metrics(s, a, b):
from distributed.diagnostics.nvml import handles

    assert "gpu" in a.metrics
len(s.workers[a.address].metrics["gpu"]["memory-used"]) == len(handles)

    assert "gpu" in a.startup_information
len(s.workers[a.address].extra["gpu"]["name"]) == len(handles)
