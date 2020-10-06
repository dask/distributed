import pytest
from distributed.utils_test import gen_cluster

pynvml = pytest.importorskip("pynvml")


@gen_cluster()
async def test_gpu_metrics(s, a, b):
    from distributed.diagnostics.nvml import _pynvml_handles as handles

    h = handles()

    assert "gpu" in a.metrics
    assert (
        s.workers[a.address].metrics["gpu"]["memory-used"]
        == pynvml.nvmlDeviceGetMemoryInfo(h).used
    )
    assert "gpu" in a.startup_information
    assert (
        s.workers[a.address].extra["gpu"]["name"]
        == pynvml.nvmlDeviceGetName(h).decode()
    )
