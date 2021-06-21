import os

import pytest

pynvml = pytest.importorskip("pynvml")

import dask

from distributed.diagnostics import nvml
from distributed.utils_test import gen_cluster


def test_one_time():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    output = nvml.one_time()
    assert "memory-total" in output
    assert "name" in output

    assert len(output["name"]) > 0


def test_enable_disable_nvml():
    try:
        pynvml.nvmlShutdown()
    except pynvml.NVMLError_Uninitialized:
        pass
    else:
        nvml.nvmlInitialized = False

    with dask.config.set({"distributed.diagnostics.nvml": False}):
        nvml.init_once()
        assert nvml.nvmlInitialized is False

    with dask.config.set({"distributed.diagnostics.nvml": True}):
        nvml.init_once()
        assert nvml.nvmlInitialized is True


def test_1_visible_devices():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    output = nvml.one_time()
    h = nvml._pynvml_handles()
    assert output["memory-total"] == pynvml.nvmlDeviceGetMemoryInfo(h).total


@pytest.mark.parametrize("CVD", ["1,0", "0,1"])
def test_2_visible_devices(CVD):
    if nvml.device_get_count() < 2:
        pytest.skip("Less than two GPUs available")

    os.environ["CUDA_VISIBLE_DEVICES"] = CVD
    idx = int(CVD.split(",")[0])

    h = nvml._pynvml_handles()
    h2 = pynvml.nvmlDeviceGetHandleByIndex(idx)

    s = pynvml.nvmlDeviceGetSerial(h)
    s2 = pynvml.nvmlDeviceGetSerial(h2)

    assert s == s2


@gen_cluster()
async def test_gpu_metrics(s, a, b):
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    h = nvml._pynvml_handles()

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


@gen_cluster()
async def test_gpu_monitoring_recent(s, a, b):
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    h = nvml._pynvml_handles()
    res = await s.get_worker_monitor_info(recent=True)

    assert (
        res[a.address]["range_query"]["gpu_utilization"]
        == pynvml.nvmlDeviceGetUtilizationRates(h).gpu
    )
    assert (
        res[a.address]["range_query"]["gpu_memory_used"]
        == pynvml.nvmlDeviceGetMemoryInfo(h).used
    )
    assert res[a.address]["gpu_name"] == pynvml.nvmlDeviceGetName(h).decode()
    assert res[a.address]["gpu_memory_total"] == pynvml.nvmlDeviceGetMemoryInfo(h).total


@gen_cluster()
async def test_gpu_monitoring_range_query(s, a, b):
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    res = await s.get_worker_monitor_info()
    ms = ["gpu_utilization", "gpu_memory_used"]
    for w in (a, b):
        assert all(res[w.address]["range_query"][m] is not None for m in ms)
        assert res[w.address]["count"] is not None
        assert res[w.address]["last_time"] is not None
