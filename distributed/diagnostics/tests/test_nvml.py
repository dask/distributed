from __future__ import annotations

import multiprocessing as mp
import os
from unittest import mock

import pytest

pytestmark = pytest.mark.gpu

pynvml = pytest.importorskip("pynvml")

import dask
from dask.utils import ensure_unicode

from distributed.diagnostics import nvml
from distributed.utils_test import gen_cluster


@pytest.fixture(autouse=True)
def reset_nvml_state():
    try:
        pynvml.nvmlShutdown()
    except pynvml.NVMLError_Uninitialized:
        pass
    nvml.NVML_STATE = nvml.NVML_STATE.UNINITIALIZED
    nvml.NVML_OWNER_PID = None


def test_one_time():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    output = nvml.one_time()
    assert "memory-total" in output
    assert "name" in output

    assert len(output["name"]) > 0


def test_enable_disable_nvml():
    with dask.config.set({"distributed.diagnostics.nvml": False}):
        nvml.init_once()
        assert not nvml.is_initialized()
        assert nvml.NVML_STATE == nvml.NVMLState.DISABLED_CONFIG

    # Idempotent (once we've decided not to turn things on with
    # configuration, it's set in stone)
    nvml.init_once()
    assert not nvml.is_initialized()
    assert nvml.NVML_STATE == nvml.NVMLState.DISABLED_CONFIG


def test_wsl_monitoring_enabled():
    nvml.init_once()
    assert nvml.NVML_STATE != nvml.NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER


def run_has_cuda_context(queue):
    try:
        assert not nvml.has_cuda_context().has_context

        import numba.cuda

        numba.cuda.current_context()
        ctx = nvml.has_cuda_context()
        assert (
            ctx.has_context
            and ctx.device_info.device_index == 0
            and isinstance(ctx.device_info.uuid, str)
        )

        queue.put(None)

    except Exception as e:
        queue.put(e)


@pytest.mark.xfail(reason="If running on Docker, requires --pid=host")
def test_has_cuda_context():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    # This test should be run in a new process so that it definitely doesn't have a CUDA context
    # and uses a queue to pass exceptions back
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    p = ctx.Process(target=run_has_cuda_context, args=(queue,))
    p.start()
    p.join()  # this blocks until the process terminates
    e = queue.get()
    if e is not None:
        raise e


def test_1_visible_devices():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0"}):
        output = nvml.one_time()
        h = nvml._pynvml_handles()
        assert output["memory-total"] == pynvml.nvmlDeviceGetMemoryInfo(h).total


@pytest.mark.parametrize("CVD", ["1,0", "0,1"])
def test_2_visible_devices(CVD):
    if nvml.device_get_count() < 2:
        pytest.skip("Less than two GPUs available")

    with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": CVD}):
        idx = int(CVD.split(",")[0])

        h = nvml._pynvml_handles()
        h2 = pynvml.nvmlDeviceGetHandleByIndex(idx)

        s = pynvml.nvmlDeviceGetSerial(h)
        s2 = pynvml.nvmlDeviceGetSerial(h2)

        assert s == s2


def test_visible_devices_uuid():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    info = nvml.get_device_index_and_uuid(0)
    assert info.uuid

    with mock.patch.dict(
        os.environ, {"CUDA_VISIBLE_DEVICES": ensure_unicode(info.uuid)}
    ):
        h = nvml._pynvml_handles()
        h_expected = pynvml.nvmlDeviceGetHandleByIndex(0)

        s = pynvml.nvmlDeviceGetSerial(h)
        s_expected = pynvml.nvmlDeviceGetSerial(h_expected)

        assert s == s_expected


@pytest.mark.parametrize("index", [0, 1])
def test_visible_devices_uuid_2(index):
    if nvml.device_get_count() < 2:
        pytest.skip("Less than two GPUs available")

    info = nvml.get_device_index_and_uuid(index)
    assert info.uuid

    with mock.patch.dict(
        os.environ, {"CUDA_VISIBLE_DEVICES": ensure_unicode(info.uuid)}
    ):
        h = nvml._pynvml_handles()
        h_expected = pynvml.nvmlDeviceGetHandleByIndex(index)

        s = pynvml.nvmlDeviceGetSerial(h)
        s_expected = pynvml.nvmlDeviceGetSerial(h_expected)

    assert s == s_expected


def test_visible_devices_bad_uuid():
    if nvml.device_get_count() < 1:
        pytest.skip("No GPUs available")

    with (
        mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "NOT-A-GPU-UUID"}),
        pytest.raises(ValueError, match="Devices in CUDA_VISIBLE_DEVICES"),
    ):
        nvml._pynvml_handles()


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
    assert s.workers[a.address].extra["gpu"]["name"] == nvml._get_name(h)


@pytest.mark.flaky(reruns=5, reruns_delay=2)
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
    assert res[a.address]["gpu_name"] == nvml._get_name(h)
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
