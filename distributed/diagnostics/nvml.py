import os

import pynvml

nvmlInitialized = False


def init_once():
    global nvmlInitialized
    if nvmlInitialized is True:
        return

    pynvml.nvmlInit()
    nvmlInitialized = True


def device_get_count():
    try:
        init_once()
    except pynvml.NVMLError_LibraryNotFound:
        return 0
    else:
        return pynvml.nvmlDeviceGetCount()


def _pynvml_handles():
    count = device_get_count()
    if count == 0:
        raise RuntimeError("No GPUs available or NVML is not installed")

    try:
        cuda_visible_devices = [
            int(idx) for idx in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",")
        ]
    except ValueError:
        # CUDA_VISIBLE_DEVICES is not set
        cuda_visible_devices = False
    if not cuda_visible_devices:
        cuda_visible_devices = list(range(count))
    gpu_idx = cuda_visible_devices[0]
    return pynvml.nvmlDeviceGetHandleByIndex(gpu_idx)


def real_time():
    h = _pynvml_handles()
    return {
        "utilization": pynvml.nvmlDeviceGetUtilizationRates(h).gpu,
        "memory-used": pynvml.nvmlDeviceGetMemoryInfo(h).used,
    }


def one_time():
    h = _pynvml_handles()
    return {
        "memory-total": pynvml.nvmlDeviceGetMemoryInfo(h).total,
        "name": pynvml.nvmlDeviceGetName(h).decode(),
    }
