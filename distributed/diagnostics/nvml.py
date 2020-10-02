import os
import pynvml

nvmlInit = None


def init_once():
    global nvmlInit
    if nvmlInit is not None:
        return

    from pynvml import nvmlInit as _nvmlInit

    nvmlInit = _nvmlInit
    nvmlInit()


def _pynvml_handles():
    count = pynvml.nvmlDeviceGetCount()
    try:
        cuda_visible_devices = [
            int(idx) for idx in os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",")
        ]
    except ValueError:
        # CUDA_VISIBLE_DEVICES is not set
        cuda_visible_devices = False
    if not cuda_visible_devices:
        cuda_visible_devices = list(range(count))
    handles = [
        pynvml.nvmlDeviceGetHandleByIndex(i)
        for i in range(count)
        if i in cuda_visible_devices
    ]
    return handles


def real_time():
    init_once()
    handles = _pynvml_handles()
    return {
        "utilization": [pynvml.nvmlDeviceGetUtilizationRates(h).gpu for h in handles],
        "memory-used": [pynvml.nvmlDeviceGetMemoryInfo(h).used for h in handles],
        "procs": [
            [p.pid for p in pynvml.nvmlDeviceGetComputeRunningProcesses(h)]
            for h in handles
        ],
    }


def one_time():
    init_once()
    handles = _pynvml_handles()
    return {
        "memory-total": [pynvml.nvmlDeviceGetMemoryInfo(h).total for h in handles],
        "name": [pynvml.nvmlDeviceGetName(h).decode() for h in handles],
        "procs": [
            [p.pid for p in pynvml.nvmlDeviceGetComputeRunningProcesses(h)]
            for h in handles
        ],
    }
