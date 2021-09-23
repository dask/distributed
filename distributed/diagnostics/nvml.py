import os

import dask

try:
    import pynvml
except ImportError:
    pynvml = None

nvmlInitialized = False
nvmlLibraryNotFound = False
nvmlOwnerPID = None
nvmlNotSupported = None


def init_once():
    global nvmlInitialized, nvmlLibraryNotFound, nvmlOwnerPID, nvmlNotSupported

    if dask.config.get("distributed.diagnostics.nvml") is False:
        nvmlInitialized = False
        return

    if pynvml is None or (nvmlInitialized is True and nvmlOwnerPID == os.getpid()):
        return

    nvmlInitialized = True
    nvmlOwnerPID = os.getpid()
    try:
        pynvml.nvmlInit()
        nvmlNotSupported = _pynvml_not_supported()
    except (
        pynvml.NVMLError_LibraryNotFound,
        pynvml.NVMLError_DriverNotLoaded,
        pynvml.NVMLError_Unknown,
    ):
        nvmlLibraryNotFound = True


def device_get_count():
    init_once()
    if nvmlLibraryNotFound or not nvmlInitialized:
        return 0
    else:
        return pynvml.nvmlDeviceGetCount()


def _pynvml_handles():
    count = device_get_count()
    if count == 0:
        if nvmlLibraryNotFound:
            raise RuntimeError("PyNVML is installed, but NVML is not")
        else:
            raise RuntimeError("No GPUs available")

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


def _pynvml_not_supported():
    h = _pynvml_handles()

    try:
        util = pynvml.nvmlDeviceGetUtilizationRates(h).gpu
    except pynvml.NVMLError_NotSupported:
        util = None
    try:
        mem = pynvml.nvmlDeviceGetMemoryInfo(h).used
    except pynvml.NVMLError_NotSupported:
        mem = None
    try:
        total = pynvml.nvmlDeviceGetMemoryInfo(h).total
    except pynvml.NVMLError_NotSupported:
        total = None
    try:
        name = pynvml.nvmlDeviceGetName(h).decode()
    except pynvml.NVMLError_NotSupported:
        name = None

    return {
        "util": util is None,
        "mem": mem is None,
        "total": total is None,
        "name": name is None,
    }


def has_cuda_context():
    """Check whether the current process already has a CUDA context created.

    Returns
    -------
    ``False`` if current process has no CUDA context created, otherwise returns the
    index of the device for which there's a CUDA context.
    """
    init_once()
    for index in range(device_get_count()):
        handle = pynvml.nvmlDeviceGetHandleByIndex(index)
        running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses_v2(handle)
        for proc in running_processes:
            if os.getpid() == proc.pid:
                return index
    return False


def real_time():
    h = _pynvml_handles()
    return {
        "utilization": None
        if nvmlNotSupported["util"]
        else pynvml.nvmlDeviceGetUtilizationRates(h).gpu,
        "memory-used": None
        if nvmlNotSupported["mem"]
        else pynvml.nvmlDeviceGetMemoryInfo(h).used,
    }


def one_time():
    h = _pynvml_handles()
    return {
        "memory-total": None
        if nvmlNotSupported["util"]
        else pynvml.nvmlDeviceGetMemoryInfo(h).total,
        "name": None
        if nvmlNotSupported["util"]
        else pynvml.nvmlDeviceGetName(h).decode(),
    }
