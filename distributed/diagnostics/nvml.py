import os

import dask

try:
    import pynvml
except ImportError:
    pynvml = None

nvmlInitialized = False
nvmlLibraryNotFound = False
nvmlOwnerPID = None


def init_once():
    global nvmlInitialized, nvmlLibraryNotFound, nvmlOwnerPID

    if dask.config.get("distributed.diagnostics.nvml") is False:
        nvmlInitialized = False
        return

    if pynvml is None or (nvmlInitialized is True and nvmlOwnerPID == os.getpid()):
        return

    nvmlInitialized = True
    nvmlOwnerPID = os.getpid()
    try:
        pynvml.nvmlInit()
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
        if hasattr(pynvml, "nvmlDeviceGetComputeRunningProcesses_v2"):
            running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses_v2(handle)
        else:
            running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
        for proc in running_processes:
            if os.getpid() == proc.pid:
                return index
    return False


def _get_utilization(h):
    try:
        return pynvml.nvmlDeviceGetUtilizationRates(h).gpu
    except pynvml.NVMLError_NotSupported:
        return None


def _get_memory_used(h):
    try:
        return pynvml.nvmlDeviceGetMemoryInfo(h).used
    except pynvml.NVMLError_NotSupported:
        return None


def _get_memory_total(h):
    try:
        return pynvml.nvmlDeviceGetMemoryInfo(h).total
    except pynvml.NVMLError_NotSupported:
        return None


def _get_name(h):
    try:
        return pynvml.nvmlDeviceGetName(h).decode()
    except pynvml.NVMLError_NotSupported:
        return None


def real_time():
    h = _pynvml_handles()
    return {
        "utilization": _get_utilization(h),
        "memory-used": _get_memory_used(h),
    }


def one_time():
    h = _pynvml_handles()
    return {
        "memory-total": _get_memory_total(h),
        "name": _get_name(h),
    }
