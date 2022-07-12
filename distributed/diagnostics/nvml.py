from __future__ import annotations

import os
from enum import IntEnum, auto
from platform import uname

from packaging.version import parse as parse_version

import dask

try:
    import pynvml
except ImportError:
    pynvml = None


class NVMLState(IntEnum):
    UNINITIALIZED = auto()
    """No attempt yet made to initialize PyNVML"""
    INITIALIZED = auto()
    """PyNVML was successfully initialized"""
    DISABLED_PYNVML_NOT_AVAILABLE = auto()
    """PyNVML not installed"""
    DISABLED_CONFIG = auto()
    """PyNVML diagnostics disabled by ``distributed.diagnostics.nvml`` config setting"""
    DISABLED_LIBRARY_NOT_FOUND = auto()
    """PyNVML available, but NVML not installed"""
    DISABLED_WSL_INSUFFICIENT_DRIVER = auto()
    """PyNVML and NVML available, but on WSL and the driver version is insufficient"""


# Initialisation must occur per-process, so an initialised state is a
# (state, pid) pair
NVML_STATE = (
    NVMLState.DISABLED_PYNVML_NOT_AVAILABLE
    if pynvml is None
    else NVMLState.UNINITIALIZED
)
"""Current initialization state"""

NVML_OWNER_PID = None
"""PID of process that successfully called pynvml.nvmlInit"""

MINIMUM_WSL_VERSION = "512.15"


def is_initialized():
    """Is pynvml initialized on this process?"""
    return NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID == os.getpid()


def _in_wsl():
    """Check if we are in Windows Subsystem for Linux; some PyNVML queries are not supported there.
    Taken from https://www.scivision.dev/python-detect-wsl/
    """
    return "microsoft-standard" in uname().release


def init_once():
    """Idempotent (per-process) initialization of PyNVML

    Notes
    -----

    Modifies global variables NVML_STATE and NVML_OWNER_PID"""
    global NVML_STATE, NVML_OWNER_PID

    if NVML_STATE in {
        NVMLState.DISABLED_PYNVML_NOT_AVAILABLE,
        NVMLState.DISABLED_CONFIG,
        NVMLState.DISABLED_LIBRARY_NOT_FOUND,
        NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER,
    }:
        return
    elif NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID == os.getpid():
        return
    elif NVML_STATE == NVMLState.UNINITIALIZED and not dask.config.get(
        "distributed.diagnostics.nvml"
    ):
        NVML_STATE = NVMLState.DISABLED_CONFIG
        return
    elif (
        NVML_STATE == NVMLState.INITIALIZED and NVML_OWNER_PID != os.getpid()
    ) or NVML_STATE == NVMLState.UNINITIALIZED:
        try:
            pynvml.nvmlInit()
        except (
            pynvml.NVMLError_LibraryNotFound,
            pynvml.NVMLError_DriverNotLoaded,
            pynvml.NVMLError_Unknown,
        ):
            NVML_STATE = NVMLState.DISABLED_LIBRARY_NOT_FOUND
            return

        if _in_wsl() and parse_version(
            pynvml.nvmlSystemGetDriverVersion().decode()
        ) < parse_version(MINIMUM_WSL_VERSION):
            NVML_STATE = NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER
            return
        else:
            from distributed.worker import add_gpu_metrics

            # initialization was successful
            NVML_STATE = NVMLState.INITIALIZED
            NVML_OWNER_PID = os.getpid()
            add_gpu_metrics()
    else:
        raise RuntimeError(
            f"Unhandled initialisation state ({NVML_STATE=}, {NVML_OWNER_PID=})"
        )


def device_get_count():
    init_once()
    if not is_initialized():
        return 0
    else:
        return pynvml.nvmlDeviceGetCount()


def _pynvml_handles():
    count = device_get_count()
    if NVML_STATE == NVMLState.DISABLED_PYNVML_NOT_AVAILABLE:
        raise RuntimeError("NVML monitoring requires PyNVML and NVML to be installed")
    elif NVML_STATE == NVMLState.DISABLED_LIBRARY_NOT_FOUND:
        raise RuntimeError("PyNVML is installed, but NVML is not")
    elif NVML_STATE == NVMLState.DISABLED_WSL_INSUFFICIENT_DRIVER:
        raise RuntimeError(
            "Outdated NVIDIA drivers for WSL, please upgrade to "
            f"{MINIMUM_WSL_VERSION} or newer"
        )
    elif NVML_STATE == NVMLState.DISABLED_CONFIG:
        raise RuntimeError(
            "PyNVML monitoring disabled by 'distributed.diagnostics.nvml' "
            "config setting"
        )
    elif count == 0:
        raise RuntimeError("No GPUs available")
    else:
        try:
            gpu_idx = next(
                map(int, os.environ.get("CUDA_VISIBLE_DEVICES", "").split(","))
            )
        except ValueError:
            # CUDA_VISIBLE_DEVICES is not set, take first device
            gpu_idx = 0
        return pynvml.nvmlDeviceGetHandleByIndex(gpu_idx)


def _running_process_matches(handle):
    """Check whether the current process is same as of handle's

    Parameters
    ----------
    handle: ``pyvnml.nvml.LP_struct_c_nvmlDevice_t``
        NVML handle to CUDA device

    Returns
    -------
    out: bool
        ``True`` if device handle's has a CUDA context on the running process,
        or ``False`` otherwise.
    """
    init_once()
    if hasattr(pynvml, "nvmlDeviceGetComputeRunningProcesses_v2"):
        running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses_v2(handle)
    else:
        running_processes = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)
    for proc in running_processes:
        if os.getpid() == proc.pid:
            return True
    return False


def has_cuda_context():
    """Check whether the current process already has a CUDA context created.

    Returns
    -------
    ``False`` if current process has no CUDA context created, otherwise returns the
    index of the device for which there's a CUDA context.
    """
    no_context = {
        "has-context": False,
        "device-index": None,
        "mig-index": None,
        "uuid": None,
    }
    init_once()
    if not is_initialized():
        return no_context
    for index in range(device_get_count()):
        handle = pynvml.nvmlDeviceGetHandleByIndex(index)
        mig_current_mode, mig_pending_mode = pynvml.nvmlDeviceGetMigMode(handle)
        if mig_current_mode == pynvml.NVML_DEVICE_MIG_ENABLE:
            for mig_index in range(pynvml.nvmlDeviceGetMaxMigDeviceCount(handle)):
                try:
                    mig_handle = pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                        handle, mig_index
                    )
                except pynvml.NVMLError_NotFound:
                    # No MIG device with that index
                    continue
                if _running_process_matches(mig_handle):
                    uuid = pynvml.nvmlDeviceGetUUID(mig_handle)
                    return {
                        "has-context": True,
                        "device-index": index,
                        "mig-index": mig_index,
                        "uuid": uuid,
                    }
        else:
            if _running_process_matches(handle):
                uuid = pynvml.nvmlDeviceGetUUID(handle)
                return {
                    "has-context": True,
                    "device-index": index,
                    "mig-index": None,
                    "uuid": uuid,
                }
    return no_context


def get_device_index_and_uuid(device):
    """Get both device index and UUID from device index or UUID

    Parameters
    ----------
    device: ``int``, ``bytes`` or``str``
        An ``int`` with the index of a GPU, or ``bytes`` or ``str`` with the UUID
        of a CUDA (either GPU or MIG) device.

    Returns
    -------
    out: ``dict``
        Dictionary containing ``"device-index"`` and ``"uuid"`` keys.

    Examples
    --------
    >>> get_device_index_and_uuid(0)
    {'device-index': 0, 'uuid': b'GPU-e1006a74-5836-264f-5c26-53d19d212dfe'}

    >>> get_device_index_and_uuid('GPU-e1006a74-5836-264f-5c26-53d19d212dfe')
    {'device-index': 0, 'uuid': b'GPU-e1006a74-5836-264f-5c26-53d19d212dfe'}

    >>> get_device_index_and_uuid('MIG-7feb6df5-eccf-5faa-ab00-9a441867e237')
    {'device-index': 0, 'uuid': b'MIG-7feb6df5-eccf-5faa-ab00-9a441867e237'}
    """
    init_once()
    try:
        device_index = int(device)
        device_handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
        uuid = pynvml.nvmlDeviceGetUUID(device_handle)
    except ValueError:
        uuid = device if isinstance(device, bytes) else bytes(device, "utf-8")

        # Validate UUID, get index and UUID as seen with `nvidia-smi -L`
        uuid_handle = pynvml.nvmlDeviceGetHandleByUUID(uuid)
        device_index = pynvml.nvmlDeviceGetIndex(uuid_handle)
        uuid = pynvml.nvmlDeviceGetUUID(uuid_handle)

    return {"device-index": device_index, "uuid": uuid}


def get_device_mig_mode(device):
    """Get MIG mode for a device index or UUID

    Parameters
    ----------
    device: ``int``, ``bytes`` or``str``
        An ``int`` with the index of a GPU, or ``bytes`` or ``str`` with the UUID
        of a CUDA (either GPU or MIG) device.

    Returns
    -------
    out: ``list``
        A ``list`` with two integers ``[current_mode, pending_mode]``.
    """
    init_once()
    try:
        device_index = int(device)
        handle = pynvml.nvmlDeviceGetHandleByIndex(device_index)
    except ValueError:
        uuid = device if isinstance(device, bytes) else bytes(device, "utf-8")
        handle = pynvml.nvmlDeviceGetHandleByUUID(uuid)
    return pynvml.nvmlDeviceGetMigMode(handle)


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
