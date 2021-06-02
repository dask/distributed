import pynvml

nvmlInit = None


def init_once():
    global nvmlInit
    if nvmlInit is not None:
        return

    from pynvml import nvmlInit as _nvmlInit

    nvmlInit = _nvmlInit
    nvmlInit()


def device_get_count():
    init_once()
    return pynvml.nvmlDeviceGetCount()


def real_time():
    init_once()
    h = pynvml.nvmlDeviceGetHandleByIndex(0)
    return {
        "utilization": pynvml.nvmlDeviceGetUtilizationRates(h).gpu,
        "memory-used": pynvml.nvmlDeviceGetMemoryInfo(h).used,
    }


def one_time():
    init_once()
    h = pynvml.nvmlDeviceGetHandleByIndex(0)
    return {
        "memory-total": pynvml.nvmlDeviceGetMemoryInfo(h).total,
        "name": pynvml.nvmlDeviceGetName(h).decode(),
    }
