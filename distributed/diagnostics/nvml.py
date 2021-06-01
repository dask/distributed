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
    return pynvml.nvmlDeviceGetHandleByIndex(0)


def real_time():
    init_once()
    h = _pynvml_handles()
    return {
        "utilization": pynvml.nvmlDeviceGetUtilizationRates(h).gpu,
        "memory-used": pynvml.nvmlDeviceGetMemoryInfo(h).used,
    }


def one_time():
    init_once()
    h = _pynvml_handles()
    return {
        "memory-total": pynvml.nvmlDeviceGetMemoryInfo(h).total,
        "name": pynvml.nvmlDeviceGetName(h).decode(),
    }
