import pynvml

pynvml_initialized = False
handles = None


def _ensure_pynvml_initialized():
    global pynvml_initialized, handles
    if not pynvml_initialized:
        pynvml.nvmlInit()
        count = pynvml.nvmlDeviceGetCount()
        handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(count)]
        pynvml_initialized = True


def real_time():
    global handles
    _ensure_pynvml_initialized()
    return {
        "utilization": [pynvml.nvmlDeviceGetUtilizationRates(h).gpu for h in handles],
        "memory-used": [pynvml.nvmlDeviceGetMemoryInfo(h).used for h in handles],
    }


def one_time():
    global handles
    _ensure_pynvml_initialized()
    return {
        "memory-total": [pynvml.nvmlDeviceGetMemoryInfo(h).total for h in handles],
        "name": [pynvml.nvmlDeviceGetName(h).decode() for h in handles],
    }
