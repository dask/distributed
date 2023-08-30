"""
Diagnostics for memory spilling managed by cuDF.
"""

from __future__ import annotations

try:
    from cudf.core.buffer.spill_manager import get_global_manager
except ImportError:
    get_global_manager = None


def real_time():
    if get_global_manager is None:
        return {}
    mgr = get_global_manager()
    if mgr is None:
        return {}
    keys = {
        "gpu-to-cpu": {"nbytes": 0, "time": 0},
        "cpu-to-gpu": {"nbytes": 0, "time": 0},
    }
    for (src, dst), (nbytes, time) in mgr.statistics.spill_totals.items():
        keys[f"{src}-to-{dst}"]["nbytes"] = nbytes
        keys[f"{src}-to-{dst}"]["time"] = time
    return keys
