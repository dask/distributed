from __future__ import annotations

import collections
import time as timemod
from collections.abc import Callable
from functools import wraps

import psutil

from distributed.compatibility import WINDOWS

_empty_namedtuple = collections.namedtuple("_empty_namedtuple", ())


def _psutil_caller(method_name, default=_empty_namedtuple):
    """
    Return a function calling the given psutil *method_name*,
    or returning *default* if psutil fails.
    """
    meth = getattr(psutil, method_name)

    @wraps(meth)
    def wrapper():  # pragma: no cover
        try:
            return meth()
        except RuntimeError:
            # This can happen on some systems (e.g. no physical disk in worker)
            return default()

    return wrapper


disk_io_counters = _psutil_caller("disk_io_counters")

net_io_counters = _psutil_caller("net_io_counters")


class _WindowsTime:
    """Combine time.time() or time.monotonic() with time.perf_counter() to get an
    absolute clock with fine resolution.
    """

    base_timer: Callable[[], float]
    delta: float
    previous: float | None
    next_resync: float
    resync_every: float

    def __init__(
        self, base: Callable[[], float], is_monotonic: bool, resync_every: float = 600.0
    ):
        self.base_timer = base
        self.previous = float("-inf") if is_monotonic else None
        self.next_resync = float("-inf")
        self.resync_every = resync_every

    def time(self) -> float:
        cur = timemod.perf_counter()
        if cur > self.next_resync:
            self.resync()
            self.next_resync = cur + self.resync_every
        cur += self.delta
        if self.previous is not None:
            # Monotonic timer
            if cur <= self.previous:
                cur = self.previous + 1e-9
            self.previous = cur
        return cur

    def resync(self) -> None:
        _time = self.base_timer
        _perf_counter = timemod.perf_counter
        min_samples = 5
        while True:
            times = [(_time(), _perf_counter()) for _ in range(min_samples * 2)]
            abs_times = collections.Counter(t[0] for t in times)
            first, nfirst = abs_times.most_common()[0]
            if nfirst < min_samples:
                # System too noisy? Start again
                continue

            perf_times = [t[1] for t in times if t[0] == first][:-1]
            assert len(perf_times) >= min_samples - 1, perf_times
            self.delta = first - sum(perf_times) / len(perf_times)
            break


# A high-resolution wall clock timer measuring the seconds since Unix epoch
if WINDOWS:
    time = _WindowsTime(timemod.time, is_monotonic=False).time
    monotonic = _WindowsTime(timemod.monotonic, is_monotonic=True).time
else:
    # Under modern Unixes, time.time() and time.monotonic() should be good enough
    time = timemod.time
    monotonic = timemod.monotonic

process_time = timemod.process_time

# Get a per-thread CPU timer function if possible, otherwise
# use a per-process CPU timer function.
try:
    # thread_time is not supported on all platforms
    thread_time = timemod.thread_time
except (AttributeError, OSError):  # pragma: no cover
    thread_time = process_time
