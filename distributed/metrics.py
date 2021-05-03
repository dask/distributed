import collections
import sys
import time as timemod
from functools import wraps

_empty_namedtuple = collections.namedtuple("_empty_namedtuple", ())


def _psutil_caller(method_name, default=_empty_namedtuple):
    """
    Return a function calling the given psutil *method_name*,
    or returning *default* if psutil is not present.
    """
    # Import only once to avoid the cost of a failing import at each wrapper() call
    try:
        import psutil
    except ImportError:
        return default

    meth = getattr(psutil, method_name)

    @wraps(meth)
    def wrapper():
        try:
            return meth()
        except RuntimeError:
            # This can happen on some systems (e.g. no physical disk in worker)
            return default()

    return wrapper


disk_io_counters = _psutil_caller("disk_io_counters")

net_io_counters = _psutil_caller("net_io_counters")


class _WindowsTime:
    """
    Combine time.time() and time.perf_counter() to get an absolute clock
    with fine resolution.
    """

    # Resync every N seconds, to avoid drifting
    RESYNC_EVERY = 600

    def __init__(self):
        self.delta = None
        self.last_resync = float("-inf")

    perf_counter = timemod.perf_counter

    def time(self):
        delta = self.delta
        cur = self.perf_counter()
        if cur - self.last_resync >= self.RESYNC_EVERY:
            delta = self.resync()
            self.last_resync = cur
        return delta + cur

    def resync(self):
        _time = timemod.time
        _perf_counter = self.perf_counter
        min_samples = 5
        while True:
            times = [(_time(), _perf_counter()) for i in range(min_samples * 2)]
            abs_times = collections.Counter(t[0] for t in times)
            first, nfirst = abs_times.most_common()[0]
            if nfirst < min_samples:
                # System too noisy? Start again
                continue
            else:
                perf_times = [t[1] for t in times if t[0] == first][:-1]
                assert len(perf_times) >= min_samples - 1, perf_times
                self.delta = first - sum(perf_times) / len(perf_times)
                return self.delta


# A high-resolution wall clock timer measuring the seconds since Unix epoch
if sys.platform.startswith("win"):
    time = _WindowsTime().time
else:
    # Under modern Unices, time.time() should be good enough
    time = timemod.time

process_time = timemod.process_time

# Get a per-thread CPU timer function if possible, otherwise
# use a per-process CPU timer function.
try:
    # thread_time is supported on Python 3.7+ but not all platforms
    thread_time = timemod.thread_time
except (AttributeError, OSError):
    # process_time is supported on Python 3.3+ everywhere
    thread_time = process_time
