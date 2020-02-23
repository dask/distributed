import collections
from functools import wraps
import sys
import time as timemod


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


def _native_thread_time():
    # Python 3.7+, not all platforms
    return timemod.thread_time()


def _linux_thread_time():
    # Use hardcoded CLOCK_THREAD_CPUTIME_ID on Python 3 <= 3.6
    if sys.platform != "linux":
        raise OSError
    return timemod.clock_gettime(3)


def _native_process_time():
    # Python 3, should work everywhere
    return timemod.process_time()


def _native_clock_func():
    # time.clock() unfortunately has different semantics depending on the
    # platform.  On POSIX it's a per-process CPU timer (with possibly
    # poor resolution).  On Windows it's a high-resolution wall clock timer.
    return timemod.clock()


def _detect_process_time():
    """
    Return a per-process CPU timer function if possible, otherwise
    a wall-clock timer.
    """
    for func in [_native_process_time]:
        try:
            func()
            return func
        except (AttributeError, OSError):
            pass
    # Only Python 2?
    return _native_clock_func


def _detect_thread_time():
    """
    Return a per-thread CPU timer function if possible, otherwise
    a per-process CPU timer function, or at worse a wall-clock timer.
    """
    for func in [_native_thread_time, _linux_thread_time, _native_process_time]:
        try:
            func()
            return func
        except (AttributeError, OSError):
            pass
    # Only Python 2?
    return time


process_time = _detect_process_time()
thread_time = _detect_thread_time()
time = timemod.time
