from __future__ import annotations

import collections
import time as timemod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from math import nan
from typing import Any, Literal

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


@dataclass
class MeterOutput:
    start: float
    stop: float
    delta: float
    __slots__ = tuple(__annotations__)


@contextmanager
def meter(
    func: Callable[[], float] = timemod.perf_counter,
    floor: float | Literal[False] = 0.0,
) -> Iterator[MeterOutput]:
    """Convenience context manager which calls func() before and after the wrapped
    code and calculates the delta.

    Parameters
    ----------
    label: str
        label to pass to the callback
    func: callable
        function to call before and after, which must return a number.
        Besides time, it could return e.g. cumulative network traffic or disk usage.
        Default: :func:`timemod.perf_counter`
    floor: float or False, optional
        Floor the delta to the given value (default: 0). This is useful for strictly
        cumulative functions that can occasionally glitch and go backwards.
        Set to False to disable.
    """
    out = MeterOutput(func(), nan, nan)
    try:
        yield out
    finally:
        out.stop = func()
        out.delta = out.stop - out.start
        if floor is not False:
            out.delta = max(floor, out.delta)


class ContextMeter:
    """Context-based general purpose meter.

    Usage
    -----
    1. In high level code, call :meth:`add_callback` to install a hook that defines an
       activity
    2. In low level code, typically many stack levels below, log quantitative events
       (e.g. elapsed time, transferred bytes, etc.) so that they will be attributed to
       the high-level code calling it, either with :meth:`meter` or
       :meth:`digest_metric`.

    Examples
    --------
    In the code that e.g. sends a Python object from A to B over the network:
    >>> from distributed.metrics import context_meter
    >>> with context_meter.add_callback(partial(print, "A->B comms:")):
    ...     await send_over_the_network(obj)

    In the serialization utilities, called many stack levels below:
    >>> with context_meter.meter("dumps"):
    ...     pik = pickle.dumps(obj)
    >>> with context_meter.meter("compress"):
    ...     pik = lz4.compress(pik)

    And finally, elsewhere, deep into the TCP stack:
    >>> with context_meter.meter("network-write"):
    ...     await comm.write(frames)

    When you call the top-level code, you'll get::
      A->B comms: dumps 0.012 seconds
      A->B comms: compress 0.034 seconds
      A->B comms: network-write 0.567 seconds
    """

    _callbacks: ContextVar[list[Callable[[str, float, str], None]]]

    def __init__(self):
        self._callbacks = ContextVar(f"MetricHook<{id(self)}>._callbacks", default=[])

    @contextmanager
    def add_callback(
        self, callback: Callable[[str, float, str], None]
    ) -> Iterator[None]:
        """Add a callback when entering the context and remove it when exiting it.
        The callback must accept the same parameters as :meth:`digest_metric`.
        """
        cbs = self._callbacks.get()
        tok = self._callbacks.set(cbs + [callback])
        try:
            yield
        finally:
            tok.var.reset(tok)

    def digest_metric(self, label: str, value: float, unit: str) -> None:
        """Invoke the currently set context callbacks for an arbitrary quantitative
        metric.
        """
        cbs = self._callbacks.get()
        for cb in cbs:
            cb(label, value, unit)

    def meter(
        self,
        label: str,
        unit: str = "seconds",
        func: Callable[[], float] = timemod.perf_counter,
        floor: float | Literal[False] = 0.0,
    ) -> Any:
        """Convenience context manager or function decorator which calls func() before
        and after the wrapped code, calculates the delta, and finally calls
        :meth:`digest_metric`. It also subtracts any other calls to :meth:`meter` or
        :meth:`digest_metric` with the same unit performed within the context, so that
        the total is strictly additive.

        Parameters
        ----------
        label: str
            label to pass to the callback
        unit: str, optional
            unit to pass to the callback. Default: seconds
        func: callable
            see :func:`meter`
        floor: bool, optional
            see :func:`meter`
        """
        parent = self

        class _ContextMeter_meter:
            def __call__(self, f2):
                @wraps(f2)
                def wrapper(*args, **kwargs):
                    with self:
                        return f2(*args, **kwargs)

                return wrapper

            def _callback(self, label2: str, value2: float, unit2: str) -> None:
                if unit2 == unit:
                    # This must be threadsafe to support when callbacks are invoked from
                    # distributed.utils.offload; '+=' on a float would not be
                    # threadsafe!
                    self.offsets.append(value2)

            def __enter__(self):
                self.offsets = []
                self.cb_ctx = parent.add_callback(self._callback)
                self.cb_ctx.__enter__()
                self.meter_ctx = meter(func, floor=False)
                self.meter_output = self.meter_ctx.__enter__()

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.cb_ctx.__exit__(exc_type, exc_val, exc_tb)
                self.meter_ctx.__exit__(exc_type, exc_val, exc_tb)
                delta = self.meter_output.delta - sum(self.offsets)
                if floor is not False:
                    delta = max(floor, delta)
                parent.digest_metric(label, delta, unit)
                del self.offsets
                del self.cb_ctx
                del self.meter_ctx

        return _ContextMeter_meter()


context_meter = ContextMeter()
