from __future__ import print_function, division, absolute_import

from collections import deque
import gc
import logging

from .metrics import thread_time


logger = _logger = logging.getLogger(__name__)


class ThrottledGC(object):
    """Wrap gc.collect to protect against excessively repeated calls.

    Allows to run throttled garbage collection in the workers as a
    countermeasure to e.g.: https://github.com/dask/zict/issues/19

    collect() does nothing when repeated calls are so costly and so frequent
    that the thread would spend more than max_in_gc_frac doing GC.

    warn_if_longer is a duration in seconds (10s by default) that can be used
    to log a warning level message whenever an actual call to gc.collect()
    lasts too long.
    """
    def __init__(self, max_in_gc_frac=0.05, warn_if_longer=1, logger=None):
        self.max_in_gc_frac = max_in_gc_frac
        self.warn_if_longer = warn_if_longer
        self.last_collect = thread_time()
        self.last_gc_duration = 0
        self.logger = logger if logger is not None else _logger

    def collect(self):
        # In case of non-monotonicity in the clock, assume that any Python
        # operation lasts at least 1e-6 second.
        MIN_RUNTIME = 1e-6

        collect_start = thread_time()
        elapsed = max(collect_start - self.last_collect, MIN_RUNTIME)
        if self.last_gc_duration / elapsed < self.max_in_gc_frac:
            self.logger.debug("Calling gc.collect(). %0.3fs elapsed since "
                              "previous call.", elapsed)
            gc.collect()
            self.last_collect = collect_start
            self.last_gc_duration = max(thread_time() - collect_start, MIN_RUNTIME)
            if self.last_gc_duration > self.warn_if_longer:
                self.logger.warning("gc.collect() took %0.3fs. This is usually"
                                    " a sign that the some tasks handle too"
                                    " many Python objects at the same time."
                                    " Rechunking the work into smaller tasks"
                                    " might help.",
                                    self.last_gc_duration)
            else:
                self.logger.debug("gc.collect() took %0.3fs",
                                  self.last_gc_duration)
        else:
            self.logger.debug("gc.collect() lasts %0.3fs but only %0.3fs "
                              "elapsed since last call: throttling.",
                              self.last_gc_duration, elapsed)


class _MeasureRelativeRuntime(object):
    """
    """

    MULT = 1e9  # convert to nanoseconds
    N_SAMPLES = 30

    def __init__(self, timer=thread_time, n_samples=None):
        self._timer = timer
        self._n_samples = n_samples or self.N_SAMPLES
        self._start_stops = deque()
        self._durations = deque()
        self._cur_start = None
        self._running_sum = None
        self._running_fraction = None

    def _add_measurement(self, start, stop):
        start_stops = self._start_stops
        durations = self._durations
        if stop < start or start < start_stops[-1][1]:
            # Ignore if non-monotonic
            return

        # Ensure exact running sum computation with integer arithmetic
        duration = int((stop - start) * self.MULT)
        start_stops.append((start, stop))
        durations.append(duration)

        n = len(durations)
        assert n == len(start_stops)
        if n >= self._n_samples:
            if self._running_sum is None:
                assert n == self._n_samples
                self._running_sum = sum(durations)
            else:
                old_start, old_stop = start_stops.popleft()
                old_duration = durations.popleft()
                self._running_sum += duration - old_duration
                if stop >= old_start:
                    self._running_fraction = (
                        self._running_sum / (stop - old_start) / self.MULT
                    )

    def start_timing(self):
        assert self._cur_start is None
        self._cur_start = self._timer()

    def stop_timing(self):
        stop = self._timer()
        start = self._cur_start
        self._cur_start = None
        assert start is not None
        self._add_measurement(start, stop)
