from __future__ import annotations

import logging
import sys

import tornado

logging_names: dict[str | int, int | str] = {}
logging_names.update(logging._levelToName)  # type: ignore
logging_names.update(logging._nameToLevel)  # type: ignore

LINUX = sys.platform == "linux"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform == "win32"


if sys.version_info >= (3, 9):
    from asyncio import to_thread
else:
    import contextvars
    import functools
    from asyncio import events

    async def to_thread(func, /, *args, **kwargs):
        """Asynchronously run function *func* in a separate thread.
        Any *args and **kwargs supplied for this function are directly passed
        to *func*. Also, the current :class:`contextvars.Context` is propagated,
        allowing context variables from the main thread to be accessed in the
        separate thread.

        Return a coroutine that can be awaited to get the eventual result of *func*.

        backport from
        https://github.com/python/cpython/blob/3f1ea163ea54513e00e0e9d5442fee1b639825cc/Lib/asyncio/threads.py#L12-L25
        """
        loop = events.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)


if sys.version_info >= (3, 9):
    from random import randbytes
else:
    from random import getrandbits

    def randbytes(size):
        return getrandbits(size * 8).to_bytes(size, "little")


if tornado.version_info >= (6, 2, 0, 0):
    from tornado.ioloop import PeriodicCallback
else:
    # Backport from https://github.com/tornadoweb/tornado/blob/a4f08a31a348445094d1efa17880ed5472db9f7d/tornado/ioloop.py#L838-L962
    # License https://github.com/tornadoweb/tornado/blob/v6.2.0/LICENSE
    # Includes minor modifications to source code to pass linting

    # This backport ensures that async callbacks are not overlapping if a run
    # takes longer than the interval
    import datetime
    import math
    import random
    from inspect import isawaitable
    from typing import Awaitable, Callable

    from tornado.ioloop import IOLoop
    from tornado.log import app_log

    class PeriodicCallback:  # type: ignore[no-redef]
        """Schedules the given callback to be called periodically.

        The callback is called every ``callback_time`` milliseconds when
        ``callback_time`` is a float. Note that the timeout is given in
        milliseconds, while most other time-related functions in Tornado use
        seconds. ``callback_time`` may alternatively be given as a
        `datetime.timedelta` object.

        If ``jitter`` is specified, each callback time will be randomly selected
        within a window of ``jitter * callback_time`` milliseconds.
        Jitter can be used to reduce alignment of events with similar periods.
        A jitter of 0.1 means allowing a 10% variation in callback time.
        The window is centered on ``callback_time`` so the total number of calls
        within a given interval should not be significantly affected by adding
        jitter.

        If the callback runs for longer than ``callback_time`` milliseconds,
        subsequent invocations will be skipped to get back on schedule.

        `start` must be called after the `PeriodicCallback` is created.

        .. versionchanged:: 5.0
        The ``io_loop`` argument (deprecated since version 4.1) has been removed.

        .. versionchanged:: 5.1
        The ``jitter`` argument is added.

        .. versionchanged:: 6.2
        If the ``callback`` argument is a coroutine, and a callback runs for
        longer than ``callback_time``, subsequent invocations will be skipped.
        Previously this was only true for regular functions, not coroutines,
        which were "fire-and-forget" for `PeriodicCallback`.

        The ``callback_time`` argument now accepts `datetime.timedelta` objects,
        in addition to the previous numeric milliseconds.
        """

        def __init__(
            self,
            callback: Callable[[], Awaitable | None],
            callback_time: datetime.timedelta | float,
            jitter: float = 0,
        ) -> None:
            self.callback = callback
            if isinstance(callback_time, datetime.timedelta):
                self.callback_time = callback_time / datetime.timedelta(milliseconds=1)
            else:
                if callback_time <= 0:
                    raise ValueError(
                        "Periodic callback must have a positive callback_time"
                    )
                self.callback_time = callback_time
            self.jitter = jitter
            self._running = False
            self._timeout = None  # type: object

        def start(self) -> None:
            """Starts the timer."""
            # Looking up the IOLoop here allows to first instantiate the
            # PeriodicCallback in another thread, then start it using
            # IOLoop.add_callback().
            self.io_loop = IOLoop.current()
            self._running = True
            self._next_timeout = self.io_loop.time()
            self._schedule_next()

        def stop(self) -> None:
            """Stops the timer."""
            self._running = False
            if self._timeout is not None:
                self.io_loop.remove_timeout(self._timeout)
                self._timeout = None

        def is_running(self) -> bool:
            """Returns ``True`` if this `.PeriodicCallback` has been started.

            .. versionadded:: 4.1
            """
            return self._running

        async def _run(self) -> None:
            if not self._running:
                return
            try:
                val = self.callback()
                if val is not None and isawaitable(val):
                    await val
            except Exception:
                app_log.error("Exception in callback %r", self.callback, exc_info=True)
            finally:
                self._schedule_next()

        def _schedule_next(self) -> None:
            if self._running:
                self._update_next(self.io_loop.time())
                self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)

        def _update_next(self, current_time: float) -> None:
            callback_time_sec = self.callback_time / 1000.0
            if self.jitter:
                # apply jitter fraction
                callback_time_sec *= 1 + (self.jitter * (random.random() - 0.5))
            if self._next_timeout <= current_time:
                # The period should be measured from the start of one call
                # to the start of the next. If one call takes too long,
                # skip cycles to get back to a multiple of the original
                # schedule.
                self._next_timeout += (
                    math.floor((current_time - self._next_timeout) / callback_time_sec)
                    + 1
                ) * callback_time_sec
            else:
                # If the clock moved backwards, ensure we advance the next
                # timeout instead of recomputing the same value again.
                # This may result in long gaps between callbacks if the
                # clock jumps backwards by a lot, but the far more common
                # scenario is a small NTP adjustment that should just be
                # ignored.
                #
                # Note that on some systems if time.time() runs slower
                # than time.monotonic() (most common on windows), we
                # effectively experience a small backwards time jump on
                # every iteration because PeriodicCallback uses
                # time.time() while asyncio schedules callbacks using
                # time.monotonic().
                # https://github.com/tornadoweb/tornado/issues/2333
                self._next_timeout += callback_time_sec
