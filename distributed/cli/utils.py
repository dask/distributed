from __future__ import annotations

import asyncio
import logging
import signal
import warnings
from typing import Any

from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)


async def wait_for_signals(signals: list[signal.Signals]) -> None:
    """Wait for the passed signals by setting global signal handlers"""
    loop = asyncio.get_running_loop()
    event = asyncio.Event()

    old_handlers: dict[int, Any] = {}

    def handle_signal(signum, frame):
        # Restore old signal handler to allow for quicker exit
        # if the user sends the signal again.
        signal.signal(signum, old_handlers[signum])
        logger.info("Received signal %s (%d)", signal.Signals(signum).name, signum)
        loop.call_soon_threadsafe(event.set)

    for sig in signals:
        old_handlers[sig] = signal.signal(sig, handle_signal)

    await event.wait()


def install_signal_handlers(loop=None, cleanup=None):
    """
    Install global signal handlers to halt the Tornado IOLoop in case of
    a SIGINT or SIGTERM.  *cleanup* is an optional callback called,
    before the loop stops, with a single signal number argument.
    """
    warnings.warn(
        "install_signal_handlers is deprecated", DeprecationWarning, stacklevel=2
    )
    import signal

    loop = loop or IOLoop.current()

    old_handlers = {}

    def handle_signal(sig, frame):
        async def cleanup_and_stop():
            try:
                if cleanup is not None:
                    await cleanup(sig)
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)
        # Restore old signal handler to allow for a quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)
