from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

logger = logging.getLogger(__name__)


async def wait_for_signals(signals: list[signal.Signals]):
    """Wait for the passed signals by setting global signal handlers"""
    loop = asyncio.get_running_loop()
    event = asyncio.Event()

    old_handlers: dict[int, Any] = {}

    def handle_signal(signum, frame):
        # Restore old signal handler to allow for quicker exit
        # if the user sends the signal again.
        signal.signal(signum, old_handlers[signum])
        logger.info("Received signal %d", signum)
        loop.call_soon_threadsafe(event.set)

    for sig in signals:
        old_handlers[sig] = signal.signal(sig, handle_signal)

    await event.wait()
