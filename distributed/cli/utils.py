import asyncio
import logging
import signal

logger = logging.getLogger(__name__)


async def wait_for_signal():
    """Wait for SIGINT or SIGTERM by setting global signal handlers"""
    loop = asyncio.get_running_loop()
    event = asyncio.Event()

    old_handlers = {}

    def handle_signal(sig, frame):
        # Restore old signal handler to allow for quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])
        logger.info("Received signal %d", sig)
        loop.call_soon_threadsafe(event.set)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)

    await event.wait()
