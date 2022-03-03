from __future__ import annotations

import logging
import platform
import sys

logging_names: dict[str | int, int | str] = {}
logging_names.update(logging._levelToName)  # type: ignore
logging_names.update(logging._nameToLevel)  # type: ignore

PYPY = platform.python_implementation().lower() == "pypy"
LINUX = sys.platform == "linux"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform.startswith("win")

if sys.version_info >= (3, 9):
    from asyncio import to_thread
else:
    import asyncio
    import contextvars
    import functools

    async def to_thread(func, /, *args, **kwargs):
        """Asynchronously run function *func* in a separate thread.
        Any *args and **kwargs supplied for this function are directly passed
        to *func*. Also, the current :class:`contextvars.Context` is propagated,
        allowing context variables from the main thread to be accessed in the
        separate thread.
        Return a coroutine that can be awaited to get the eventual result of *func*.
        """
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)
