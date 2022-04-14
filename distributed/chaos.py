import asyncio
import random
import sys

from dask.utils import parse_timedelta

from distributed.diagnostics.plugin import WorkerPlugin


class KillWorker(WorkerPlugin):
    def __init__(self, delay: str | int | float = "100 s", mode: Literal["sys.exit", "graceful", "segfault"] = "sys.exit"):
        self.delay = parse_timedelta(delay)
        if mode not in ("sys.exit", "graceful", "segfault"):
            raise ValueError(
                f"Three modes supported, 'sys.exit', 'graceful', and 'segfault'. "
                f"got {mode!r}"
            )
        self.mode = mode

    async def setup(self, worker):
        self.worker = worker
        if self.mode == "graceful":
            f = self.graceful
        elif self.mode == "sys.exit":
            f = self.sys_exit
        elif self.mode == "segfault":
            f = self.segfault

        self.worker.loop.asyncio_loop.call_later(
            delay=random.expovariate(1 / self.delay),
            callback=f,
        )

    def graceful(self):
        asyncio.create_task(self.worker.close(report=False))

    def sys_exit(self):
        sys.exit(0)

    def segfault(self):
        """
        Magic, from https://gist.github.com/coolreader18/6dbe0be2ae2192e90e1a809f1624c694?permalink_comment_id=3874116#gistcomment-3874116
        """
        eval((lambda: 0).__code__.replace(co_consts=()))
