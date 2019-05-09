import asyncio

import toolz

from .cluster import Cluster
from ..utils import LoopRunner
from ..scheduler import Scheduler


class SpecCluster(Cluster):
    """ Cluster that requires a full specification of workers

    This attempts to handle much of the logistics of cleanly setting up and
    tearing down a scheduler and workers, without handling any of the logic
    around user inputs.  It should form the base of other cluster creation
    functions.

    Parameters
    ----------
    workers: dict
        A dictionary mapping names to worker classes and their specifications
        See example below
    Scheduler: dict, optional
        A similar mapping for a scheduler
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await

    Examples
    --------
    >>> spec = {
    ...     'my-worker': {"cls": Worker, "options": {"ncores": 1}},
    ...     'my-nanny': {"cls": Nanny, "options": {"ncores": 2}},
    ... }
    >>> cluster = SpecCluster(workers=spec)
    """

    def __init__(self, workers, scheduler=None, asynchronous=False, loop=None):
        if scheduler is None:
            try:
                from distributed.bokeh.scheduler import BokehScheduler
            except ImportError:
                services = {}
            else:
                services = {("bokeh", 8787): BokehScheduler}
            scheduler = {"cls": Scheduler, "options": {"services": services}}

        self.scheduler_spec = scheduler
        self.worker_spec = workers
        self.workers = {}
        self._asynchronous = asynchronous

        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self.scheduler = self.scheduler_spec["cls"](
            loop=self.loop, **self.scheduler_spec["options"]
        )
        self.status = "created"

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def _start(self):
        if self.status != "created":
            return
        self.status = "starting"
        self.scheduler = await self.scheduler

        self.workers = {}
        for name, d in self.worker_spec.items():
            cls, opts = d["cls"], d["options"]
            if "name" not in opts:
                opts = toolz.merge({"name": name}, opts, {"loop": self.loop})
            worker = cls(self.scheduler.address, **opts)

            self.workers[name] = worker

        self.workers = {k: (await v) for k, v in self.workers.items()}
        self.status = "starting"

    def __await__(self):
        async def _():
            await self._start()
            await self.scheduler
            await asyncio.wait(self.workers.values())  # maybe there are more
            return self

        return _().__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        self.status = "closing"
        await self.scheduler.close(close_workers=True)
        self.status = "closed"

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.sync(self.scheduler.close, close_workers=True)
        self._loop_runner.stop()
