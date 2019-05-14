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

    def __init__(
        self, workers=None, scheduler=None, asynchronous=False, loop=None, worker=None
    ):
        if scheduler is None:
            try:
                from distributed.bokeh.scheduler import BokehScheduler
            except ImportError:
                services = {}
            else:
                services = {("bokeh", 8787): BokehScheduler}
            scheduler = {"cls": Scheduler, "options": {"services": services}}

        self.scheduler_spec = scheduler
        self.worker_spec = workers or {}
        self.new_spec = worker
        self.workers = {}
        self._i = 0
        self._asynchronous = asynchronous
        self._lock = asyncio.Lock()

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

        self.status = "starting"

    async def _correct_state(self):
        async with self._lock:
            pre = list(set(self.workers))
            to_close = set(self.workers) - set(self.worker_spec)
            if to_close:
                await self.scheduler.retire_workers(workers=list(to_close))
                await asyncio.wait([self.workers[w].close() for w in to_close])
            for name in to_close:
                del self.workers[name]

            to_open = set(self.worker_spec) - set(self.workers)
            workers = []
            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d["options"]
                if "name" not in opts:
                    opts = toolz.merge({"name": name}, opts, {"loop": self.loop})
                worker = cls(self.scheduler.address, **opts)
                workers.append(worker)
            if workers:
                await asyncio.wait(workers)
            self.workers.update(dict(zip(to_open, workers)))

    def __await__(self):
        async def _():
            await self._start()
            await self.scheduler
            await self._correct_state()
            if self.workers:
                await asyncio.wait(list(self.workers.values()))  # maybe there are more
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
        self.sync(self._correct_state)
        return self

    def __exit__(self, typ, value, traceback):
        self.sync(self.scheduler.close, close_workers=True)
        self._loop_runner.stop()

    def scale(self, n):
        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        while len(self.worker_spec) < n:
            while self._i in self.worker_spec:
                self._i += 1
            self.worker_spec[self._i] = self.new_spec

        self.loop.add_callback(self._correct_state)
