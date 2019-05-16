import asyncio
import weakref

import toolz
from tornado import gen

from .cluster import Cluster
from ..utils import LoopRunner, silence_logging
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
        self,
        workers=None,
        scheduler=None,
        asynchronous=False,
        loop=None,
        worker=None,
        silence_logs=False,
    ):
        self._created = weakref.WeakSet()
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

        if silence_logs:
            self._old_logging_level = silence_logging(level=silence_logs)

        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        self.scheduler = self.scheduler_spec["cls"](
            loop=self.loop, **self.scheduler_spec["options"]
        )
        self.status = "created"

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

        self._correct_state_waiting = None

    async def _start(self):
        if self.status != "created":
            return
        self._lock = asyncio.Lock()
        self.status = "starting"
        self.scheduler = await self.scheduler

        self.status = "starting"

    def _correct_state(self):
        if self._correct_state_waiting:
            # If people call this frequently, we only want to run it once
            return self._correct_state_waiting
        else:
            task = asyncio.create_task(self._correct_state_internal())
            self._correct_state_waiting = task
            return task

    async def _correct_state_internal(self):
        async with self._lock:
            self._correct_state_waiting = None

            pre = list(set(self.workers))
            to_close = set(self.workers) - set(self.worker_spec)
            if to_close:
                await self.scheduler.retire_workers(workers=list(to_close))
                tasks = [self.workers[w].close() for w in to_close]
                for task in tasks:
                    await task
            for name in to_close:
                del self.workers[name]

            to_open = set(self.worker_spec) - set(self.workers)
            workers = []
            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d.get("options", {})
                if "name" not in opts:
                    opts = toolz.merge({"name": name}, opts, {"loop": self.loop})
                worker = cls(self.scheduler.address, **opts)
                self._created.add(worker)
                workers.append(worker)
            if workers:
                await asyncio.wait(workers)
                for w in workers:
                    w._cluster = weakref.ref(self)
                    if self.status == "running":
                        await w
            self.workers.update(dict(zip(to_open, workers)))

    def __await__(self):
        async def _():
            await self._start()
            await self.scheduler
            await self._correct_state()
            if self.workers:
                await asyncio.wait(list(self.workers.values()))  # maybe there are more
            await self._wait_for_workers()
            return self

        return _().__await__()

    async def _wait_for_workers(self):
        # TODO: this function needs to query scheduler and worker state
        # remotely without assuming that they are local
        while {d["name"] for d in self.scheduler.identity()["workers"].values()} != set(
            self.workers
        ):
            if (
                any(w.status == "closed" for w in self.workers.values())
                and self.scheduler.status == "running"
            ):
                raise gen.TimeoutError("Worker unexpectedly closed")
            await asyncio.sleep(0.1)

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()

    async def _close(self):
        if self.status.startswith("clos"):
            return
        self.status = "closing"
        async with self._lock:
            await self.scheduler.close(close_workers=True)
        self.scale(0)
        await self._correct_state()
        for w in self._created:
            assert w.status == "closed"

        if hasattr(self, "_old_logging_level"):
            silence_logging(self._old_logging_level)

        self.status = "closed"

    def close(self):
        return self.sync(self._close)

    def __enter__(self):
        self.sync(self._correct_state)
        self.sync(self._wait_for_workers)
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
        self._loop_runner.stop()

    def scale(self, n):
        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        while len(self.worker_spec) < n:
            while self._i in self.worker_spec:
                self._i += 1
            self.worker_spec[self._i] = self.new_spec

        self.loop.add_callback(self._correct_state)

    async def scale_down(self, workers):
        workers = set(workers)

        # TODO: this is linear cost.  We should be indexing by name or something
        to_close = [w for w in self.workers.values() if w.address in workers]
        for k, v in self.workers.items():
            if v.worker_address in workers:
                del self.worker_spec[k]

        await self

    scale_up = scale  # backwards compatibility

    def __repr__(self):
        return "SpecCluster(%r, workers=%d)" % (
            self.scheduler_address,
            len(self.workers),
        )
