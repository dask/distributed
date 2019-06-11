import asyncio
import atexit
import logging
import weakref

from tornado import gen

from .cluster import Cluster
from ..utils import LoopRunner, silence_logging, ignoring
from ..scheduler import Scheduler


logger = logging.getLogger(__name__)


class SpecCluster(Cluster):
    """ Cluster that requires a full specification of workers

    The SpecCluster class expects a full specification of the Scheduler and
    Workers to use.  It removes any handling of user inputs (like threads vs
    processes, number of cores, and so on) and any handling of cluster resource
    managers (like pods, jobs, and so on).  Instead, it expects this
    information to be passed in scheduler and worker specifications.  This
    class does handle all of the logic around asynchronously cleanly setting up
    and tearing things down at the right times.  Hopefully it can form a base
    for other more user-centric classes.

    Parameters
    ----------
    workers: dict
        A dictionary mapping names to worker classes and their specifications
        See example below
    scheduler: dict, optional
        A similar mapping for a scheduler
    worker: dict
        A specification of a single worker.
        This is used for any new workers that are created.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.

    Examples
    --------
    To create a SpecCluster you specify how to set up a Scheduler and Workers

    >>> from dask.distributed import Scheduler, Worker, Nanny
    >>> scheduler = {'cls': Scheduler, 'options': {"dashboard_address": ':8787'}}
    >>> workers = {
    ...     'my-worker': {"cls": Worker, "options": {"ncores": 1}},
    ...     'my-nanny': {"cls": Nanny, "options": {"ncores": 2}},
    ... }
    >>> cluster = SpecCluster(scheduler=scheduler, workers=workers)

    The worker spec is stored as the ``.worker_spec`` attribute

    >>> cluster.worker_spec
    {
       'my-worker': {"cls": Worker, "options": {"ncores": 1}},
       'my-nanny': {"cls": Nanny, "options": {"ncores": 2}},
    }

    While the instantiation of this spec is stored in the ``.workers``
    attribute

    >>> cluster.workers
    {
        'my-worker': <Worker ...>
        'my-nanny': <Nanny ...>
    }

    Should the spec change, we can await the cluster or call the
    ``._correct_state`` method to align the actual state to the specified
    state.

    We can also ``.scale(...)`` the cluster, which adds new workers of a given
    form.

    >>> worker = {'cls': Worker, 'options': {}}
    >>> cluster = SpecCluster(scheduler=scheduler, worker=worker)
    >>> cluster.worker_spec
    {}

    >>> cluster.scale(3)
    >>> cluster.worker_spec
    {
        0: {'cls': Worker, 'options': {}},
        1: {'cls': Worker, 'options': {}},
        2: {'cls': Worker, 'options': {}},
    }

    Note that above we are using the standard ``Worker`` and ``Nanny`` classes,
    however in practice other classes could be used that handle resource
    management like ``KubernetesPod`` or ``SLURMJob``.  The spec does not need
    to conform to the expectations of the standard Dask Worker class.  It just
    needs to be called with the provided options, support ``__await__`` and
    ``close`` methods and the ``worker_address`` property..

    Also note that uniformity of the specification is not required.  Other API
    could be added externally (in subclasses) that adds workers of different
    specifications into the same dictionary.
    """

    _instances = weakref.WeakSet()

    def __init__(
        self,
        workers=None,
        scheduler=None,
        worker=None,
        asynchronous=False,
        loop=None,
        silence_logs=False,
    ):
        self._created = weakref.WeakSet()
        if scheduler is None:
            try:
                from distributed.dashboard import BokehScheduler
            except ImportError:
                services = {}
            else:
                services = {("dashboard", 8787): BokehScheduler}
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
        self._instances.add(self)
        self._correct_state_waiting = None

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    def __repr__(self):
        return "SpecCluster(%r, workers=%d)" % (
            self.scheduler_address,
            len(self.workers),
        )

    def __await__(self):
        return self._await_().__await__()

    async def _await_(self):
        # Wait for startup
        await self._start()

        # Wait for state to correct, reporting errors
        start_errors = await self._correct_state(return_errors=True)
        if start_errors:
            self._on_startup_errors(start_errors)
        await self._wait_for_workers()

        return self

    async def __aenter__(self):
        return await self

    async def __aexit__(self, typ, value, traceback):
        with ignoring(RuntimeError):
            await self._close()

    def __enter__(self):
        self.sync(self._start)
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
        self._loop_runner.stop()

    def __del__(self):
        if not self.status.startswith("clos"):
            self.close()
        self._loop_runner.stop()

    async def _start_internal(self):
        # First wait for the scheduler to start
        await self.scheduler
        self.status = "running"

        # Wait for workers to correct
        await self._await_()

    async def _start(self):
        if self.status == "created":
            if not hasattr(self, "_start_future"):
                self.status = "starting"
                self._lock = asyncio.Lock()
                self._start_future = asyncio.ensure_future(self._start_internal())
            await self._start_future

        elif self.status == "starting":
            await self._start_future

        elif self.status == "running":
            pass

        elif self.status in ("closing", "closed"):
            raise ValueError("Cluster is closed")

    def _correct_state(self, return_errors=False):
        if self._correct_state_waiting:
            # If people call this frequently, we only want to run it once
            return self._correct_state_waiting
        else:
            task = asyncio.ensure_future(
                self._correct_state_internal(return_errors=return_errors)
            )
            self._correct_state_waiting = task
            return task

    async def _correct_state_internal(self, return_errors=False):
        async with self._lock:
            self._correct_state_waiting = None

            to_close = list(set(self.workers).difference(self.worker_spec))

            if to_close:
                await self.scheduler.retire_workers(workers=list(to_close))
                tasks = [self.workers[w].close() for w in to_close]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for w_key, res in zip(to_close, results):
                    if isinstance(res, Exception):
                        logger.warning(
                            "Worker %s errored during shutdown with exception: %s",
                            w_key,
                            res,
                        )
                for name in to_close:
                    del self.workers[name]

            to_open = list(set(self.worker_spec) - set(self.workers))
            workers = []
            start_errors = {}

            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d.get("options", {})
                if "name" not in opts:
                    opts = opts.copy()
                    opts["name"] = name
                worker = cls(self.scheduler.address, **opts)
                self._created.add(worker)
                workers.append(worker)

            if workers:
                results = await asyncio.gather(*workers, return_exceptions=True)
                for w_key, w, res in zip(to_open, workers, results):
                    if isinstance(res, Exception):
                        start_errors[w_key] = res
                    else:
                        w._cluster = weakref.ref(self)
                        self.workers[w_key] = w

            if not return_errors:
                for k, exc in sorted(start_errors.items()):
                    logger.warning(
                        "Worker %s failed to start with exception: %s", k, exc
                    )
            else:
                return start_errors

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

    async def _close(self):
        while self.status == "closing":
            await asyncio.sleep(0.1)
        if self.status == "closed":
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

    def close(self, timeout=None):
        with ignoring(RuntimeError):  # loop closed during process shutdown
            return self.sync(self._close, callback_timeout=timeout)

    def scale(self, n):
        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        # If we're already shutting down, don't add any workers
        if self.status not in ("closing", "closed"):
            while len(self.worker_spec) < n:
                k, spec = self.new_worker_spec()
                self.worker_spec[k] = spec

        self.loop.add_callback(self._correct_state)

    def new_worker_spec(self):
        """ Return name and spec for the next worker

        Returns
        -------
        name: identifier for worker
        spec: dict

        See Also
        --------
        scale
        """
        while self._i in self.worker_spec:
            self._i += 1

        return self._i, self.new_spec

    async def scale_down(self, workers):
        workers = set(workers)

        # TODO: this is linear cost.  We should be indexing by name or something
        to_close = [w for w in self.workers.values() if w.address in workers]
        for k, v in self.workers.items():
            if v.worker_address in workers:
                del self.worker_spec[k]

        await self

    scale_up = scale  # backwards compatibility

    def _on_startup_errors(self, errors):
        """Called when the initial workers all fail to start.

        Failures for workers that are dynamically scaled up later result are
        logged, as there's no where to raise them.
        """
        for k, exc in sorted(errors.items()):
            logger.warning("Worker %s failed to start with exception: %s", k, exc)
        raise RuntimeError(
            "%d workers failed to properly start, see logs for more information"
            % len(errors)
        )


@atexit.register
def close_clusters():
    for cluster in list(SpecCluster._instances):
        with ignoring(gen.TimeoutError):
            if cluster.status != "closed":
                cluster.close(timeout=10)
