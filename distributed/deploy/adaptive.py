from __future__ import print_function, division, absolute_import

from collections import deque
import logging
import math

from tornado import gen

from ..metrics import time
from ..utils import log_errors, PeriodicCallback, parse_timedelta

logger = logging.getLogger(__name__)


class Adaptive(object):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.
    This class needs to be paired with a system that can create and destroy
    Dask workers using a cluster resource manager.  Typically it is built into
    already existing solutions, rather than used directly by users.
    It is most commonly used from the ``.adapt(...)`` method of various Dask
    cluster classes.

    Parameters
    ----------
    scheduler: distributed.Scheduler
    cluster: object
        Must have scale_up and scale_down methods/coroutines
    startup_cost : timedelta or str, default "1s"
        Estimate of the number of seconds for nnFactor representing how costly it is to start an additional worker.
        Affects quickly to adapt to high tasks per worker loads
    interval : timedelta or str, default "1000 ms"
        Milliseconds between checks
    wait_count: int, default 3
        Number of consecutive times that a worker should be suggested for
        removal before we remove it.
    scale_factor : int, default 2
        Factor to scale by when it's determined additional workers are needed
    target_duration: timedelta or str, default "5s"
        Amount of time we want a computation to take.
        This affects how aggressively we scale up.
    worker_key: Callable[WorkerState]
        Function to group workers together when scaling down
        See Scheduler.workers_to_close for more information
    minimum: int
        Minimum number of workers to keep around
    maximum: int
        Maximum number of workers to keep around
    **kwargs:
        Extra parameters to pass to Scheduler.workers_to_close

    Examples
    --------

    This is commonly used from existing Dask classes, like KubeCluster

    >>> from dask_kubernetes import KubeCluster
    >>> cluster = KubeCluster()
    >>> cluster.adapt(minimum=10, maximum=100)

    Alternatively you can use it from your own Cluster class by subclassing
    from Dask's Cluster superclass

    >>> from distributed.deploy import Cluster
    >>> class MyCluster(Cluster):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """

    >>> cluster = MyCluster()
    >>> cluster.adapt(minimum=10, maximum=100)

    Notes
    -----
    Subclasses can override :meth:`Adaptive.should_scale_up` and
    :meth:`Adaptive.workers_to_close` to control when the cluster should be
    resized. The default implementation checks if there are too many tasks
    per worker or too little memory available (see :meth:`Adaptive.needs_cpu`
    and :meth:`Adaptive.needs_memory`).

    :meth:`Adaptive.get_scale_up_kwargs` method controls the arguments passed to
    the cluster's ``scale_up`` method.
    '''

    def __init__(
        self,
        scheduler,
        cluster=None,
        interval="1s",
        startup_cost="1s",
        scale_factor=2,
        minimum=0,
        maximum=None,
        wait_count=3,
        target_duration="5s",
        worker_key=lambda x: x,
        **kwargs
    ):
        interval = parse_timedelta(interval, default="ms")
        self.worker_key = worker_key
        self.scheduler = scheduler
        self.cluster = cluster
        self.startup_cost = parse_timedelta(startup_cost, default="s")
        self.scale_factor = scale_factor
        if self.cluster:
            self._adapt_callback = PeriodicCallback(
                self._adapt, interval * 1000, io_loop=scheduler.loop
            )
            self.scheduler.loop.add_callback(self._adapt_callback.start)
        self._adapting = False
        self._workers_to_close_kwargs = kwargs
        self.minimum = minimum
        self.maximum = maximum
        self.log = deque(maxlen=1000)
        self.close_counts = {}
        self.wait_count = wait_count
        self.target_duration = parse_timedelta(target_duration)

        self.scheduler.handlers["adaptive_recommendations"] = self.recommendations

    def stop(self):
        if self.cluster:
            self._adapt_callback.stop()
            self._adapt_callback = None
            del self._adapt_callback

    def workers_to_close(self, **kwargs):
        """
        Determine which, if any, workers should potentially be removed from
        the cluster.

        Notes
        -----
        ``Adaptive.workers_to_close`` dispatches to Scheduler.workers_to_close(),
        but may be overridden in subclasses.

        Returns
        -------
        List of worker addresses to close, if any

        See Also
        --------
        Scheduler.workers_to_close
        """
        if len(self.scheduler.workers) <= self.minimum:
            return []

        kw = dict(self._workers_to_close_kwargs)
        kw.update(kwargs)

        if self.maximum is not None and len(self.scheduler.workers) > self.maximum:
            kw["n"] = len(self.scheduler.workers) - self.maximum

        L = self.scheduler.workers_to_close(**kw)
        if len(self.scheduler.workers) - len(L) < self.minimum:
            L = L[: len(self.scheduler.workers) - self.minimum]

        return L

    @gen.coroutine
    def _retire_workers(self, workers=None):
        if workers is None:
            workers = self.workers_to_close(key=self.worker_key, minimum=self.minimum)
        if not workers:
            raise gen.Return(workers)
        with log_errors():
            yield self.scheduler.retire_workers(
                workers=workers, remove=True, close_workers=True
            )

            logger.info("Retiring workers %s", workers)
            f = self.cluster.scale_down(workers)
            if hasattr(f, "__await__"):
                yield f

            raise gen.Return(workers)

    def get_scale_up_kwargs(self):
        """
        Get the arguments to be passed to ``self.cluster.scale_up``.

        Notes
        -----
        By default the desired number of total workers is returned (``n``).
        Subclasses should ensure that the return dictionary includes a key-
        value pair for ``n``, either by implementing it or by calling the
        parent's ``get_scale_up_kwargs``.

        See Also
        --------
        LocalCluster.scale_up
        """
        target = math.ceil(self.scheduler.total_occupancy / self.target_duration)
        instances = max(
            1, len(self.scheduler.workers) * self.scale_factor, target, self.minimum
        )

        if self.maximum:
            instances = min(self.maximum, instances)

        instances = int(instances)
        logger.info("Scaling up to %d workers", instances)
        return {"n": instances}

    def recommendations(self, comm=None):
        n = self.scheduler.adaptive_target(target_duration=self.target_duration)
        if self.maximum is not None:
            n = min(self.maximum, n)
        if self.minimum is not None:
            n = max(self.minimum, n)
        workers = set(self.workers_to_close(key=self.worker_key, minimum=self.minimum))
        try:
            current = len(self.cluster.worker_spec)
        except AttributeError:
            current = len(self.cluster.workers)
        if n > current and workers:
            logger.info("Attempting to scale up and scale down simultaneously.")
            self.close_counts.clear()
            return {
                "status": "error",
                "msg": "Trying to scale up and down simultaneously",
            }

        elif n > current:
            self.close_counts.clear()
            return {"status": "up", "n": n}

        elif workers:
            d = {}
            to_close = []
            for w, c in self.close_counts.items():
                if w in workers:
                    if c >= self.wait_count:
                        to_close.append(w)
                    else:
                        d[w] = c

            for w in workers:
                d[w] = d.get(w, 0) + 1

            self.close_counts = d

            if to_close:
                return {"status": "down", "workers": to_close}
        else:
            self.close_counts.clear()
            return None

    @gen.coroutine
    def _adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return

        self._adapting = True
        try:
            recommendations = self.recommendations()
            if not recommendations:
                return
            status = recommendations.pop("status")
            if status == "up":
                f = self.cluster.scale(**recommendations)
                self.log.append((time(), "up", recommendations))
                if hasattr(f, "__await__"):
                    yield f

            elif status == "down":
                self.log.append((time(), "down", recommendations["workers"]))
                workers = yield self._retire_workers(workers=recommendations["workers"])
        finally:
            self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
