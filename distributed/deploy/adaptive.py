from __future__ import print_function, division, absolute_import

import logging
from ..utils import log_errors

from tornado import gen
from tornado.ioloop import PeriodicCallback

logger = logging.getLogger(__name__)


class Adaptive(object):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.

    Parameters
    ----------
    scheduler: distributed.Scheduler
    cluster: object
        Must have scale_up and scale_down methods/coroutines

    Examples
    --------
    >>> class MyCluster(object):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """
    '''
    def __init__(self, scheduler, cluster, interval=1000, startup_cost=1):
        self.scheduler = scheduler
        self.cluster = cluster
        self.startup_cost = startup_cost
        self._adapt_callback = PeriodicCallback(self._adapt, interval,
                                                self.scheduler.loop)
        self.scheduler.loop.add_callback(self._adapt_callback.start)
        self._adapting = False

    def should_scale_up(self):
        with log_errors():
            if self.scheduler.unrunnable and not self.scheduler.ncores:
                return True

            total_occupancy = sum(self.scheduler.occupancy.values())
            total_cores = sum(self.scheduler.ncores.values())

            if total_occupancy / (total_cores + 1e-9) > self.startup_cost * 2:
                return True

            limit_bytes = {w: self.scheduler.worker_info[w]['memory_limit']
                            for w in self.scheduler.worker_info}
            worker_bytes = self.scheduler.worker_bytes

            limit = sum(limit_bytes.values())
            total = sum(worker_bytes.values())

            if total > 0.6 * limit:
                return True

            return False

    @gen.coroutine
    def _retire_workers(self):
        with log_errors():
            workers = yield self.scheduler.retire_workers(remove=True,
                    close_workers=True)

            logger.info("Retiring workers %s", workers)
            f = self.cluster.scale_down(workers)
            if gen.is_future(f):
                yield f

    @gen.coroutine
    def _adapt(self):
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return

        self._adapting = True
        try:
            if self.should_scale_up():
                instances = max(1, len(self.scheduler.ncores) * 2)
                logger.info("Scaling up to %d workers", instances)
                f = self.cluster.scale_up(instances)
                if gen.is_future(f):
                    yield f

            yield self._retire_workers()
        finally:
            self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
