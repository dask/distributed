from __future__ import print_function, division, absolute_import

from functools import partial
import logging
import math
from threading import Thread
from time import sleep

from tornado import gen
from tornado.ioloop import IOLoop

from ..core import CommClosedError
from ..utils import sync, ignoring, All
from ..nanny import Nanny
from ..scheduler import Scheduler
from ..worker import Worker, _ncores

logger = logging.getLogger(__name__)


class LocalCluster(object):
    """ Create local Scheduler and Workers

    This creates a "cluster" of a scheduler and workers running on the local
    machine.

    Parameters
    ----------
    n_workers: int
        Number of workers to start
    threads_per_worker: int
        Number of threads per each worker
    nanny: boolean
        If true start the workers in separate processes managed by a nanny.
        If False keep the workers in the main calling process
    scheduler_port: int
        Port of the scheduler.  8786 by default, use 0 to choose a random port
    silence_logs: logging level
        Level of logs to print out to stdout.  ``logging.CRITICAL`` by default.
        Use a falsey value like False or None for no change.
    ip: string
        IP address on which the scheduler will listen, defaults to only localhost
    kwargs: dict
        Extra worker arguments, will be passed to the Worker constructor.

    Examples
    --------
    >>> c = LocalCluster()  # Create a local cluster with as many workers as cores  # doctest: +SKIP
    >>> c  # doctest: +SKIP
    LocalCluster("127.0.0.1:8786", workers=8, ncores=8)

    >>> c = Client(c)  # connect to local cluster  # doctest: +SKIP

    Add a new worker to the cluster
    >>> w = c.start_worker(ncores=2)  # doctest: +SKIP

    Shut down the extra worker
    >>> c.remove_worker(w)  # doctest: +SKIP

    Start a diagnostic web server and open a new browser tab
    >>> c.start_diagnostics_server(show=True)  # doctest: +SKIP
    """
    def __init__(self, n_workers=None, threads_per_worker=None, nanny=True,
                 loop=None, start=True, ip='127.0.0.1', scheduler_port=8786,
                 silence_logs=logging.CRITICAL, diagnostics_port=8787,
                 services={}, worker_services={}, **worker_kwargs):
        self.status = None
        self.nanny = nanny
        self.silence_logs = silence_logs
        if silence_logs:
            for l in ['distributed.scheduler',
                      'distributed.worker',
                      'distributed.core',
                      'distributed.nanny']:
                logging.getLogger(l).setLevel(silence_logs)
        if n_workers is None and threads_per_worker is None:
            if nanny:
                n_workers = _ncores
                threads_per_worker = 1
            else:
                n_workers = 1
                threads_per_worker = _ncores
        if n_workers is None and threads_per_worker is not None:
            n_workers = max(1, _ncores // threads_per_worker)
        if n_workers and threads_per_worker is None:
            # Overcommit threads per worker, rather than undercommit
            threads_per_worker = max(1, int(math.ceil(_ncores / n_workers)))

        self.loop = loop or IOLoop()
        if start and not self.loop._running:
            self._thread = Thread(target=self.loop.start)
            self._thread.daemon = True
            self._thread.start()
            while not self.loop._running:
                sleep(0.001)

        self.scheduler = Scheduler(loop=self.loop,
                                   services=services)
        self.scheduler_port = scheduler_port

        self.diagnostics_port = diagnostics_port
        self.diagnostics = None

        self.workers = []
        self.n_workers = n_workers
        self.threads_per_worker = threads_per_worker
        self.worker_services = worker_services
        self.worker_kwargs = worker_kwargs

        if start:
            sync(self.loop, self._start, ip)

    def __str__(self):
        return ('LocalCluster(%r, workers=%d, ncores=%d)' %
                (self.scheduler_address, len(self.workers),
                 sum(w.ncores for w in self.workers))
                )

    __repr__ = __str__

    @gen.coroutine
    def _start(self, ip='127.0.0.1'):
        """
        Start all cluster services.
        Wait on this if you passed `start=False` to the LocalCluster
        constructor.
        """
        if self.status == 'running':
            return
        self.scheduler.start((ip, self.scheduler_port))

        yield self._start_all_workers(
            self.n_workers, ncores=self.threads_per_worker,
            nanny=self.nanny, services=self.worker_services,
            **self.worker_kwargs)

        if self.diagnostics_port is not None:
            self.start_diagnostics_server(self.diagnostics_port,
                                          silence=self.silence_logs)

        self.status = 'running'

    @gen.coroutine
    def _start_all_workers(self, n_workers, **kwargs):
        yield [self._start_worker(**kwargs) for i in range(n_workers)]

    @gen.coroutine
    def _start_worker(self, port=0, nanny=None, **kwargs):
        if nanny is None:
            nanny = self.nanny
        if nanny:
            W = Nanny
            kwargs['quiet'] = True
        else:
            W = Worker
        try:
            w = W(self.scheduler.address, loop=self.loop,
                  silence_logs=self.silence_logs, **kwargs)
            yield w._start(port)
        except Exception as e:
            raise

        self.workers.append(w)

        while w.worker_address not in self.scheduler.worker_info:
            yield gen.sleep(0.01)

        raise gen.Return(w)

    def start_worker(self, port=0, ncores=0, **kwargs):
        """ Add a new worker to the running cluster

        Parameters
        ----------
        port: int (optional)
            Port on which to serve the worker, defaults to 0 or random
        ncores: int (optional)
            Number of threads to use.  Defaults to number of logical cores
        nanny: boolean
            If true start worker in separate process managed by a nanny

        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> c.start_worker(ncores=2)  # doctest: +SKIP

        Returns
        -------
        The created Worker or Nanny object.  Can be discarded.
        """
        return sync(self.loop, self._start_worker, port, ncores=ncores, **kwargs)

    @gen.coroutine
    def _stop_worker(self, w):
        yield w._close()
        self.workers.remove(w)

    def stop_worker(self, w):
        """ Stop a running worker

        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> w = c.start_worker(ncores=2)  # doctest: +SKIP
        >>> c.stop_worker(w)  # doctest: +SKIP
        """
        sync(self.loop, self._stop_worker, w)

    def start_diagnostics_server(self, port=8787, show=False,
            silence=logging.CRITICAL):
        """ Start Diagnostics Web Server

        This starts a web application to show diagnostics of what is happening
        on the cluster.  This application runs in a separate process and is
        generally available at the following location:

            http://localhost:8787/status/
        """
        try:
            from distributed.bokeh.application import BokehWebInterface
        except ImportError:
            logger.info("To start diagnostics web server please install Bokeh")
            return
        from ..http.scheduler import HTTPScheduler

        assert self.diagnostics is None
        if 'http' not in self.scheduler.services:
            self.scheduler.services['http'] = HTTPScheduler(self.scheduler,
                    io_loop=self.scheduler.loop)
            self.scheduler.services['http'].listen(0)
        self.diagnostics = BokehWebInterface(
                tcp_port=self.scheduler.port,
                http_port=self.scheduler.services['http'].port,
                bokeh_port=port, show=show,
                log_level=logging.getLevelName(silence).lower())

    @gen.coroutine
    def _close(self):
        with ignoring(gen.TimeoutError, CommClosedError, OSError):
            yield All([w._close() for w in self.workers])
        with ignoring(gen.TimeoutError, CommClosedError, OSError):
            yield self.scheduler.close(fast=True)
        del self.workers[:]
        if self.diagnostics:
            self.diagnostics.close()

    def close(self):
        """ Close the cluster """
        if self.status == 'running':
            self.status = 'closed'
            if self.loop._running:
                sync(self.loop, self._close)
            if hasattr(self, '_thread'):
                sync(self.loop, self.loop.stop)
                self._thread.join(timeout=1)
                self.loop.close()
                del self._thread

    @gen.coroutine
    def scale_up(self, n, **kwargs):
        """ Bring the total count of workers up to ``n``

        This function/coroutine should bring the total number of workers up to
        the number ``n``.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        yield [self._start_worker(**kwargs)
                for i in range(n - len(self.workers))]

    @gen.coroutine
    def scale_down(self, workers):
        """ Remove ``workers`` from the cluster

        Given a list of worker addresses this function should remove those
        workers from the cluster.  This may require tracking which jobs are
        associated to which worker address.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        workers = set(workers)
        yield [self._stop_worker(w)
                for w in self.workers
                if w.worker_address in workers]
        while workers & set(self.workers):
            yield gen.sleep(0.01)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def scheduler_address(self):
        try:
            return self.scheduler.address
        except ValueError:
            return '<unstarted>'
