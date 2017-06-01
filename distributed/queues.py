from __future__ import print_function, division, absolute_import

from collections import deque, defaultdict
import logging
from time import sleep
import threading
import uuid
import warnings

from tornado import gen
import tornado.queues

from .client import Future, _get_global_client
from .core import CommClosedError
from .utils import tokey, log_errors, sync

logger = logging.getLogger(__name__)


class QueueExtension(object):
    """ An extension for the scheduler to manage queues

    This adds the following routes to the scheduler

    *  queue-create
    *  queue-release
    *  queue-put
    *  queue-get
    *  queue-size
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.queues = dict()
        self.refcount = defaultdict(lambda: 0)

        self.scheduler.handlers.update({'queue_create': self.create,
                                        'queue_release': self.release,
                                        'queue_put': self.put,
                                        'queue_get': self.get,
                                        'queue_qsize': self.qsize})

        self.scheduler.extensions['queues'] = self

    def create(self, stream=None, name=None, client=None, maxsize=0):
        if name not in self.queues:
            self.queues[name] = tornado.queues.Queue(maxsize=maxsize)
            self.refcount[name] = 1
        else:
            self.refcount[name] += 1

    def release(self, stream=None, name=None, client=None):
        self.refcount[name] -= 1
        if self.refcount[name] == 0:
            del self.refcount[name]
            futures = self.queues[name].queue
            del self.queues[name]
            self.scheduler.client_releases_keys(keys=[f.key for f in futures],
                                                client='queue-plugin')

    @gen.coroutine
    def put(self, stream=None, name=None, key=None, client=None, timeout=None):
        yield self.queues[name].put(key, timeout=timeout)
        self.scheduler.client_desires_keys(keys=[key], client='queue-plugin')

    @gen.coroutine
    def get(self, stream=None, name=None, client=None, timeout=None):
        key = yield self.queues[name].get(timeout=timeout)
        raise gen.Return(key)

    def qsize(self, stream=None, name=None, client=None):
        return self.queues[name].qsize()


class Queue(object):
    """ Distributed Queue

    This allows multiple clients to share futures between each other with a
    multi-producer/multi-consumer queue.  All metadata is sequentialized
    through the scheduler.

    Examples
    --------
    >>> from dask.distributed import Client, Queue  # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> queue = Queue('x')  # doctest: +SKIP
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> queue.put(future)  # doctest: +SKIP
    """
    def __init__(self, name=None, client=None, maxsize=0):
        self.client = client or _get_global_client()
        self.name = name or 'queue-' + uuid.uuid4().hex
        self._started = self.client.scheduler.queue_create(name=name,
                                                           maxsize=maxsize)
        if self.client.loop._thread_ident != threading.get_ident():
            sync(self.client.loop, gen.coroutine(lambda: self._started))

    def __await__(self):
        @gen.coroutine
        def _():
            yield self._started
            raise gen.Return(self)
        return _().__await__()

    @gen.coroutine
    def _put(self, future, timeout=None):
        yield self.client.scheduler.queue_put(key=tokey(future.key),
                                              timeout=timeout,
                                              name=self.name)

    @gen.coroutine
    def _get(self, timeout=None):
        key = yield self.client.scheduler.queue_get(timeout=timeout, name=self.name)
        future = Future(key, self.client)
        raise gen.Return(future)

    @gen.coroutine
    def _qsize(self):
        result = yield self.client.scheduler.queue_qsize(name=self.name)
        raise gen.Return(result)

    def _release(self):
        if self.client.status == 'running':  # TODO: can leave zombie futures
            self.client._send_to_scheduler({'op': 'queue_release',
                                            'name': self.name})

    def __del__(self):
        self._release()
