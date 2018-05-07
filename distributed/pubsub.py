from collections import defaultdict, deque
import weakref

import tornado.locks
from tornado import gen

from .compatibility import finalize
from .utils import sync
from .protocol.serialize import to_serialize


class PubSubSchedulerExtension(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.publishers = defaultdict(set)
        self.subscribers = defaultdict(set)

        self.scheduler.handlers.update({
            'pubsub_add_publisher': self.add_publisher,
        })

        self.scheduler.stream_handlers.update({
            'pubsub-add-subscriber': self.add_subscriber,
            'pubsub-remove-publisher': self.remove_publisher,
            'pubsub-remove-subscriber': self.remove_subscriber,
        })

        self.scheduler.extensions['pubsub'] = self

    def add_publisher(self, comm=None, name=None, address=None, worker=None):
        self.publishers[name].add(address)
        return {'subscribers': {addr: {} for addr in self.subscribers[name]}}

    def add_subscriber(self, comm=None, name=None, address=None, worker=None):
        self.subscribers[name].add(address)
        for pub in self.publishers[name]:
            self.scheduler.worker_send(pub, {'op': 'pubsub-add-subscriber',
                                             'address': address,
                                             'name': name})

    def remove_publisher(self, comm=None, name=None, address=None, worker=None):
        self.publishers[name].remove(address)

        if not self.subscribers[name] and not self.publishers[name]:
            del self.subscribers[name]
            del self.publishers[name]

    def remove_subscriber(self, comm=None, name=None, address=None, worker=None):
        self.subscribers[name].remove(address)
        for pub in self.publishers[name]:
            self.scheduler.worker_send(pub, {'op': 'pubsub-remove-subscriber',
                                             'address': address,
                                             'name': name})

        if not self.subscribers[name] and not self.publishers[name]:
            del self.subscribers[name]
            del self.publishers[name]


class PubSubWorkerExtension(object):
    def __init__(self, worker):
        self.worker = worker
        self.worker.stream_handlers.update({
            'pubsub-add-subscriber': self.add_subscriber,
            'pubsub-remove-subscriber': self.remove_subscriber,
            'pubsub-msg': self.handle_message,
        })

        self.subscribers = defaultdict(weakref.WeakSet)
        self.publishers = defaultdict(weakref.WeakSet)

        self.worker.extensions['pubsub'] = self  # circular reference

    def add_subscriber(self, name=None, address=None, **info):
        for pub in self.publishers[name]:
            pub.subscribers[address] = info

    def remove_subscriber(self, name=None, address=None):
        for pub in self.publishers[name]:
            del pub.subscribers[address]

    def handle_message(self, name=None, msg=None):
        for sub in self.subscribers.get(name, []):
            sub._put(msg)

    def cleanup(self):
        for name, s in self.subscribers.items():
            if not s:
                msg = {'op': 'pubsub-remove-subscriber',
                       'name': name,
                       'address': self.worker.address}
                self.worker.batched_stream.send(msg)


class Pub(object):
    def __init__(self, name, worker=None):
        from distributed.worker import get_worker

        self.subscribers = dict()
        self.worker = worker or get_worker()
        self.name = name
        self._started = False
        self._buffer = []

        self.worker.loop.add_callback(self._start)

    @gen.coroutine
    def _start(self):
        result = yield self.worker.scheduler.pubsub_add_publisher(
                address=self.worker.address,
                name=self.name
        )
        self.subscribers.update(result['subscribers'])
        self._started = True
        for msg in self._buffer:
            self.put(msg)
        del self._buffer[:]

    def put(self, msg):
        if not self._started:
            self._buffer.append(msg)

        for sub in self.subscribers:
            self.worker.send_to_worker(sub, {'op': 'pubsub-msg',
                                             'name': self.name,
                                             'msg': to_serialize(msg)})


class Sub(object):
    def __init__(self, name, worker=None):
        from distributed.worker import get_worker
        self.worker = worker or get_worker()
        self.name = name
        self.buffer = deque()
        self.condition = tornado.locks.Condition()

        pubsub = self.worker.extensions['pubsub']
        pubsub.subscribers[name].add(self)

        self.worker.batched_stream.send({'op': 'pubsub-add-subscriber',
                                         'name': self.name,
                                         'address': self.worker.address})

        finalize(self, pubsub.cleanup)

    @gen.coroutine
    def __anext__(self):
        while not self.buffer:
            yield self.condition.wait()

        raise gen.Return(self.buffer.popleft())

    def __next__(self):
        return sync(self.worker.loop, self.__anext__)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def _put(self, msg):
        self.buffer.append(msg)
        self.condition.notify()
