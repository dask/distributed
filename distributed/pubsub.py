from collections import defaultdict, deque
import weakref

import tornado.locks
from tornado import gen

from .compatibility import finalize
from .core import CommClosedError
from .utils import sync
from .protocol.serialize import to_serialize


class PubSubSchedulerExtension(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.publishers = defaultdict(set)
        self.subscribers = defaultdict(set)
        self.client_subscribers = defaultdict(set)

        self.scheduler.handlers.update({
            'pubsub_add_publisher': self.add_publisher,
        })

        self.scheduler.stream_handlers.update({
            'pubsub-add-subscriber': self.add_subscriber,
            'pubsub-remove-publisher': self.remove_publisher,
            'pubsub-remove-subscriber': self.remove_subscriber,
            'pubsub-msg': self.handle_message,
        })

        self.scheduler.extensions['pubsub'] = self

    def add_publisher(self, comm=None, name=None, worker=None):
        self.publishers[name].add(worker)
        return {'subscribers': {addr: {} for addr in self.subscribers[name]},
                'publish-scheduler': name in self.client_subscribers and
                                     len(self.client_subscribers[name]) > 0}

    def add_subscriber(self, comm=None, name=None, worker=None, client=None):
        if worker:
            self.subscribers[name].add(worker)
            for pub in self.publishers[name]:
                self.scheduler.worker_send(pub, {'op': 'pubsub-add-subscriber',
                                                 'address': worker,
                                                 'name': name})
        elif client:
            for pub in self.publishers[name]:
                self.scheduler.worker_send(pub, {'op': 'pubsub-publish-scheduler',
                                                 'name': name,
                                                 'publish': True})
            self.client_subscribers[name].add(client)

    def remove_publisher(self, comm=None, name=None, worker=None):
        self.publishers[name].remove(worker)

        if not self.subscribers[name] and not self.publishers[name]:
            del self.subscribers[name]
            del self.publishers[name]

    def remove_subscriber(self, comm=None, name=None, worker=None, client=None):
        if worker:
            self.subscribers[name].remove(worker)
            for pub in self.publishers[name]:
                self.scheduler.worker_send(pub, {'op': 'pubsub-remove-subscriber',
                                                 'address': worker,
                                                 'name': name})
        elif client:
            self.client_subscribers[name].remove(client)
            if not self.client_subscribers[name]:
                del self.client_subscribers[name]
                for pub in self.publishers[name]:
                    self.scheduler.worker_send(pub, {'op': 'pubsub-publish-scheduler',
                                                     'name': name,
                                                     'publish': False})

        if not self.subscribers[name] and not self.publishers[name]:
            del self.subscribers[name]
            del self.publishers[name]

    def handle_message(self, name=None, msg=None, worker=None, client=None):
        for c in list(self.client_subscribers[name]):
            try:
                self.scheduler.client_comms[c].send({'op': 'pubsub-msg',
                                                     'name': name,
                                                     'msg': msg})
            except (KeyError, CommClosedError):
                self.remove_subscriber(name=name, client=c)

        if client:
            for sub in self.subscribers[name]:
                self.scheduler.worker_send(sub, {'op': 'pubsub-msg',
                                                 'name': name,
                                                 'msg': msg})


class PubSubWorkerExtension(object):
    def __init__(self, worker):
        self.worker = worker
        self.worker.stream_handlers.update({
            'pubsub-add-subscriber': self.add_subscriber,
            'pubsub-remove-subscriber': self.remove_subscriber,
            'pubsub-msg': self.handle_message,
            'pubsub-publish-scheduler': self.publish_scheduler,
        })

        self.subscribers = defaultdict(weakref.WeakSet)
        self.publishers = defaultdict(weakref.WeakSet)
        self.publish_to_scheduler = defaultdict(lambda: False)

        self.worker.extensions['pubsub'] = self  # circular reference

    def add_subscriber(self, name=None, address=None, **info):
        for pub in self.publishers[name]:
            pub.subscribers[address] = info

    def remove_subscriber(self, name=None, address=None):
        for pub in self.publishers[name]:
            del pub.subscribers[address]

    def publish_scheduler(self, name=None, publish=None):
        self.publish_to_scheduler[name] = publish

    def handle_message(self, name=None, msg=None):
        for sub in self.subscribers.get(name, []):
            sub._put(msg)

    def trigger_cleanup(self):
        self.worker.loop.add_callback(self.cleanup)

    def cleanup(self):
        for name, s in dict(self.subscribers).items():
            if not len(s):
                msg = {'op': 'pubsub-remove-subscriber',
                       'name': name}
                self.worker.batched_stream.send(msg)
                del self.subscribers[name]

        for name, p in dict(self.publishers).items():
            if not len(p):
                msg = {'op': 'pubsub-remove-publisher',
                       'name': name}
                self.worker.batched_stream.send(msg)
                del self.publishers[name]
                del self.publish_to_scheduler[name]


class PubSubClientExtension(object):
    def __init__(self, client):
        self.client = client
        self.client._stream_handlers.update({
            'pubsub-msg': self.handle_message
        })

        self.subscribers = defaultdict(weakref.WeakSet)
        self.client.extensions['pubsub'] = self  # TODO: circular reference

    def handle_message(self, name=None, msg=None):
        for sub in self.subscribers[name]:
            sub._put(msg)

        if not self.subscribers[name]:
            self.client.scheduler_comm.send({'op': 'pubsub-remove-subscribers',
                                             'name': name})

    def trigger_cleanup(self):
        self.client.loop.add_callback(self.cleanup)

    def cleanup(self):
        for name, s in self.subscribers.items():
            if not s:
                msg = {'op': 'pubsub-remove-subscriber',
                       'name': name}
                self.client.scheduler_comm.send(msg)


class Pub(object):
    def __init__(self, name, worker=None, client=None):
        if worker is None and client is None:
            from distributed import get_worker, get_client
            try:
                worker = get_worker()
            except Exception:
                client = get_client()

        self.subscribers = dict()
        self.worker = worker
        self.client = client
        assert client or worker
        if self.worker:
            self.scheduler = self.worker.scheduler
            self.loop = self.worker.loop
        elif self.client:
            self.scheduler = self.client.scheduler
            self.loop = self.client.loop

        self.name = name
        self._started = False
        self._buffer = []

        self.loop.add_callback(self._start)

        if self.worker:
            pubsub = self.worker.extensions['pubsub']
            self.loop.add_callback(pubsub.publishers[name].add, self)
            finalize(self, pubsub.trigger_cleanup)

    @gen.coroutine
    def _start(self):
        if self.worker:
            result = yield self.scheduler.pubsub_add_publisher(
                    name=self.name,
                    worker=self.worker.address
            )
            self.subscribers.update(result['subscribers'])
            pubsub = self.worker.extensions['pubsub']
            pubsub.publish_to_scheduler[self.name] = result['publish-scheduler']

        self._started = True

        for msg in self._buffer:
            self.put(msg)
        del self._buffer[:]

    def put(self, msg):
        if not self._started:
            self._buffer.append(msg)
            return

        data = {'op': 'pubsub-msg', 'name': self.name, 'msg': to_serialize(msg)}

        if self.worker:
            for sub in self.subscribers:
                self.worker.send_to_worker(sub, data)

            if self.worker.extensions['pubsub'].publish_to_scheduler[self.name]:
                self.worker.batched_stream.send(data)
        elif self.client:
            self.client.scheduler_comm.send(data)


class Sub(object):
    def __init__(self, name, worker=None, client=None):
        if worker is None and client is None:
            from distributed.worker import get_worker, get_client
            try:
                worker = get_worker()
            except Exception:
                client = get_client()

        self.worker = worker
        self.client = client
        if self.worker:
            self.loop = self.worker.loop
        elif self.client:
            self.loop = self.client.loop
        self.name = name
        self.buffer = deque()
        self.condition = tornado.locks.Condition()

        if self.worker:
            pubsub = self.worker.extensions['pubsub']
        elif self.client:
            pubsub = self.client.extensions['pubsub']
        self.loop.add_callback(pubsub.subscribers[name].add, self)

        msg = {'op': 'pubsub-add-subscriber', 'name': self.name}
        if self.worker:
            self.worker.batched_stream.send(msg)
        elif self.client:
            self.client.scheduler_comm.send(msg)

        finalize(self, pubsub.trigger_cleanup)

    @gen.coroutine
    def __anext__(self):
        while not self.buffer:
            yield self.condition.wait()

        raise gen.Return(self.buffer.popleft())

    _get = __anext__

    def get(self):
        if self.client:
            return self.client.sync(self._get)
        else:
            raise NotImplementedError()

    def __next__(self):
        if self.buffer:  # fastpath
            return self.buffer.popleft()
        else:
            return sync(self.loop, self.__anext__)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def _put(self, msg):
        self.buffer.append(msg)
        self.condition.notify()
