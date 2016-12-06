from collections import deque, defaultdict
from functools import partial
from time import sleep
import threading

from tornado.iostream import StreamClosedError

from .client import Future
from .utils import tokey, log_errors


class ChannelScheduler(object):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.deques = dict()
        self.counts = dict()
        self.clients = dict()

        handlers = {'topic-subscribe': self.subscribe,
                    'topic-unsubscribe': self.unsubscribe,
                    'topic-append': self.append}

        self.scheduler.compute_handlers.update(handlers)

    def subscribe(self, topic=None, client=None, maxlen=None):
        if topic not in self.deques:
            self.deques[topic] = deque(maxlen=maxlen)
            self.counts[topic] = 0
            self.clients[topic] = set()
        self.clients[topic].add(client)

        stream = self.scheduler.streams[client]
        for key in self.deques[topic]:
            stream.send({'op': 'topic-append',
                         'key': key,
                         'topic': topic})

    def unsubscribe(self, topic=None, client=None):
        self.clients[topic].remove(client)
        if self.clients[topic]:
            del self.deques[topic]
            del self.counts[topic]
            del self.clients[topic]

    def append(self, topic=None, key=None):
        if len(self.deques[topic]) == self.deques[topic].maxlen:
            self.scheduler.client_releases_keys(keys=[self.deques[topic][0]],
                                                client='streaming-%s' % topic)

        self.deques[topic].append(key)
        self.counts[topic] += 1
        self.report(topic, key)
        self.scheduler.update_graph(keys=[key], client='streaming-%s' % topic)

    def report(self, topic, key):
        for client in list(self.clients[topic]):
            try:
                stream = self.scheduler.streams[client]
                stream.send({'op': 'topic-append',
                             'key': key,
                             'topic': topic})
            except (KeyError, StreamClosedError):
                self.unsubscribe(topic, client)


class ChannelClient(object):
    def __init__(self, client):
        self.client = client
        self.channels = dict()
        self.client._channel_handler = self

        handlers = {'topic-append': self.receive_key}

        self.client._handlers.update(handlers)

        self.client.channel = self._create_channel  # monkey patch

    def _create_channel(self, topic, maxlen=None):
        if topic not in self.channels:
            c = Channel(self.client, topic, maxlen=maxlen)
            self.channels[topic] = c
            return c
        else:
            return self.channels[topic]

    def receive_key(self, topic=None, key=None):
        self.channels[topic]._receive_update(key)

    def add_channel(self, channel):
        if channel.topic not in self.channels:
            self.channels[channel.topic] = {channel}
        else:
            self.channels[channel.topic].add(channel)


class Channel(object):
    def __init__(self, client, topic, maxlen=None):
        self.client = client
        self.topic = topic
        self.futures = deque(maxlen=maxlen)
        self.count = 0
        self._pending = dict()
        self.client._channel_handler.add_channel(self)  # circular reference
        self._thread_condition = threading.Condition()

        self.client._send_to_scheduler({'op': 'topic-subscribe',
                                        'topic': topic,
                                        'maxlen': maxlen,
                                        'client': self.client.id})

    def append(self, future):
        self.client._send_to_scheduler({'op': 'topic-append',
                                        'topic': self.topic,
                                        'key': tokey(future.key)})
        self._pending[future.key] = future  # hold on to reference until ack

    def _receive_update(self, key=None):
        self.count += 1
        self.futures.append(Future(key, self.client))
        self.client._send_to_scheduler({'op': 'update-graph',
                                        'keys': [key],
                                        'client': self.client.id})
        if key in self._pending:
            del self._pending[key]

        with self._thread_condition:
            self._thread_condition.notify_all()

    def flush(self):
        while self._pending:
            sleep(0.01)

    def __del__(self):
        if not self.client.scheduler_stream.stream:
            self.client._send_to_scheduler({'op': 'topic-unsubscribe',
                                            'topic': self.topic,
                                            'client': self.client.id})

    def __iter__(self):
        with log_errors():
            last = self.count
            L = list(self.futures)
            for future in L:
                yield future

            while True:
                if self.count == last:
                    self._thread_condition.acquire()
                    self._thread_condition.wait()
                    self._thread_condition.release()

                n = min(self.count - last, len(self.futures))
                L = [self.futures[i] for i in range(-n, 0)]
                last = self.count
                for f in L:
                    yield f


    def __len__(self):
        return len(self.futures)

    def __str__(self):
        return "<Channel: %s - %d elements>" % (self.topic, len(self.futures))

    __repr__ = __str__
