from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import Collection
from functools import partial
from typing import TYPE_CHECKING, Any

from distributed.metrics import time

if TYPE_CHECKING:
    from distributed import Scheduler

logger = logging.getLogger(__name__)


class Topic:
    events: deque
    count: int
    subscribers: set

    def __init__(self, maxlen: int):
        self.events = deque(maxlen=maxlen)
        self.count = 0
        self.subscribers = set()

    def subscribe(self, subscriber: str) -> None:
        self.subscribers.add(subscriber)

    def unsubscribe(self, subscriber: str) -> None:
        self.subscribers.discard(subscriber)

    def publish(self, event: Any) -> None:
        self.events.append(event)
        self.count += 1

    def clear(self) -> None:
        self.events.clear()


class Broker:
    _scheduler: Scheduler
    _topics: defaultdict[str, Topic]

    def __init__(self, maxlen: int, scheduler: Scheduler) -> None:
        self._scheduler = scheduler
        self._topics = defaultdict(partial(Topic, maxlen=maxlen))

    def subscribe(self, topic: str, subscriber: str) -> None:
        self._topics[topic].subscribe(subscriber)

    def unsubscribe(self, topic: str, subscriber: str) -> None:
        self._topics[topic].unsubscribe(subscriber)

    def publish(self, topics: str | Collection[str], msg: Any) -> None:
        event = (time(), msg)
        if isinstance(topics, str):
            topics = [topics]
        for name in topics:
            topic = self._topics[name]
            topic.publish(event)
            self._send_to_subscribers(name, event)

            for plugin in list(self._scheduler.plugins.values()):
                try:
                    plugin.log_event(name, event)
                except Exception:
                    logger.info("Plugin failed with exception", exc_info=True)

    def clear(self, topic: str) -> None:
        if topic in self._topics:
            self._topics[topic].clear()

    def _send_to_subscribers(self, topic: str, event: Any) -> None:
        msg = {
            "op": "event",
            "topic": topic,
            "event": event,
        }
        client_msgs = {client: [msg] for client in self._topics[topic].subscribers}
        self._scheduler.send_all(client_msgs, worker_msgs={})

    def get_events(self, topic=None):
        if topic is not None:
            return tuple(self._topics[topic].events)
        else:
            return {name: tuple(topic.events) for name, topic in self._topics.items()}
