from __future__ import annotations

import asyncio
import pickle
from functools import partial
from unittest import mock

import pytest

from distributed import Client, Nanny, Scheduler, get_worker
from distributed.core import error_message
from distributed.diagnostics import SchedulerPlugin
from distributed.metrics import time
from distributed.utils_test import captured_logger, gen_cluster


@gen_cluster(nthreads=[])
async def test_log_event(s):
    before = time()
    s.log_event("foo", {"action": "test", "value": 1})
    after = time()
    assert len(s.get_events("foo")) == 1
    timestamp, event = s.get_events("foo")[0]
    assert before <= timestamp <= after
    assert event == {"action": "test", "value": 1}


@gen_cluster(nthreads=[])
async def test_log_events(s):
    s.log_event("foo", {"action": "test", "value": 1})
    s.log_event(["foo", "bar"], {"action": "test", "value": 2})

    actual = [event for _, event in s.get_events("foo")]
    assert actual == [{"action": "test", "value": 1}, {"action": "test", "value": 2}]

    actual = [event for _, event in s.get_events("bar")]
    assert actual == [{"action": "test", "value": 2}]

    actual = {
        topic: [event for _, event in events]
        for topic, events in s.get_events().items()
    }
    assert actual == {
        "foo": [{"action": "test", "value": 1}, {"action": "test", "value": 2}],
        "bar": [{"action": "test", "value": 2}],
    }


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_log_event_e2e(c, s, a):
    # Log an event from inside a task
    def foo():
        get_worker().log_event("topic1", {"foo": "bar"})

    assert not await c.get_events("topic1")
    await c.submit(foo)
    events = await c.get_events("topic1")
    assert len(events) == 1
    assert events[0][1] == {"foo": "bar", "worker": a.address}

    # Log an event while on the scheduler
    def log_scheduler(dask_scheduler):
        dask_scheduler.log_event("topic2", {"woo": "hoo"})

    await c.run_on_scheduler(log_scheduler)
    events = await c.get_events("topic2")
    assert len(events) == 1
    assert events[0][1] == {"woo": "hoo"}

    # Log an event from the client process
    await c.log_event("topic2", ("alice", "bob"))
    events = await c.get_events("topic2")
    assert len(events) == 2
    assert events[1][1] == ("alice", "bob")


@gen_cluster(client=True, nthreads=[])
async def test_log_event_multiple_clients(c, s):
    async with Client(s.address, asynchronous=True) as c2, Client(
        s.address, asynchronous=True
    ) as c3:
        received_events = []

        def get_event_handler(handler_id):
            def handler(event):
                received_events.append((handler_id, event))

            return handler

        c.subscribe_topic("test-topic", get_event_handler(1))
        c2.subscribe_topic("test-topic", get_event_handler(2))

        while len(s._broker._topics["test-topic"].subscribers) != 2:
            await asyncio.sleep(0.01)

        with captured_logger("distributed.client") as logger:
            await c.log_event("test-topic", {})

        while len(received_events) < 2:
            await asyncio.sleep(0.01)

        assert len(received_events) == 2
        assert {handler_id for handler_id, _ in received_events} == {1, 2}
        assert "ValueError" not in logger.getvalue()


@gen_cluster(client=True, config={"distributed.admin.low-level-log-length": 3})
async def test_configurable_events_log_length(c, s, a, b):
    s.log_event("test", "dummy message 1")
    assert len(s.get_events("test")) == 1
    s.log_event("test", "dummy message 2")
    s.log_event("test", "dummy message 3")
    assert len(s.get_events("test")) == 3
    assert s._broker._topics["test"].count == 3

    # adding a fourth message will drop the first one and length stays at 3
    s.log_event("test", "dummy message 4")
    assert len(s.get_events("test")) == 3
    assert s._broker._topics["test"].count == 4
    events = [event for _, event in s.get_events("test").items()]
    assert events == ["dummy message 2", "dummy message 3", "dummy message 4"]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_events_subscribe_topic(c, s, a):
    log = []

    def user_event_handler(event):
        log.append(event)

    c.subscribe_topic("test-topic", user_event_handler)

    while not s._broker._topics["test-topic"].subscribers:
        await asyncio.sleep(0.01)

    a.log_event("test-topic", {"important": "event"})

    while len(log) != 1:
        await asyncio.sleep(0.01)

    time_, msg = log[0]
    assert isinstance(time_, float)
    assert msg == {"important": "event", "worker": a.address}

    c.unsubscribe_topic("test-topic")

    while s._broker._topics["test-topic"].subscribers:
        await asyncio.sleep(0.01)

    a.log_event("test-topic", {"forget": "me"})

    while len(s.get_events("test-topic")) == 1:
        await asyncio.sleep(0.01)

    assert len(log) == 1

    async def async_user_event_handler(event):
        log.append(event)
        await asyncio.sleep(0)

    c.subscribe_topic("test-topic", async_user_event_handler)

    while not s._broker._topics["test-topic"].subscribers:
        await asyncio.sleep(0.01)

    a.log_event("test-topic", {"async": "event"})

    while len(log) == 1:
        await asyncio.sleep(0.01)

    assert len(log) == 2
    time_, msg = log[1]
    assert isinstance(time_, float)
    assert msg == {"async": "event", "worker": a.address}

    # Even though the middle event was not subscribed to, the scheduler still
    # knows about all and we can retrieve them
    all_events = await c.get_events(topic="test-topic")
    assert len(all_events) == 3


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_events_subscribe_topic_cancelled(c, s, a):
    event_handler_started = asyncio.Event()
    exc_info = None

    async def user_event_handler(event):
        nonlocal exc_info
        c.unsubscribe_topic("test-topic")
        event_handler_started.set()
        with pytest.raises(asyncio.CancelledError) as exc_info:
            await asyncio.sleep(0.5)

    c.subscribe_topic("test-topic", user_event_handler)
    while not s._broker._topics["test-topic"].subscribers:
        await asyncio.sleep(0.01)

    a.log_event("test-topic", {})
    await event_handler_started.wait()
    await c._close(fast=True)
    assert exc_info is not None


@gen_cluster(nthreads=[])
async def test_topic_subscribe_unsubscribe(s):
    async with Client(s.address, asynchronous=True) as c1, Client(
        s.address, asynchronous=True
    ) as c2:

        def event_handler(recorded_events, event):
            _, msg = event
            recorded_events.append(msg)

        c1_events = []
        c1.subscribe_topic("foo", partial(event_handler, c1_events))
        while not s._broker._topics["foo"].subscribers:
            await asyncio.sleep(0.01)
        s.log_event("foo", {"value": 1})

        c2_events = []
        c2.subscribe_topic("foo", partial(event_handler, c2_events))
        c2.subscribe_topic("bar", partial(event_handler, c2_events))

        while (
            not s._broker._topics["bar"].subscribers
            and len(s._broker._topics["foo"].subscribers) < 2
        ):
            await asyncio.sleep(0.01)

        s.log_event("foo", {"value": 2})
        s.log_event("bar", {"value": 3})

        c2.unsubscribe_topic("foo")

        while len(s._broker._topics["foo"].subscribers) > 1:
            await asyncio.sleep(0.01)

        s.log_event("foo", {"value": 4})
        s.log_event("bar", {"value": 5})

        c1.unsubscribe_topic("foo")

        while s._broker._topics["foo"].subscribers:
            await asyncio.sleep(0.01)

        s.log_event("foo", {"value": 6})
        s.log_event("bar", {"value": 7})

        c2.unsubscribe_topic("bar")

        while s._broker._topics["bar"].subscribers:
            await asyncio.sleep(0.01)

        s.log_event("bar", {"value": 8})

        assert c1_events == [{"value": 1}, {"value": 2}, {"value": 4}]
        assert c2_events == [{"value": 2}, {"value": 3}, {"value": 5}, {"value": 7}]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_events_all_servers_use_same_channel(c, s, a):
    """Ensure that logs from all server types (scheduler, worker, nanny)
    and the clients themselves arrive"""

    log = []

    def user_event_handler(event):
        log.append(event)

    c.subscribe_topic("test-topic", user_event_handler)

    while not s._broker._topics["test-topic"].subscribers:
        await asyncio.sleep(0.01)

    async with Nanny(s.address) as n:
        a.log_event("test-topic", "worker")
        n.log_event("test-topic", "nanny")
        s.log_event("test-topic", "scheduler")
        await c.log_event("test-topic", "client")

    while not len(log) == 4 == len(set(log)):
        await asyncio.sleep(0.1)


@gen_cluster(client=True, nthreads=[])
async def test_events_unsubscribe_raises_if_unknown(c, s):
    with pytest.raises(ValueError, match="No event handler known for topic unknown"):
        c.unsubscribe_topic("unknown")


@gen_cluster(client=True)
async def test_log_event_to_warn(c, s, a, b):
    def foo():
        get_worker().log_event(["foo", "warn"], "Hello!")

    with pytest.warns(UserWarning, match="Hello!"):
        await c.submit(foo)

    def no_message():
        # missing "message" key should log TypeError
        get_worker().log_event("warn", {})

    with captured_logger("distributed.client") as log:
        await c.submit(no_message)
        assert "TypeError" in log.getvalue()

    def no_category():
        # missing "category" defaults to `UserWarning`
        get_worker().log_event("warn", {"message": pickle.dumps("asdf")})

    with pytest.warns(UserWarning, match="asdf"):
        await c.submit(no_category)


@gen_cluster(client=True, nthreads=[])
async def test_log_event_msgpack(c, s, a, b):
    await c.log_event("test-topic", "foo")
    with pytest.raises(TypeError, match="msgpack"):

        class C:
            pass

        await c.log_event("test-topic", C())
    await c.log_event("test-topic", "bar")
    await c.log_event("test-topic", error_message(Exception()))

    # assertion reversed for mock.ANY.__eq__(Serialized())
    assert [
        "foo",
        "bar",
        {
            "status": "error",
            "exception": mock.ANY,
            "traceback": mock.ANY,
            "exception_text": "Exception()",
            "traceback_text": "",
        },
    ] == [msg[1] for msg in s.get_events("test-topic")]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_log_event_worker(c, s, a):
    def log_event(msg):
        w = get_worker()
        w.log_event("test-topic", msg)

    await c.submit(log_event, "foo")

    class C:
        pass

    with pytest.raises(TypeError, match="msgpack"):
        await c.submit(log_event, C())

    # Worker still works
    await c.submit(log_event, "bar")
    await c.submit(log_event, error_message(Exception()))

    # assertion reversed for mock.ANY.__eq__(Serialized())
    assert [
        "foo",
        "bar",
        {
            "status": "error",
            "exception": mock.ANY,
            "traceback": mock.ANY,
            "exception_text": "Exception()",
            "traceback_text": "",
            "worker": a.address,
        },
    ] == [msg[1] for msg in s.get_events("test-topic")]


@gen_cluster(client=True, nthreads=[])
async def test_log_event_nanny(c, s):
    async with Nanny(s.address) as n:
        n.log_event("test-topic1", "foo")

        class C:
            pass

        with pytest.raises(TypeError, match="msgpack"):
            n.log_event("test-topic2", C())
        n.log_event("test-topic3", "bar")
        n.log_event("test-topic4", error_message(Exception()))

        # Worker unaffected
        assert await c.submit(lambda x: x + 1, 1) == 2

    assert [msg[1] for msg in s.get_events("test-topic1")] == ["foo"]
    assert [msg[1] for msg in s.get_events("test-topic3")] == ["bar"]
    # assertion reversed for mock.ANY.__eq__(Serialized())
    assert [
        {
            "status": "error",
            "exception": mock.ANY,
            "traceback": mock.ANY,
            "exception_text": "Exception()",
            "traceback_text": "",
        },
    ] == [msg[1] for msg in s.get_events("test-topic4")]


@gen_cluster(client=True)
async def test_log_event_plugin(c, s, a, b):
    class EventPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            self.scheduler = scheduler
            self.scheduler._recorded_events = list()  # type: ignore

        def log_event(self, topic, msg):
            self.scheduler._recorded_events.append((topic, msg))

    await c.register_plugin(EventPlugin())

    def f():
        get_worker().log_event("foo", 123)

    await c.submit(f)

    assert ("foo", 123) in s._recorded_events


@gen_cluster(client=True)
async def test_log_event_plugin_multiple_topics(c, s, a, b):
    class EventPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            self.scheduler = scheduler
            self.scheduler._recorded_events = list()  # type: ignore

        def log_event(self, topic, msg):
            self.scheduler._recorded_events.append((topic, msg))

    await c.register_plugin(EventPlugin())

    def f():
        get_worker().log_event(["foo", "bar"], 123)

    await c.submit(f)

    assert ("foo", 123) in s._recorded_events
    assert ("bar", 123) in s._recorded_events
