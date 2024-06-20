from __future__ import annotations

from distributed.metrics import time
from distributed.utils_test import gen_cluster


@gen_cluster(nthreads=[])
async def test_clean_initialization(s):
    assert not s.events
    assert not s.event_counts
    assert not s.event_subscriber


@gen_cluster(nthreads=[])
async def test_log_event(s):
    before = time()
    s.log_event("foo", {"action": "test", "value": 1})
    after = time()
    assert len(s.get_events("foo")) == 1
    assert s.event_counts["foo"] == 1
    timestamp, event = s.get_events("foo")[0]
    assert before <= timestamp <= after
    assert event == {"action": "test", "value": 1}


@gen_cluster(nthreads=[])
async def test_log_events(s):
    s.log_event("foo", {"action": "test", "value": 1})
    s.log_event(["foo", "bar"], {"action": "test", "value": 2})

    assert s.event_counts["foo"] == 2
    events = [event for _, event in s.get_events("foo")]
    assert events == [{"action": "test", "value": 1}, {"action": "test", "value": 2}]

    assert s.event_counts["bar"] == 1
    events = [event for _, event in s.get_events("bar")]
    assert events == [{"action": "test", "value": 2}]


@gen_cluster(nthreads=[])
async def test_get_events(s):
    s.log_event("foo", {"action": "test", "value": 1})
    s.log_event(["foo", "bar"], {"action": "test", "value": 2})

    actual = s.get_events("foo")
    assert actual == tuple(map(tuple, s.events["foo"]))

    actual = s.get_events()
    assert actual == {
        topic: tuple(map(tuple, events)) for topic, events in s.events.items()
    }


@gen_cluster(nthreads=[])
async def test_subscribe_unsubscribe(s):
    s.subscribe_topic("foo", "client-1")
    assert s.event_subscriber == {"foo": {"client-1"}}
    s.subscribe_topic("foo", "client-2")
    s.subscribe_topic("bar", "client-2")
    assert s.event_subscriber == {"foo": {"client-1", "client-2"}, "bar": {"client-2"}}

    s.unsubscribe_topic("foo", "client-2")
    assert s.event_subscriber == {"foo": {"client-1"}, "bar": {"client-2"}}
    s.unsubscribe_topic("foo", "client-1")
    assert s.event_subscriber == {"foo": set(), "bar": {"client-2"}}


@gen_cluster(client=True, config={"distributed.admin.low-level-log-length": 3})
async def test_configurable_events_log_length(c, s, a, b):
    s.log_event("test", "dummy message 1")
    assert len(s.events["test"]) == 1
    assert s.event_counts["test"] == 1
    s.log_event("test", "dummy message 2")
    s.log_event("test", "dummy message 3")
    assert len(s.events["test"]) == 3
    assert s.event_counts["test"] == 3

    # adding a fourth message will drop the first one and length stays at 3
    s.log_event("test", "dummy message 4")
    assert len(s.events["test"]) == 3
    assert s.event_counts["test"] == 4
    assert s.events["test"][0][1] == "dummy message 2"
    assert s.events["test"][1][1] == "dummy message 3"
    assert s.events["test"][2][1] == "dummy message 4"
