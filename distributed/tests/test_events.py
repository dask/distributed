import pickle

from distributed import Event
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_event(c, s, a, b):
    def wait_for_it_failing(x):
        event = Event("x")

        # Event is not set in another task so far
        assert not event.wait(timeout=0.05)
        assert not event.is_set()

    def wait_for_it_ok(x):
        event = Event("x")

        # Event is set in another task
        assert event.wait(timeout=0.5)
        assert event.is_set()

    def set_it():
        event = Event("x")
        event.set()

    def clear_it():
        event = Event("x")
        event.clear()

    wait_futures = c.map(wait_for_it_failing, range(10))
    await c.gather(wait_futures)

    set_future = c.submit(set_it)
    await c.gather(set_future)

    wait_futures = c.map(wait_for_it_ok, range(10))
    await c.gather(wait_futures)

    clear_future = c.submit(clear_it)
    await c.gather(clear_future)

    wait_futures = c.map(wait_for_it_ok, range(10))
    await c.gather(wait_futures)

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


def test_default_event(client):
    event = Event("x")
    assert not event.is_set()


def test_set_not_set(client):
    event = Event("x")

    event.clear()
    assert not event.is_set()

    event.set()
    assert event.is_set()

    event.set()
    assert event.is_set()

    event.clear()
    assert not event.is_set()


def test_timeout_sync(client):
    event = Event("x")
    assert Event("x").wait(timeout=0.1) is False


def test_event_sync(client):
    def wait_for_it_failing(x):
        event = Event("x")

        # Event is not set in another task so far
        assert not event.wait(timeout=0.05)
        assert not event.is_set()

    def wait_for_it_ok(x):
        event = Event("x")

        # Event is set in another task
        assert event.wait(timeout=0.5)
        assert event.is_set()

    def set_it():
        event = Event("x")
        event.set()

    wait_futures = client.map(wait_for_it_failing, range(10))
    client.gather(wait_futures)

    set_future = client.submit(set_it)
    client.gather(set_future)

    wait_futures = client.map(wait_for_it_ok, range(10))
    client.gather(wait_futures)


@gen_cluster(client=True)
async def test_event_types(c, s, a, b):
    for name in [1, ("a", 1), ["a", 1], b"123", "123"]:
        event = Event(name)
        assert event.name == name

        await event.set()
        await event.clear()
        result = await event.is_set()
        assert not result

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    def f(x, event=None):
        assert event.name == "x"
        return x + 1

    event = Event("x")
    futures = c.map(f, range(10), event=event)
    await c.gather(futures)

    event2 = pickle.loads(pickle.dumps(event))
    assert event2.name == event.name
    assert event2.client is event.client


@gen_cluster(client=True)
async def test_two_events(c, s, a, b):
    def event_not_set(event_name):
        assert not Event(event_name).wait(timeout=0.05)

    def event_is_set(event_name):
        assert Event(event_name).wait(timeout=0.5)

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    await Event("first_event").set()

    await c.gather(c.submit(event_is_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    await Event("first_event").clear()
    await Event("second_event").set()

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_is_set, "second_event"))

    await Event("first_event").clear()
    await Event("second_event").clear()

    await c.gather(c.submit(event_not_set, "first_event"))
    await c.gather(c.submit(event_not_set, "second_event"))

    assert not s.extensions["events"]._events
    assert not s.extensions["events"]._waiter_count
