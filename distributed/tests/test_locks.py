import pickle
from time import sleep
from datetime import timedelta

import pytest

from distributed import Lock, get_client, Client
from distributed.metrics import time
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_lock(c, s, a, b):
    await c.set_metadata("locked", False)

    def f(x):
        client = get_client()
        with Lock("x") as lock:
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    futures = c.map(f, range(20))
    await c.gather(futures)
    assert not s.extensions["locks"].events
    assert not s.extensions["locks"].ids


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    locks = s.extensions["locks"]
    lock = Lock("x")
    result = await lock.acquire()
    assert result is True
    assert locks.ids["x"] == lock.id

    lock2 = Lock("x")
    assert lock.id != lock2.id

    start = time()
    result = await lock2.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False
    assert locks.ids["x"] == lock.id
    assert not locks.events["x"]

    await lock.release()


@gen_cluster(client=True)
async def test_acquires_with_zero_timeout(c, s, a, b):
    lock = Lock("x")
    await lock.acquire(timeout=0)
    assert lock.locked()
    await lock.release()

    await lock.acquire(timeout="1s")
    await lock.release()
    await lock.acquire(timeout=timedelta(seconds=1))
    await lock.release()


@gen_cluster(client=True)
async def test_acquires_blocking(c, s, a, b):
    lock = Lock("x")
    await lock.acquire(blocking=False)
    assert lock.locked()
    await lock.release()
    assert not lock.locked()

    with pytest.raises(ValueError):
        lock.acquire(blocking=False, timeout=1)


def test_timeout_sync(client):
    with Lock("x") as lock:
        assert Lock("x").acquire(timeout=0.1) is False


@gen_cluster(client=True)
async def test_errors(c, s, a, b):
    lock = Lock("x")
    with pytest.raises(ValueError):
        await lock.release()


def test_lock_sync(client):
    def f(x):
        with Lock("x") as lock:
            client = get_client()
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    client.set_metadata("locked", False)
    futures = client.map(f, range(10))
    client.gather(futures)


@gen_cluster(client=True)
async def test_lock_types(c, s, a, b):
    for name in [1, ("a", 1), ["a", 1], b"123", "123"]:
        lock = Lock(name)
        assert lock.name == name

        await lock.acquire()
        await lock.release()

    assert not s.extensions["locks"].events


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    def f(x, lock=None):
        with lock:
            assert lock.name == "x"
            return x + 1

    lock = Lock("x")
    futures = c.map(f, range(10), lock=lock)
    await c.gather(futures)

    lock2 = pickle.loads(pickle.dumps(lock))
    assert lock2.name == lock.name
    assert lock2.client is lock.client


@pytest.mark.asyncio
async def test_locks():
    async with Client(processes=False, asynchronous=True) as c:
        assert c.asynchronous
        async with Lock("x"):
            lock2 = Lock("x")
            result = await lock2.acquire(timeout=0.1)
            assert result is False
