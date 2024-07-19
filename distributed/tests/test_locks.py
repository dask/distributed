from __future__ import annotations

import pickle
from datetime import timedelta
from time import sleep

import pytest

import dask

from distributed import Lock, get_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster


@gen_cluster(client=True, nthreads=[("", 8)] * 2)
async def test_lock(c, s, a, b):
    await c.set_metadata("locked", False)

    def f(x):
        client = get_client()
        with Lock("x"):
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.01)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    futures = c.map(f, range(20))
    await c.gather(futures)


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    lock = Lock("x")
    result = await lock.acquire()
    assert result is True

    lock2 = Lock("x")
    assert lock.id != lock2.id

    start = time()
    result = await lock2.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False
    await lock.release()


@gen_cluster(client=True)
async def test_acquires_with_zero_timeout(c, s, a, b):
    lock = Lock("x")
    await lock.acquire(timeout=0)
    assert await lock.locked()
    await lock.release()

    await lock.acquire(timeout="1s")
    await lock.release()
    await lock.acquire(timeout=timedelta(seconds=1))
    await lock.release()


@gen_cluster(client=True)
async def test_acquires_blocking(c, s, a, b):
    lock = Lock("x")
    await lock.acquire(blocking=False)
    assert await lock.locked()
    await lock.release()
    assert not await lock.locked()

    with pytest.raises(ValueError):
        lock.acquire(blocking=False, timeout=0.1)


def test_timeout_sync(client):
    with Lock("x") as lock:
        assert Lock("x").acquire(timeout=0.1) is False


@gen_cluster(client=True)
async def test_errors(c, s, a, b):
    lock = Lock("x")
    with pytest.raises(RuntimeError):
        await lock.release()


def test_lock_sync(client):
    def f(x):
        with Lock("x") as lock:
            client = get_client()
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.01)
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


@gen_cluster(client=True)
async def test_serializable_no_ctx(c, s, a, b):
    def f(x, lock=None):
        lock.acquire()
        try:
            assert lock.name == "x"
            return x + 1
        finally:
            lock.release()

    lock = Lock("x")
    futures = c.map(f, range(10), lock=lock)
    await c.gather(futures)

    lock2 = pickle.loads(pickle.dumps(lock))
    assert lock2.name == lock.name


@gen_cluster(client=True, nthreads=[])
async def test_locks(c, s):
    async with Lock("x") as l1:
        l2 = Lock("x")
        assert await l2.acquire(timeout=0.01) is False


@gen_cluster(client=True, nthreads=[])
async def test_locks_inf_lease_timeout(c, s):
    sem_ext = s.extensions["semaphores"]
    async with Lock("x"):
        assert sem_ext.lease_timeouts["x"]

    with dask.config.set({"distributed.scheduler.locks.lease-timeout": "inf"}):
        async with Lock("y"):
            assert sem_ext.lease_timeouts.get("y") is None
