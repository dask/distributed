from __future__ import annotations

import asyncio
import pickle

import pytest

from distributed import Condition, Lock, get_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_condition_basic(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter():
        async with condition:
            results.append("waiting")
            woken = await condition.wait()
            assert woken is True
            results.append("notified")

    task = asyncio.ensure_future(waiter())
    while results != ["waiting"]:
        await asyncio.sleep(0.01)

    async with condition:
        await condition.notify()

    await task
    assert results == ["waiting", "notified"]


@gen_cluster(client=True)
async def test_condition_notify_one(c, s, a, b):
    condition = Condition()
    woken = []

    async def waiter(i):
        async with condition:
            await condition.wait()
            woken.append(i)

    tasks = [asyncio.ensure_future(waiter(i)) for i in range(3)]
    while len(s.extensions["conditions"].waiters.get(condition.name, {})) < 3:
        await asyncio.sleep(0.01)

    async with condition:
        await condition.notify()

    while not woken:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)
    assert len(woken) == 1

    async with condition:
        await condition.notify(2)
    await asyncio.gather(*tasks)
    assert sorted(woken) == [0, 1, 2]


@gen_cluster(client=True)
async def test_condition_notify_all(c, s, a, b):
    condition = Condition()
    woken = []

    async def waiter(i):
        async with condition:
            await condition.wait()
            woken.append(i)

    tasks = [asyncio.ensure_future(waiter(i)) for i in range(5)]
    while len(s.extensions["conditions"].waiters.get(condition.name, {})) < 5:
        await asyncio.sleep(0.01)

    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert sorted(woken) == list(range(5))


@gen_cluster(client=True)
async def test_condition_wait_timeout(c, s, a, b):
    condition = Condition()
    async with condition:
        start = time()
        woken = await condition.wait(timeout=0.1)
        stop = time()
    assert woken is False
    assert stop - start < 2


@gen_cluster(client=True)
async def test_condition_wait_for(c, s, a, b):
    condition = Condition()
    state = {"flag": False}

    async def setter():
        async with condition:
            state["flag"] = True
            await condition.notify()

    async def waiter():
        async with condition:
            result = await condition.wait_for(lambda: state["flag"])
            assert result is True

    wait_task = asyncio.ensure_future(waiter())
    await asyncio.sleep(0.1)
    await asyncio.ensure_future(setter())
    await wait_task


@gen_cluster(client=True)
async def test_condition_wait_for_timeout(c, s, a, b):
    condition = Condition()
    async with condition:
        start = time()
        result = await condition.wait_for(lambda: False, timeout=0.2)
        stop = time()
    assert result is False
    assert stop - start < 2


@gen_cluster(client=True)
async def test_condition_requires_lock(c, s, a, b):
    condition = Condition()
    with pytest.raises(RuntimeError):
        await condition.wait()


@gen_cluster(client=True)
async def test_condition_cleanup(c, s, a, b):
    condition = Condition()

    async def waiter():
        async with condition:
            await condition.wait()

    task = asyncio.ensure_future(waiter())
    while not s.extensions["conditions"].waiters.get(condition.name):
        await asyncio.sleep(0.01)

    async with condition:
        await condition.notify_all()
    await task

    assert not s.extensions["conditions"].waiters


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)] * 2)
async def test_condition_on_workers(c, s, a, b):
    def wait_for_it():
        client = get_client()
        condition = Condition("x", client=client)
        with condition:
            woken = condition.wait(timeout=5)
        return woken

    def set_it():
        client = get_client()
        condition = Condition("x", client=client)
        with condition:
            condition.notify_all()

    wait_futures = c.map(lambda _: wait_for_it(), range(2), pure=False)
    await asyncio.sleep(0.2)
    set_future = c.submit(set_it)
    await c.gather(set_future)

    results = await c.gather(wait_futures)
    assert all(results)


def test_condition_sync(client):
    condition = Condition("y")

    def waiter():
        with Condition("y") as cond:
            return cond.wait(timeout=5)

    future = client.submit(waiter, pure=False)

    import time as time_module

    time_module.sleep(0.2)
    with condition:
        condition.notify_all()

    assert future.result() is True


@gen_cluster(client=True)
async def test_condition_custom_lock(c, s, a, b):
    lock = Lock("my-lock")
    condition = Condition(lock=lock)
    assert condition._lock is lock

    with pytest.raises(TypeError):
        Condition(lock="not-a-lock")


@gen_cluster(client=True)
async def test_condition_pickle_roundtrip(c, s, a, b):
    condition = Condition("z")
    condition2 = pickle.loads(pickle.dumps(condition))
    assert condition2.name == condition.name

    async with condition:
        pass
    async with condition2:
        pass
