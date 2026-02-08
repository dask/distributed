from __future__ import annotations

import asyncio

import pytest

from distributed import Condition, Lock
from distributed.metrics import time
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_condition_basic(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter():
        async with condition:
            results.append("waiting")
            await condition.wait()
            results.append("notified")

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)
    assert results == ["waiting"]

    async with condition:
        await condition.notify()

    await task
    assert results == ["waiting", "notified"]


@gen_cluster(client=True)
async def test_condition_notify_one(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.1)

    async with condition:
        await condition.notify()
    await asyncio.sleep(0.1)
    assert len(results) == 1

    async with condition:
        await condition.notify()
    await asyncio.sleep(0.1)
    assert len(results) == 2

    async with condition:
        await condition.notify_all()
    await asyncio.gather(*tasks)
    assert len(results) == 3


@gen_cluster(client=True)
async def test_condition_notify_n(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(5)]
    await asyncio.sleep(0.1)

    async with condition:
        await condition.notify(2)
    await asyncio.sleep(0.1)
    assert len(results) == 2

    async with condition:
        await condition.notify(3)
    await asyncio.gather(*tasks)
    assert len(results) == 5


@gen_cluster(client=True)
async def test_condition_notify_all(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(10)]
    await asyncio.sleep(0.1)

    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert len(results) == 10


@gen_cluster(client=True)
async def test_condition_wait_timeout(c, s, a, b):
    condition = Condition()

    async with condition:
        start = time()
        result = await condition.wait(timeout=0.2)
        elapsed = time() - start

    assert result is False
    assert 0.15 < elapsed < 1.0


@gen_cluster(client=True)
async def test_condition_wait_timeout_then_notify(c, s, a, b):
    condition = Condition()

    async def notifier():
        await asyncio.sleep(0.1)
        async with condition:
            await condition.notify()

    task = asyncio.create_task(notifier())

    async with condition:
        result = await condition.wait(timeout=2.0)

    await task
    assert result is True


@gen_cluster(client=True)
async def test_condition_wait_for(c, s, a, b):
    condition = Condition()
    state = {"value": 0}

    async def incrementer():
        for _ in range(5):
            await asyncio.sleep(0.05)
            async with condition:
                state["value"] += 1
                await condition.notify_all()

    task = asyncio.create_task(incrementer())

    async with condition:
        result = await condition.wait_for(lambda: state["value"] >= 3)

    await task
    assert result is True
    assert state["value"] >= 3


@gen_cluster(client=True)
async def test_condition_wait_for_timeout(c, s, a, b):
    condition = Condition()

    async with condition:
        result = await condition.wait_for(lambda: False, timeout=0.2)

    assert result is False


@gen_cluster(client=True)
async def test_condition_wait_for_already_true(c, s, a, b):
    """wait_for returns immediately if predicate is already true."""
    condition = Condition()

    async with condition:
        result = await condition.wait_for(lambda: True)

    assert result is True


@gen_cluster(client=True)
async def test_condition_context_manager(c, s, a, b):
    condition = Condition()
    assert not condition.locked()

    async with condition:
        assert condition.locked()

    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_with_explicit_lock(c, s, a, b):
    lock = Lock()
    condition = Condition(lock=lock)

    async with lock:
        assert condition.locked()

    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_multiple_notify_calls(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.1)

    for _ in range(3):
        async with condition:
            await condition.notify()
        await asyncio.sleep(0.05)

    await asyncio.gather(*tasks)
    assert set(results) == {0, 1, 2}


@gen_cluster(client=True)
async def test_condition_notify_without_waiters(c, s, a, b):
    condition = Condition()

    async with condition:
        await condition.notify()
        await condition.notify_all()
        await condition.notify(5)


@gen_cluster(client=True)
async def test_condition_producer_consumer(c, s, a, b):
    condition = Condition()
    queue = []

    async def producer():
        for i in range(5):
            await asyncio.sleep(0.05)
            async with condition:
                queue.append(i)
                await condition.notify()

    async def consumer():
        items = []
        for _ in range(5):
            async with condition:
                await condition.wait_for(lambda: len(queue) > 0)
                items.append(queue.pop(0))
        return items

    prod = asyncio.create_task(producer())
    cons = asyncio.create_task(consumer())

    result = await cons
    await prod
    assert result == [0, 1, 2, 3, 4]


@gen_cluster(client=True)
async def test_condition_same_name(c, s, a, b):
    cond1 = Condition(name="shared")
    cond2 = Condition(name="shared")
    result = []

    async def waiter():
        async with cond1:
            await cond1.wait()
            result.append("done")

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    async with cond2:
        await cond2.notify()

    await task
    assert result == ["done"]


@gen_cluster(client=True)
async def test_condition_repr(c, s, a, b):
    condition = Condition(name="test-cond")
    assert "test-cond" in repr(condition)


@gen_cluster(client=True)
async def test_condition_waiter_cancelled(c, s, a, b):
    condition = Condition()

    async def waiter():
        async with condition:
            await condition.wait()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Lock should be released — verify no deadlock
    async with condition:
        await condition.notify()


@gen_cluster(client=True)
async def test_condition_locked_status(c, s, a, b):
    condition = Condition()
    assert not condition.locked()

    await condition.acquire()
    assert condition.locked()

    await condition.release()
    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_reacquire_after_wait(c, s, a, b):
    condition = Condition()
    lock_states = []

    async def waiter():
        async with condition:
            lock_states.append(("before_wait", condition.locked()))
            await condition.wait()
            lock_states.append(("after_wait", condition.locked()))

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    async with condition:
        lock_states.append(("notifier", condition.locked()))
        await condition.notify()

    await task

    assert lock_states[0] == ("before_wait", True)
    assert lock_states[1] == ("notifier", True)
    assert lock_states[2] == ("after_wait", True)


@gen_cluster(client=True)
async def test_condition_many_waiters(c, s, a, b):
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    tasks = [asyncio.create_task(waiter(i)) for i in range(50)]
    await asyncio.sleep(0.3)

    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert len(results) == 50
    assert set(results) == set(range(50))
