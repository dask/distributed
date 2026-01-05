from __future__ import annotations

import asyncio
import pytest

from distributed import Condition, Lock, Client
from distributed.utils_test import gen_cluster, inc
from distributed.metrics import time


@gen_cluster(client=True)
async def test_condition_basic(c, s, a, b):
    """Basic condition wait and notify"""
    condition = Condition()

    results = []

    async def waiter():
        async with condition:
            results.append("waiting")
            await condition.wait()
            results.append("notified")

    # Start waiter task
    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)  # Let waiter acquire lock and start waiting

    assert results == ["waiting"]

    # Notify the waiter
    async with condition:
        await condition.notify()

    await waiter_task
    assert results == ["waiting", "notified"]


@gen_cluster(client=True)
async def test_condition_notify_one(c, s, a, b):
    """notify() wakes only one waiter"""
    condition = Condition()

    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    # Start multiple waiters
    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.1)

    # Notify once - only one should wake
    async with condition:
        await condition.notify()

    await asyncio.sleep(0.1)
    assert len(results) == 1

    # Notify again
    async with condition:
        await condition.notify()

    await asyncio.sleep(0.1)
    assert len(results) == 2

    # Cleanup remaining
    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert len(results) == 3


@gen_cluster(client=True)
async def test_condition_notify_n(c, s, a, b):
    """notify(n) wakes exactly n waiters"""
    condition = Condition()

    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    # Start 5 waiters
    tasks = [asyncio.create_task(waiter(i)) for i in range(5)]
    await asyncio.sleep(0.1)

    # Notify 2
    async with condition:
        await condition.notify(2)

    await asyncio.sleep(0.1)
    assert len(results) == 2

    # Notify 3 more
    async with condition:
        await condition.notify(3)

    await asyncio.gather(*tasks)
    assert len(results) == 5


@gen_cluster(client=True)
async def test_condition_notify_all(c, s, a, b):
    """notify_all() wakes all waiters"""
    condition = Condition()

    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    # Start multiple waiters
    tasks = [asyncio.create_task(waiter(i)) for i in range(10)]
    await asyncio.sleep(0.1)

    # Wake all at once
    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert len(results) == 10


@gen_cluster(client=True)
async def test_condition_wait_timeout(c, s, a, b):
    """wait() with timeout returns False if not notified"""
    condition = Condition()

    async with condition:
        start = time()
        result = await condition.wait(timeout=0.2)
        elapsed = time() - start

    assert result is False
    assert 0.15 < elapsed < 0.5


@gen_cluster(client=True)
async def test_condition_wait_timeout_then_notify(c, s, a, b):
    """wait() with timeout returns True if notified before timeout"""
    condition = Condition()

    async def notifier():
        await asyncio.sleep(0.1)
        async with condition:
            await condition.notify()

    notifier_task = asyncio.create_task(notifier())

    async with condition:
        result = await condition.wait(timeout=1.0)

    await notifier_task
    assert result is True


@gen_cluster(client=True)
async def test_condition_wait_for(c, s, a, b):
    """wait_for() waits until predicate is true"""
    condition = Condition()
    state = {"value": 0}

    async def incrementer():
        for i in range(5):
            await asyncio.sleep(0.05)
            async with condition:
                state["value"] += 1
                await condition.notify_all()

    inc_task = asyncio.create_task(incrementer())

    async with condition:
        result = await condition.wait_for(lambda: state["value"] >= 3)

    await inc_task
    assert result is True
    assert state["value"] >= 3


@gen_cluster(client=True)
async def test_condition_wait_for_timeout(c, s, a, b):
    """wait_for() returns predicate result on timeout"""
    condition = Condition()

    async with condition:
        result = await condition.wait_for(lambda: False, timeout=0.2)

    assert result is False


@gen_cluster(client=True)
async def test_condition_context_manager(c, s, a, b):
    """Condition works as async context manager"""
    condition = Condition()

    assert not condition.locked()

    async with condition:
        assert condition.locked()

    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_with_explicit_lock(c, s, a, b):
    """Condition can use an explicit Lock"""
    lock = Lock()
    condition = Condition(lock=lock)

    async with lock:
        assert condition.locked()

    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_multiple_notify_calls(c, s, a, b):
    """Multiple notify calls work correctly"""
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    # Start 3 waiters
    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.1)

    # Notify one at a time
    for _ in range(3):
        async with condition:
            await condition.notify()
        await asyncio.sleep(0.05)

    await asyncio.gather(*tasks)
    assert set(results) == {0, 1, 2}


@gen_cluster(client=True)
async def test_condition_notify_without_waiters(c, s, a, b):
    """notify() with no waiters is a no-op"""
    condition = Condition()

    async with condition:
        await condition.notify()
        await condition.notify_all()
        await condition.notify(5)

    # Should not raise or hang


@gen_cluster(client=True)
async def test_condition_producer_consumer(c, s, a, b):
    """Producer-consumer pattern with Condition"""
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

    prod_task = asyncio.create_task(producer())
    cons_task = asyncio.create_task(consumer())

    result = await cons_task
    await prod_task

    assert result == [0, 1, 2, 3, 4]


@gen_cluster(client=True)
async def test_condition_same_name(c, s, a, b):
    """Multiple Condition instances with same name share state"""
    cond1 = Condition(name="shared")
    cond2 = Condition(name="shared")

    result = []

    async def waiter():
        async with cond1:
            await cond1.wait()
            result.append("done")

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    # Notify via different instance
    async with cond2:
        await cond2.notify()

    await waiter_task
    assert result == ["done"]


@gen_cluster(client=True)
async def test_condition_repr(c, s, a, b):
    """Condition has readable repr"""
    condition = Condition(name="test-cond")
    assert "test-cond" in repr(condition)


@gen_cluster(client=True)
async def test_condition_waiter_cancelled(c, s, a, b):
    """Cancelled waiter properly cleans up"""
    condition = Condition()

    async def waiter():
        async with condition:
            await condition.wait()

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    # Cancel the waiter
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # Should not deadlock - lock should be released
    async with condition:
        await condition.notify()  # No-op since no waiters


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_condition_across_workers(c, s, a):
    """Condition works across different worker tasks"""
    condition = Condition(name="cross-worker")

    def worker_wait():
        import asyncio
        from distributed import Condition

        async def wait_task():
            cond = Condition(name="cross-worker")
            async with cond:
                await cond.wait()
                return "notified"

        return asyncio.run(wait_task())

    # Submit waiter to worker
    future = c.submit(worker_wait, pure=False)
    await asyncio.sleep(0.2)

    # Notify from client
    async with condition:
        await condition.notify()

    result = await future
    assert result == "notified"


@gen_cluster(client=True)
async def test_condition_locked_status(c, s, a, b):
    """locked() returns correct status"""
    condition = Condition()

    assert not condition.locked()

    await condition.acquire()
    assert condition.locked()

    await condition.release()
    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_reacquire_after_wait(c, s, a, b):
    """wait() reacquires lock after being notified"""
    condition = Condition()
    lock_states = []

    async def waiter():
        async with condition:
            lock_states.append(("before_wait", condition.locked()))
            await condition.wait()
            lock_states.append(("after_wait", condition.locked()))

    waiter_task = asyncio.create_task(waiter())
    await asyncio.sleep(0.1)

    async with condition:
        lock_states.append(("notifier", condition.locked()))
        await condition.notify()

    await waiter_task

    # Waiter should hold lock before and after wait
    assert lock_states[0] == ("before_wait", True)
    assert lock_states[1] == ("notifier", True)
    assert lock_states[2] == ("after_wait", True)


@gen_cluster(client=True)
async def test_condition_many_waiters(c, s, a, b):
    """Handle many waiters efficiently"""
    condition = Condition()
    results = []

    async def waiter(n):
        async with condition:
            await condition.wait()
            results.append(n)

    # Start many waiters
    tasks = [asyncio.create_task(waiter(i)) for i in range(50)]
    await asyncio.sleep(0.2)

    # Wake them all
    async with condition:
        await condition.notify_all()

    await asyncio.gather(*tasks)
    assert len(results) == 50
    assert set(results) == set(range(50))
