import asyncio

import pytest

from distributed import Condition
from distributed.metrics import time
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_condition_acquire_release(c, s, a, b):
    """Test basic lock acquire/release"""
    condition = Condition("test-lock")
    assert not condition.locked()
    await condition.acquire()
    assert condition.locked()
    await condition.release()
    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_context_manager(c, s, a, b):
    """Test context manager interface"""
    condition = Condition("test-context")
    assert not condition.locked()
    async with condition:
        assert condition.locked()
    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_wait_notify(c, s, a, b):
    """Test basic wait/notify"""
    condition = Condition("test-notify")
    results = []

    async def waiter():
        async with condition:
            results.append("waiting")
            await condition.wait()
            results.append("notified")

    async def notifier():
        await asyncio.sleep(0.2)
        async with condition:
            results.append("notifying")
            condition.notify()

    await asyncio.gather(waiter(), notifier())
    assert results == ["waiting", "notifying", "notified"]


@gen_cluster(client=True)
async def test_condition_notify_all(c, s, a, b):
    """Test notify_all wakes all waiters"""
    condition = Condition("test-notify-all")
    results = []

    async def waiter(i):
        async with condition:
            await condition.wait()
            results.append(i)

    async def notifier():
        await asyncio.sleep(0.2)
        async with condition:
            condition.notify_all()

    await asyncio.gather(waiter(1), waiter(2), waiter(3), notifier())
    assert sorted(results) == [1, 2, 3]


@gen_cluster(client=True)
async def test_condition_notify_n(c, s, a, b):
    """Test notify with specific count"""
    condition = Condition("test-notify-n")
    results = []

    async def waiter(i):
        async with condition:
            await condition.wait()
            results.append(i)

    async def notifier():
        await asyncio.sleep(0.2)
        async with condition:
            condition.notify(n=2)  # Wake only 2 waiters
        await asyncio.sleep(0.2)
        async with condition:
            condition.notify()  # Wake remaining waiter

    await asyncio.gather(waiter(1), waiter(2), waiter(3), notifier())
    assert sorted(results) == [1, 2, 3]


@gen_cluster(client=True)
async def test_condition_wait_timeout(c, s, a, b):
    """Test wait with timeout"""
    condition = Condition("test-timeout")

    start = time()
    async with condition:
        result = await condition.wait(timeout=0.5)
    elapsed = time() - start

    assert result is False
    assert 0.4 < elapsed < 0.7


@gen_cluster(client=True)
async def test_condition_wait_timeout_then_notify(c, s, a, b):
    """Test that timeout doesn't prevent subsequent notifications"""
    condition = Condition("test-timeout-notify")
    results = []

    async def waiter():
        async with condition:
            result = await condition.wait(timeout=0.2)
            results.append(f"timeout: {result}")
        async with condition:
            result = await condition.wait()
            results.append(f"notified: {result}")

    async def notifier():
        await asyncio.sleep(0.5)
        async with condition:
            condition.notify()

    await asyncio.gather(waiter(), notifier())
    assert results == ["timeout: False", "notified: True"]


@gen_cluster(client=True)
async def test_condition_error_without_lock(c, s, a, b):
    """Test errors when calling wait/notify without holding lock"""
    condition = Condition("test-error")

    with pytest.raises(RuntimeError, match="without holding the lock"):
        await condition.wait()

    with pytest.raises(RuntimeError, match="Cannot notify"):
        condition.notify()

    with pytest.raises(RuntimeError, match="Cannot notify"):
        condition.notify_all()


@gen_cluster(client=True)
async def test_condition_error_release_without_acquire(c, s, a, b):
    """Test error when releasing without acquiring"""
    condition = Condition("test-release-error")

    with pytest.raises(RuntimeError, match="Released too often"):
        await condition.release()


@gen_cluster(client=True)
async def test_condition_producer_consumer(c, s, a, b):
    """Test classic producer-consumer pattern"""
    condition = Condition("prod-cons")
    queue = []

    async def producer():
        for i in range(5):
            await asyncio.sleep(0.1)
            async with condition:
                queue.append(i)
                condition.notify()

    async def consumer():
        results = []
        for _ in range(5):
            async with condition:
                while not queue:
                    await condition.wait()
                results.append(queue.pop(0))
        return results

    prod_task = asyncio.create_task(producer())
    cons_task = asyncio.create_task(consumer())

    await prod_task
    results = await cons_task
    assert results == [0, 1, 2, 3, 4]


@gen_cluster(client=True)
async def test_condition_multiple_producers_consumers(c, s, a, b):
    """Test multiple producers and consumers"""
    condition = Condition("multi-prod-cons")
    queue = []

    async def producer(start):
        for i in range(start, start + 3):
            await asyncio.sleep(0.05)
            async with condition:
                queue.append(i)
                condition.notify()

    async def consumer():
        results = []
        for _ in range(3):
            async with condition:
                while not queue:
                    await condition.wait()
                results.append(queue.pop(0))
        return results

    results = await asyncio.gather(producer(0), producer(10), consumer(), consumer())
    # Last two results are from consumers
    consumed = results[2] + results[3]
    assert sorted(consumed) == [0, 1, 2, 10, 11, 12]


@gen_cluster(client=True)
async def test_condition_same_name_different_instances(c, s, a, b):
    """Test that multiple instances with same name share state"""
    name = "shared-condition"
    cond1 = Condition(name)
    cond2 = Condition(name)
    results = []

    async def waiter():
        async with cond1:
            results.append("waiting")
            await cond1.wait()
            results.append("notified")

    async def notifier():
        await asyncio.sleep(0.2)
        async with cond2:
            results.append("notifying")
            cond2.notify()

    await asyncio.gather(waiter(), notifier())
    assert results == ["waiting", "notifying", "notified"]


@gen_cluster(client=True)
async def test_condition_unique_names_independent(c, s, a, b):
    """Test conditions with different names are independent"""
    cond1 = Condition("cond-1")
    cond2 = Condition("cond-2")

    async with cond1:
        assert cond1.locked()
        assert not cond2.locked()

    async with cond2:
        assert not cond1.locked()
        assert cond2.locked()


@gen_cluster(client=True)
async def test_condition_barrier_pattern(c, s, a, b):
    """Test barrier synchronization pattern"""
    condition = Condition("barrier")
    arrived = []
    n_workers = 3

    async def worker(i):
        async with condition:
            arrived.append(i)
            if len(arrived) < n_workers:
                await condition.wait()
            else:
                condition.notify_all()
        return f"worker-{i}-done"

    results = await asyncio.gather(worker(0), worker(1), worker(2))
    assert sorted(results) == ["worker-0-done", "worker-1-done", "worker-2-done"]
    assert len(arrived) == 3


def test_condition_sync_interface(client):
    """Test synchronous interface via SyncMethodMixin"""
    condition = Condition("sync-test")
    results = []

    def worker():
        with condition:
            results.append("locked")
        results.append("released")

    worker()
    assert results == ["locked", "released"]


@gen_cluster(client=True)
async def test_condition_multiple_notify_calls(c, s, a, b):
    """Test multiple notify calls in sequence"""
    condition = Condition("multi-notify")
    results = []

    async def waiter(i):
        async with condition:
            await condition.wait()
            results.append(i)

    async def notifier():
        await asyncio.sleep(0.2)
        async with condition:
            condition.notify()
        await asyncio.sleep(0.1)
        async with condition:
            condition.notify()
        await asyncio.sleep(0.1)
        async with condition:
            condition.notify()

    await asyncio.gather(waiter(1), waiter(2), waiter(3), notifier())
    assert sorted(results) == [1, 2, 3]


@gen_cluster(client=True)
async def test_condition_predicate_loop(c, s, a, b):
    """Test typical predicate-based wait loop pattern"""
    condition = Condition("predicate")
    state = {"value": 0, "target": 5}

    async def waiter():
        async with condition:
            while state["value"] < state["target"]:
                await condition.wait()
        return state["value"]

    async def updater():
        for i in range(1, 6):
            await asyncio.sleep(0.1)
            async with condition:
                state["value"] = i
                condition.notify_all()

    result, _ = await asyncio.gather(waiter(), updater())
    assert result == 5


@gen_cluster(client=True)
async def test_condition_repr(c, s, a, b):
    """Test string representation"""
    condition = Condition("test-repr")
    assert "test-repr" in repr(condition)
    assert "Condition" in repr(condition)


@gen_cluster(client=True)
async def test_condition_reentrant_acquire(c, s, a, b):
    """Test that the same client can re-acquire the lock"""
    condition = Condition("reentrant")

    await condition.acquire()
    assert condition.locked()

    # Should succeed without blocking (reentrant)
    await condition.acquire()
    assert condition.locked()

    await condition.release()
    assert not condition.locked()


@gen_cluster(client=True)
async def test_condition_multiple_instances_share_client_id(c, s, a, b):
    """Test that multiple Condition instances in same client share client_id"""
    cond1 = Condition("test-1")
    cond2 = Condition("test-2")

    # Both should have same client ID (the client 'c')
    assert cond1._client_id == cond2._client_id == c.id
