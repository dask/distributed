from __future__ import annotations

import asyncio

import pytest

from distributed.metrics import time
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import wait_for
from distributed.utils_test import captured_context_meter, gen_test


@gen_test()
async def test_limiter_basic():
    res = ResourceLimiter(5)

    assert isinstance(repr(res), str)
    res.increase(2)
    assert res.available == 3
    res.increase(3)

    assert not res.available
    # This is too much
    res.increase(1)
    assert not res.available
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert not res.available

    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert res.available == 1
    await res.wait_for_available()

    res.increase(1)
    assert not res.available
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(5)
    assert res.available == 5

    with pytest.raises(RuntimeError, match="more"):
        await res.decrease(1)

    res.increase(10)
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(3)
    assert not res.available

    await res.decrease(5)
    assert res.available == 3


@gen_test()
async def test_unlimited_limiter():
    res = ResourceLimiter(None)
    with captured_context_meter() as metrics:
        assert res.empty
        assert res.available is None
        assert not res.full

        res.increase(3)
        assert not res.empty
        assert res.available is None
        assert not res.full

        res.increase(2**40)
        assert not res.empty
        assert res.available is None
        assert not res.full

        await res.wait_for_available()
        assert not metrics  # Did not block


@gen_test()
async def test_limiter_concurrent_decrease_releases_waiter():
    res = ResourceLimiter(5)
    res.increase(5)

    wait_for_available = asyncio.create_task(res.wait_for_available())
    event = asyncio.Event()

    async def decrease():
        await event.wait()
        await res.decrease(5)

    decrease_buffer = asyncio.create_task(decrease())
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(asyncio.shield(wait_for_available), 0.1)

    event.set()
    await wait_for_available


@gen_test()
async def test_limiter_statistics():
    with captured_context_meter() as metrics:
        res = ResourceLimiter(1, "foo")
        await res.wait_for_available()
        assert metrics == {("foo", "count"): 1}

        res.increase(1)
        start = time()

        blocked_wait = asyncio.create_task(res.wait_for_available())
        await asyncio.sleep(0.1)
        assert not blocked_wait.done()
        await res.decrease(1)
        await blocked_wait

        stop = time()
        t_before = metrics["foo", "seconds"]
        assert 0 < t_before < stop - start
        assert metrics["foo", "count"] == 2

        await res.wait_for_available()
        assert metrics == {("foo", "count"): 3, ("foo", "seconds"): t_before}


@gen_test()
async def test_limiter_no_limit_no_statistics():
    with captured_context_meter() as metrics:
        res = ResourceLimiter(None)
        await res.wait_for_available()
        assert not metrics
