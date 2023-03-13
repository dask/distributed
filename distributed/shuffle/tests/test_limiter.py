from __future__ import annotations

import asyncio

import pytest

from distributed.metrics import time
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import wait_for
from distributed.utils_test import gen_test


@gen_test()
async def test_limiter_basic():
    res = ResourceLimiter(5)

    assert isinstance(repr(res), str)
    res.increase(2)
    assert res.available() == 3
    res.increase(3)

    assert not res.available()
    # This is too much
    res.increase(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert not res.available()

    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert res.available() == 1
    await res.wait_for_available()

    res.increase(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(5)
    assert res.available() == 5

    with pytest.raises(RuntimeError, match="more"):
        await res.decrease(1)

    res.increase(10)
    with pytest.raises(asyncio.TimeoutError):
        await wait_for(res.wait_for_available(), 0.1)

    await res.decrease(3)
    assert not res.available()

    await res.decrease(5)
    assert res.available() == 3


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
    res = ResourceLimiter(1)

    assert res.time_blocked_avg == 0.0
    assert res.time_blocked_total == 0.0

    await res.wait_for_available()

    assert res.time_blocked_avg == 0.0
    assert res.time_blocked_total == 0.0

    res.increase(1)
    start = time()
    blocked_wait = asyncio.create_task(res.wait_for_available())

    await asyncio.sleep(0.05)

    assert not blocked_wait.done()

    await res.decrease(1)

    await blocked_wait
    stop = time()
    assert stop - start >= res.time_blocked_total > 0.0
    assert res.time_blocked_total > res.time_blocked_avg

    before_total = res.time_blocked_total
    before_avg = res.time_blocked_avg

    await res.wait_for_available()
    assert before_total == res.time_blocked_total
    assert before_avg > res.time_blocked_avg
