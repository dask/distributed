from __future__ import annotations

import asyncio

import pytest

from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils_test import gen_test


@gen_test()
async def test_limiter_basic():
    res = ResourceLimiter(5)

    res.acquire(2)
    assert res.available() == 3
    res.acquire(3)

    assert not res.available()
    # This is too much
    res.acquire(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.release(1)
    assert not res.available()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.release(1)
    assert res.available() == 1
    await res.wait_for_available()

    res.acquire(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.release(5)
    assert res.available() == 5

    with pytest.raises(RuntimeError, match="more"):
        await res.release(1)

    res.acquire(10)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.release(3)
    assert not res.available()

    await res.release(5)
    assert res.available() == 3
