from __future__ import annotations

import asyncio

import pytest

from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils_test import gen_test


@gen_test()
async def test_limiter_basic():
    res = ResourceLimiter(5)

    res.increase(2)
    assert res.available() == 3
    res.increase(3)

    assert not res.available()
    # This is too much
    res.increase(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert not res.available()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.decrease(1)
    assert res.available() == 1
    await res.wait_for_available()

    res.increase(1)
    assert not res.available()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.decrease(5)
    assert res.available() == 5

    with pytest.raises(RuntimeError, match="more"):
        await res.decrease(1)

    res.increase(10)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(res.wait_for_available(), 0.1)

    await res.decrease(3)
    assert not res.available()

    await res.decrease(5)
    assert res.available() == 3
