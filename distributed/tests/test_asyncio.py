from __future__ import annotations

import asyncio

import pytest

from distributed._asyncio import RLock
from distributed.utils_test import gen_test


@gen_test()
async def test_rlock():
    l = RLock()

    # Owners are treated as reentrant as long as they compare as equal.
    # Make sure that the comparison uses '==' and not 'is'.
    x1 = ["x"]
    x2 = ["x"]
    assert x1 is not x2, "internalized objects"

    async with l(x1), l(x2):
        assert l._owner == ["x"]
        assert l._count == 2

        fut = asyncio.create_task(l.acquire("y"))
        await asyncio.sleep(0.01)
        assert not fut.done()

        with pytest.raises(RuntimeError):
            l.release("z")

    await fut
    l.release("y")

    with pytest.raises(RuntimeError):
        l.release("w")

    assert l._owner is None
    assert l._count == 0
    assert not l._lock.locked()


@gen_test()
async def test_rlock_nonreentrant():
    """Use the RLock with non-reentrant owners"""
    l = RLock()
    x = object()
    y = object()
    assert x != y

    async with l(x):
        assert l._owner is x
        fut = asyncio.create_task(l.acquire(y))
        await asyncio.sleep(0.01)
        assert not fut.done()
    await fut
    assert l._owner is y
    l.release(y)

    assert l._owner is None
    assert l._count == 0
    assert not l._lock.locked()


@gen_test()
async def test_rlock_none():
    """None is a valid owner"""
    l = RLock()

    async with l(None), l(None):
        assert l._owner is None
        assert l._count == 2
        assert l._lock.locked()

        fut = asyncio.create_task(l.acquire("x"))
        await asyncio.sleep(0.01)
        assert not fut.done()

    await fut
    assert l._owner == "x"
    l.release("x")

    assert l._owner is None
    assert l._count == 0
    assert not l._lock.locked()


@gen_test()
async def test_rlock_release_on_raise():
    """None is a valid owner"""
    l = RLock()
    with pytest.raises(ZeroDivisionError):
        async with l("x"):
            1 / 0

    assert l._owner is None
    assert l._count == 0
    assert not l._lock.locked()
