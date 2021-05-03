import asyncio
from time import sleep

from distributed import MultiLock, get_client
from distributed.metrics import time
from distributed.multi_lock import MultiLockExtension
from distributed.utils_test import (  # noqa F401
    client,
    cluster_fixture,
    gen_cluster,
    loop,
)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_single_lock(c, s, a, b):
    await c.set_metadata("locked", False)

    def f(_):
        client = get_client()
        with MultiLock(names=["x"]):
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    futures = c.map(f, range(20))
    await c.gather(futures)
    ext: MultiLockExtension = s.extensions["multi_locks"]
    assert not ext.events
    assert not ext.requests
    assert not ext.requests_left
    assert all(len(l) == 0 for l in ext.locks.values())


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    lock1 = MultiLock(names=["x"])
    result = await lock1.acquire()
    assert result is True
    assert ext.requests_left[lock1.id] == 0
    assert ext.locks["x"] == [lock1.id]
    assert not ext.events

    lock2 = MultiLock(names=["x"])
    assert lock1.id != lock2.id

    start = time()
    result = await lock2.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False
    assert ext.locks["x"] == [lock1.id]
    assert not ext.events

    await lock1.release()


@gen_cluster(client=True)
async def test_timeout_wake_waiter(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    l1 = MultiLock(names=["x"])
    l2 = MultiLock(names=["x", "y"])
    l3 = MultiLock(names=["y"])
    await l1.acquire()

    l2_acquire = asyncio.ensure_future(l2.acquire(timeout=1))
    try:
        await asyncio.wait_for(asyncio.shield(l2_acquire), 0.1)
    except asyncio.TimeoutError:
        pass
    else:
        assert False

    l3_acquire = asyncio.ensure_future(l3.acquire())
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        pass
    else:
        assert False

    assert await l2_acquire is False
    assert await l3_acquire
    l1.release()
    l3.release()


@gen_cluster(client=True)
async def test_multiple_locks(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    l1 = MultiLock(names=["l1"])
    l2 = MultiLock(names=["l2"])
    l3 = MultiLock(names=["l1", "l2"])

    # Both `l1` and `l2` are free to acquire
    assert await l1.acquire()
    assert await l2.acquire()
    assert list(ext.locks.keys()) == ["l1", "l2"]
    assert list(ext.locks.values()) == [[l1.id], [l2.id]]
    assert list(ext.requests.keys()) == [l1.id, l2.id]
    assert list(ext.requests_left.values()) == [0, 0]
    assert not ext.events

    # Since `l3` requires both `l1` and `l2`, it isn't available immediately
    l3_acquire = asyncio.ensure_future(l3.acquire())
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        assert list(ext.locks.keys()) == ["l1", "l2"]
        assert list(ext.locks.values()) == [[l1.id, l3.id], [l2.id, l3.id]]
        assert ext.requests[l3.id] == {"l1", "l2"}
        assert ext.requests_left[l3.id] == 2
        assert l3.id in ext.events
    else:
        assert False  # We except a TimeoutError since `l3` isn't availabe

    # Releasing `l1` isn't enough since `l3` also requires `l2`
    await l1.release()
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        # `l3` now only wait on `l2`
        assert list(ext.locks.keys()) == ["l1", "l2"]
        assert list(ext.locks.values()) == [[l3.id], [l2.id, l3.id]]
        assert ext.requests[l3.id] == {"l1", "l2"}
        assert ext.requests_left[l3.id] == 1
        assert l3.id in ext.events
    else:
        assert False

    # Releasing `l2` should make `l3` available
    await l2.release()
    assert list(ext.locks.keys()) == ["l1", "l2"]
    assert list(ext.locks.values()) == [[l3.id], [l3.id]]
    assert ext.requests[l3.id] == {"l1", "l2"}
    assert ext.requests_left[l3.id] == 0

    await l3.release()
    assert not ext.events
    assert not ext.requests
    assert not ext.requests_left
    assert all(len(l) == 0 for l in ext.locks.values())


@gen_cluster(client=True)
async def test_num_locks(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    l1 = MultiLock(names=["l1", "l2", "l3"])
    l2 = MultiLock(names=["l1", "l2", "l3"])
    l3 = MultiLock(names=["l1", "l2", "l3", "l4"])

    # Even though `l1` and `l2` uses the same lock names they
    # only requires a subset of the locks
    assert await l1.acquire(num_locks=1)
    assert await l2.acquire(num_locks=2)
    assert list(ext.locks.keys()) == ["l1", "l2", "l3"]
    assert list(ext.locks.values()) == [[l1.id], [l2.id], [l2.id]]
    assert list(ext.requests.keys()) == [l1.id, l2.id]
    assert list(ext.requests_left.values()) == [0, 0]
    assert not ext.events

    # Since `l3` requires three out of four locks it has to wait
    l3_acquire = asyncio.ensure_future(l3.acquire(num_locks=3))
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        assert list(ext.locks.keys()) == ["l1", "l2", "l3", "l4"]
        assert list(ext.locks.values()) == [
            [l1.id, l3.id],
            [l2.id, l3.id],
            [l2.id, l3.id],
            [l3.id],
        ]
        assert list(ext.requests_left.values()) == [0, 0, 2]
        assert l3.id in ext.events
    else:
        assert False  # We except a TimeoutError since `l3` isn't availabe

    # Releasing `l1` isn't enough since `l3` also requires three locks
    await l1.release()
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        assert list(ext.locks.keys()) == ["l1", "l2", "l3", "l4"]
        assert list(ext.locks.values()) == [
            [l3.id],
            [l2.id, l3.id],
            [l2.id, l3.id],
            [l3.id],
        ]
        assert list(ext.requests.keys()) == [l2.id, l3.id]
        assert list(ext.requests_left.values()) == [0, 1]
        assert l3.id in ext.events
    else:
        assert False

    # Releasing `l2` is enough to release `l3`
    await l2.release()
    await asyncio.sleep(0.1)  # Give `l3` a change to wake up and acquire its locks
    assert list(ext.locks.keys()) == ["l1", "l2", "l3", "l4"]
    assert list(ext.locks.values()) == [[l3.id], [l3.id], [l3.id], [l3.id]]
    assert list(ext.requests.keys()) == [l3.id]
    assert list(ext.requests_left.values()) == [0]
    assert l3.id not in ext.events

    await l3.release()
    assert not ext.events
    assert not ext.requests
    assert not ext.requests_left
    assert all(len(l) == 0 for l in ext.locks.values())
