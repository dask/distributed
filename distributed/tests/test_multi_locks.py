import asyncio
from distributed.multi_lock import MultiLockExtension
from time import sleep


from distributed import MultiLock, get_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_single_lock(c, s, a, b):
    await c.set_metadata("locked", False)

    def f(_):
        client = get_client()
        with MultiLock(lock_names=["x"]):
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.05)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    futures = c.map(f, range(20))
    await c.gather(futures)
    assert not s.extensions["multi_locks"].events
    assert not s.extensions["multi_locks"].waiters
    for lock in s.extensions["multi_locks"].locks.values():
        assert not lock


@gen_cluster(client=True)
async def test_timeout(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    lock1 = MultiLock(lock_names=["x"])
    result = await lock1.acquire()
    assert result is True
    assert not ext.waiters[lock1.id]
    assert ext.locks["x"] == [lock1.id]
    assert not ext.events

    lock2 = MultiLock(lock_names=["x"])
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
async def test_multiple_locks(c, s, a, b):
    ext: MultiLockExtension = s.extensions["multi_locks"]
    l1 = MultiLock(lock_names=["l1"])
    l2 = MultiLock(lock_names=["l2"])
    l3 = MultiLock(lock_names=["l1", "l2"])

    # Both `l1` and `l2` are free to acquire
    assert await l1.acquire()
    assert await l2.acquire()
    assert list(ext.locks.keys()) == ["l1", "l2"]
    assert list(ext.locks.values()) == [[l1.id], [l2.id]]
    assert list(ext.waiters.keys()) == [l1.id, l2.id]
    assert all(len(w) == 0 for w in ext.waiters.values())
    assert not ext.events

    # Since `l3` requires both `l1` and `l2`, it isn't available immediately
    l3_acquire = l3.acquire()
    try:
        await asyncio.wait_for(asyncio.shield(l3_acquire), 0.1)
    except asyncio.TimeoutError:
        assert list(ext.locks.keys()) == ["l1", "l2"]
        assert list(ext.locks.values()) == [[l1.id, l3.id], [l2.id, l3.id]]
        assert ext.waiters[l3.id] == {"l1", "l2"}
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
        assert ext.waiters[l3.id] == {"l2"}
        assert l3.id in ext.events
    else:
        assert False

    # Releasing `l2` should make `l3` available
    await l2.release()
    assert list(ext.locks.keys()) == ["l1", "l2"]
    assert list(ext.locks.values()) == [[l3.id], [l3.id]]
    assert not ext.waiters[l3.id]

    await l3.release()
    assert not ext.events
    assert not ext.waiters
    assert all(len(l) == 0 for l in ext.locks.values())
