from time import sleep

from distributed import MultiLock, get_client
from distributed.utils_test import gen_cluster
from distributed.utils_test import client, cluster_fixture, loop  # noqa F401


@gen_cluster(client=True, nthreads=[("127.0.0.1", 8)] * 2)
async def test_lock(c, s, a, b):
    await c.set_metadata("locked", False)

    def f(x):
        client = get_client()
        with MultiLock(lock_names=["x"]) as lock:
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
