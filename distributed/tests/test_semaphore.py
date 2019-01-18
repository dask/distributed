from __future__ import absolute_import, division, print_function

import pickle
from time import sleep

import dask
from distributed import Semaphore, get_client
from distributed.metrics import time
from distributed.utils_test import cluster_fixture, client, gen_cluster, loop  # noqa F401
from distributed import Client


@gen_cluster(client=True)
async def test_semaphore(c, s, a, b):
    semaphore = await Semaphore(max_leases=2, name="x")

    result = await semaphore.acquire()
    assert result is True

    second = await semaphore.acquire()
    assert second is True
    start = time()
    result = await semaphore.acquire(timeout=0.1)
    stop = time()
    assert stop - start < 0.3
    assert result is False


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    sem = await Semaphore(max_leases=2, name="x")
    res = await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
    assert res
    sem2 = pickle.loads(pickle.dumps(sem))
    assert sem2.name == sem.name
    assert sem2.client.scheduler.address == sem.client.scheduler.address

    # actual leases didn't change
    assert len(s.extensions["semaphores"].leases["x"]) == 1

    res = await sem2.acquire()
    assert res

    # Ensure that both objects access the same semaphore
    res = await sem.acquire(timeout=0)

    assert not res
    res = await sem2.acquire(timeout=0)

    assert not res


@gen_cluster(client=True)
async def test_release_simple(c, s, a, b):

    def f(x, semaphore=None):
        with semaphore:
            assert semaphore.name == "x"
            return x + 1

    sem = await Semaphore(max_leases=2, name="x")
    futures = c.map(f, list(range(10)), semaphore=sem)
    await c.gather(futures)


@gen_cluster(client=True)
async def test_acquires_with_zero_timeout(c, s, a, b):
    sem = await Semaphore(1, "x")

    assert await sem.acquire(timeout=0)

    assert not await sem.acquire(timeout=0)

    await sem.release()

    assert await sem.acquire(timeout=1)
    await sem.release()

    assert await sem.acquire(timeout=1)
    await sem.release()


def test_timeout_sync(client):
    with Semaphore(name="x"):
        assert Semaphore(1, "x").acquire(timeout=0.05) is False


def test_lock_name_only(client):

    def f(x):
        with Semaphore(name="x"):
            client = get_client()
            assert client.get_metadata("locked") is False
            client.set_metadata("locked", True)
            sleep(0.01)
            assert client.get_metadata("locked") is True
            client.set_metadata("locked", False)

    client.set_metadata("locked", False)
    futures = client.map(f, range(10))
    client.gather(futures)


@gen_cluster(client=True)
async def test_release_semaphore_after_timeout(c, s, a, b):
    with dask.config.set({
        "distributed.scheduler.locks.lease-validation-interval": "100ms"
    }):
        sem = await Semaphore(name="x", max_leases=2)
        await sem.acquire()
        semY = Semaphore(name="y")

        async with Client(s.address, asynchronous=True, name="ClientB") as clientB:
            semB = await Semaphore(name="x", max_leases=2, client=clientB)
            semYB = await Semaphore(name="y", client=clientB)

            assert (await semB.acquire())
            assert (await semYB.acquire())

            assert not (await sem.acquire(timeout=0))
            assert not (await semB.acquire(timeout=0))
            assert not (await semYB.acquire(timeout=0))

        # At this point, we should be able to acquire x and y once
        assert (await sem.acquire())
        assert (await semY.acquire())

        assert not (await semY.acquire(timeout=0))
        assert not (await sem.acquire(timeout=0))


@gen_cluster()
async def test_async_ctx(s, a, b):
    sem = await Semaphore(name="x")
    async with sem:
        assert not await sem.acquire(timeout=0.001)
    assert await sem.acquire()
