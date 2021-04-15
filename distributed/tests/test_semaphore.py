import asyncio
import logging
import pickle
from datetime import timedelta
from time import sleep

import pytest

import dask
from dask.distributed import Client

from distributed import Semaphore, fire_and_forget
from distributed.comm import Comm
from distributed.compatibility import WINDOWS
from distributed.core import ConnectionPool
from distributed.metrics import time
from distributed.utils_test import (  # noqa: F401
    async_wait_for,
    captured_logger,
    cleanup,
    client,
    cluster,
    cluster_fixture,
    gen_cluster,
    loop,
    slowidentity,
)


@gen_cluster(client=True)
async def test_semaphore_trivial(c, s, a, b):
    semaphore = await Semaphore(max_leases=2, name="resource_we_want_to_limit")

    result = await semaphore.acquire()  # allowed_leases: 2 - 1 -> 1
    assert result is True

    second = await semaphore.acquire()  # allowed_leases: 1 - 1 -> 0
    assert second is True
    start = time()
    result = await semaphore.acquire(timeout=0.025)  # allowed_leases: 0 -> False
    stop = time()
    assert stop - start < 0.2
    assert result is False


@gen_cluster(client=True)
async def test_serializable(c, s, a, b):
    sem = await Semaphore(max_leases=2, name="x")
    res = await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
    assert res
    sem2 = pickle.loads(pickle.dumps(sem))
    assert sem2.name == sem.name
    # assert sem2.client.scheduler.address == sem.client.scheduler.address

    # actual leases didn't change
    assert len(s.extensions["semaphores"].leases["x"]) == 1

    res = await sem2.acquire()
    assert res
    assert len(s.extensions["semaphores"].leases["x"]) == 2

    # Ensure that both objects access the same semaphore
    res = await sem.acquire(timeout=0.025)

    assert not res
    res = await sem2.acquire(timeout=0.025)

    assert not res


@gen_cluster(client=True)
async def test_release_simple(c, s, a, b):
    def f(x, semaphore):
        with semaphore:
            assert semaphore.name == "x"
            return x + 1

    sem = await Semaphore(max_leases=2, name="x")

    assert s.extensions["semaphores"]._semaphore_exists("x")
    futures = c.map(f, list(range(10)), semaphore=sem)
    await c.gather(futures)


@gen_cluster(client=True)
async def test_acquires_with_timeout(c, s, a, b):
    sem = await Semaphore(1, "x")
    assert await sem.acquire(timeout="25ms")
    assert not await sem.acquire(timeout=0.025)
    assert await sem.release() is True
    assert await sem.acquire(timeout=timedelta(seconds=0.025))
    assert await sem.release() is True


def test_timeout_sync(client):
    s = Semaphore(name="x")
    # Using the context manager already acquires a lease, so the line below won't be able to acquire another one
    with s:
        assert s.acquire(timeout=0.025) is False


@gen_cluster(
    client=True,
    timeout=20,
    config={
        "distributed.scheduler.locks.lease-validation-interval": "200ms",
        "distributed.scheduler.locks.lease-timeout": "200ms",
    },
)
async def test_release_semaphore_after_timeout(c, s, a, b):
    sem = await Semaphore(name="x", max_leases=2)
    await sem.acquire()  # leases: 2 - 1 = 1

    semB = await Semaphore(name="x", max_leases=2)

    assert await semB.acquire()  # leases: 1 - 1 = 0

    assert not (await sem.acquire(timeout=0.01))
    assert not (await semB.acquire(timeout=0.01))

    # B goes out of scope / cannot refresh anymore. For instance, because its
    # worker died

    semB.refresh_callback.stop()
    del semB

    assert await sem.acquire(timeout=1)

    assert not (await sem.acquire(timeout=0.1))


@gen_cluster()
async def test_async_ctx(s, a, b):
    sem = await Semaphore(name="x")
    async with sem:
        assert not await sem.acquire(timeout=0.025)
    assert await sem.acquire()


@pytest.mark.slow
def test_worker_dies():
    with cluster(
        config={
            "distributed.scheduler.locks.lease-timeout": "0.1s",
        }
    ) as (scheduler, workers):
        with Client(scheduler["address"]) as client:
            sem = Semaphore(name="x", max_leases=1)

            def f(x, sem, kill_address):
                with sem:
                    from distributed.worker import get_worker

                    worker = get_worker()
                    if worker.address == kill_address:
                        import os

                        os.kill(os.getpid(), 15)
                    return x

            futures = client.map(
                f, range(10), sem=sem, kill_address=workers[0]["address"]
            )
            results = client.gather(futures)

            assert sorted(results) == list(range(10))


@gen_cluster(client=True)
async def test_access_semaphore_by_name(c, s, a, b):
    def f(x, release=True):
        sem = Semaphore(name="x")
        if not sem.acquire(timeout=0.1):
            return False
        if release:
            assert sem.release() is True

        return True

    sem = await Semaphore(name="x")
    futures = c.map(f, list(range(10)))
    assert all(await c.gather(futures))

    # Clean-up the state, otherwise we would get the same result when calling `f` with the same arguments
    del futures

    assert len(s.extensions["semaphores"].leases["x"]) == 0
    assert await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1
    futures = c.map(f, list(range(10)))
    assert not any(await c.gather(futures))
    assert await sem.release() is True

    del futures

    futures = c.map(f, list(range(10)), release=False)
    result = await c.gather(futures)
    assert result.count(True) == 1
    assert result.count(False) == 9


@gen_cluster(client=True)
async def test_close_async(c, s, a, b):
    sem = await Semaphore(name="test")

    assert await sem.acquire()
    with pytest.warns(
        RuntimeWarning,
        match="Closing semaphore .* but there remain unreleased leases .*",
    ):
        await sem.close()

    with pytest.raises(
        RuntimeError, match="Semaphore `test` not known or already closed."
    ):
        await sem.acquire()

    sem2 = await Semaphore(name="t2", max_leases=1)
    assert await sem2.acquire()

    def f(sem_):
        return sem_.acquire()

    semaphore_object = s.extensions["semaphores"]
    fire_and_forget(c.submit(f, sem_=sem2))
    while not semaphore_object.metrics["pending"]["t2"]:  # Wait for the pending lease
        await asyncio.sleep(0.01)
    with pytest.warns(
        RuntimeWarning, match="Closing semaphore .* but there remain pending leases"
    ):
        await sem2.close()

    assert not semaphore_object.max_leases
    assert not semaphore_object.leases
    assert not semaphore_object.events
    for metric_dict in semaphore_object.metrics.values():
        assert not metric_dict


def test_close_sync(client):
    sem = Semaphore()
    sem.close()

    with pytest.raises(RuntimeError, match="Semaphore .* not known or already closed."):
        sem.acquire()


@gen_cluster(client=True)
async def test_release_once_too_many(c, s, a, b):
    sem = await Semaphore(name="x")
    assert await sem.acquire()
    assert await sem.release() is True

    with pytest.raises(RuntimeError, match="Released too often"):
        sem.release()

    assert await sem.acquire()
    assert await sem.release() is True


@gen_cluster(client=True)
async def test_release_once_too_many_resilience(c, s, a, b):
    def f(x, sem):
        sem.acquire()
        assert sem.release() is True
        with pytest.raises(RuntimeError, match="Released too often"):
            sem.release()
        return x

    sem = await Semaphore(max_leases=3, name="x")

    inpt = list(range(20))
    futures = c.map(f, inpt, sem=sem)
    assert sorted(await c.gather(futures)) == inpt

    assert not s.extensions["semaphores"].leases["x"]
    await sem.acquire()
    assert len(s.extensions["semaphores"].leases["x"]) == 1


class BrokenComm(Comm):
    peer_address = None
    local_address = None

    def close(self):
        pass

    def closed(self):
        return True

    def abort(self):
        pass

    def read(self, deserializers=None):
        raise EnvironmentError

    def write(self, msg, serializers=None, on_error=None):
        raise EnvironmentError


class FlakyConnectionPool(ConnectionPool):
    def __init__(self, *args, failing_connections=0, **kwargs):
        self.cnn_count = 0
        self.failing_connections = failing_connections
        self._flaky_active = False
        super().__init__(*args, **kwargs)

    def activate(self):
        self._flaky_active = True

    def deactivate(self):
        self._flaky_active = False

    async def connect(self, *args, **kwargs):
        if self.cnn_count >= self.failing_connections or not self._flaky_active:
            return await super().connect(*args, **kwargs)
        else:
            self.cnn_count += 1
            return BrokenComm()

    def reuse(self, addr, comm):
        pass


@gen_cluster(client=True)
async def test_retry_acquire(c, s, a, b):
    with dask.config.set({"distributed.comm.retry.count": 1}):

        pool = await FlakyConnectionPool(failing_connections=1)

        semaphore = await Semaphore(
            max_leases=2,
            name="resource_we_want_to_limit",
            scheduler_rpc=pool(s.address),
        )
        pool.activate()

        result = await semaphore.acquire()
        assert result is True

        second = await semaphore.acquire()
        assert second is True
        start = time()
        result = await semaphore.acquire(timeout=0.025)
        stop = time()
        assert stop - start < 0.2
        assert result is False


@pytest.mark.flaky(reruns=10, reruns_delay=5, condition=WINDOWS)
@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.locks.lease-timeout": "100ms",
        "distributed.scheduler.locks.lease-validation-interval": "100ms",
    },
)
async def test_oversubscribing_leases(c, s, a, b):
    """
    This test ensures that we detect oversubscription scenarios and will not
    accept new leases as long as the semaphore is oversubscribed.

    Oversubscription may occur if tasks hold the GIL for a longer time than the
    lease-timeout is configured causing the lease refresh to go stale and timeout.

    We cannot protect ourselves entirely from this but we can ensure that while
    a task with a timed out lease is still running, we block further
    acquisitions until we return to normal.

    An example would be a task which continuously locks the GIL for a longer
    time than the lease timeout but this continuous lock only makes up a
    fraction of the tasks runtime.
    """
    # GH3705

    from distributed.worker import Worker, get_client

    # Using the metadata as a crude "asyncio.Event" since the proper event
    # implementation cannot be serialized. For the purpose of this test a
    # metadata check with a sleep loop is not elegant but practical.
    await c.set_metadata("release", False)
    sem = await Semaphore()
    sem.refresh_callback.stop()

    def guaranteed_lease_timeout(x, sem):
        """
        This function simulates a payload computation with some GIL
        locking in the beginning.

        To simulate this we will manually disable the refresh callback, i.e.
        all leases will eventually timeout. The function will only
        release/return once the "Event" is set, i.e. our observer is done.
        """
        sem.refresh_leases = False
        client = get_client()

        with sem:
            # This simulates a task which holds the GIL for longer than the
            # lease-timeout. This is twice the lease timeout to ensurre that the
            # leases are actually timed out
            slowidentity(delay=0.2)

            assert sem._leases
            # Now the GIL is free again, i.e. we enable the callback again
            sem.refresh_leases = True
            sleep(0.1)

            # This is the poormans Event.wait()
            while client.get_metadata("release") is not True:
                sleep(0.05)

            assert sem.get_value() >= 1
            return x

    def observe_state(sem):
        """
        This function is 100% artificial and acts as an observer to verify
        our assumptions. The function will wait until both payload tasks are
        executing, i.e. we're in an oversubscription scenario. It will then
        try to acquire and hopefully fail showing that the semaphore is
        protected if the oversubscription is recognized.
        """
        sem.refresh_callback.stop()
        # We wait until we're in an oversubscribed state, i.e. both tasks
        # are executed although there should only be one allowed
        while not sem.get_value() > 1:
            sleep(0.2)

        # Once we're in an oversubscribed state, we must not be able to
        # acquire a lease.
        assert not sem.acquire(timeout=0)

        client = get_client()
        client.set_metadata("release", True)

    observer = await Worker(s.address)

    futures = c.map(
        guaranteed_lease_timeout, range(2), sem=sem, workers=[a.address, b.address]
    )
    fut_observe = c.submit(observe_state, sem=sem, workers=[observer.address])

    with captured_logger("distributed.semaphore", level=logging.DEBUG) as caplog:
        payload, observer = await c.gather([futures, fut_observe])

    logs = caplog.getvalue().split("\n")
    timeouts = [log for log in logs if "timed out" in log]
    refresh_unknown = [log for log in logs if "Refreshing an unknown lease ID" in log]
    assert len(timeouts) == 2
    assert len(refresh_unknown) == 2

    assert sorted(payload) == [0, 1]
    # Back to normal
    assert await sem.get_value() == 0


@gen_cluster(client=True)
async def test_timeout_zero(c, s, a, b):
    # Depending on the internals a timeout zero cannot work, e.g. when the
    # initial try already includes a wait. Since some test cases use this, it is
    # worth testing against.

    sem = await Semaphore()

    assert await sem.acquire(timeout=0)
    assert not await sem.acquire(timeout=0)
    assert await sem.release() is True


@gen_cluster(client=True)
async def test_getvalue(c, s, a, b):

    sem = await Semaphore()

    assert await sem.get_value() == 0
    await sem.acquire()
    assert await sem.get_value() == 1
    assert await sem.release() is True
    assert await sem.get_value() == 0


@gen_cluster(client=True)
async def test_metrics(c, s, a, b):
    from collections import defaultdict

    sem = await Semaphore(name="test", max_leases=5)

    before_acquiring = time()

    assert await sem.acquire()
    assert await sem.acquire()

    expected_average_pending_lease_time = (time() - before_acquiring) / 2
    epsilon = max(0.1, 0.5 * expected_average_pending_lease_time)

    sem_ext = s.extensions["semaphores"]

    actual = sem_ext.metrics.copy()
    assert (
        expected_average_pending_lease_time - epsilon
        <= actual.pop("average_pending_lease_time")["test"]
        <= expected_average_pending_lease_time + epsilon
    )
    expected = {
        "acquire_total": defaultdict(int, {"test": 2}),
        "release_total": defaultdict(int),
        "pending": defaultdict(int, {"test": 0}),
    }
    assert actual == expected


def test_threadpoolworkers_pick_correct_ioloop(cleanup):
    # gh4057

    # About picking appropriate values for the various timings
    # * Sleep time in `access_limited` impacts test runtime but is arbitrary
    # * `lease-timeout` should be smaller than the sleep time. This is what the
    #   test builds on. assuming the leases cannot be refreshed, e.g. wrong
    #   event loop picked / PeriodicCallback never scheduled, the semaphore
    #   would become oversubscribed and the len(protected_resources) becomes
    #   non zero. This should also trigger a log message about "unknown leases"
    #   and fails the test.
    # * `lease-validation-interval` interval should be the smallest quantity.
    #   How often leases are checked for staleness is hard coded atm and a fifth
    #   of the `lease-timeout`. Accounting for this and some jitter, this should
    #   be sufficiently small to ensure smooth operation.

    with dask.config.set(
        {
            "distributed.scheduler.locks.lease-validation-interval": 0.01,
            "distributed.scheduler.locks.lease-timeout": 0.1,
        }
    ):
        with Client(processes=False, threads_per_worker=4) as client:
            sem = Semaphore(max_leases=1, name="database")
            protected_resource = []

            def access_limited(val, sem):
                import time

                with sem:
                    assert len(protected_resource) == 0
                    protected_resource.append(val)
                    # Interact with the DB
                    time.sleep(0.2)
                    protected_resource.remove(val)

            client.gather(client.map(access_limited, range(10), sem=sem))


@gen_cluster(client=True)
async def test_release_retry(c, s, a, b):
    """Verify that we can properly retry a semaphore release operation"""
    with dask.config.set({"distributed.comm.retry.count": 1}):
        pool = await FlakyConnectionPool(failing_connections=1)

        semaphore = await Semaphore(
            max_leases=2,
            name="resource_we_want_to_limit",
            scheduler_rpc=pool(s.address),
        )
        await semaphore.acquire()
        pool.activate()  # Comm chaos starts
        with captured_logger("distributed.utils_comm") as caplog:
            assert await semaphore.release() is True
        logs = caplog.getvalue().split("\n")
        log = logs[0]
        assert log.startswith("Retrying semaphore release:") and log.endswith(
            "after exception in attempt 0/1: "
        )

        assert await semaphore.acquire() is True
        assert await semaphore.release() is True


@pytest.mark.flaky(reruns=10, reruns_delay=5, condition=WINDOWS)
@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.locks.lease-timeout": "100ms",
        "distributed.scheduler.locks.lease-validation-interval": "100ms",
    },
)
async def test_release_failure(c, s, a, b):
    """Don't raise even if release fails: lease will be cleaned up by the lease-validation after
    a specified interval anyways (see config parameters used)."""

    with dask.config.set({"distributed.comm.retry.count": 1}):
        pool = await FlakyConnectionPool(failing_connections=5)

        semaphore = await Semaphore(
            max_leases=2,
            name="resource_we_want_to_limit",
            scheduler_rpc=pool(s.address),
        )
        await semaphore.acquire()
        pool.activate()  # Comm chaos starts

        # Release fails (after a single retry) because of broken connections
        with captured_logger(
            "distributed.semaphore", level=logging.ERROR
        ) as semaphore_log:
            with captured_logger("distributed.utils_comm") as retry_log:
                assert await semaphore.release() is False

        with captured_logger(
            "distributed.semaphore", level=logging.DEBUG
        ) as semaphore_cleanup_log:
            pool.deactivate()  # comm chaos stops
            assert await semaphore.get_value() == 1  # lease is still registered
            await asyncio.sleep(0.2)  # Wait for lease to be cleaned up

        # Check release was retried
        retry_log = retry_log.getvalue().split("\n")[0]
        assert retry_log.startswith(
            "Retrying semaphore release:"
        ) and retry_log.endswith("after exception in attempt 0/1: ")
        # Check release failed
        semaphore_log = semaphore_log.getvalue().split("\n")[0]
        assert semaphore_log.startswith(
            "Release failed for id="
        ) and semaphore_log.endswith("Cluster network might be unstable?")

        # Check lease has timed out
        assert any(
            log.startswith("Lease") and "timed out after" in log
            for log in semaphore_cleanup_log.getvalue().split("\n")
        )
        assert await semaphore.get_value() == 0
