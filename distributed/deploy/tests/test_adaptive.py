import asyncio
import gc
import math
from time import sleep

import pytest

import dask

from distributed import Adaptive, Client, LocalCluster, SpecCluster, Worker, wait
from distributed.metrics import time
from distributed.utils_test import (  # noqa: F401
    async_wait_for,
    clean,
    cleanup,
    gen_test,
    loop,
    nodebug,
    slowinc,
)


def test_adaptive_local_cluster(loop):
    with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
        loop=loop,
    ) as cluster:
        alc = cluster.adapt(interval="100 ms")
        with Client(cluster, loop=loop) as c:
            assert not c.nthreads()
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.nthreads()

            sleep(0.1)
            assert c.nthreads()  # still there after some time

            del future

            start = time()
            while cluster.scheduler.nthreads:
                sleep(0.01)
                assert time() < start + 5

            assert not c.nthreads()


@pytest.mark.asyncio
async def test_adaptive_local_cluster_multi_workers(cleanup):
    async with LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
    ) as cluster:

        cluster.scheduler.allowed_failures = 1000
        adapt = cluster.adapt(interval="100 ms")
        async with Client(cluster, asynchronous=True) as c:
            futures = c.map(slowinc, range(100), delay=0.01)

            start = time()
            while not cluster.scheduler.workers:
                await asyncio.sleep(0.01)
                assert time() < start + 15, adapt.log

            await c.gather(futures)
            del futures

            start = time()
            # while cluster.workers:
            while cluster.scheduler.workers:
                await asyncio.sleep(0.01)
                assert time() < start + 15, adapt.log

            # no workers for a while
            for i in range(10):
                assert not cluster.scheduler.workers
                await asyncio.sleep(0.05)

            futures = c.map(slowinc, range(100), delay=0.01)
            await c.gather(futures)


@pytest.mark.xfail(reason="changed API")
@pytest.mark.asyncio
async def test_adaptive_scale_down_override(cleanup):
    class TestAdaptive(Adaptive):
        def __init__(self, *args, **kwargs):
            self.min_size = kwargs.pop("min_size", 0)
            Adaptive.__init__(self, *args, **kwargs)

        async def workers_to_close(self, **kwargs):
            num_workers = len(self.cluster.workers)
            to_close = await self.scheduler.workers_to_close(**kwargs)
            if num_workers - len(to_close) < self.min_size:
                to_close = to_close[: num_workers - self.min_size]

            return to_close

    class TestCluster(LocalCluster):
        def scale_up(self, n, **kwargs):
            assert False

    async with TestCluster(n_workers=10, processes=False, asynchronous=True) as cluster:
        ta = cluster.adapt(
            min_size=2, interval=0.1, scale_factor=2, Adaptive=TestAdaptive
        )
        await asyncio.sleep(0.3)

        # Assert that adaptive cycle does not reduce cluster below minimum size
        # as determined via override.
        assert len(cluster.scheduler.workers) == 2


@gen_test()
async def test_min_max():
    cluster = await LocalCluster(
        n_workers=0,
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
        threads_per_worker=1,
    )
    try:
        adapt = cluster.adapt(minimum=1, maximum=2, interval="20 ms", wait_count=10)
        c = await Client(cluster, asynchronous=True)

        start = time()
        while not cluster.scheduler.workers:
            await asyncio.sleep(0.01)
            assert time() < start + 1

        await asyncio.sleep(0.2)
        assert len(cluster.scheduler.workers) == 1
        assert len(adapt.log) == 1 and adapt.log[-1][1] == {"status": "up", "n": 1}

        futures = c.map(slowinc, range(100), delay=0.1)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            await asyncio.sleep(0.01)
            assert time() < start + 1

        assert len(cluster.scheduler.workers) == 2
        await asyncio.sleep(0.5)
        assert len(cluster.scheduler.workers) == 2
        assert len(cluster.workers) == 2
        assert len(adapt.log) == 2 and all(d["status"] == "up" for _, d in adapt.log)

        del futures
        gc.collect()

        start = time()
        while len(cluster.scheduler.workers) != 1:
            await asyncio.sleep(0.01)
            assert time() < start + 2
        assert adapt.log[-1][1]["status"] == "down"
    finally:
        await c.close()
        await cluster.close()


@pytest.mark.asyncio
async def test_avoid_churn(cleanup):
    """We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    async with LocalCluster(
        n_workers=0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            adapt = cluster.adapt(interval="20 ms", wait_count=5)

            for i in range(10):
                await client.submit(slowinc, i, delay=0.040)
                await asyncio.sleep(0.040)

            assert len(adapt.log) == 1


@pytest.mark.asyncio
async def test_adapt_quickly():
    """We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    cluster = await LocalCluster(
        n_workers=0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    )
    client = await Client(cluster, asynchronous=True)
    adapt = cluster.adapt(interval="20 ms", wait_count=5, maximum=10)
    try:
        future = client.submit(slowinc, 1, delay=0.100)
        await wait(future)
        assert len(adapt.log) == 1

        # Scale up when there is plenty of available work
        futures = client.map(slowinc, range(1000), delay=0.100)
        while len(adapt.log) == 1:
            await asyncio.sleep(0.01)
        assert len(adapt.log) == 2
        assert adapt.log[-1][1]["status"] == "up"
        d = [x for x in adapt.log[-1] if isinstance(x, dict)][0]
        assert 2 < d["n"] <= adapt.maximum

        while len(cluster.workers) < adapt.maximum:
            await asyncio.sleep(0.01)

        del futures

        while len(cluster.scheduler.tasks) > 1:
            await asyncio.sleep(0.01)

        await cluster

        while len(cluster.scheduler.workers) > 1 or len(cluster.worker_spec) > 1:
            await asyncio.sleep(0.01)

        # Don't scale up for large sequential computations
        x = await client.scatter(1)
        log = list(cluster._adaptive.log)
        for i in range(100):
            x = client.submit(slowinc, x)

        await asyncio.sleep(0.1)
        assert len(cluster.workers) == 1
    finally:
        await client.close()
        await cluster.close()


@gen_test(timeout=None)
async def test_adapt_down():
    """Ensure that redefining adapt with a lower maximum removes workers"""
    async with LocalCluster(
        0,
        asynchronous=True,
        processes=False,
        scheduler_port=0,
        silence_logs=False,
        dashboard_address=None,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            cluster.adapt(interval="20ms", maximum=5)

            futures = client.map(slowinc, range(1000), delay=0.1)
            while len(cluster.scheduler.workers) < 5:
                await asyncio.sleep(0.1)

            cluster.adapt(maximum=2)

            start = time()
            while len(cluster.scheduler.workers) != 2:
                await asyncio.sleep(0.1)
                assert time() < start + 60


@gen_test()
async def test_no_more_workers_than_tasks():
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"slowinc": 1000}}
    ):
        async with LocalCluster(
            0,
            scheduler_port=0,
            silence_logs=False,
            processes=False,
            dashboard_address=None,
            asynchronous=True,
        ) as cluster:
            adapt = cluster.adapt(minimum=0, maximum=4, interval="10 ms")
            async with Client(cluster, asynchronous=True) as client:
                await client.submit(slowinc, 1, delay=0.100)
                assert len(cluster.scheduler.workers) <= 1


def test_basic_no_loop(loop):
    with clean(threads=False):
        try:
            with LocalCluster(
                0, scheduler_port=0, silence_logs=False, dashboard_address=None
            ) as cluster:
                with Client(cluster) as client:
                    cluster.adapt()
                    future = client.submit(lambda x: x + 1, 1)
                    assert future.result() == 2
                loop = cluster.loop
        finally:
            loop.add_callback(loop.stop)


@pytest.mark.asyncio
async def test_target_duration():
    """Ensure that redefining adapt with a lower maximum removes workers"""
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"slowinc": 1}}
    ):
        async with LocalCluster(
            0,
            asynchronous=True,
            processes=False,
            scheduler_port=0,
            silence_logs=False,
            dashboard_address=None,
        ) as cluster:
            adapt = cluster.adapt(interval="20ms", minimum=2, target_duration="5s")
            async with Client(cluster, asynchronous=True) as client:
                while len(cluster.scheduler.workers) < 2:
                    await asyncio.sleep(0.01)

                futures = client.map(slowinc, range(100), delay=0.3)

                while len(adapt.log) < 2:
                    await asyncio.sleep(0.01)

                assert adapt.log[0][1] == {"status": "up", "n": 2}
                assert adapt.log[1][1] == {"status": "up", "n": 20}


@pytest.mark.asyncio
async def test_worker_keys(cleanup):
    """Ensure that redefining adapt with a lower maximum removes workers"""
    async with SpecCluster(
        workers={
            "a-1": {"cls": Worker},
            "a-2": {"cls": Worker},
            "b-1": {"cls": Worker},
            "b-2": {"cls": Worker},
        },
        asynchronous=True,
    ) as cluster:

        def key(ws):
            return ws.name.split("-")[0]

        cluster._adaptive_options = {"worker_key": key}

        adaptive = cluster.adapt(minimum=1)
        await adaptive.adapt()

        while len(cluster.scheduler.workers) == 4:
            await asyncio.sleep(0.01)

        names = {ws.name for ws in cluster.scheduler.workers.values()}
        assert names == {"a-1", "a-2"} or names == {"b-1", "b-2"}


@pytest.mark.asyncio
async def test_adapt_cores_memory(cleanup):
    async with LocalCluster(
        0,
        threads_per_worker=2,
        memory_limit="3 GB",
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
    ) as cluster:
        adapt = cluster.adapt(minimum_cores=3, maximum_cores=9)
        assert adapt.minimum == 2
        assert adapt.maximum == 4

        adapt = cluster.adapt(minimum_memory="7GB", maximum_memory="20 GB")
        assert adapt.minimum == 3
        assert adapt.maximum == 6

        adapt = cluster.adapt(
            minimum_cores=1,
            minimum_memory="7GB",
            maximum_cores=10,
            maximum_memory="1 TB",
        )
        assert adapt.minimum == 3
        assert adapt.maximum == 5


def test_adaptive_config():
    with dask.config.set(
        {"distributed.adaptive.minimum": 10, "distributed.adaptive.wait-count": 8}
    ):
        adapt = Adaptive(interval="5s")
        assert adapt.minimum == 10
        assert adapt.maximum == math.inf
        assert adapt.interval == 5
        assert adapt.wait_count == 8


@pytest.mark.asyncio
async def test_update_adaptive(cleanup):
    async with LocalCluster(
        0,
        threads_per_worker=2,
        memory_limit="3 GB",
        scheduler_port=0,
        silence_logs=False,
        processes=False,
        dashboard_address=None,
        asynchronous=True,
    ) as cluster:
        first = cluster.adapt(maxmimum=1)
        second = cluster.adapt(maxmimum=2)
        await asyncio.sleep(0.2)
        assert first.periodic_callback is None
        assert second.periodic_callback.is_running()


@pytest.mark.asyncio
async def test_adaptive_no_memory_limit(cleanup):
    """Make sure that adapt() does not keep creating workers when no memory limit is set."""
    async with LocalCluster(
        n_workers=0, threads_per_worker=1, memory_limit=0, asynchronous=True
    ) as cluster:
        cluster.adapt(minimum=1, maximum=10, interval="1 ms")
        async with Client(cluster, asynchronous=True) as client:
            await client.gather(client.map(slowinc, range(5), delay=0.35))
        assert (
            sum(
                state[1]["n"]
                for state in cluster._adaptive.log
                if state[1]["status"] == "up"
            )
            <= 5
        )


@pytest.mark.asyncio
async def test_scale_needs_to_be_awaited(cleanup):
    """
    This tests that the adaptive class works fine if the scale method uses the
    `sync` method to schedule its task instead of loop.add_callback
    """

    class RequiresAwaitCluster(LocalCluster):
        def scale(self, n):
            # super invocation in the nested function scope is messy
            method = super().scale

            async def _():
                return method(n)

            return self.sync(_)

    async with RequiresAwaitCluster(n_workers=0, asynchronous=True) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            futures = client.map(slowinc, range(5), delay=0.05)
            assert len(cluster.workers) == 0
            cluster.adapt()

            await client.gather(futures)

            del futures
            await async_wait_for(lambda: not cluster.workers, 10)


@pytest.mark.asyncio
async def test_adaptive_stopped():
    """
    We should ensure that the adapt PC is actually stopped once the cluster
    stops.
    """
    async with LocalCluster(n_workers=0, asynchronous=True) as cluster:
        instance = cluster.adapt(interval="10ms")
        assert instance.periodic_callback is not None

        await async_wait_for(lambda: instance.periodic_callback.is_running(), timeout=5)

        pc = instance.periodic_callback

    await async_wait_for(lambda: not pc.is_running(), timeout=5)
