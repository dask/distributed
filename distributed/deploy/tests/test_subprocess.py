from __future__ import annotations

from distributed import Client
from distributed.deploy.subprocess import SubprocessCluster
from distributed.utils_test import gen_test


@gen_test()
async def test_basic():
    async with SubprocessCluster(
        asynchronous=True,
        dashboard_address=":0",
        scheduler_options={"idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
        assert cluster._supports_scaling
        assert "Subprocess" in repr(cluster)


@gen_test()
async def test_n_workers():
    async with SubprocessCluster(
        asynchronous=True, dashboard_address=":0", n_workers=2
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert len(cluster.workers) == 2
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
        assert cluster._supports_scaling
        assert "Subprocess" in repr(cluster)


@gen_test()
async def test_scale_up_and_down():
    async with SubprocessCluster(
        n_workers=0,
        silence_logs=False,
        dashboard_address=":0",
        asynchronous=True,
    ) as cluster:
        async with Client(cluster, asynchronous=True) as c:

            assert not cluster.workers

            cluster.scale(2)
            await c.wait_for_workers(2)
            assert len(cluster.workers) == 2
            assert len(cluster.scheduler.workers) == 2

            cluster.scale(1)
            await cluster

            assert len(cluster.workers) == 1
