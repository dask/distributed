from dask.distributed import SpecCluster, Worker, Client, Scheduler
from distributed.utils_test import loop  # noqa: F401
import pytest


class MyWorker(Worker):
    pass


spec = {
    0: {"cls": Worker, "options": {"ncores": 1}},
    1: {"cls": Worker, "options": {"ncores": 2}},
    "my-worker": {"cls": MyWorker, "options": {"ncores": 3}},
}
scheduler = {"cls": Scheduler, "options": {"port": 0}}


@pytest.mark.asyncio
async def test_specification():
    async with SpecCluster(
        workers=spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        assert cluster.worker_spec is spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].ncores == 1
        assert cluster.workers[1].ncores == 2
        assert cluster.workers["my-worker"].ncores == 3

        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11

        for name in cluster.workers:
            assert cluster.workers[name].name == name


def test_spec_sync(loop):
    with SpecCluster(workers=spec, scheduler=scheduler, loop=loop) as cluster:
        assert cluster.worker_spec is spec
        assert cluster.worker_spec is spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].ncores == 1
        assert cluster.workers[1].ncores == 2
        assert cluster.workers["my-worker"].ncores == 3

        with Client(cluster, loop=loop) as client:
            assert cluster.loop is cluster.scheduler.loop
            assert cluster.loop is client.loop
            result = client.submit(lambda x: x + 1, 10).result()
            assert result == 11


def test_loop_started():
    cluster = SpecCluster(spec)


@pytest.mark.asyncio
async def test_scale():
    worker = {"cls": Worker, "options": {"ncores": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        assert not cluster.workers
        assert not cluster.worker_spec

        # Scale up
        cluster.scale(2)
        assert not cluster.workers
        assert cluster.worker_spec

        await cluster
        assert len(cluster.workers) == 2

        # Scale down
        cluster.scale(1)
        assert len(cluster.workers) == 2

        await cluster
        assert len(cluster.workers) == 1
