from dask.distributed import SpecCluster, Worker, Client
from distributed.utils_test import loop  # noqa: F401
import pytest


class MyWorker(Worker):
    pass


@pytest.mark.asyncio
async def test_specification():

    spec = {
        0: {"cls": Worker, "options": {"ncores": 1}},
        1: {"cls": Worker, "options": {"ncores": 2}},
        "my-worker": {"cls": MyWorker, "options": {"ncores": 3}},
    }
    async with SpecCluster(workers=spec, asynchronous=True) as cluster:
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
    spec = {
        0: {"cls": Worker, "options": {"ncores": 1}},
        1: {"cls": Worker, "options": {"ncores": 2}},
        "my-worker": {"cls": MyWorker, "options": {"ncores": 3}},
    }
    with SpecCluster(workers=spec, loop=loop) as cluster:
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
            result = client.submit(lambda x: x + 1, 10).result()
            assert result == 11
