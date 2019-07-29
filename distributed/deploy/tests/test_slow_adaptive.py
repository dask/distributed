import pytest

from dask.distributed import Worker, Scheduler, SpecCluster, Client
from distributed.utils_test import cleanup  # noqa: F401
from distributed.metrics import time


class SlowWorker(object):
    def __init__(self, *args, delay=0, **kwargs):
        self.worker = Worker(*args, **kwargs)
        self.delay = delay
        self.status = None

    @property
    def address(self):
        return self.worker.address

    def __await__(self):
        async def now():
            if self.status != "running":
                self.worker.loop.call_later(self.delay, self.worker.start)
                self.status = "running"
            return self

        return now().__await__()

    async def close(self):
        await self.worker.close()
        self.status = "closed"


scheduler = {"cls": Scheduler, "options": {"port": 0}}


@pytest.mark.asyncio
async def test_startup(cleanup):
    start = time()
    async with SpecCluster(
        scheduler=scheduler,
        workers={
            0: {"cls": Worker, "options": {}},
            1: {"cls": SlowWorker, "options": {"delay": 5}},
            2: {"cls": SlowWorker, "options": {"delay": 0}},
        },
        asynchronous=True,
    ) as cluster:
        assert len(cluster.workers) == len(cluster.worker_spec) == 3
        assert time() < start + 5
        assert 1 <= len(cluster.scheduler_info["workers"]) <= 2

        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(n_workers=2)
