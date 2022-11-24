from __future__ import annotations

import asyncio
import re
import warnings
from time import sleep

import pytest
import tlz as toolz

import dask
from dask.distributed import Client, Nanny, Scheduler, SpecCluster, Worker

from distributed.compatibility import WINDOWS
from distributed.core import Status
from distributed.deploy.spec import ProcessInterface, close_clusters, run_spec
from distributed.metrics import time
from distributed.utils import is_valid_xml
from distributed.utils_test import gen_cluster, gen_test


class MyWorker(Worker):
    pass


worker_spec = {
    0: {"cls": "dask.distributed.Worker", "options": {"nthreads": 1}},
    1: {"cls": Worker, "options": {"nthreads": 2}},
    "my-worker": {"cls": MyWorker, "options": {"nthreads": 3}},
}
scheduler = {"cls": Scheduler, "options": {"dashboard_address": ":0"}}


@gen_test()
async def test_specification():
    async with SpecCluster(
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        assert cluster.worker_spec == worker_spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(worker_spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].state.nthreads == 1
        assert cluster.workers[1].state.nthreads == 2
        assert cluster.workers["my-worker"].state.nthreads == 3

        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11

        for name in cluster.workers:
            assert cluster.workers[name].name == name


def test_spec_sync(loop):
    worker_spec = {
        0: {"cls": Worker, "options": {"nthreads": 1}},
        1: {"cls": Worker, "options": {"nthreads": 2}},
        "my-worker": {"cls": MyWorker, "options": {"nthreads": 3}},
    }
    with SpecCluster(workers=worker_spec, scheduler=scheduler, loop=loop) as cluster:
        assert cluster.worker_spec == worker_spec

        assert len(cluster.workers) == 3
        assert set(cluster.workers) == set(worker_spec)
        assert isinstance(cluster.workers[0], Worker)
        assert isinstance(cluster.workers[1], Worker)
        assert isinstance(cluster.workers["my-worker"], MyWorker)

        assert cluster.workers[0].state.nthreads == 1
        assert cluster.workers[1].state.nthreads == 2
        assert cluster.workers["my-worker"].state.nthreads == 3

        with Client(cluster, loop=loop) as client:
            assert cluster.loop is cluster.scheduler.loop
            assert cluster.loop is client.loop
            result = client.submit(lambda x: x + 1, 10).result()
            assert result == 11


@pytest.mark.filterwarnings("ignore:There is no current event loop:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:make_current is deprecated:DeprecationWarning")
def test_loop_started_in_constructor(cleanup):
    # test that SpecCluster.__init__ starts a loop in another thread
    cluster = SpecCluster(worker_spec, scheduler=scheduler, loop=None)
    try:
        assert cluster.loop.asyncio_loop.is_running()
    finally:
        with cluster:
            pass


@gen_test()
async def test_repr():
    worker = {"cls": Worker, "options": {"nthreads": 1}}

    class MyCluster(SpecCluster):
        pass

    async with MyCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        assert "MyCluster" in str(cluster)


@gen_test()
async def test_scale():
    worker = {"cls": Worker, "options": {"nthreads": 1}}
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

        # Can use with await
        await cluster.scale(2)
        await cluster
        assert len(cluster.workers) == 2


@pytest.mark.slow
@gen_test()
async def test_adaptive_killed_worker():
    with dask.config.set({"distributed.deploy.lost-worker-timeout": 0.1}):

        async with SpecCluster(
            asynchronous=True,
            worker={"cls": Nanny, "options": {"nthreads": 1}},
            scheduler=scheduler,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                # Scale up a cluster with 1 worker.
                cluster.adapt(minimum=1, maximum=1)
                while not cluster.workers:
                    await asyncio.sleep(0.01)

                future = client.submit(sleep, 0.1)

                # Kill the only worker.
                [worker_id] = cluster.workers
                await cluster.workers[worker_id].kill()

                # Wait for the worker to re-spawn and finish sleeping.
                await future


@gen_test()
async def test_unexpected_closed_worker():
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "10ms"}):
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

            # Close one
            await list(cluster.workers.values())[0].close()
            start = time()
            while len(cluster.workers) > 1:  # wait for messages to flow around
                await asyncio.sleep(0.01)
                assert time() < start + 2
            assert len(cluster.workers) == 1
            assert len(cluster.worker_spec) == 2

            await cluster
            assert len(cluster.workers) == 2


@gen_test(timeout=60)
async def test_restart():
    """Regression test for https://github.com/dask/distributed/issues/3062"""
    worker = {"cls": Nanny, "options": {"nthreads": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            cluster.scale(2)
            await cluster
            assert len(cluster.workers) == 2
            await client.restart()
            while len(cluster.workers) < 2:
                await asyncio.sleep(0.01)


@pytest.mark.skipif(WINDOWS, reason="HTTP Server doesn't close out")
@gen_test()
async def test_broken_worker():
    class BrokenWorkerException(Exception):
        pass

    class BrokenWorker(Worker):
        def __await__(self):
            async def _():
                self.status = Status.closed
                raise BrokenWorkerException("Worker Broken")

            return _().__await__()

    cluster = SpecCluster(
        asynchronous=True,
        workers={"good": {"cls": Worker}, "bad": {"cls": BrokenWorker}},
        scheduler=scheduler,
    )
    try:
        with pytest.raises(BrokenWorkerException, match=r"Worker Broken"):
            async with cluster:
                pass
    finally:
        # FIXME: SpecCluster leaks if SpecCluster.__aenter__ raises
        await cluster.close()


@pytest.mark.skipif(WINDOWS, reason="HTTP Server doesn't close out")
def test_spec_close_clusters(loop):
    workers = {0: {"cls": Worker}}
    cluster = SpecCluster(workers=workers, scheduler=scheduler, loop=loop)
    assert cluster in SpecCluster._instances
    close_clusters()
    assert cluster.status == Status.closed


@gen_test()
async def test_new_worker_spec():
    class MyCluster(SpecCluster):
        def new_worker_spec(self):
            i = len(self.worker_spec)
            return {i: {"cls": Worker, "options": {"nthreads": i + 1}}}

    async with MyCluster(asynchronous=True, scheduler=scheduler) as cluster:
        cluster.scale(3)
        for i in range(3):
            assert cluster.worker_spec[i]["options"]["nthreads"] == i + 1


@gen_test()
async def test_nanny_port():
    workers = {0: {"cls": Nanny, "options": {"port": 9200}}}
    async with SpecCluster(scheduler=scheduler, workers=workers, asynchronous=True):
        pass


@gen_test()
async def test_spec_process():
    proc = ProcessInterface()
    assert proc.status == Status.created
    await proc
    assert proc.status == Status.running
    await proc.close()
    assert proc.status == Status.closed


@gen_test()
async def test_logs():
    worker = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster

        logs = await cluster.get_logs()
        assert isinstance(logs, dict)
        assert all(isinstance(log, str) for log in logs)
        assert is_valid_xml("<div>" + logs._repr_html_() + "</div>")
        assert "Scheduler" in logs
        for worker in cluster.scheduler.workers:
            assert worker in logs

        assert "Registered" in str(logs)

        logs = await cluster.get_logs(cluster=True, scheduler=False, workers=False)
        assert list(logs) == ["Cluster"]

        logs = await cluster.get_logs(cluster=False, scheduler=True, workers=False)
        assert list(logs) == ["Scheduler"]

        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=False)
        assert list(logs) == []

        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=True)
        assert set(logs) == set(cluster.scheduler.workers)

        w = toolz.first(cluster.scheduler.workers)
        logs = await cluster.get_logs(cluster=False, scheduler=False, workers=[w])
        assert set(logs) == {w}


@gen_test()
async def test_scheduler_info():
    async with SpecCluster(
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        assert (
            cluster.scheduler_info["id"] == cluster.scheduler.id
        )  # present at startup

        start = time()  # wait for all workers
        while len(cluster.scheduler_info["workers"]) < len(cluster.workers):
            await asyncio.sleep(0.01)
            assert time() < start + 1

        assert set(cluster.scheduler.identity()["workers"]) == set(
            cluster.scheduler_info["workers"]
        )
        assert (
            cluster.scheduler.identity()["services"]
            == cluster.scheduler_info["services"]
        )
        assert len(cluster.scheduler_info["workers"]) == len(cluster.workers)


@gen_test()
async def test_dashboard_link():
    async with SpecCluster(
        workers=worker_spec,
        scheduler={"cls": Scheduler, "options": {"dashboard_address": ":12345"}},
        asynchronous=True,
    ) as cluster:
        assert "12345" in cluster.dashboard_link


@gen_test()
async def test_widget():
    async with SpecCluster(
        workers=worker_spec,
        scheduler=scheduler,
        asynchronous=True,
        worker={"cls": Worker, "options": {"nthreads": 1}},
    ) as cluster:

        start = time()  # wait for all workers
        while len(cluster.scheduler_info["workers"]) < len(cluster.worker_spec):
            await asyncio.sleep(0.01)
            assert time() < start + 1

        cluster.scale(5)
        assert "3 / 5" in cluster._scaling_status()


@gen_test()
async def test_scale_cores_memory():
    async with SpecCluster(
        scheduler=scheduler,
        worker={"cls": Worker, "options": {"nthreads": 1}},
        asynchronous=True,
    ) as cluster:
        cluster.scale(cores=2)
        assert len(cluster.worker_spec) == 2
        with pytest.raises(ValueError) as info:
            cluster.scale(memory="5GB")

        assert "memory" in str(info.value)


@gen_test()
async def test_ProcessInterfaceValid():
    async with SpecCluster(
        scheduler=scheduler, worker={"cls": ProcessInterface}, asynchronous=True
    ) as cluster:
        cluster.scale(2)
        await cluster
        assert len(cluster.worker_spec) == len(cluster.workers) == 2

        cluster.scale(1)
        await cluster
        assert len(cluster.worker_spec) == len(cluster.workers) == 1


class MultiWorker(Worker, ProcessInterface):
    def __init__(self, *args, n=1, name=None, nthreads=None, **kwargs):
        self.workers = [
            Worker(
                *args, name=str(name) + "-" + str(i), nthreads=nthreads // n, **kwargs
            )
            for i in range(n)
        ]
        self._startup_lock = asyncio.Lock()

    @property
    def status(self):
        return self.workers[0].status

    @status.setter
    def status(self, value):
        raise NotImplementedError()

    def __str__(self):
        return "<MultiWorker n=%d>" % len(self.workers)

    __repr__ = __str__

    async def start(self):
        await asyncio.gather(*self.workers)

    async def close(self):
        await asyncio.gather(*(w.close() for w in self.workers))


@gen_test()
async def test_MultiWorker():
    async with SpecCluster(
        scheduler=scheduler,
        worker={
            "cls": MultiWorker,
            "options": {"n": 2, "nthreads": 4, "memory_limit": "4 GB"},
            "group": ["-0", "-1"],
        },
        asynchronous=True,
    ) as cluster:
        s = cluster.scheduler
        async with Client(cluster, asynchronous=True) as client:
            cluster.scale(2)
            await cluster
            assert len(cluster.worker_spec) == 2
            await client.wait_for_workers(4)
            while len(cluster.scheduler_info["workers"]) < 4:
                await asyncio.sleep(0.01)

            while "workers=4" not in repr(cluster):
                await asyncio.sleep(0.1)

            workers_line = re.search("(Workers.+)", cluster._repr_html_()).group(1)
            assert re.match("Workers.*4", workers_line)

            cluster.scale(1)
            await cluster
            assert len(s.workers) == 2

            cluster.scale(memory="6GB")
            await cluster
            assert len(cluster.worker_spec) == 2
            assert len(s.workers) == 4
            assert cluster.plan == {ws.name for ws in s.workers.values()}

            cluster.scale(cores=10)
            await cluster
            assert len(cluster.workers) == 3

            adapt = cluster.adapt(minimum=0, maximum=4)

            for _ in range(adapt.wait_count):  # relax down to 0 workers
                await adapt.adapt()
            await cluster
            assert not s.workers

            future = client.submit(lambda x: x + 1, 10)
            await future
            assert len(cluster.workers) == 1


@gen_cluster(client=True, nthreads=[])
async def test_run_spec(c, s):
    workers = await run_spec(worker_spec, s.address)
    await c.wait_for_workers(len(worker_spec))
    await asyncio.gather(*(w.close() for w in workers.values()))
    assert not s.workers
    await asyncio.gather(*(w.finished() for w in workers.values()))


@gen_test()
async def test_run_spec_cluster_worker_names():
    worker = {"cls": Worker, "options": {"nthreads": 1}}

    class MyCluster(SpecCluster):
        def _new_worker_name(self, worker_number):
            return f"prefix-{self.name}-{worker_number}-suffix"

    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster
        worker_names = [0, 1]
        assert list(cluster.worker_spec) == worker_names
        assert sorted(list(cluster.workers)) == worker_names

    async with MyCluster(
        asynchronous=True, scheduler=scheduler, worker=worker, name="test-name"
    ) as cluster:
        worker_names = ["prefix-test-name-0-suffix", "prefix-test-name-1-suffix"]
        cluster.scale(2)
        await cluster
        assert list(cluster.worker_spec) == worker_names
        assert sorted(list(cluster.workers)) == worker_names


@gen_test()
async def test_bad_close():
    with warnings.catch_warnings(record=True) as record:
        cluster = SpecCluster(
            workers=worker_spec, scheduler=scheduler, asynchronous=True
        )
        await cluster.close()

    assert not record
