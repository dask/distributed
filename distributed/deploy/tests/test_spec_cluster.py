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


@gen_test()
async def test_broken_worker():
    class BrokenWorkerException(Exception):
        pass

    class BrokenWorker(Worker):
        def __await__(self):
            async def _():
                raise BrokenWorkerException("Worker Broken")

            return _().__await__()

    cluster = SpecCluster(
        asynchronous=True,
        workers={"good": {"cls": Worker}, "bad": {"cls": BrokenWorker}},
        scheduler=scheduler,
    )
    with pytest.raises(BrokenWorkerException, match=r"Worker Broken"):
        async with cluster:
            pass


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
async def test_get_logs():
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
async def test_logs_deprecated():
    async with SpecCluster(asynchronous=True, scheduler=scheduler) as cluster:
        with pytest.warns(FutureWarning, match="get_logs"):
            logs = await cluster.logs()
    assert logs["Scheduler"]


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

    async def start_unsafe(self):
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
            # Scale to 4 workers (2 specs with 2 workers each)
            cluster.scale(4)
            await cluster
            assert len(cluster.worker_spec) == 2
            await client.wait_for_workers(4)
            while len(cluster.scheduler_info["workers"]) < 4:
                await asyncio.sleep(0.01)

            while "workers=4" not in repr(cluster):
                await asyncio.sleep(0.1)

            workers_line = re.search("(Workers.+)", cluster._repr_html_()).group(1)
            assert re.match("Workers.*4", workers_line)

            # Scale to 2 workers (1 spec with 2 workers)
            cluster.scale(2)
            await cluster
            assert len(s.workers) == 2

            # Scale to 6 GB memory: 4GB per spec, conservatively scales to 1 spec = 4GB
            # (rounds DOWN to avoid exceeding 6GB limit)
            cluster.scale(memory="6GB")
            await cluster
            assert len(cluster.worker_spec) == 1
            assert len(s.workers) == 2
            assert cluster.plan == {ws.name for ws in s.workers.values()}

            # Scale to 10 cores: 4 cores per spec, conservatively scales to 2 specs = 8 cores
            # (rounds DOWN to avoid exceeding 10 cores limit)
            cluster.scale(cores=10)
            await cluster
            assert len(cluster.workers) == 2

            # Adaptive with maximum=4 means maximum 4 workers = 2 specs
            adapt = cluster.adapt(minimum=0, maximum=4)

            for _ in range(adapt.wait_count):  # relax down to 0 workers
                await adapt.adapt()
            await cluster
            assert not s.workers

            # Submit work - adaptive will request workers based on workload
            future = client.submit(lambda x: x + 1, 10)
            await future
            # With 2-worker specs and conservative scaling with minimum viability:
            # When adaptive requests 1 worker, scale creates 1 spec = 2 workers
            # (special case: creates at least 1 spec when scaling from 0)
            assert len(cluster.workers) == 1  # 1 spec created


@gen_test()
async def test_grouped_worker_death_removes_spec():
    """Test that when a single worker in a group dies, the entire spec is removed."""
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "100ms"}):
        async with SpecCluster(
            scheduler=scheduler,
            worker={
                "cls": MultiWorker,
                "options": {"n": 2, "nthreads": 2, "memory_limit": "2 GB"},
                "group": ["-0", "-1"],
            },
            asynchronous=True,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                # Scale to 4 workers (2 specs with 2 workers each)
                cluster.scale(4)
                await cluster
                assert len(cluster.worker_spec) == 2
                await client.wait_for_workers(4)

                # Get the spec names
                spec_names = list(cluster.worker_spec.keys())
                assert len(spec_names) == 2

                # Get worker names for the first spec
                first_spec_name = spec_names[0]
                worker_names = cluster._spec_name_to_worker_names(first_spec_name)
                assert len(worker_names) == 2

                # Kill one worker from the first group
                worker_to_kill = list(worker_names)[0]
                worker_addr = [
                    addr
                    for addr, ws in cluster.scheduler.workers.items()
                    if ws.name == worker_to_kill
                ][0]

                # Simulate abrupt worker death (like HPC pre-emption)
                await cluster.scheduler.remove_worker(
                    address=worker_addr, close=False, stimulus_id="test"
                )

                # Wait for lost-worker-timeout
                await asyncio.sleep(0.2)

                # The entire spec should be removed (not just the one worker)
                assert first_spec_name not in cluster.worker_spec
                # The other spec should still exist
                assert spec_names[1] in cluster.worker_spec


@gen_cluster(client=True, nthreads=[])
async def test_run_spec(c, s):
    workers = await run_spec(worker_spec, s.address)
    await c.wait_for_workers(len(worker_spec))
    await asyncio.gather(*(w.close() for w in workers.values()))
    assert not s.workers
    await asyncio.gather(*(w.finished() for w in workers.values()))


@gen_test()
async def test_run_spec_cluster_custom_spec_names():
    """Test that _new_spec_name() can be overridden to customize spec names"""
    worker = {"cls": Worker, "options": {"nthreads": 1}}

    class MyCluster(SpecCluster):
        def _new_spec_name(self, spec_number):
            return f"prefix-{self.name}-{spec_number}-suffix"

    async with SpecCluster(
        asynchronous=True, scheduler=scheduler, worker=worker
    ) as cluster:
        cluster.scale(2)
        await cluster
        spec_names = ["0", "1"]
        assert list(cluster.worker_spec) == spec_names
        assert sorted(list(cluster.workers)) == spec_names

    async with MyCluster(
        asynchronous=True, scheduler=scheduler, worker=worker, name="test-name"
    ) as cluster:
        spec_names = ["prefix-test-name-0-suffix", "prefix-test-name-1-suffix"]
        cluster.scale(2)
        await cluster
        assert list(cluster.worker_spec) == spec_names
        assert sorted(list(cluster.workers)) == spec_names


@gen_test()
async def test_bad_close():
    with warnings.catch_warnings(record=True) as record:
        cluster = SpecCluster(
            workers=worker_spec, scheduler=scheduler, asynchronous=True
        )
        await cluster.close()

    assert not record


@gen_test()
async def test_shutdown_scheduler_disabled():
    async with SpecCluster(
        workers=worker_spec,
        scheduler=scheduler,
        asynchronous=True,
        shutdown_scheduler=False,
    ) as cluster:
        s = cluster.scheduler
        assert isinstance(s, Scheduler)

    assert s.status == Status.running


@gen_test()
async def test_shutdown_scheduler():
    async with SpecCluster(
        workers=worker_spec, scheduler=scheduler, asynchronous=True
    ) as cluster:
        s = cluster.scheduler
        assert isinstance(s, Scheduler)

    assert s.status == Status.closed


@gen_test()
async def test_new_spec_name_returns_string():
    """Test that _new_spec_name() returns strings, not integers.

    Spec names (keys in worker_spec dict) should always be strings, whether
    auto-generated or user-provided. This ensures type consistency and
    eliminates int/str conversion issues throughout the codebase.
    """
    async with SpecCluster(
        workers={}, scheduler=scheduler, asynchronous=True
    ) as cluster:
        # Test that _new_spec_name returns a string
        name = cluster._new_spec_name(0)
        assert isinstance(name, str), f"Expected str, got {type(name).__name__}"
        assert name == "0"

        name = cluster._new_spec_name(42)
        assert isinstance(name, str), f"Expected str, got {type(name).__name__}"
        assert name == "42"


@gen_test()
async def test_worker_spec_keys_are_strings():
    """Test that worker_spec keys are strings after scaling.

    When workers are added via scale(), the resulting spec names (keys in
    worker_spec dict) should be strings to maintain consistency with
    user-provided specs.
    """
    worker_template = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        workers={}, scheduler=scheduler, worker=worker_template, asynchronous=True
    ) as cluster:
        # Scale up to create auto-generated worker specs
        cluster.scale(2)
        await cluster

        # All keys in worker_spec should be strings
        for key in cluster.worker_spec.keys():
            assert isinstance(
                key, str
            ), f"Expected str key, got {type(key).__name__}: {key}"


@gen_test()
async def test_spec_name_to_worker_names_regular():
    """Test _spec_name_to_worker_names() with regular (non-grouped) workers."""
    worker_template = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        workers={}, scheduler=scheduler, worker=worker_template, asynchronous=True
    ) as cluster:
        # Scale to create regular workers
        cluster.scale(2)
        await cluster

        # Regular workers: spec name == worker name (1:1)
        assert cluster._spec_name_to_worker_names("0") == {"0"}
        assert cluster._spec_name_to_worker_names("1") == {"1"}

        # Non-existent spec returns empty set
        assert cluster._spec_name_to_worker_names("nonexistent") == set()


@gen_test()
async def test_spec_name_to_worker_names_grouped():
    """Test _spec_name_to_worker_names() with grouped workers."""
    async with SpecCluster(
        workers={
            "0": {
                "cls": Worker,
                "options": {"nthreads": 1},
                "group": ["-0", "-1", "-2"],
            },
            "1": {
                "cls": Worker,
                "options": {"nthreads": 1},
                "group": ["-a", "-b"],
            },
        },
        scheduler=scheduler,
        asynchronous=True,
    ) as cluster:
        # Grouped workers: one spec name → multiple worker names
        assert cluster._spec_name_to_worker_names("0") == {"0-0", "0-1", "0-2"}
        assert cluster._spec_name_to_worker_names("1") == {"1-a", "1-b"}


@gen_test()
async def test_worker_name_to_spec_name_regular():
    """Test _worker_name_to_spec_name() with regular (non-grouped) workers."""
    worker_template = {"cls": Worker, "options": {"nthreads": 1}}
    async with SpecCluster(
        workers={}, scheduler=scheduler, worker=worker_template, asynchronous=True
    ) as cluster:
        # Scale to create regular workers
        cluster.scale(2)
        await cluster

        # Regular workers: worker name == spec name
        assert cluster._worker_name_to_spec_name("0") == "0"
        assert cluster._worker_name_to_spec_name("1") == "1"

        # Non-existent worker returns None
        assert cluster._worker_name_to_spec_name("nonexistent") is None


@gen_test()
async def test_worker_name_to_spec_name_grouped():
    """Test _worker_name_to_spec_name() with grouped workers."""
    async with SpecCluster(
        workers={
            "0": {
                "cls": Worker,
                "options": {"nthreads": 1},
                "group": ["-0", "-1", "-2"],
            },
            "1": {
                "cls": Worker,
                "options": {"nthreads": 1},
                "group": ["-a", "-b"],
            },
        },
        scheduler=scheduler,
        asynchronous=True,
    ) as cluster:
        # All workers from group "0" map back to spec "0"
        assert cluster._worker_name_to_spec_name("0-0") == "0"
        assert cluster._worker_name_to_spec_name("0-1") == "0"
        assert cluster._worker_name_to_spec_name("0-2") == "0"

        # All workers from group "1" map back to spec "1"
        assert cluster._worker_name_to_spec_name("1-a") == "1"
        assert cluster._worker_name_to_spec_name("1-b") == "1"

        # Non-existent worker returns None
        assert cluster._worker_name_to_spec_name("nonexistent") is None


@gen_test()
async def test_worker_name_to_spec_name_mixed():
    """Test _worker_name_to_spec_name() with mixed regular and grouped workers."""
    async with SpecCluster(
        workers={
            "regular": {"cls": Worker, "options": {"nthreads": 1}},
            "grouped": {
                "cls": Worker,
                "options": {"nthreads": 1},
                "group": ["-0", "-1"],
            },
        },
        scheduler=scheduler,
        asynchronous=True,
    ) as cluster:
        # Regular worker
        assert cluster._worker_name_to_spec_name("regular") == "regular"

        # Grouped workers
        assert cluster._worker_name_to_spec_name("grouped-0") == "grouped"
        assert cluster._worker_name_to_spec_name("grouped-1") == "grouped"


@gen_test()
async def test_unexpected_close_whole_worker_group():
    """Test that when all workers in a group die abruptly, the spec is removed and recreated."""
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "100ms"}):
        async with SpecCluster(
            scheduler=scheduler,
            worker={
                "cls": MultiWorker,
                "options": {"n": 2, "nthreads": 2, "memory_limit": "2 GB"},
                "group": ["-0", "-1"],
            },
            asynchronous=True,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                # Scale to 4 workers (2 specs with 2 workers each)
                cluster.scale(4)
                await cluster
                assert len(cluster.worker_spec) == 2
                await client.wait_for_workers(4)

                # Get the spec names
                spec_names = list(cluster.worker_spec.keys())
                assert len(spec_names) == 2

                # Get all worker names for the first spec
                first_spec_name = spec_names[0]
                worker_names = cluster._spec_name_to_worker_names(first_spec_name)
                assert len(worker_names) == 2

                # Kill all workers from the first group (simulate HPC job kill)
                for worker_name in worker_names:
                    worker_addr = [
                        addr
                        for addr, ws in cluster.scheduler.workers.items()
                        if ws.name == worker_name
                    ][0]
                    await cluster.scheduler.remove_worker(
                        address=worker_addr, close=False, stimulus_id="test"
                    )

                # Wait for lost-worker-timeout
                await asyncio.sleep(0.2)

                # The entire spec should be removed
                assert first_spec_name not in cluster.worker_spec
                # The other spec should still exist
                assert spec_names[1] in cluster.worker_spec
                # Should have 1 spec remaining
                assert len(cluster.worker_spec) == 1

                # With adaptive enabled (minimum=4 workers), the cluster should recreate the missing spec
                cluster.adapt(minimum=4, maximum=4)
                await client.wait_for_workers(4)

                # Should have 2 specs again (but with a new spec name for the recreated one)
                assert len(cluster.worker_spec) == 2
                # Old spec name should not exist
                assert first_spec_name not in cluster.worker_spec
                # Should have 4 workers total
                assert len(cluster.scheduler.workers) == 4


@gen_test()
async def test_scale_down_with_grouped_workers():
    """Test that scale_down correctly maps worker names to spec names."""
    async with SpecCluster(
        scheduler=scheduler,
        worker={
            "cls": MultiWorker,
            "options": {"n": 2, "nthreads": 2, "memory_limit": "2 GB"},
            "group": ["-0", "-1"],
        },
        asynchronous=True,
    ) as cluster:
        # Scale to 4 workers (2 specs with 2 workers each)
        cluster.scale(4)
        await cluster
        assert len(cluster.worker_spec) == 2

        # Get spec names
        spec_names = list(cluster.worker_spec.keys())
        first_spec_name = spec_names[0]

        # Get worker names for the first spec
        worker_names = cluster._spec_name_to_worker_names(first_spec_name)
        worker_names_list = list(worker_names)

        # Call scale_down with actual worker names (what scheduler knows)
        await cluster.scale_down(worker_names_list)

        # The first spec should be removed
        assert first_spec_name not in cluster.worker_spec
        # The second spec should still exist
        assert spec_names[1] in cluster.worker_spec
        # Should have 1 spec and 2 workers left
        assert len(cluster.worker_spec) == 1


@gen_test()
async def test_mixed_regular_and_grouped_workers():
    """Test cluster with both regular and grouped worker specs."""
    with dask.config.set({"distributed.deploy.lost-worker-timeout": "100ms"}):
        async with SpecCluster(
            workers={
                "regular-1": {"cls": Worker, "options": {"nthreads": 2}},
                "regular-2": {"cls": Worker, "options": {"nthreads": 2}},
                "grouped": {
                    "cls": MultiWorker,
                    "options": {"n": 2, "nthreads": 2},
                    "group": ["-0", "-1"],
                },
            },
            scheduler=scheduler,
            asynchronous=True,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                # Should have 3 specs, 4 workers total (2 regular + 2 grouped)
                await client.wait_for_workers(4)
                assert len(cluster.worker_spec) == 3

                # Test regular worker failure - spec should remain
                regular_addr = [
                    addr
                    for addr, ws in cluster.scheduler.workers.items()
                    if ws.name == "regular-1"
                ][0]
                await cluster.scheduler.remove_worker(
                    address=regular_addr, close=False, stimulus_id="test"
                )
                await asyncio.sleep(0.2)

                # Regular worker spec should still exist (cluster can recreate it)
                assert "regular-1" in cluster.worker_spec
                assert len(cluster.worker_spec) == 3

                # Test grouped worker failure - entire spec should be removed
                grouped_worker_names = cluster._spec_name_to_worker_names("grouped")
                one_grouped_worker = list(grouped_worker_names)[0]
                grouped_addr = [
                    addr
                    for addr, ws in cluster.scheduler.workers.items()
                    if ws.name == one_grouped_worker
                ][0]
                await cluster.scheduler.remove_worker(
                    address=grouped_addr, close=False, stimulus_id="test"
                )
                await asyncio.sleep(0.2)

                # Grouped spec should be removed entirely
                assert "grouped" not in cluster.worker_spec
                # Regular specs should still exist
                assert "regular-1" in cluster.worker_spec
                assert "regular-2" in cluster.worker_spec
                assert len(cluster.worker_spec) == 2
