import sys

import pytest

import dask

from distributed import Client
from distributed.core import Status
from distributed.deploy.local_env import LocalEnvCluster


@pytest.mark.asyncio
async def test_basic():
    async with LocalEnvCluster(
        sys.executable,
        asynchronous=True,
        scheduler_options={"port": 20000, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        assert len(cluster.workers) == 1
        assert cluster.scheduler.python_executable == sys.executable
        assert cluster.workers[0].python_executable == sys.executable
        assert "LocalEnv" in repr(cluster)
    assert cluster.status == Status.closed


@pytest.mark.asyncio
async def test_job_submission():
    async with LocalEnvCluster(
        sys.executable,
        asynchronous=True,
        scheduler_options={"port": 20001, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11


@pytest.mark.asyncio
async def test_multiple_workers():
    n_workers = 2
    async with LocalEnvCluster(
        sys.executable,
        n_workers=n_workers,
        asynchronous=True,
        scheduler_options={"port": 20002, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        assert len(cluster.workers) == n_workers


@pytest.mark.asyncio
async def test_bad_executable():
    with pytest.raises(Exception):
        async with LocalEnvCluster(
            "/foo/bar/baz/python",
            asynchronous=True,
            scheduler_options={"port": 20003, "idle_timeout": "5s"},
            worker_options={"death_timeout": "5s"},
        ) as cluster:
            assert cluster
        cluster.close()


@pytest.mark.asyncio
async def test_set_env():
    value = 100

    def f():
        return dask.config.get("foo")

    with dask.config.set(foo=value):
        async with LocalEnvCluster(
            sys.executable,
            asynchronous=True,
            scheduler_options={"port": 20004, "idle_timeout": "5s"},
            worker_options={"death_timeout": "5s"},
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                result = await client.submit(f)
                assert result == value
