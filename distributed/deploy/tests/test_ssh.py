import pytest

pytest.importorskip("asyncssh")

import sys

import dask

from distributed import Client
from distributed.compatibility import MACOS, WINDOWS
from distributed.deploy.ssh import SSHCluster
from distributed.utils_test import loop  # noqa: F401

pytestmark = [
    pytest.mark.xfail(MACOS, reason="very high flakiness; see distributed/issues/4543"),
    pytest.mark.skipif(WINDOWS, reason="no CI support; see distributed/issues/4509"),
]


def test_ssh_hosts_None():
    with pytest.raises(ValueError):
        SSHCluster(hosts=None)


def test_ssh_hosts_empty_list():
    with pytest.raises(ValueError):
        SSHCluster(hosts=[])


@pytest.mark.asyncio
async def test_basic():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_options=dict(known_hosts=None),
        asynchronous=True,
        scheduler_options={"port": 0, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        assert len(cluster.workers) == 2
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
        assert not cluster._supports_scaling

        assert "SSH" in repr(cluster)


@pytest.mark.asyncio
async def test_keywords():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_options=dict(known_hosts=None),
        asynchronous=True,
        worker_options={
            "nprocs": 2,  # nprocs checks custom arguments with cli_keywords
            "nthreads": 2,
            "memory_limit": "2 GiB",
            "death_timeout": "5s",
        },
        scheduler_options={"idle_timeout": "10s", "port": 0},
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert (
                await client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.idle_timeout
                )
            ) == 10
            d = client.scheduler_info()["workers"]
            assert all(v["nthreads"] == 2 for v in d.values())


@pytest.mark.avoid_ci
def test_defer_to_old(loop):
    with pytest.warns(Warning):
        with SSHCluster(
            scheduler_addr="127.0.0.1",
            scheduler_port=7437,
            worker_addrs=["127.0.0.1", "127.0.0.1"],
        ) as c:
            from distributed.deploy.old_ssh import SSHCluster as OldSSHCluster

            assert isinstance(c, OldSSHCluster)


@pytest.mark.avoid_ci
def test_old_ssh_with_local_dir(loop):
    with pytest.warns(Warning):
        from distributed.deploy.old_ssh import SSHCluster as OldSSHCluster

        with OldSSHCluster(
            scheduler_addr="127.0.0.1",
            scheduler_port=7437,
            worker_addrs=["127.0.0.1", "127.0.0.1"],
            local_directory="/tmp",
        ) as c:
            assert len(c.workers) == 2
            with Client(c) as client:
                result = client.submit(lambda x: x + 1, 10)
                result = result.result()
                assert result == 11


@pytest.mark.asyncio
async def test_config_inherited_by_subprocess(loop):
    def f(x):
        return dask.config.get("foo") + 1

    with dask.config.set(foo=100):
        async with SSHCluster(
            ["127.0.0.1"] * 2,
            connect_options=dict(known_hosts=None),
            asynchronous=True,
            scheduler_options={"port": 0, "idle_timeout": "5s"},
            worker_options={"death_timeout": "5s"},
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                result = await client.submit(f, 1)
                assert result == 101


@pytest.mark.asyncio
async def test_unimplemented_options():
    with pytest.raises(Exception):
        async with SSHCluster(
            ["127.0.0.1"] * 3,
            connect_kwargs=dict(known_hosts=None),
            asynchronous=True,
            worker_kwargs={
                "nthreads": 2,
                "memory_limit": "2 GiB",
                "death_timeout": "5s",
                "unimplemented_option": 2,
            },
            scheduler_kwargs={"idle_timeout": "5s", "port": 0},
        ) as cluster:
            assert cluster


@pytest.mark.asyncio
async def test_list_of_connect_options():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_options=[dict(known_hosts=None)] * 3,
        asynchronous=True,
        scheduler_options={"port": 0, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
    ) as cluster:
        assert len(cluster.workers) == 2
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
        assert not cluster._supports_scaling

        assert "SSH" in repr(cluster)


@pytest.mark.asyncio
async def test_list_of_connect_options_raises():
    with pytest.raises(RuntimeError):
        async with SSHCluster(
            ["127.0.0.1"] * 3,
            connect_options=[dict(known_hosts=None)] * 4,  # Mismatch in length 4 != 3
            asynchronous=True,
            scheduler_options={"port": 0, "idle_timeout": "5s"},
            worker_options={"death_timeout": "5s"},
        ) as _:
            pass


@pytest.mark.asyncio
async def test_remote_python():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_options=[dict(known_hosts=None)] * 3,
        asynchronous=True,
        scheduler_options={"port": 0, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
        remote_python=sys.executable,
    ) as cluster:
        assert cluster.workers[0].remote_python == sys.executable


@pytest.mark.asyncio
async def test_remote_python_as_dict():
    async with SSHCluster(
        ["127.0.0.1"] * 3,
        connect_options=[dict(known_hosts=None)] * 3,
        asynchronous=True,
        scheduler_options={"port": 0, "idle_timeout": "5s"},
        worker_options={"death_timeout": "5s"},
        remote_python=[sys.executable] * 3,
    ) as cluster:
        assert cluster.workers[0].remote_python == sys.executable


@pytest.mark.asyncio
async def test_list_of_remote_python_raises():
    with pytest.raises(RuntimeError):
        async with SSHCluster(
            ["127.0.0.1"] * 3,
            connect_options=[dict(known_hosts=None)] * 3,
            asynchronous=True,
            scheduler_options={"port": 0, "idle_timeout": "5s"},
            worker_options={"death_timeout": "5s"},
            remote_python=[sys.executable] * 4,  # Mismatch in length 4 != 3
        ) as _:
            pass
