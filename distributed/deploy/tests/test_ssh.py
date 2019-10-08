import pytest

pytest.importorskip("asyncssh")

from dask.distributed import Client
from distributed.deploy.ssh import SSHCluster
from distributed.utils_test import loop  # noqa: F401


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
        worker_options={"nthreads": 2, "memory_limit": "2 GiB", "death_timeout": "5s"},
        scheduler_options={"idle_timeout": "5s", "port": 0},
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert (
                await client.run_on_scheduler(
                    lambda dask_scheduler: dask_scheduler.idle_timeout
                )
            ) == 5
            d = client.scheduler_info()["workers"]
            assert all(v["nthreads"] == 2 for v in d.values())


@pytest.mark.avoid_travis
def test_defer_to_old(loop):
    with pytest.warns(Warning):
        with SSHCluster(
            scheduler_addr="127.0.0.1",
            scheduler_port=7437,
            worker_addrs=["127.0.0.1", "127.0.0.1"],
        ) as c:
            from distributed.deploy.old_ssh import SSHCluster as OldSSHCluster

            assert isinstance(c, OldSSHCluster)
