from __future__ import annotations

import pytest

from distributed import LocalCluster
from distributed.deploy.cluster import Cluster
from distributed.utils_test import gen_test


@gen_test()
async def test_eq():
    async with Cluster(asynchronous=True, name="A") as clusterA, Cluster(
        asynchronous=True, name="A2"
    ) as clusterA2, Cluster(asynchronous=True, name="B") as clusterB:
        assert clusterA != "A"
        assert not (clusterA == "A")
        assert clusterA == clusterA
        assert not (clusterA != clusterA)
        assert clusterA != clusterA2
        assert not (clusterA == clusterA2)
        assert clusterA != clusterB
        assert not (clusterA == clusterB)


@gen_test()
async def test_repr():
    async with Cluster(asynchronous=True, name="A") as cluster:
        assert cluster.scheduler_address == "<Not Connected>"
        res = repr(cluster)
        expected = "Cluster(A, '<Not Connected>', workers=0, threads=0, memory=0 B)"
        assert res == expected


@gen_test()
async def test_logs_deprecated():
    async with Cluster(asynchronous=True) as cluster:
        with pytest.warns(FutureWarning, match="get_logs"):
            cluster.logs()


@gen_test()
async def test_cluster_info(loop_in_thread):
    class FooCluster(Cluster):
        def __init__(self):
            self._cluster_info["foo"] = "bar"
            super().__init__(asynchronous=False, loop=loop_in_thread)

    cluster = FooCluster()
    assert "foo" in cluster._cluster_info  # exists before start() called
    with cluster:  # start and stop the cluster to avoid a resource warning
        pass


@gen_test()
async def test_cluster_wait_for_worker(loop):
    with LocalCluster(n_workers=3, loop=loop) as cluster:
        assert all(
            [
                worker.status.name == "running"
                for _, worker in cluster.scheduler.workers.items()
            ]
        )
        assert len(cluster.scheduler.workers) == 3
        cluster.scale(10)
        cluster.wait_for_workers(10)
        assert all(
            [
                worker.status.name == "running"
                for _, worker in cluster.scheduler.workers.items()
            ]
        )
        assert len(cluster.scheduler.workers) == 10

