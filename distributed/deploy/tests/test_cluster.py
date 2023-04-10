from __future__ import annotations

import pytest
from tornado.ioloop import IOLoop

from distributed import LocalCluster, Status
from distributed.deploy.cluster import Cluster, _exponential_backoff
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
async def test_cluster_wait_for_worker(loop):
    with LocalCluster(n_workers=3, loop=loop) as cluster:
        assert len(cluster.scheduler.workers) == 3
        cluster.scale(10)
        cluster.wait_for_workers(10)
        assert all(
            [
                worker["status"] == Status.running.name
                for _, worker in cluster.scheduler_info["workers"].items()
            ]
        )
        assert len(cluster.scheduler.workers) == 10


@gen_test()
async def test_deprecated_loop_properties():
    class ExampleCluster(Cluster):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.loop = self.io_loop = IOLoop.current()

    with pytest.warns(DeprecationWarning) as warninfo:
        async with ExampleCluster(asynchronous=True, loop=IOLoop.current()):
            pass

    assert [(w.category, *w.message.args) for w in warninfo] == [
        (DeprecationWarning, "setting the loop property is deprecated")
    ]


def test_exponential_backoff():
    assert _exponential_backoff(0, 1.5, 3, 20) == 1.5
    assert _exponential_backoff(1, 1.5, 3, 20) == 4.5
    assert _exponential_backoff(2, 1.5, 3, 20) == 13.5
    assert _exponential_backoff(5, 1.5, 3, 20) == 20
    # avoid overflow
    assert _exponential_backoff(1000, 1.5, 3, 20) == 20
