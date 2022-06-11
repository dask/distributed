import pytest

from distributed.deploy.cluster import Cluster
from distributed.utils_test import gen_test


@gen_test()
async def test_eq():
    clusterA = Cluster(asynchronous=True, name="A")
    clusterA2 = Cluster(asynchronous=True, name="A2")
    clusterB = Cluster(asynchronous=True, name="B")
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
    cluster = Cluster(asynchronous=True, name="A")
    assert cluster.scheduler_address == "<Not Connected>"
    res = repr(cluster)
    expected = "Cluster(A, '<Not Connected>', workers=0, threads=0, memory=0 B)"
    assert res == expected


@gen_test()
async def test_logs_deprecated():
    cluster = Cluster(asynchronous=True)
    with pytest.warns(FutureWarning, match="get_logs"):
        cluster.logs()


@gen_test()
async def test_cluster_info():
    class FooCluster(Cluster):
        def __init__(self):
            self._cluster_info["foo"] = "bar"
            super().__init__(asynchronous=False)

    cluster = FooCluster()
    assert "foo" in cluster._cluster_info
