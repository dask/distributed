import pytest

from distributed.deploy.cluster import Cluster


@pytest.mark.asyncio
async def test_eq(cleanup):
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


@pytest.mark.asyncio
async def test_repr(cleanup):
    cluster = Cluster(asynchronous=True, name="A")
    assert cluster.scheduler_address == "<Not Connected>"
    res = repr(cluster)
    expected = "Cluster(A, '<Not Connected>', workers=0, threads=0, memory=0 B)"
    assert res == expected


@pytest.mark.asyncio
async def test_logs_deprecated(cleanup):
    cluster = Cluster(asynchronous=True)
    with pytest.warns(FutureWarning, match="get_logs"):
        cluster.logs()
