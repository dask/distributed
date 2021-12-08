from types import CoroutineType

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


@pytest.mark.asyncio
async def test_cluster_info():
    class FooCluster(Cluster):
        def __init__(self):
            self._cluster_info["foo"] = "bar"
            super().__init__(asynchronous=False)

    cluster = FooCluster()
    assert "foo" in cluster._cluster_info


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous_cluster", [True, False])
async def test_sync_defaults_to_cluster_setting(
    asynchronous_cluster, loop_in_thread, cleanup
):

    cluster = Cluster(asynchronous=asynchronous_cluster, loop=loop_in_thread)

    async def foo():
        return 1

    result = cluster.sync(foo)

    if asynchronous_cluster:
        assert isinstance(result, CoroutineType)
        assert await result == 1
    else:
        assert result == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous_cluster", [True, False])
async def test_sync_allows_override_of_asychronous(
    asynchronous_cluster, loop_in_thread, cleanup
):
    cluster = Cluster(asynchronous=asynchronous_cluster, loop=loop_in_thread)

    async def foo():
        return 1

    async_result = cluster.sync(foo, asynchronous=True)
    sync_result = cluster.sync(foo, asynchronous=False)

    assert isinstance(async_result, CoroutineType)
    assert await async_result == 1
    assert sync_result == 1
