from __future__ import annotations

from types import CoroutineType

import pytest
from tornado.ioloop import IOLoop

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


@pytest.mark.asyncio
async def test_cluster_info():
    class FooCluster(Cluster):
        def __init__(self):
            self._cluster_info["foo"] = "bar"
            super().__init__(asynchronous=False)

    cluster = FooCluster()
    assert "foo" in cluster._cluster_info


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
