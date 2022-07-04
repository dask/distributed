from __future__ import annotations

import pytest

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