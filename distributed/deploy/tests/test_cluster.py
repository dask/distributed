from __future__ import annotations

import pytest

from distributed.client import Client
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
async def test_cluster_get_client():
    async with Cluster(asynchronous=True, name="A") as clusterA, Cluster(
        asynchronous=True, name="B"
    ) as clusterB:
        clientA1 = clusterA.get_client()

        assert isinstance(clientA1, Client)
        assert clientA1.cluster == clusterA
        assert clientA1 == clusterA.get_client()

        clientB1 = clusterB.get_client()
        assert clientB1 != clientA1

        clientB2 = Client(clusterB)
        assert clientB1 != clientB2
        assert clientB2 == clusterB.get_client()
