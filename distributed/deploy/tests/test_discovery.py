import pytest
import prybar

from typing import AsyncIterator

from distributed import LocalCluster, ProxyCluster
from distributed.deploy.discovery import (
    discover_cluster_names,
    discover_clusters,
    list_discovery_methods,
)


def test_discovery_methods():
    assert "proxycluster" in list_discovery_methods()


@pytest.mark.asyncio
async def test_discover_cluster_names():
    assert isinstance(discover_cluster_names(), AsyncIterator)
    with LocalCluster() as _:
        count = 0
        async for _ in discover_cluster_names():
            count += 1
        assert count == 1


@pytest.mark.asyncio
async def test_discover_clusters():
    with LocalCluster() as cluster:
        async for discovered_cluster in discover_clusters():
            if isinstance(discovered_cluster, ProxyCluster):
                assert cluster.scheduler_info == discovered_cluster.scheduler_info
