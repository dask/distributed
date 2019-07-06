from distributed.deploy.ssh2 import SSHCluster
from dask.distributed import Client

import pytest


@pytest.mark.asyncio
async def test_basic():
    async with SSHCluster(["localhost"] * 3, asynchronous=True) as cluster:
        assert len(cluster.workers) == 2
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
