import pytest

aiohttp = pytest.importorskip('aiohttp')  # actually pytest-aiohttp

from operator import add
from dask import delayed
from distributed.utils_test import gen_cluster, inc, div
from distributed.asyncio import AsyncClient, AsyncFuture


@delayed
def decrement(x):
    return x - 1


async def test_asyncio_client():
    async with AsyncClient() as c:
        future = c.submit(inc, 1)
        assert isinstance(future, AsyncFuture)

        result = await future
        assert result == 2

        result = await c.submit(inc, 10)
        assert result == 11

        x = c.submit(add, 1, 2)
        assert await c.gather(x) == 3
        assert await c.gather([x, [x], x]) == [3, [3], 3]

        seq = await c.gather(iter([x, x]))
        assert await next(seq) == 3

        seq = await c.scatter([1, 2, 3])
        assert await seq[0] == 1
        assert await seq[1] == 2
        assert await seq[2] == 3

        assert await c.compute(decrement(10)) == 9






