# flake8: noqa

import pytest

asyncio = pytest.importorskip('asyncio')
aiohttp = pytest.importorskip('aiohttp')  # pytest-aiohttp

import sys
from operator import add
from dask import delayed
from toolz import isdistinct
from distributed.deploy import LocalCluster
from distributed.utils_test import gen_cluster, inc, div, slowinc, slowadd, slowdec, randominc

from distributed.asyncio import AioClient, AioFuture
from distributed import Client, Worker, Scheduler

from tornado.platform.asyncio import to_asyncio_future, BaseAsyncIOLoop
from tornado.ioloop import IOLoop


async def test_asyncio_submit(loop):
    async with AioClient(loop=loop, processes=False) as c:
        x = c.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, AioFuture)
        assert x.client is c

        result = await x.result()
        assert result == 11
        assert x.done()

        y = c.submit(inc, 20)
        z = c.submit(add, x, y)

        result = await z.result()
        assert result == 11 + 21


async def test_asyncio_future_await(loop):
    async with AioClient(loop=loop, processes=False) as c:
        x = c.submit(inc, 10)
        assert not x.done()

        assert isinstance(x, AioFuture)
        assert x.client is c

        result = await x
        assert result == 11
        assert x.done()

        y = c.submit(inc, 20)
        z = c.submit(add, x, y)

        result = await z
        assert result == 11 + 21


async def test_asyncio_map(loop):
    async with AioClient(loop=loop, processes=False) as c:
        L1 = c.map(inc, range(5))
        assert len(L1) == 5
        assert isdistinct(x.key for x in L1)
        assert all(isinstance(x, AioFuture) for x in L1)

        result = await L1[0]
        assert result == inc(0)

        L2 = c.map(inc, L1)

        result = await L2[1]
        assert result == inc(inc(1))

        total = c.submit(sum, L2)
        result = await total
        assert result == sum(map(inc, map(inc, range(5))))

        L3 = c.map(add, L1, L2)
        result = await L3[1]
        assert result == inc(1) + inc(inc(1))

        L4 = c.map(add, range(3), range(4))
        results = await c.gather(L4)
        assert results == list(map(add, range(3), range(4)))

        def f(x, y=10):
            return x + y

        L5 = c.map(f, range(5), y=5)
        results = await c.gather(L5)
        assert results == list(range(5, 10))

        y = c.submit(f, 10)
        L6 = c.map(f, range(5), y=y)
        results = await c.gather(L6)
        assert results == list(range(20, 25))


async def test_asyncio_gather(loop):
    async with AioClient(loop=loop, processes=False) as c:
        x = c.submit(inc, 10)
        y = c.submit(inc, x)

        result = await c.gather(x)
        assert result == 11
        result = await c.gather([x])
        assert result == [11]
        result = await c.gather({'x': x, 'y': [y]})
        assert result == {'x': 11, 'y': [12]}


async def test_asyncio_get(loop):
    async with AioClient(loop=loop, processes=False) as c:
        result = await c.get({'x': (inc, 1)}, 'x')
        assert result == 2

        result = await c.get({'x': (inc, 1)}, ['x'])
        assert result == [2]

        result = await c.get({}, [])
        assert result == []

        result = await c.get({('x', 1): (inc, 1), ('x', 2): (inc, ('x', 1))},
                              ('x', 2))
        assert result == 3


async def test_asyncio_exceptions(loop):
    async with AioClient(loop=loop, processes=False) as c:
        result = await c.submit(div, 1, 2)
        assert result == 1 / 2

        with pytest.raises(ZeroDivisionError):
            result = await c.submit(div, 1, 0)

        result = await c.submit(div, 10, 2)  # continues to operate
        assert result == 10 / 2


async def test_asyncio_channels(loop):
    async with AioClient(loop=loop, processes=False) as c:
        x = c.channel('x')
        y = c.channel('y')

        assert len(x) == 0

        while set(c.extensions['channels'].channels) != {'x', 'y'}:
            await asyncio.sleep(0.01)

        xx = c.channel('x')
        yy = c.channel('y')

        assert len(x) == 0

        await asyncio.sleep(0.1)
        assert set(c.extensions['channels'].channels) == {'x', 'y'}

        future = c.submit(inc, 1)

        x.append(future)

        while not x.data:
            await asyncio.sleep(0.01)

        assert len(x) == 1

        assert xx.data[0].key == future.key

        xxx = c.channel('x')
        while not xxx.data:
            await asyncio.sleep(0.01)

        assert xxx.data[0].key == future.key

        assert 'x' in repr(x)
        assert '1' in repr(x)


async def test_asyncio_exception_on_exception(loop):
    async with AioClient(loop=loop, processes=False) as c:
        x = c.submit(lambda: 1 / 0)
        y = c.submit(inc, x)

        with pytest.raises(ZeroDivisionError):
            await y

        z = c.submit(inc, y)
        with pytest.raises(ZeroDivisionError):
            await z
