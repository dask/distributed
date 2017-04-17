import pytest

import asyncio
from distributed.utils_test import gen_cluster, inc, div
from distributed import as_completed, Scheduler, Client, Worker


@gen_cluster(client=True)
def test_await_future(c, s, a, b):
    future = c.submit(inc, 1)

    async def f():
        result = await future
        assert result == 2

    yield f()

    future = c.submit(div, 1, 0)

    async def f():
        with pytest.raises(ZeroDivisionError):
            await future

    yield f()


@gen_cluster(client=True)
def test_as_completed_async_for(c, s, a, b):
    futures = c.map(inc, range(10))
    ac = as_completed(futures)
    results = []

    async def f():
        async for future in ac:
            result = await future
            results.append(result)

    yield f()

    assert set(results) == set(range(1, 11))


def test_asyncio_await():
    from tornado.platform.asyncio import to_asyncio_future
    ioloop = asyncio.get_event_loop()

    async def f():
        scheduler = Scheduler(loop=ioloop)
        scheduler.start('127.0.0.1')
        worker = Worker(scheduler.address, loop=ioloop)
        await to_asyncio_future(worker._start())

        client = Client(scheduler.address, loop=ioloop, start=False)
        await to_asyncio_future(client._start())

        future = client.submit(inc, 1)
        result = await future
        assert result == 2

        await to_asyncio_future(client._shutdown())
        await to_asyncio_future(worker._close())
        await to_asyncio_future(scheduler.close())

    try:
        ioloop.run_until_complete(f())
    finally:
        ioloop.close()
