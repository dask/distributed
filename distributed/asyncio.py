"""Experimental interface for asyncio, may disappear without warning"""

# flake8: noqa

import asyncio
from functools import wraps

from toolz import merge

from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.platform.asyncio import to_asyncio_future

from . import client
from .client import Client, Future
from .variable import Variable
from .utils import ignoring


def to_asyncio(fn, **default_kwargs):
    """Converts Tornado gen.coroutines and futures to asyncio ones"""
    @wraps(fn)
    def convert(*args, **kwargs):
        if default_kwargs:
            kwargs = merge(default_kwargs, kwargs)
        return to_asyncio_future(fn(*args, **kwargs))
    return convert


class AioFuture(Future):
    """Provides awaitable syntax for a distributed Future"""

    def __await__(self):
        return self.result().__await__()

    result = to_asyncio(Future._result)
    exception = to_asyncio(Future._exception)
    traceback = to_asyncio(Future._traceback)


class AioClient(Client):
    """ Connect to and drive computation on a distributed Dask cluster

    This class provides an asyncio compatible async/await interface for
    dask.distributed.

    The Client connects users to a dask.distributed compute cluster. It
    provides an asynchronous user interface around functions and futures.
    This class resembles executors in ``concurrent.futures`` but also
    allows ``Future`` objects within ``submit/map`` calls.

    AioClient is an **experimental** interface for distributed and may
    disappear without warning!

    Parameters
    ----------
    address: string, or Cluster
        This can be the address of a ``Scheduler`` server like a string
        ``'127.0.0.1:8786'`` or a cluster object like ``LocalCluster()``

    Examples
    --------
    Provide cluster's scheduler address on initialization::

        client = AioClient('127.0.0.1:8786')

    Start the client::

        async def start_the_client():
            client = await AioClient()

            # Use the client....

            await client.close()

    An ``async with`` statement is a more convenient way to start and shut down
    the client::

        async def start_the_client():
            async with AioClient() as client:
                # Use the client within this block.
                pass

    Use the ``submit`` method to send individual computations to the cluster,
    and await the returned future to retrieve the result::

        async def add_two_numbers():
            async with AioClient() as client:
                a = client.submit(add, 1, 2)
                result = await a

    Continue using submit or map on results to build up larger computations,
    and gather results with the ``gather`` method::

        async def gather_some_results():
            async with AioClient() as client:
                a = client.submit(add, 1, 2)
                b = client.submit(add, 10, 20)
                c = client.submit(add, a, b)
                result = await client.gather([c])

    See Also
    --------
    distributed.client.Client: Blocking Client
    distributed.scheduler.Scheduler: Internal scheduler
    """
    _Future = AioFuture

    def __init__(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        ioloop = BaseAsyncIOLoop(loop)
        super().__init__(*args, loop=ioloop, asynchronous=True, **kwargs)

    async def __aenter__(self):
        await to_asyncio_future(self._started)
        return self

    async def __aexit__(self, type, value, traceback):
        await to_asyncio_future(self._close())

    def __await__(self):
        return to_asyncio_future(self._started).__await__()

    gather = to_asyncio(Client.gather)
    scatter = to_asyncio(Client.scatter)
    cancel = to_asyncio(Client.cancel)
    publish_dataset = to_asyncio(Client.publish_dataset)
    get_dataset = to_asyncio(Client.get_dataset)
    run_on_scheduler = to_asyncio(Client.run_on_scheduler)
    run = to_asyncio(Client.run)
    run_coroutine = to_asyncio(Client.run_coroutine)
    get = to_asyncio(Client.get, sync=False)
    upload_environment = to_asyncio(Client.upload_environment)
    restart = to_asyncio(Client.restart)
    upload_file = to_asyncio(Client.upload_file)
    rebalance = to_asyncio(Client.rebalance)
    replicate = to_asyncio(Client.replicate)
    start_ipython_workers = to_asyncio(Client.start_ipython_workers)
    close = to_asyncio(Client.close)
    shutdown = to_asyncio(Client.shutdown)

    def __enter__(self):
        raise RuntimeError("Use AioClient in an 'async with' block, not 'with'")


class AioVariable(Variable):

    _Client = AioClient
    _Future = AioFuture

    get = to_asyncio(Variable.get)
    set = to_asyncio(Variable.set)


class as_completed(client.as_completed):
    __anext__ = to_asyncio(client.as_completed.__anext__)


wait = to_asyncio(client._wait)
