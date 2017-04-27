# flake8: noqa

import asyncio
from functools import wraps

from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.platform.asyncio import to_asyncio_future, to_tornado_future

from .client import Client, Future, AsCompleted, _wait

from tornado.ioloop import IOLoop


def to_asyncio(method):
    """Converts Tornado gen.coroutines and futures to asyncio ones"""
    @wraps(method)
    def convert(*args, **kwargs):
        return to_asyncio_future(method(*args, **kwargs))
    return convert


class AioLoop(BaseAsyncIOLoop):

    @property
    def _running(self):
        """Distributed checks IOLoop's _running property extensively"""
        return self.asyncio_loop.is_running()


class AioFuture(Future):
    """Provides awaitable syntax for a distributed Future"""

    def __await__(self):
        return self.result().__await__()

    result = to_asyncio(Future._result)
    exception = to_asyncio(Future._exception)
    traceback = to_asyncio(Future._traceback)


class AioClient(Client):

    _Future = AioFuture

    def __init__(self, *args, loop=None, start=True, set_as_default=False,
                 **kwargs):
        if set_as_default:
            raise Exception("AioClient instance can't be sat as default")
        if loop is None:
            loop = asyncio.get_event_loop()
        # required to handle IOLoop.current() calls
        # ioloop is not injected in nanny and comm protocols
        self._make_current = start

        ioloop = AioLoop(loop, make_current=False)
        super().__init__(*args, loop=ioloop, start=False, set_as_default=False,
                         **kwargs)

    async def __aenter__(self):
        if self._make_current:
            self.loop.make_current()

        if self.status is not 'running':
            await self.start()

        return self

    async def __aexit__(self, type, value, traceback):
        await self.shutdown()
        if self._make_current:
            IOLoop.clear_current()

    def __del__(self):
        self.loop.asyncio_loop.run_until_complete(self.shutdown())

    start = to_asyncio(Client._start)
    shutdown = to_asyncio(Client._shutdown)
    gather = to_asyncio(Client._gather)
    scatter = to_asyncio(Client._scatter)
    cancel = to_asyncio(Client._cancel)
    publish_dataset = to_asyncio(Client._publish_dataset)
    get_dataset = to_asyncio(Client._get_dataset)
    run_on_scheduler = to_asyncio(Client._run_on_scheduler)
    run = to_asyncio(Client._run)
    run_cocoutine = to_asyncio(Client._run_coroutine)
    get = to_asyncio(Client._get)
    upload_environment = to_asyncio(Client._upload_environment)
    restart = to_asyncio(Client._restart)
    upload_file = to_asyncio(Client._upload_file)
    upload_large_file = to_asyncio(Client._upload_large_file)
    rebalance = to_asyncio(Client._rebalance)
    replicate = to_asyncio(Client._replicate)
    start_ipython_workers = to_asyncio(Client._start_ipython_workers)


class AioAsCompleted(AsCompleted):

    __anext__ = to_asyncio(AsCompleted.__anext__)


wait = to_asyncio(_wait)
as_completed = AioAsCompleted
