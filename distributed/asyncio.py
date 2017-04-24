import asyncio
from functools import wraps

from tornado.gen import is_coroutine_function
from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.platform.asyncio import to_asyncio_future, to_tornado_future

from .client import Client, Future

from tornado.ioloop import IOLoop


def to_asyncio(method):
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

    def __await__(self):
        return self.result().__await__()

    result = to_asyncio(Future._result)
    exception = to_asyncio(Future._exception)
    traceback = to_asyncio(Future._traceback)


class AioClient(Client):

    _future = AioFuture

    def __init__(self, *args, loop=None, start=True, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        ioloop = AioLoop(loop, make_current=False)

        # required to handle IOLoop.current() calls
        # ioloop is not injected in nanny and comm protocols
        self._make_current = start
        super().__init__(*args, loop=ioloop, start=False, **kwargs)

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
    reconnect = to_asyncio(Client._reconnect)
    ensure_connected = to_asyncio(Client._ensure_connected)
    handle_report = to_asyncio(Client._handle_report)
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
