import asyncio
from functools import wraps

from tornado.gen import is_coroutine_function
from tornado.platform.asyncio import BaseAsyncIOLoop
from tornado.platform.asyncio import to_asyncio_future, to_tornado_future

from .client import Client, Future


def to_asyncio(method):
    @wraps(method)
    def convert(*args, **kwargs):
        return to_asyncio_future(method(*args, **kwargs))
    return convert


class Aiofy(type):

    def __new__(meta, cls, bases, attrs):
        for base in bases:
            for name, method in base.__dict__.items():
                if name.startswith('_') and is_coroutine_function(method):
                    attrs[name[1:]] = to_asyncio(method)
        return type.__new__(meta, cls, bases, attrs)


class AsyncIOLoop(BaseAsyncIOLoop):

    @property
    def _running(self):
        """Distributed checks IOLoop's _running property extensively"""
        return self.asyncio_loop.is_running()


class AsyncIOFuture(Future, metaclass=Aiofy):

    def __await__(self):
        return self.result().__await__()


class AsyncIOClient(Client, metaclass=Aiofy):
    _future = AsyncIOFuture

    def __init__(self, address=None, loop=None, timeout=3, start=True,
                 set_as_default=False, scheduler_file=None, **kwargs):
        self.aioloop = loop or asyncio.get_event_loop()
        tornloop = AsyncIOLoop(self.aioloop)
        super().__init__(address=address, start=start, loop=tornloop,
                         timeout=timeout, set_as_default=False,
                         scheduler_file=None, **kwargs)

    async def __aenter__(self):
        if self.status is not 'running':
            await self.start()
        return self

    async def __aexit__(self, type, value, traceback):
        await self.shutdown()

    def __del__(self):
        self.aioloop.run_until_complete(self.shutdown())
