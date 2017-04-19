import asyncio
from functools import wraps

from tornado.gen import is_coroutine_function
from tornado.platform.asyncio import BaseAsyncIOLoop, to_asyncio_future, to_tornado_future


from .client import Client, Future


def to_asyncio(method):
    @wraps(method)
    def convert(*args, **kwargs):
        return to_asyncio_future(method(*args, **kwargs))
    return convert


class Asyncify(type):

    def __new__(meta, cls, bases, attrs):
        for base in bases:
            for name, method in base.__dict__.items():
                if name.startswith('_') and is_coroutine_function(method):
                    attrs[name[1:]] = to_asyncio(method)
        return type.__new__(meta, cls, bases, attrs)


class AsyncFuture(Future, metaclass=Asyncify):

    def __await__(self):
        return self.result().__await__()


class AsyncClient(Client, metaclass=Asyncify):
    _future = AsyncFuture

    def __init__(self, address=None, loop=None, timeout=3,
                 set_as_default=True, scheduler_file=None, **kwargs):
        loop = loop or asyncio.get_event_loop()
        ioloop = BaseAsyncIOLoop(loop)
        super().__init__(address=address, start=False, loop=ioloop,
                         timeout=timeout, set_as_default=False,
                         scheduler_file=None, **kwargs)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    client = AsyncClient('tcp://127.0.0.1:53696', loop=loop)

    async def run():
        print('starting')
        await client.start()
        print('started')

        out = await client.scatter([1, 2, 3, 4])
        print(out)

        first = await out[0]
        print(first)

    loop.run_until_complete(run())
