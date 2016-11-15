from distributed.protocol.pickle import dumps, loads

import pytest

from operator import add
from functools import partial

from tornado import gen, ioloop


@gen.coroutine
def coro():
    yield gen.moment


class CoroObject(object):
    @gen.coroutine
    def f(self, x):
        yield gen.moment
        raise gen.Return(x + 1)


def test_pickle_data():
    data = [1, b'123', '123', [123], {}, set()]
    for d in data:
        assert loads(dumps(d)) == d


def test_pickle_numpy():
    np = pytest.importorskip('numpy')
    x = np.ones(5)
    assert (loads(dumps(x)) == x).all()

    x = np.ones(5000)
    assert (loads(dumps(x)) == x).all()


def test_pickle_functions():
    value = 1
    def f(x):  # closure
        return x + value

    for func in [f, lambda x: x + 1, partial(add, 1)]:
        assert loads(dumps(func))(1) == func(1)


def test_global_coroutine():
    data = dumps(coro)
    assert loads(data) is coro
    # Should be tiny
    assert len(data) < 80


def test_local_coroutine():
    @gen.coroutine
    def f(x, y):
        yield gen.sleep(x)
        raise gen.Return(y + 1)

    @gen.coroutine
    def g(y):
        res = yield f(0.01, y)
        raise gen.Return(res + 1)

    data = dumps([g, g])
    f = g = None
    g2, g3 = loads(data)
    assert g2 is g3
    loop = ioloop.IOLoop.current()
    res = loop.run_sync(partial(g2, 5))
    assert res == 7


def test_coroutine_method():
    obj = CoroObject()
    data = dumps(obj.f)
    del obj
    f2 = loads(data)
    loop = ioloop.IOLoop.current()
    res = loop.run_sync(partial(f2, 5))
    assert res == 6
