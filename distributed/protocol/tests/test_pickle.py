from functools import partial
import gc
from operator import add
import weakref
import sys

import pytest

from distributed.protocol import deserialize, serialize
from distributed.protocol.pickle import HIGHEST_PROTOCOL, dumps, loads

try:
    from pickle import PickleBuffer
except ImportError:
    pass


def test_pickle_data():
    data = [1, b"123", "123", [123], {}, set()]
    for d in data:
        assert loads(dumps(d)) == d
        assert deserialize(*serialize(d, serializers=("pickle",))) == d


def test_pickle_out_of_band():
    class MemoryviewHolder:
        def __init__(self, mv):
            self.mv = memoryview(mv)

        def __reduce_ex__(self, protocol):
            if protocol >= 5:
                return MemoryviewHolder, (PickleBuffer(self.mv),)
            else:
                return MemoryviewHolder, (self.mv.tobytes(),)

    mv = memoryview(b"123")
    mvh = MemoryviewHolder(mv)

    if HIGHEST_PROTOCOL >= 5:
        l = []
        d = dumps(mvh, buffer_callback=l.append)
        mvh2 = loads(d, buffers=l)
    else:
        mvh2 = loads(dumps(mvh))

    assert isinstance(mvh2, MemoryviewHolder)
    assert isinstance(mvh2.mv, memoryview)
    assert mvh2.mv == mv

    mvh3 = deserialize(*serialize(mvh, serializers=("pickle",)))

    assert isinstance(mvh3, MemoryviewHolder)
    assert isinstance(mvh3.mv, memoryview)
    assert mvh3.mv == mv


def test_pickle_numpy():
    np = pytest.importorskip("numpy")
    x = np.ones(5)
    assert (loads(dumps(x)) == x).all()
    assert (deserialize(*serialize(x, serializers=("pickle",))) == x).all()

    x = np.ones(5000)
    assert (loads(dumps(x)) == x).all()
    assert (deserialize(*serialize(x, serializers=("pickle",))) == x).all()

    if HIGHEST_PROTOCOL >= 5:
        x = np.ones(5000)
        l = []
        d = dumps(x, buffer_callback=l.append)
        assert (loads(d, buffers=l) == x).all()
        assert (deserialize(*serialize(x, serializers=("pickle",))) == x).all()


@pytest.mark.xfail(
    sys.version_info[:2] == (3, 8),
    reason="Sporadic failure on Python 3.8",
    strict=False,
)
def test_pickle_functions():
    def make_closure():
        value = 1

        def f(x):  # closure
            return x + value

        return f

    def funcs():
        yield make_closure()
        yield (lambda x: x + 1)
        yield partial(add, 1)

    for func in funcs():
        wr = weakref.ref(func)

        func2 = loads(dumps(func))
        wr2 = weakref.ref(func2)
        assert func2(1) == func(1)

        func3 = deserialize(*serialize(func, serializers=("pickle",)))
        wr3 = weakref.ref(func3)
        assert func3(1) == func(1)

        del func, func2, func3
        gc.collect()
        assert wr() is None
        assert wr2() is None
        assert wr3() is None
