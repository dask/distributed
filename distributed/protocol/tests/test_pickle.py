import gc
import sys
import weakref
from functools import partial
from operator import add

import pytest

from distributed.protocol import deserialize, serialize
from distributed.protocol.pickle import HIGHEST_PROTOCOL, dumps, loads

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle
    except ImportError:
        import pickle
else:
    import pickle


class MemoryviewHolder:
    def __init__(self, mv):
        self.mv = memoryview(mv)

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return MemoryviewHolder, (pickle.PickleBuffer(self.mv),)
        else:
            return MemoryviewHolder, (self.mv.tobytes(),)


@pytest.mark.parametrize("protocol", {4, HIGHEST_PROTOCOL})
def test_pickle_data(protocol):
    context = {"pickle-protocol": protocol}

    data = [1, b"123", "123", [123], {}, set()]
    for d in data:
        assert loads(dumps(d, protocol=protocol)) == d
        assert deserialize(*serialize(d, serializers=("pickle",), context=context)) == d


@pytest.mark.parametrize("protocol", {4, HIGHEST_PROTOCOL})
def test_pickle_out_of_band(protocol):
    context = {"pickle-protocol": protocol}

    mv = memoryview(b"123")
    mvh = MemoryviewHolder(mv)

    if protocol >= 5:
        l = []
        d = dumps(mvh, protocol=protocol, buffer_callback=l.append)
        mvh2 = loads(d, buffers=l)

        assert len(l) == 1
        assert isinstance(l[0], pickle.PickleBuffer)
        assert memoryview(l[0]) == mv
    else:
        mvh2 = loads(dumps(mvh, protocol=protocol))

    assert isinstance(mvh2, MemoryviewHolder)
    assert isinstance(mvh2.mv, memoryview)
    assert mvh2.mv == mv

    h, f = serialize(mvh, serializers=("pickle",), context=context)
    mvh3 = deserialize(h, f)

    assert isinstance(mvh3, MemoryviewHolder)
    assert isinstance(mvh3.mv, memoryview)
    assert mvh3.mv == mv

    if protocol >= 5:
        assert len(f) == 2
        assert isinstance(f[0], bytes)
        assert isinstance(f[1], memoryview)
        assert f[1] == mv
    else:
        assert len(f) == 1
        assert isinstance(f[0], bytes)


@pytest.mark.parametrize("protocol", {4, HIGHEST_PROTOCOL})
def test_pickle_empty(protocol):
    context = {"pickle-protocol": protocol}

    x = MemoryviewHolder(bytearray())  # Empty view
    header, frames = serialize(x, serializers=("pickle",), context=context)

    assert header["serializer"] == "pickle"
    assert len(frames) >= 1
    assert isinstance(frames[0], bytes)

    if protocol >= 5:
        assert len(frames) == 2
        assert len(header["writeable"]) == 1

        header["writeable"] = (False,) * len(frames)
    else:
        assert len(frames) == 1
        assert len(header["writeable"]) == 0

    y = deserialize(header, frames)

    assert isinstance(y, MemoryviewHolder)
    assert isinstance(y.mv, memoryview)
    assert y.mv == x.mv
    assert y.mv.nbytes == 0
    assert y.mv.readonly


@pytest.mark.parametrize("protocol", {4, HIGHEST_PROTOCOL})
def test_pickle_numpy(protocol):
    np = pytest.importorskip("numpy")
    context = {"pickle-protocol": protocol}

    x = np.ones(5)
    assert (loads(dumps(x, protocol=protocol)) == x).all()
    assert (
        deserialize(*serialize(x, serializers=("pickle",), context=context)) == x
    ).all()

    x = np.ones(5000)
    assert (loads(dumps(x, protocol=protocol)) == x).all()
    assert (
        deserialize(*serialize(x, serializers=("pickle",), context=context)) == x
    ).all()

    x = np.array([np.arange(3), np.arange(4, 6)], dtype=object)
    x2 = loads(dumps(x, protocol=protocol))
    assert x.shape == x2.shape
    assert x.dtype == x2.dtype
    assert x.strides == x2.strides
    for e_x, e_x2 in zip(x.flat, x2.flat):
        np.testing.assert_equal(e_x, e_x2)
    h, f = serialize(x, serializers=("pickle",), context=context)
    if protocol >= 5:
        assert len(f) == 3
    else:
        assert len(f) == 1
    x3 = deserialize(h, f)
    assert x.shape == x3.shape
    assert x.dtype == x3.dtype
    assert x.strides == x3.strides
    for e_x, e_x3 in zip(x.flat, x3.flat):
        np.testing.assert_equal(e_x, e_x3)

    if protocol >= 5:
        x = np.ones(5000)

        l = []
        d = dumps(x, protocol=protocol, buffer_callback=l.append)
        assert len(l) == 1
        assert isinstance(l[0], pickle.PickleBuffer)
        assert memoryview(l[0]) == memoryview(x)
        assert (loads(d, buffers=l) == x).all()

        h, f = serialize(x, serializers=("pickle",), context=context)
        assert len(f) == 2
        assert isinstance(f[0], bytes)
        assert isinstance(f[1], memoryview)
        assert (deserialize(h, f) == x).all()


@pytest.mark.parametrize("protocol", {4, HIGHEST_PROTOCOL})
def test_pickle_functions(protocol):
    context = {"pickle-protocol": protocol}

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

        func2 = loads(dumps(func, protocol=protocol))
        wr2 = weakref.ref(func2)
        assert func2(1) == func(1)

        func3 = deserialize(*serialize(func, serializers=("pickle",), context=context))
        wr3 = weakref.ref(func3)
        assert func3(1) == func(1)

        del func, func2, func3
        gc.collect()
        assert wr() is None
        assert wr2() is None
        assert wr3() is None
