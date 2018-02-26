from __future__ import print_function, division, absolute_import

import copy
import pickle

import numpy as np
import pytest
from toolz import identity

from distributed.protocol import (register_serialization, serialize,
                                  deserialize, nested_deserialize, Serialize,
                                  Serialized, to_serialize, serialize_bytes,
                                  deserialize_bytes, serialize_bytelist,)
from distributed.utils import nbytes


class MyObj(object):
    def __init__(self, data):
        self.data = data

    def __getstate__(self):
        raise Exception('Not picklable')


def serialize_myobj(x):
    return {}, [pickle.dumps(x.data)]


def deserialize_myobj(header, frames):
    return MyObj(pickle.loads(frames[0]))


register_serialization(MyObj, serialize_myobj, deserialize_myobj)


def test_dumps_serialize():
    for x in [123, [1, 2, 3]]:
        header, frames = serialize(x)
        assert not header
        assert len(frames) == 1

        result = deserialize(header, frames)
        assert result == x

    x = MyObj(123)
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    result = deserialize(header, frames)
    assert result.data == x.data


def test_serialize_bytestrings():
    for b in (b'123', bytearray(b'4567')):
        header, frames = serialize(b)
        assert frames[0] is b
        bb = deserialize(header, frames)
        assert bb == b


def test_Serialize():
    s = Serialize(123)
    assert '123' in str(s)
    assert s.data == 123

    t = Serialize((1, 2))
    assert str(t)

    u = Serialize(123)
    assert s == u
    assert not (s != u)
    assert s != t
    assert not (s == t)
    assert hash(s) == hash(u)
    assert hash(s) != hash(t)  # most probably


def test_Serialized():
    s = Serialized(*serialize(123))
    t = Serialized(*serialize((1, 2)))
    u = Serialized(*serialize(123))
    assert s == u
    assert not (s != u)
    assert s != t
    assert not (s == t)


def test_nested_deserialize():
    x = {'op': 'update',
         'x': [to_serialize(123), to_serialize(456), 789],
         'y': {'a': ['abc', Serialized(*serialize('def'))],
               'b': b'ghi'}
         }
    x_orig = copy.deepcopy(x)

    assert nested_deserialize(x) == {'op': 'update',
                                     'x': [123, 456, 789],
                                     'y': {'a': ['abc', 'def'],
                                           'b': b'ghi'}
                                     }
    assert x == x_orig  # x wasn't mutated


from distributed.utils_test import gen_cluster
from dask import delayed


@gen_cluster(client=True)
def test_object_in_graph(c, s, a, b):
    o = MyObj(123)
    v = delayed(o)
    v2 = delayed(identity)(v)

    future = c.compute(v2)
    result = yield future

    assert isinstance(result, MyObj)
    assert result.data == 123


@gen_cluster(client=True)
def test_scatter(c, s, a, b):
    o = MyObj(123)
    [future] = yield c._scatter([o])
    yield c._replicate(o)
    o2 = yield c._gather(future)
    assert isinstance(o2, MyObj)
    assert o2.data == 123


@gen_cluster(client=True)
def test_inter_worker_comms(c, s, a, b):
    o = MyObj(123)
    [future] = yield c._scatter([o], workers=a.address)
    future2 = c.submit(identity, future, workers=b.address)
    o2 = yield c._gather(future2)
    assert isinstance(o2, MyObj)
    assert o2.data == 123


class Empty(object):
    def __getstate__(self):
        raise Exception('Not picklable')


def serialize_empty(x):
    return {}, []


def deserialize_empty(header, frames):
    return Empty()


register_serialization(Empty, serialize_empty, deserialize_empty)


def test_empty():
    e = Empty()
    e2 = deserialize(*serialize(e))
    assert isinstance(e2, Empty)


def test_empty_loads():
    from distributed.protocol import loads, dumps
    e = Empty()
    e2 = loads(dumps([to_serialize(e)]))
    assert isinstance(e2[0], Empty)


def test_serialize_bytes():
    for x in [1, 'abc', np.arange(5)]:
        b = serialize_bytes(x)
        assert isinstance(b, bytes)
        y = deserialize_bytes(b)
        assert str(x) == str(y)


def test_serialize_list_compress():
    pytest.importorskip('lz4')
    x = np.ones(1000000)
    L = serialize_bytelist(x)
    assert sum(map(nbytes, L)) < x.nbytes / 2

    b = b''.join(L)
    y = deserialize_bytes(b)
    assert (x == y).all()
