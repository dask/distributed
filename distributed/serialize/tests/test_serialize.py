from __future__ import print_function, division, absolute_import

import pickle

import numpy as np
import pytest

from distributed.serialize.core import (register_serialization, serialize,
        deserialize, Serialize, Serialized)
from distributed.protocol import decompress

import distributed.serialize.numpy


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

    x = np.ones((5, 5))
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    frames = decompress(header, frames)
    result = deserialize(header, frames)
    assert (result == x).all()

    x = MyObj(123)
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    result = deserialize(header, frames)
    assert result.data == x.data


@pytest.mark.parametrize('x',
        [np.ones(5),
         np.asfortranarray(np.random.random((5, 5))),
         np.random.random(5).astype('f4'),
         np.empty(shape=(5, 3), dtype=[('total', '<f8'), ('n', '<f8')])])
def test_dumps_serialize_numpy(x):
    header, frames = serialize(x)
    frames = decompress(header, frames)
    y = deserialize(header, frames)

    np.testing.assert_equal(x, y)


def test_serialize_bytes():
    b = b'123'
    header, frames = serialize(b)
    assert frames[0] is b
