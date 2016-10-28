from __future__ import print_function, division, absolute_import

import pickle

import numpy as np
import pytest

from distributed.serialize.core import (register_serialization, serialize,
        deserialize, Serialize, Serialized)
from distributed.protocol import decompress


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


def test_serialize_bytes():
    b = b'123'
    header, frames = serialize(b)
    assert frames[0] is b
