from __future__ import print_function, division, absolute_import

from functools import partial

from dask.base import normalize_token

from . import pickle



serializers = {}
deserializers = {None: lambda header, frames: pickle.loads(b''.join(frames))}


def register_serialization(cls, serialize, deserialize):
    name = typename(cls)
    serializers[name] = serialize
    deserializers[name] = deserialize


def typename(typ):
    try:
        return typ.__qualname__
    except AttributeError:
        return typ.__module__ + '.' + typ.__name__


def serialize(x):
    if isinstance(x, Serialized):
        return x.header, x.frames

    name = typename(type(x))
    if name in serializers:
        header, frames = serializers[name](x)
        header['type'] = name
    else:
        header, frames = {}, [pickle.dumps(x)]

    return header, frames


def deserialize(header, frames):
    f = deserializers[header.get('type')]
    return f(header, frames)


class Serialize(object):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return "<Serialize: %s>" % self.data

    __repr__ = __str__


class Serialized(object):
    def __init__(self, header, frames):
        self.header = header
        self.frames = frames


@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames


register_serialization(bytes, lambda b: ({}, [b]),
                              lambda header, frames: b''.join(frames))
