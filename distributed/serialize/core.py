from __future__ import print_function, division, absolute_import

from functools import partial
import pickle

import cloudpickle


def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b'__main__' in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if isinstance(x, pickle_types) or b'__main__' not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            logger.info("Failed to serialize %s", x, exc_info=True)
            raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise


serializers = {}
deserializers = {None: lambda header, frames: loads(b''.join(frames))}


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
        header, frames = {}, [dumps(x)]

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

from dask.base import normalize_token

@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames


register_serialization(bytes, lambda b: ({}, [b]),
                              lambda header, frames: b''.join(frames))

