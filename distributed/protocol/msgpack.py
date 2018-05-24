from __future__ import print_function, division, absolute_import

from functools import partial

import msgpack

_MSGPACK_EXT_TUPLE = 0
_MSGPACK_EXT_SET = 1
_MSGPACK_EXT_FROZENSET = 2


def _msgpack_default(o):
    """ Default handler to allow encoding some other collection types correctly

    """
    if isinstance(o, (tuple, set, frozenset)):
        payload = msgpack.packb(
            list(o), strict_types=True, use_bin_type=True, default=_msgpack_default)
        if isinstance(o, tuple):
            ext_type = _MSGPACK_EXT_TUPLE
        elif isinstance(o, frozenset):
            ext_type = _MSGPACK_EXT_FROZENSET
        elif isinstance(o, set):
            ext_type = _MSGPACK_EXT_SET
        else:
            raise TypeError("Unknown type %s" % type(o))
        return msgpack.ExtType(ext_type, payload)
    else:
        raise TypeError("Unknown type %s for %s" % (repr(o), type(o)))


def _msgpack_ext_hook(code, payload):
    if code in {_MSGPACK_EXT_TUPLE, _MSGPACK_EXT_SET, _MSGPACK_EXT_FROZENSET}:
        l = msgpack.unpackb(payload, encoding='utf-8', ext_hook=_msgpack_ext_hook)
        if code == _MSGPACK_EXT_TUPLE:
            return tuple(l)
        elif code == _MSGPACK_EXT_SET:
            return set(l)
        elif code == _MSGPACK_EXT_FROZENSET:
            return frozenset(l)
    raise ValueError("Unknown Ext code %s, payload: %s" % (code, payload))


dumps = partial(msgpack.dumps, default=_msgpack_default)
loads = partial(msgpack.loads, ext_hook=_msgpack_ext_hook)
