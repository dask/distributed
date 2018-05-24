from __future__ import print_function, division, absolute_import

import logging

import msgpack

try:
    from cytoolz import get_in
except ImportError:
    from toolz import get_in

from .compression import compressions, maybe_compress, decompress
from .serialize import (serialize, deserialize, Serialize, Serialized,
                        extract_serialize)
from .utils import frame_split_size, merge_frames
from ..utils import nbytes

_deserialize = deserialize


logger = logging.getLogger(__name__)


def dumps(msg, serializers=None, on_error='message'):
    """ Transform Python message to bytestream suitable for communication """
    try:
        data = {}
        # Only lists and dicts can contain serialized values
        if isinstance(msg, (list, dict)):
            msg, data, bytestrings = extract_serialize(msg)
        small_header, small_payload = dumps_msgpack(msg)

        if not data:  # fast path without serialized data
            return small_header, small_payload

        pre = {key: (value.header, value.frames)
               for key, value in data.items()
               if type(value) is Serialized}

        data = {key: serialize(value.data,
                               serializers=serializers,
                               on_error=on_error)
                for key, value in data.items()
                if type(value) is Serialize}

        header = {'headers': {},
                  'keys': [],
                  'bytestrings': list(bytestrings)}

        out_frames = []

        for key, (head, frames) in data.items():
            if 'lengths' not in head:
                head['lengths'] = tuple(map(nbytes, frames))
            if 'compression' not in head:
                frames = frame_split_size(frames)
                if frames:
                    compression, frames = zip(*map(maybe_compress, frames))
                else:
                    compression = []
                head['compression'] = compression
            head['count'] = len(frames)
            header['headers'][key] = head
            header['keys'].append(key)
            out_frames.extend(frames)

        for key, (head, frames) in pre.items():
            if 'lengths' not in head:
                head['lengths'] = tuple(map(nbytes, frames))
            head['count'] = len(frames)
            header['headers'][key] = head
            header['keys'].append(key)
            out_frames.extend(frames)

        for i, frame in enumerate(out_frames):
            if type(frame) is memoryview and frame.strides != (1,):
                try:
                    frame = frame.cast('b')
                except TypeError:
                    frame = frame.tobytes()
                out_frames[i] = frame

        return [small_header, small_payload,
                msgpack.dumps(header, use_bin_type=True, default=msgpack_default)] + out_frames
    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True, deserializers=None):
    """ Transform bytestream back into Python value """
    frames = frames[::-1]  # reverse order to improve pop efficiency
    if not isinstance(frames, list):
        frames = list(frames)
    try:
        small_header = frames.pop()
        small_payload = frames.pop()
        msg = loads_msgpack(small_header, small_payload)
        if not frames:
            return msg

        header = frames.pop()
        header = msgpack.loads(header, encoding='utf8', use_list=False, ext_hook=msgpack_ext_hook)
        keys = header['keys']
        headers = header['headers']
        bytestrings = set(header['bytestrings'])

        for key in keys:
            head = headers[key]
            count = head['count']
            if count:
                fs = frames[-count::][::-1]
                del frames[-count:]
            else:
                fs = []

            if deserialize or key in bytestrings:
                if 'compression' in head:
                    fs = decompress(head, fs)
                fs = merge_frames(head, fs)
                value = _deserialize(head, fs, deserializers=deserializers)
            else:
                value = Serialized(head, fs)

            get_in(key[:-1], msg)[key[-1]] = value

        return msg
    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise


_MSGPACK_EXT_TUPLE = 0
_MSGPACK_EXT_SET = 1
_MSGPACK_EXT_FROZENSET = 2


def msgpack_default(o):
    """ Default handler to allow encoding some other collection types correctly

    """
    if isinstance(o, (tuple, set, frozenset)):
        payload = msgpack.packb(
            list(o), strict_types=True, use_bin_type=True, default=msgpack_default)
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


def msgpack_ext_hook(code, payload):
    if code in {_MSGPACK_EXT_TUPLE, _MSGPACK_EXT_SET, _MSGPACK_EXT_FROZENSET}:
        l = msgpack.unpackb(payload, encoding='utf-8', ext_hook=msgpack_ext_hook)
        if code == _MSGPACK_EXT_TUPLE:
            return tuple(l)
        elif code == _MSGPACK_EXT_SET:
            return set(l)
        elif code == _MSGPACK_EXT_FROZENSET:
            return frozenset(l)
    raise ValueError("Unknown Ext code %s, payload: %s" % (code, payload))


def dumps_msgpack(msg):
    """ Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, use_bin_type=True, default=msgpack_default)

    fmt, payload = maybe_compress(payload)
    if fmt:
        header['compression'] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True, default=msgpack_default)
    else:
        header_bytes = b''

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """ Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    if header:
        header = msgpack.loads(header, encoding='utf8', ext_hook=msgpack_ext_hook)
    else:
        header = {}

    if header.get('compression'):
        try:
            decompress = compressions[header['compression']]['decompress']
            payload = decompress(payload)
        except KeyError:
            raise ValueError("Data is compressed as %s but we don't have this"
                             " installed" % str(header['compression']))

    return msgpack.loads(payload, encoding='utf8', ext_hook=msgpack_ext_hook)
