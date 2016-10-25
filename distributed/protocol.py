"""
The distributed message protocol consists of the following parts:

1.  The length of the header, stored as a uint32
2.  The header, stored as msgpack.
    If there are no fields in the header then we skip it entirely.
3.  The payload, stored as possibly compressed msgpack
4.  A sentinel value

**Header**

The Header contains the following fields:

* **compression**: string, optional
    One of the following: ``'snappy', 'lz4', 'zlib'`` or missing for None

**Payload**

The payload is any msgpack serializable value.  It may be compressed based
on the header.

**Sentinel**

We often terminate each message with a sentinel value.  This happens
outside of this module though and is not baked in.
"""
from __future__ import print_function, division, absolute_import

from functools import partial
from copy import deepcopy
import random

try:
    import pandas.msgpack as msgpack
except ImportError:
    import msgpack

from toolz import identity, get_in, valmap

from .utils import ignoring
from .serialize import serialize, deserialize, Serialize, Serialized

_deserialize = deserialize

compressions = {None: {'compress': identity,
                       'decompress': identity}}

default_compression = None


with ignoring(ImportError):
    import zlib
    compressions['zlib'] = {'compress': zlib.compress,
                            'decompress': zlib.decompress}

with ignoring(ImportError):
    import snappy
    compressions['snappy'] = {'compress': snappy.compress,
                              'decompress': snappy.decompress}
    default_compression = 'snappy'

with ignoring(ImportError):
    import lz4
    compressions['lz4'] = {'compress': lz4.LZ4_compress,
                           'decompress': lz4.LZ4_uncompress}
    default_compression = 'lz4'

with ignoring(ImportError):
    import blosc
    compressions['blosc'] = {'compress': partial(blosc.compress, clevel=5,
                                                 cname='lz4'),
                             'decompress': blosc.decompress}


from .config import config
default = config.get('compression', 'auto')
if default != 'auto':
    if default in compressions:
        default_compression = default
    else:
        raise ValueError("Default compression '%s' not found.\n"
                "Choices include auto, %s" % (
                    default, ', '.join(sorted(map(str, compressions)))))


to_serialize = Serialize


BIG_BYTES_SHARD_SIZE = 2**28


def extract_serialize(x):
    ser = {}
    _extract_serialize(x, ser)
    if ser:
        x = deepcopy(x)
        for path in ser:
            t = get_in(path[:-1], x)
            if isinstance(t, dict):
                del t[path[-1]]
            else:
                t[path[-1]] = None
    return x, ser


def _extract_serialize(x, ser, path=()):
    if type(x) is dict:
        for k, v in x.items():
            if isinstance(v, (list, dict)):
                _extract_serialize(v, ser, path + (k,))
            elif type(v) is Serialize or type(v) is Serialized:
                ser[path + (k,)] = v
    elif type(x) is list:
        for k, v in enumerate(x):
            if isinstance(v, (list, dict)):
                _extract_serialize(v, ser, path + (k,))
            elif type(v) is Serialize or type(v) is Serialized:
                ser[path + (k,)] = v


def frame_split_size(frames, n=BIG_BYTES_SHARD_SIZE):
    """ Split a list of frames into a list of frames of maximum size

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)
    [b'123', b'45', b'678']
    """
    if max(map(len, frames)) <= n:
        return frames

    out = []
    for frame in frames:
        if len(frame) > n:
            if isinstance(frame, bytes):
                frame = memoryview(frame)
            for i in range(0, len(frame), n):
                out.append(frame[i: i + n])
        else:
            out.append(frame)
    return out


def dumps(msg):
    """ Transform Python value to bytestream suitable for communication """
    data = {}
    # Only lists and dicts can contain serialized values
    if isinstance(msg, (list, dict)):
        msg, data = extract_serialize(msg)
    small_header, small_payload = dumps_msgpack(msg)

    if not data:  # fast path without serialized data
        return small_header, small_payload

    pre = {key: (value.header, value.frames)
           for key, value in data.items()
           if type(value) is Serialized}

    data = {key: serialize(value.data)
                 for key, value in data.items()
                 if type(value) is Serialize}

    header = {'headers': {},
              'keys': []}
    out_frames = []

    for key, (head, frames) in data.items():
        if 'compression' not in head:
            frames = frame_split_size(frames)
            compression, frames = zip(*map(maybe_compress, frames))
            head['compression'] = compression
        head['lengths'] = list(map(len, frames))
        header['headers'][key] = head
        header['keys'].append(key)
        out_frames.extend(frames)

    for key, (head, frames) in pre.items():
        head['lengths'] = list(map(len, frames))
        header['headers'][key] = head
        header['keys'].append(key)
        out_frames.extend(frames)

    return [small_header, small_payload,
            msgpack.dumps(header, use_bin_type=True)] + out_frames


def loads(frames, deserialize=True):
    """ Transform bytestream back into Python value """
    small_header, small_payload, frames = frames[0], frames[1], frames[2:]
    msg = loads_msgpack(small_header, small_payload)
    if not frames:
        return msg

    header, frames = frames[0], frames[1:]
    header = msgpack.loads(header, encoding='utf8', use_list=False)
    keys = header['keys']
    headers = header['headers']

    for key in keys:
        head = headers[key]
        lengths = head['lengths']
        fs, frames = frames[:len(lengths)], frames[len(lengths):]
        # assert all(len(f) == l for f, l in zip(frames, lengths))

        if deserialize:
            fs = decompress(head, fs)
            value = _deserialize(head, fs)
        else:
            value = Serialized(head, fs)

        get_in(key[:-1], msg)[key[-1]] = value

    return msg


def byte_sample(b, size, n):
    """ Sample a bytestring from many locations """
    starts = [random.randint(0, len(b) - size) for j in range(n)]
    ends = []
    for i, start in enumerate(starts[:-1]):
        ends.append(min(start + size, starts[i + 1]))
    ends.append(starts[-1] + size)

    return b''.join([b[start:end] for start, end in zip(starts, ends)])


def maybe_compress(payload, compression=default_compression, min_size=1e4,
                   sample_size=1e4, nsamples=5):
    """ Maybe compress payload

    1.  We don't compress small messages
    2.  We sample the payload in a few spots, compress that, and if it doesn't
        do any good we return the original
    3.  We then compress the full original, it it doesn't compress well then we
        return the original
    4.  We return the compressed result
    """
    if not compression:
        return None, payload
    if len(payload) < min_size:
        return None, payload
    if len(payload) > 2**31:
        return None, payload

    min_size = int(min_size)
    sample_size = int(sample_size)

    compress = compressions[compression]['compress']

    # Compress a sample, return original if not very compressed
    sample = byte_sample(payload, sample_size, nsamples)
    if len(compress(sample)) > 0.9 * len(sample):  # not very compressible
        return None, payload

    compressed = compress(payload)
    if len(compressed) > 0.9 * len(payload):  # not very compressible
        return None, payload
    else:
        return compression, compressed


def dumps_msgpack(msg):
    """ Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, use_bin_type=True)

    fmt, payload = maybe_compress(payload)
    if fmt:
        header['compression'] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b''

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """ Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    if header:
        header = msgpack.loads(header, encoding='utf8')
    else:
        header = {}

    if header.get('compression'):
        try:
            decompress = compressions[header['compression']]['decompress']
            payload = decompress(payload)
        except KeyError:
            raise ValueError("Data is compressed as %s but we don't have this"
                             " installed" % header['compression'].decode())

    return msgpack.loads(payload, encoding='utf8')


def decompress(header, frames):
    return [compressions[c]['decompress'](frame)
            for c, frame in zip(header['compression'], frames)]
