import struct
import msgpack

from ..utils import ensure_bytes, nbytes

BIG_BYTES_SHARD_SIZE = 2 ** 26


msgpack_opts = {
    ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"]
}
msgpack_opts["strict_map_key"] = False

try:
    msgpack.loads(msgpack.dumps(""), raw=False, **msgpack_opts)
    msgpack_opts["raw"] = False
except TypeError:
    # Backward compat with old msgpack (prior to 0.5.2)
    msgpack_opts["encoding"] = "utf-8"


def frame_split_size(frame, n=BIG_BYTES_SHARD_SIZE) -> list:
    """
    Split a frame into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    if nbytes(frame) <= n:
        return [frame]

    if nbytes(frame) > n:
        if isinstance(frame, (bytes, bytearray)):
            frame = memoryview(frame)
        try:
            itemsize = frame.itemsize
        except AttributeError:
            itemsize = 1

        return [
            frame[i : i + n // itemsize]
            for i in range(0, nbytes(frame) // itemsize, n // itemsize)
        ]


def merge_frames(header, frames):
    """ Merge frames into original lengths

    Examples
    --------
    >>> merge_frames({'lengths': [3, 3]}, [b'123456'])
    [b'123', b'456']
    >>> merge_frames({'lengths': [6]}, [b'123', b'456'])
    [b'123456']
    """
    lengths = list(header["lengths"])

    assert sum(lengths) == sum(map(nbytes, frames))

    if all(len(f) == l for f, l in zip(frames, lengths)):
        return frames

    frames = frames[::-1]
    lengths = lengths[::-1]

    out = []
    while lengths:
        l = lengths.pop()
        L = []
        while l:
            frame = frames.pop()
            if nbytes(frame) <= l:
                L.append(frame)
                l -= nbytes(frame)
            else:
                mv = memoryview(frame)
                L.append(mv[:l])
                frames.append(mv[l:])
                l = 0
        if len(L) == 1:  # no work necessary
            out.extend(L)
        else:
            out.append(b"".join(map(ensure_bytes, L)))
    return out


def pack_frames_prelude(frames):
    lengths = [struct.pack("Q", len(frames))] + [
        struct.pack("Q", nbytes(frame)) for frame in frames
    ]
    return b"".join(lengths)


def pack_frames(frames):
    """ Pack frames into a byte-like object

    This prepends length information to the front of the bytes-like object

    See Also
    --------
    unpack_frames
    """
    prelude = [pack_frames_prelude(frames)]

    if not isinstance(frames, list):
        frames = list(frames)

    return b"".join(prelude + frames)


def unpack_frames(b):
    """ Unpack bytes into a sequence of frames

    This assumes that length information is at the front of the bytestring,
    as performed by pack_frames

    See Also
    --------
    pack_frames
    """
    (n_frames,) = struct.unpack("Q", b[:8])

    frames = []
    start = 8 + n_frames * 8
    for i in range(n_frames):
        (length,) = struct.unpack("Q", b[(i + 1) * 8 : (i + 2) * 8])
        frame = b[start : start + length]
        frames.append(frame)
        start += length

    return frames
