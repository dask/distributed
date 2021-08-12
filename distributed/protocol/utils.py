import struct
from typing import Sequence

import dask

from ..utils import nbytes

BIG_BYTES_SHARD_SIZE = dask.utils.parse_bytes(dask.config.get("distributed.comm.shard"))


msgpack_opts = {
    ("max_%s_len" % x): 2 ** 31 - 1 for x in ["str", "bin", "array", "map", "ext"]
}
msgpack_opts["strict_map_key"] = False
msgpack_opts["raw"] = False


def frame_split_size(frame, n=BIG_BYTES_SHARD_SIZE) -> list:
    """
    Split a frame into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    n = n or BIG_BYTES_SHARD_SIZE
    frame = memoryview(frame)

    if frame.nbytes <= n:
        return [frame]

    nitems = frame.nbytes // frame.itemsize
    items_per_shard = n // frame.itemsize

    return [frame[i : i + items_per_shard] for i in range(0, nitems, items_per_shard)]


def pack_frames_prelude(frames):
    nframes = len(frames)
    nbytes_frames = map(nbytes, frames)
    return struct.pack(f"Q{nframes}Q", nframes, *nbytes_frames)


def pack_frames(frames):
    """Pack frames into a byte-like object

    This prepends length information to the front of the bytes-like object

    See Also
    --------
    unpack_frames
    """
    return b"".join([pack_frames_prelude(frames), *frames])


def unpack_frames(b):
    """Unpack bytes into a sequence of frames

    This assumes that length information is at the front of the bytestring,
    as performed by pack_frames

    See Also
    --------
    pack_frames
    """
    b = memoryview(b)

    fmt = "Q"
    fmt_size = struct.calcsize(fmt)

    (n_frames,) = struct.unpack_from(fmt, b)
    lengths = struct.unpack_from(f"{n_frames}{fmt}", b, fmt_size)

    frames = []
    start = fmt_size * (1 + n_frames)
    for length in lengths:
        end = start + length
        frames.append(b[start:end])
        start = end

    return frames


try:
    import numpy as np
except ImportError:

    import warnings

    warnings.warn(
        "Some data transfer may be less memory-efficient because NumPy is not installed."
    )

    def merge_memoryviews(mvs: Sequence[memoryview]) -> memoryview:
        """Concatenate a sequence of memoryviews via copy"""
        return memoryview(bytearray().join(mvs))


else:

    def merge_memoryviews(mvs: Sequence[memoryview]) -> memoryview:
        """
        Zero-copy "concatenate" a sequence of contiguous memoryviews.

        Returns a new memoryview which slices into the underlying buffer
        to extract out the portion equivalent to all of ``mvs`` being concatenated.

        All the memoryviews must:
        * Share the same underlying buffer (``.obj``)
        * When merged, cover a continuous portion of that buffer with no gaps
        * Have the same strides
        * Be 1-dimensional
        * Have the same format
        * Be contiguous

        Note: This requires NumPy to get the underlying pointers of the memoryview
        objects via the buffer interface, since there is no API exposed to do so.
        """
        if not mvs:
            return memoryview(bytearray(0))
        if len(mvs) == 1:
            return mvs[0]

        first = mvs[0]
        obj = first.obj
        itemsize = first.itemsize
        format = first.format
        strides = first.strides
        assert all(
            mv.contiguous
            and mv.obj is obj
            and mv.strides == strides
            and mv.ndim == 1
            and mv.format == format
            for mv in mvs[1:]
        )

        first_start_addr = 0
        n = 0
        for mv in mvs:
            arr = np.asarray(mv)
            start_addr = arr.__array_interface__["data"][0]
            if first_start_addr == 0:
                first_start_addr = start_addr
            else:
                assert start_addr == first_start_addr + n * itemsize
            n += arr.size

        base_mv = memoryview(obj).cast(format)
        assert base_mv.itemsize == itemsize
        assert base_mv.strides == strides
        base_start_addr = np.asarray(base_mv).__array_interface__["data"][0]
        start_index, remainder = divmod(first_start_addr - base_start_addr, itemsize)
        assert remainder == 0

        return base_mv[start_index : start_index + n]
