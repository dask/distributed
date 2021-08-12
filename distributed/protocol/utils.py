import ctypes
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
    """
    # NOTE: this method relies on pointer arithmetic to figure out
    # where each memoryview starts within the underlying buffer.
    # There's no direct API to get the address of a memoryview,
    # so we use a trick through ctypes and the buffer protocol:
    # https://mattgwwalker.wordpress.com/2020/10/15/address-of-a-buffer-in-python/
    one_byte_carr = ctypes.c_byte * 1
    # ^ length and type don't matter, just use it to get the address of the first byte

    if not mvs:
        return memoryview(bytearray(0))
    if len(mvs) == 1:
        return mvs[0]

    first = mvs[0]
    obj = first.obj
    format = first.format

    first_start_addr = 0
    nbytes = 0
    for i, mv in enumerate(mvs):
        if mv.nbytes == 0:
            continue

        assert mv.obj is obj, f"{i}: memoryview has different buffer: {mv.obj} vs {obj}"
        assert mv.contiguous, f"{i}: memoryview non-contiguous"
        assert mv.ndim == 1, f"{i}: memoryview has {mv.ndim} dimensions, not 1"
        assert mv.format == format, f"{i}: inconsistent format: {mv.format} vs {format}"

        start_addr = ctypes.addressof(one_byte_carr.from_buffer(mv))
        if first_start_addr == 0:
            first_start_addr = start_addr
        else:
            expected_addr = first_start_addr + nbytes
            assert start_addr == expected_addr, (
                f"memoryview {i} does not start where the previous ends. "
                f"Expected {expected_addr:x}, starts {start_addr - expected_addr} byte(s) away."
            )
        nbytes += mv.nbytes

    if nbytes == 0:
        # all memoryviews were zero-length
        assert len(first) == 0
        return first

    assert first_start_addr != 0, "Underlying buffer is null pointer?!"

    base_mv = memoryview(obj).cast("B")
    base_start_addr = ctypes.addressof(one_byte_carr.from_buffer(base_mv))
    start_index = first_start_addr - base_start_addr

    return base_mv[start_index : start_index + nbytes].cast(format)
