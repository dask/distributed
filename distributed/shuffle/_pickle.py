from __future__ import annotations

import pickle
from collections.abc import Iterator
from typing import Any

from distributed.protocol.utils import pack_frames_prelude, unpack_frames


def pickle_bytelist(obj: object, prelude: bool = True) -> list[pickle.PickleBuffer]:
    """Variant of :func:`serialize_bytelist`, that doesn't support compression, locally
    defined classes, or any of its other fancy features but runs 10x faster for numpy
    arrays

    See Also
    --------
    serialize_bytelist
    unpickle_bytestream
    """
    frames: list = []
    pik = pickle.dumps(obj, protocol=5, buffer_callback=frames.append)
    frames.insert(0, pickle.PickleBuffer(pik))
    if prelude:
        frames.insert(0, pickle.PickleBuffer(pack_frames_prelude(frames)))
    return frames


def unpickle_bytestream(b: bytes | bytearray | memoryview) -> Iterator[Any]:
    """Unpickle the concatenated output of multiple calls to :func:`pickle_bytelist`

    See Also
    --------
    pickle_bytelist
    deserialize_bytes
    """
    while True:
        pik, *buffers, remainder = unpack_frames(b, remainder=True)
        yield pickle.loads(pik, buffers=buffers)
        if remainder.nbytes == 0:
            break
        b = remainder
