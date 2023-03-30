"""
Record known compressors

Includes utilities for determining whether or not to compress
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import suppress
from functools import partial
from random import randint
from typing import Literal

from packaging.version import parse as parse_version
from tlz import identity

import dask

from distributed.metrics import context_meter
from distributed.utils import ensure_memoryview, nbytes, no_default

compressions: dict[
    str | None | Literal[False],
    dict[Literal["compress", "decompress"], Callable[[bytes], bytes]],
] = {None: {"compress": identity, "decompress": identity}}

compressions[False] = compressions[None]  # alias


default_compression = None


logger = logging.getLogger(__name__)


with suppress(ImportError):
    import zlib

    compressions["zlib"] = {"compress": zlib.compress, "decompress": zlib.decompress}

with suppress(ImportError):
    import snappy

    # In python-snappy 0.5.3, support for the Python Buffer Protocol was added.
    # This is needed to handle other objects (like `memoryview`s) without
    # copying to `bytes` first.
    #
    # Note: `snappy.__version__` doesn't exist in a release yet.
    #       So do a little test that will fail if snappy is not 0.5.3 or later.
    try:
        snappy.compress(memoryview(b""))
    except TypeError:
        raise ImportError("Need snappy >= 0.5.3")

    compressions["snappy"] = {
        "compress": snappy.compress,
        "decompress": snappy.decompress,
    }
    default_compression = "snappy"

with suppress(ImportError):
    import lz4

    # Required to use `lz4.block` APIs and Python Buffer Protocol support.
    if parse_version(lz4.__version__) < parse_version("0.23.1"):
        raise ImportError("Need lz4 >= 0.23.1")

    import lz4.block

    compressions["lz4"] = {
        "compress": lz4.block.compress,
        # Avoid expensive deep copies when deserializing writeable numpy arrays
        # See distributed.protocol.numpy.deserialize_numpy_ndarray
        # Note that this is only useful for buffers smaller than distributed.comm.shard;
        # larger ones are deep-copied between decompression and serialization anyway in
        # order to merge them.
        "decompress": partial(lz4.block.decompress, return_bytearray=True),
    }
    default_compression = "lz4"


with suppress(ImportError):
    import zstandard

    # Required for Python Buffer Protocol support.
    if parse_version(zstandard.__version__) < parse_version("0.9.0"):
        raise ImportError("Need zstandard >= 0.9.0")

    def zstd_compress(data):
        zstd_compressor = zstandard.ZstdCompressor(
            level=dask.config.get("distributed.comm.zstd.level"),
            threads=dask.config.get("distributed.comm.zstd.threads"),
        )
        return zstd_compressor.compress(data)

    def zstd_decompress(data):
        zstd_decompressor = zstandard.ZstdDecompressor()
        return zstd_decompressor.decompress(data)

    compressions["zstd"] = {"compress": zstd_compress, "decompress": zstd_decompress}


def get_default_compression():
    default = dask.config.get("distributed.comm.compression")
    if default == "auto":
        return default_compression
    if default in compressions:
        return default
    raise ValueError(
        "Default compression '%s' not found.\n"
        "Choices include auto, %s"
        % (default, ", ".join(sorted(map(str, compressions))))
    )


get_default_compression()


def byte_sample(b, size, n):
    """Sample a bytestring from many locations

    Parameters
    ----------
    b : bytes or memoryview
    size : int
        target size of each sample to collect
        (may be smaller if samples collide)
    n : int
        number of samples to collect
    """
    assert size >= 0 and n >= 0
    if size == 0 or n == 0:
        return memoryview(b"")

    b = ensure_memoryview(b)

    parts = n * [None]
    max_start = b.nbytes - size
    start = randint(0, max_start)
    for i in range(n - 1):
        next_start = randint(0, max_start)
        end = min(start + size, next_start)
        parts[i] = b[start:end]
        start = next_start
    parts[-1] = b[start : start + size]

    if n == 1:
        return parts[0]
    else:
        return memoryview(b"".join(parts))


@context_meter.meter("compress")
def maybe_compress(
    payload,
    min_size=10_000,
    sample_size=10_000,
    nsamples=5,
    compression=no_default,
):
    """
    Maybe compress payload

    1.  We don't compress small messages
    2.  We sample the payload in a few spots, compress that, and if it doesn't
        do any good we return the original
    3.  We then compress the full original, it it doesn't compress well then we
        return the original
    4.  We return the compressed result
    """
    if compression is no_default:
        compression = dask.config.get("distributed.comm.compression")
    if not compression:
        return None, payload
    if not (min_size <= nbytes(payload) <= 2**31):
        # Either too small to bother
        # or too large (compression libraries often fail)
        return None, payload

    # Normalize function arguments
    if compression == "auto":
        compression = default_compression
    compress = compressions[compression]["compress"]

    # Take a view of payload for efficient usage
    mv = ensure_memoryview(payload)

    # Try compressing a sample to see if it compresses well
    sample = byte_sample(mv, sample_size, nsamples)
    if len(compress(sample)) <= 0.9 * sample.nbytes:
        # Try compressing the real thing and check how compressed it is
        compressed = compress(mv)
        if len(compressed) <= 0.9 * mv.nbytes:
            return compression, compressed
    # Skip compression as the sample or the data didn't compress well
    return None, payload


@context_meter.meter("decompress")
def decompress(header, frames):
    """Decompress frames according to information in the header"""
    return [
        compressions[c]["decompress"](frame)
        for c, frame in zip(header["compression"], frames)
    ]
