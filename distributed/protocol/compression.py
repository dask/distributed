"""
Record known compressors

Includes utilities for determining whether or not to compress
"""
from __future__ import annotations

import logging
import random
from collections.abc import Callable
from contextlib import suppress
from typing import Literal

from packaging.version import parse as parse_version
from tlz import identity

import dask

from distributed.utils import ensure_bytes

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

    from lz4.block import compress as lz4_compress
    from lz4.block import decompress as lz4_decompress

    compressions["lz4"] = {
        "compress": lz4_compress,
        "decompress": lz4_decompress,
    }
    default_compression = "lz4"


with suppress(ImportError):
    import zstandard

    # Required for Python Buffer Protocol support.
    if parse_version(zstandard.__version__) < parse_version("0.9.0"):
        raise ImportError("Need zstandard >= 0.9.0")

    zstd_compressor = zstandard.ZstdCompressor(
        level=dask.config.get("distributed.comm.zstd.level"),
        threads=dask.config.get("distributed.comm.zstd.threads"),
    )

    zstd_decompressor = zstandard.ZstdDecompressor()

    def zstd_compress(data):
        return zstd_compressor.compress(data)

    def zstd_decompress(data):
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
        size of each sample to collect
    n : int
        number of samples to collect
    """
    starts = [random.randint(0, len(b) - size) for j in range(n)]
    ends = []
    for i, start in enumerate(starts[:-1]):
        ends.append(min(start + size, starts[i + 1]))
    ends.append(starts[-1] + size)

    parts = [b[start:end] for start, end in zip(starts, ends)]
    return b"".join(map(ensure_bytes, parts))


def maybe_compress(
    payload,
    min_size=1e4,
    sample_size=1e4,
    nsamples=5,
    compression=dask.config.get("distributed.comm.compression"),
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
    if compression == "auto":
        compression = default_compression

    if not compression:
        return None, payload
    if len(payload) < min_size:
        return None, payload
    if len(payload) > 2**31:  # Too large, compression libraries often fail
        return None, payload

    min_size = int(min_size)
    sample_size = int(sample_size)

    compress = compressions[compression]["compress"]

    # Compress a sample, return original if not very compressed
    sample = byte_sample(payload, sample_size, nsamples)
    if len(compress(sample)) > 0.9 * len(sample):  # sample not very compressible
        return None, payload

    if type(payload) is memoryview:
        nbytes = payload.itemsize * len(payload)
    else:
        nbytes = len(payload)

    compressed = compress(ensure_bytes(payload))

    if len(compressed) > 0.9 * nbytes:  # full data not very compressible
        return None, payload
    else:
        return compression, compressed


def decompress(header, frames):
    """Decompress frames according to information in the header"""
    return [
        compressions[c]["decompress"](frame)
        for c, frame in zip(header["compression"], frames)
    ]
