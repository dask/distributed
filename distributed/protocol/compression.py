"""
Record known compressors

Includes utilities for determining whether or not to compress
"""
import logging
import os, random

import dask
from toolz import identity, partial

try:
    import blosc

    n = blosc.set_nthreads(2)
    if hasattr("blosc", "releasegil"):
        blosc.set_releasegil(True)
except ImportError:
    blosc = False

from ..utils import ignoring, ensure_bytes


compressions = {None: {"compress": identity, "decompress": identity}}

compressions[False] = compressions[None]  # alias


default_compression = None


logger = logging.getLogger(__name__)


with ignoring(ImportError):
    import zlib

    compressions["zlib"] = {"compress": zlib.compress, "decompress": zlib.decompress}

with ignoring(ImportError):
    import snappy

    def _fixed_snappy_decompress(data):
        # snappy.decompress() doesn't accept memoryviews
        if isinstance(data, (memoryview, bytearray)):
            data = bytes(data)
        return snappy.decompress(data)

    compressions["snappy"] = {
        "compress": snappy.compress,
        "decompress": _fixed_snappy_decompress,
    }
    default_compression = "snappy"

with ignoring(ImportError):
    import lz4

    try:
        # try using the new lz4 API
        import lz4.block

        lz4_compress = lz4.block.compress
        lz4_decompress = lz4.block.decompress
    except ImportError:
        # fall back to old one
        lz4_compress = lz4.LZ4_compress
        lz4_decompress = lz4.LZ4_uncompress

    # helper to bypass missing memoryview support in current lz4
    # (fixed in later versions)

    def _fixed_lz4_compress(data):
        try:
            return lz4_compress(data)
        except TypeError:
            if isinstance(data, (memoryview, bytearray)):
                return lz4_compress(bytes(data))
            else:
                raise

    def _fixed_lz4_decompress(data):
        try:
            return lz4_decompress(data)
        except (ValueError, TypeError):
            if isinstance(data, (memoryview, bytearray)):
                return lz4_decompress(bytes(data))
            else:
                raise

    compressions["lz4"] = {
        "compress": _fixed_lz4_compress,
        "decompress": _fixed_lz4_decompress,
    }
    default_compression = "lz4"

with ignoring(ImportError):
    _import_policy = dask.config.get("distributed.comm.zstd.import_policy", 'default')
    os.environ['PYTHON_ZSTANDARD_IMPORT_POLICY'] = _import_policy

    import zstandard as zstd

    zstd_level = dask.config.get("distributed.comm.zstd.level", None)
    zstd_kwargs = dict(
        write_checksum=dask.config.get("distributed.comm.zstd.write_checksum", None),
        write_content_size=dask.config.get("distributed.comm.zstd.write_content_size", True),
        write_dict_id=dask.config.get("distributed.comm.zstd.write_dict_id", True),
        threads=dask.config.get("distributed.comm.zstd.threads", 0),
    )

    if zstd_level is not None:
        zstd_kwargs['level'] = zstd_level

    zstd_compressor = zstd.ZstdCompressor(**zstd_kwargs)
    zstd_decompressor = zstd.ZstdDecompressor()

    def zstd_compress(data):
        if isinstance(data, (memoryview, bytearray)):
            data = bytes(data)
        return zstd_compressor.compress(data)

    def zstd_decompress(data):
        if isinstance(data, (memoryview, bytearray)):
            data = bytes(data)
        return zstd_decompressor.decompress(data)

    compressions["zstd"] = {
        "compress": zstd_compress,
        "decompress": zstd_decompress,
    }
    default_compression = "zstd"

with ignoring(ImportError):
    import blosc

    compressions["blosc"] = {
        "compress": partial(blosc.compress, clevel=5, cname="lz4"),
        "decompress": blosc.decompress,
    }


default = dask.config.get("distributed.comm.compression")
if default != "auto":
    if default in compressions:
        default_compression = default
    else:
        raise ValueError(
            "Default compression '%s' not found.\n"
            "Choices include auto, %s"
            % (default, ", ".join(sorted(map(str, compressions))))
        )


def byte_sample(b, size, n):
    """ Sample a bytestring from many locations

    Parameters
    ----------
    b: bytes or memoryview
    size: int
        size of each sample to collect
    n: int
        number of samples to collect
    """
    starts = [random.randint(0, len(b) - size) for j in range(n)]
    ends = []
    for i, start in enumerate(starts[:-1]):
        ends.append(min(start + size, starts[i + 1]))
    ends.append(starts[-1] + size)

    parts = [b[start:end] for start, end in zip(starts, ends)]
    return b"".join(map(ensure_bytes, parts))


def maybe_compress(payload, min_size=1e4, sample_size=1e4, nsamples=5):
    """
    Maybe compress payload

    1.  We don't compress small messages
    2.  We sample the payload in a few spots, compress that, and if it doesn't
        do any good we return the original
    3.  We then compress the full original, it it doesn't compress well then we
        return the original
    4.  We return the compressed result
    """
    compression = dask.config.get("distributed.comm.compression")
    if compression == "auto":
        compression = default_compression

    if not compression:
        return None, payload
    if len(payload) < min_size:
        return None, payload
    if len(payload) > 2 ** 31:  # Too large, compression libraries often fail
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

    if default_compression and blosc and type(payload) is memoryview:
        # Blosc does itemsize-aware shuffling, resulting in better compression
        compressed = blosc.compress(
            payload, typesize=payload.itemsize, cname="lz4", clevel=5
        )
        compression = "blosc"
    else:
        compressed = compress(ensure_bytes(payload))

    if len(compressed) > 0.9 * nbytes:  # full data not very compressible
        return None, payload
    else:
        return compression, compressed


def decompress(header, frames):
    """ Decompress frames according to information in the header """
    return [
        compressions[c]["decompress"](frame)
        for c, frame in zip(header["compression"], frames)
    ]
