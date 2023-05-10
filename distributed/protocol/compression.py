"""
Record known compressors

Includes utilities for determining whether or not to compress
"""
from __future__ import annotations

import zlib
from collections.abc import Callable, Sequence
from contextlib import suppress
from functools import partial
from random import randint
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from packaging.version import parse as parse_version
from tlz import identity

import dask

from distributed.metrics import context_meter
from distributed.utils import ensure_memoryview, nbytes

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

# TODO remove quotes (requires Python >=3.10)
AnyBytes: TypeAlias = "bytes | bytearray | memoryview"


class Compression(NamedTuple):
    name: None | str
    compress: Callable[[AnyBytes], AnyBytes]
    decompress: Callable[[AnyBytes], AnyBytes]
    decompress_into: Callable[[AnyBytes, memoryview], None] | None


compressions: dict[str | None | Literal[False], Compression] = {
    None: Compression(None, identity, identity, None),
    False: Compression(None, identity, identity, None),  # alias
    "auto": Compression(None, identity, identity, None),
    "zlib": Compression("zlib", zlib.compress, zlib.decompress, None),
}


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

    compressions["snappy"] = Compression(
        "snappy", snappy.compress, snappy.decompress, None
    )
    compressions["auto"] = compressions["snappy"]

with suppress(ImportError):
    import lz4

    # Required to use `lz4.block` APIs and Python Buffer Protocol support.
    if parse_version(lz4.__version__) < parse_version("0.23.1"):
        raise ImportError("Need lz4 >= 0.23.1")  # pragma: nocover

    import lz4.block

    compressions["lz4"] = Compression(
        "lz4",
        lz4.block.compress,
        # Avoid expensive deep copies when deserializing writeable numpy arrays
        # See distributed.protocol.numpy.deserialize_numpy_ndarray
        # Note that this is only useful for buffers smaller than distributed.comm.shard;
        # larger ones are deep-copied between decompression and serialization anyway in
        # order to merge them.
        partial(lz4.block.decompress, return_bytearray=True),
        None,
    )
    compressions["auto"] = compressions["lz4"]


with suppress(ImportError):
    import cramjam

    # TODO change to 2.7.0. This is a hack to make it work with 2.7.0-rc3.
    if parse_version(cramjam.__version__) < parse_version("2.6.99"):
        raise ImportError("Need cramjam >= 2.7.0")  # pragma: nocover

    compressions["cramjam.lz4"] = Compression(
        "cramjam.lz4",
        cramjam.lz4.compress_block,
        cramjam.lz4.decompress_block,
        cramjam.lz4.decompress_block_into,
    )
    compressions["auto"] = compressions["cramjam.lz4"]


with suppress(ImportError):
    import zstandard

    # Required for Python Buffer Protocol support.
    if parse_version(zstandard.__version__) < parse_version("0.9.0"):
        raise ImportError("Need zstandard >= 0.9.0")  # pragma: nocover

    def zstd_compress(data):
        zstd_compressor = zstandard.ZstdCompressor(
            level=dask.config.get("distributed.comm.zstd.level"),
            threads=dask.config.get("distributed.comm.zstd.threads"),
        )
        return zstd_compressor.compress(data)

    def zstd_decompress(data):
        zstd_decompressor = zstandard.ZstdDecompressor()
        return zstd_decompressor.decompress(data)

    compressions["zstd"] = Compression("zstd", zstd_compress, zstd_decompress, None)


def get_compression_settings(key: str) -> str | None:
    """Fetch and validate compression settings, with a nice error message in case of
    failure. This also resolves 'auto', which may differ between different hosts of the
    same cluster.
    """
    name = dask.config.get(key)
    try:
        return compressions[name].name
    except KeyError:
        valid = ",".join(repr(n) for n in compressions)
        raise ValueError(
            f"Invalid compression setting {key}={name}. Valid options are {valid}."
        )


def byte_sample(b: memoryview, size: int, n: int) -> memoryview:
    """Sample a bytestring from many locations

    Parameters
    ----------
    b : full memoryview
    size : int
        target size of each sample to collect
        (may be smaller if samples collide)
    n : int
        number of samples to collect
    """
    assert size >= 0 and n >= 0
    if size == 0 or n == 0:
        return memoryview(b"")

    parts = []
    max_start = b.nbytes - size
    start = randint(0, max_start)
    for _ in range(n - 1):
        next_start = randint(0, max_start)
        end = min(start + size, next_start)
        parts.append(b[start:end])
        start = next_start
    parts.append(b[start : start + size])

    if n == 1:
        return parts[0]
    else:
        return memoryview(b"".join(parts))


@context_meter.meter("compress")
def maybe_compress(
    payload: bytes | bytearray | memoryview,
    *,
    min_size: int = 10_000,
    sample_size: int = 10_000,
    nsamples: int = 5,
    min_ratio: float = 0.7,
    compression: str | None | Literal[False] = "auto",
) -> tuple[str | None, AnyBytes]:
    """Maybe compress payload

    1. Don't compress payload if smaller than min_size
    2. Sample the payload in <nsamples> spots, compress those, and if it doesn't
       compress to at least <min_ratio> to the original, return the original
    3. Then compress the full original; it doesn't compress at least to <min_ratio>,
       return the original
    4. Return the compressed output

    Returns
    -------
    - Name of compression algorithm used
    - Either compressed or original payload
    """
    comp = compressions[compression]
    if not comp.name:
        return None, payload
    if not (min_size <= nbytes(payload) <= 2**31):
        # Either too small to bother
        # or too large (compression libraries often fail)
        return None, payload

    # Take a view of payload for efficient usage
    mv = ensure_memoryview(payload)

    # Try compressing a sample to see if it compresses well
    sample = byte_sample(mv, sample_size, nsamples)
    if len(comp.compress(sample)) <= min_ratio * sample.nbytes:
        # Try compressing the real thing and check how compressed it is
        compressed = comp.compress(mv)
        if len(compressed) <= min_ratio * mv.nbytes:
            return comp.name, compressed
    # Skip compression as the sample or the data didn't compress well
    return None, payload


@context_meter.meter("decompress")
def decompress(
    header: dict[str, Any], frames: Sequence[AnyBytes]
) -> tuple[dict[str, Any], list[AnyBytes]]:
    """Decompress frames according to information in the header.

    See also
    --------
    merge_and_deserialize
    """
    from distributed.comm.utils import host_array

    merged_frames: list[AnyBytes] = []
    split_num_sub_frames: list[int] = []
    split_offsets: list[int] = []

    for n, offset in zip(header["split-num-sub-frames"], header["split-offsets"]):
        compression_names = header["compression"][offset : offset + n]
        compression = compressions[compression_names[0]]
        subframes = frames[offset : offset + n]

        if compression.decompress_into and len(set(compression_names)) == 1:
            nbytes = header["uncompressed_size"][offset : offset + n]
            merged = host_array(sum(nbytes))
            merged_offset = 0
            for frame_i, nbytes_i in zip(subframes, nbytes):
                merged_i = merged[merged_offset : merged_offset + nbytes_i]
                compression.decompress_into(frame_i, merged_i)
                merged_offset += nbytes_i
            merged_frames.append(merged)
            split_num_sub_frames.append(1)
            split_offsets.append(len(split_offsets))

        else:
            for name, frame in zip(compression_names, subframes):
                merged_frames.append(compressions[name].decompress(frame))
            split_num_sub_frames.append(n)
            split_offsets.append(offset)

    header = header.copy()
    header["split-num-sub-frames"] = split_num_sub_frames
    header["split-offsets"] = split_offsets
    return header, merged_frames
