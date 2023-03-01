from __future__ import annotations

from collections import defaultdict
from itertools import product
from typing import TYPE_CHECKING, NamedTuple

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.shuffle._shuffle import (
    ShuffleId,
    ShuffleType,
    _get_worker_extension,
    barrier_key,
    shuffle_barrier,
)

if TYPE_CHECKING:
    import numpy as np
    from typing_extensions import TypeAlias

    import dask.array as da


# TODO remove quotes (requires Python >=3.9)
ChunkedAxis: TypeAlias = "tuple[float, ...]"  # chunks must either be an int or NaN
ChunkedAxes: TypeAlias = "tuple[ChunkedAxis, ...]"
NIndex: TypeAlias = "tuple[int, ...]"
NSlice: TypeAlias = "tuple[slice, ...]"


def rechunk_transfer(
    input: np.ndarray,
    id: ShuffleId,
    input_chunk: NIndex,
    new: ChunkedAxes,
    old: ChunkedAxes,
) -> int:
    try:
        return _get_worker_extension().add_partition(
            input,
            input_partition=input_chunk,
            shuffle_id=id,
            type=ShuffleType.ARRAY_RECHUNK,
            new=new,
            old=old,
        )
    except Exception as e:
        raise RuntimeError(f"rechunk_transfer failed during shuffle {id}") from e


def rechunk_unpack(
    id: ShuffleId, output_chunk: NIndex, barrier_run_id: int
) -> np.ndarray:
    try:
        return _get_worker_extension().get_output_partition(
            id, barrier_run_id, output_chunk
        )
    except Exception as e:
        raise RuntimeError(f"rechunk_unpack failed during shuffle {id}") from e


def rechunk_p2p(x: da.Array, chunks: ChunkedAxes) -> da.Array:
    import numpy as np

    import dask.array as da

    if x.size == 0:
        # Special case for empty array, as the algorithm below does not behave correctly
        return da.empty(x.shape, chunks=chunks, dtype=x.dtype)

    if dask.config.get("optimization.fuse.active") is not False:
        raise RuntimeError(
            "P2P rechunking requires the fuse optimization to be turned off. "
            "Set the 'optimization.fuse.active' config to False to deactivate."
        )

    dsk: dict = {}
    token = tokenize(x, chunks)
    _barrier_key = barrier_key(ShuffleId(token))
    name = f"rechunk-transfer-{token}"
    transfer_keys = []
    for index in np.ndindex(tuple(len(dim) for dim in x.chunks)):
        transfer_keys.append((name,) + index)
        dsk[(name,) + index] = (
            rechunk_transfer,
            (x.name,) + index,
            token,
            index,
            chunks,
            x.chunks,
        )

    dsk[_barrier_key] = (shuffle_barrier, token, transfer_keys)

    name = f"rechunk-p2p-{token}"

    for index in np.ndindex(tuple(len(dim) for dim in chunks)):
        dsk[(name,) + index] = (rechunk_unpack, token, index, _barrier_key)

    with dask.annotate(shuffle=lambda key: key[1:]):
        layer = MaterializedLayer(dsk)
        graph = HighLevelGraph.from_collections(name, layer, dependencies=[x])

        return da.Array(graph, name, chunks, meta=x)


class ShardID(NamedTuple):
    """Unique identifier of an individual shard within an array rechunk

    When rechunking a 1d-array with two chunks into a 1d-array with a single chunk
    >>> old = ((2, 2),)  # doctest: +SKIP
    >>> new = ((4),)  # doctest: +SKIP
    >>> rechunk_slicing(old, new)  # doctest: +SKIP
    {
        # The first chunk of the old array belongs to the first
        # chunk of the new array at the first sub-index
        (0,): [(ShardID((0,), (0,)), (slice(0, 2, None),))],

        # The second chunk of the old array belongs to the first
        # chunk of the new array at the second sub-index
        (1,): [(ShardID((0,), (1,)), (slice(0, 2, None),))],
    }
    """

    #: Index of the new output chunk to which the shard belongs
    chunk_index: NIndex
    #: Index of the shard within the multi-dimensional array of shards that will be
    # concatenated into the new chunk
    shard_index: NIndex


def rechunk_slicing(
    old: ChunkedAxes, new: ChunkedAxes
) -> dict[NIndex, list[tuple[ShardID, NSlice]]]:
    """Calculate how to slice the old chunks to create the new chunks

    Returns
    -------
        Mapping of each old chunk to a list of tuples defining where each slice
        of the old chunk belongs. Each tuple consists of the index
        of the new chunk, the index of the slice within the composition of slices
        creating the new chunk, and the slice to be applied to the old chunk.
    """
    from dask.array.rechunk import intersect_chunks

    ndim = len(old)
    intersections = intersect_chunks(old, new)
    new_indices = product(*(range(len(c)) for c in new))

    slicing = defaultdict(list)

    for new_index, new_chunk in zip(new_indices, intersections):
        sub_shape = [len({slice[dim][0] for slice in new_chunk}) for dim in range(ndim)]

        shard_indices = product(*(range(dim) for dim in sub_shape))

        for shard_index, sliced_chunk in zip(shard_indices, new_chunk):
            old_index, nslice = zip(*sliced_chunk)
            slicing[old_index].append((ShardID(new_index, shard_index), nslice))
    return slicing
