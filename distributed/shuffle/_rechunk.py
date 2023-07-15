from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.exceptions import Reschedule
from distributed.shuffle._shuffle import (
    ShuffleId,
    ShuffleType,
    _get_worker_plugin,
    barrier_key,
    shuffle_barrier,
)

if TYPE_CHECKING:
    import numpy as np
    from typing_extensions import TypeAlias

    import dask.array as da


ChunkedAxis: TypeAlias = tuple[float, ...]  # chunks must either be an int or NaN
ChunkedAxes: TypeAlias = tuple[ChunkedAxis, ...]
NDIndex: TypeAlias = tuple[int, ...]
NDSlice: TypeAlias = tuple[slice, ...]


def rechunk_transfer(
    input: np.ndarray,
    id: ShuffleId,
    input_chunk: NDIndex,
    new: ChunkedAxes,
    old: ChunkedAxes,
) -> int:
    try:
        return _get_worker_plugin().add_partition(
            input,
            partition_id=input_chunk,
            shuffle_id=id,
            type=ShuffleType.ARRAY_RECHUNK,
            new=new,
            old=old,
        )
    except Exception as e:
        raise RuntimeError(f"rechunk_transfer failed during shuffle {id}") from e


def rechunk_unpack(
    id: ShuffleId, output_chunk: NDIndex, barrier_run_id: int
) -> np.ndarray:
    try:
        return _get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_chunk
        )
    except Reschedule as e:
        raise e
    except Exception as e:
        raise RuntimeError(f"rechunk_unpack failed during shuffle {id}") from e


def rechunk_p2p(x: da.Array, chunks: ChunkedAxes) -> da.Array:
    import numpy as np

    import dask.array as da

    if x.size == 0:
        # Special case for empty array, as the algorithm below does not behave correctly
        return da.empty(x.shape, chunks=chunks, dtype=x.dtype)

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


class Split(NamedTuple):
    """Slice of a chunk that is concatenated with other splits to create a new chunk

    Splits define how to slice an input chunk on a single axis into small pieces
    that can be concatenated together with splits from other input chunks to create
    output chunks of a rechunk operation.
    """

    #: Index of the new output chunk to which this split belongs.
    chunk_index: int

    #: Index of the split within the list of splits that are concatenated
    #: to create the new chunk.
    split_index: int

    #: Slice of the input chunk.
    slice: slice


SplitChunk: TypeAlias = list[Split]
SplitAxis: TypeAlias = list[SplitChunk]
SplitAxes: TypeAlias = list[SplitAxis]


def split_axes(old: ChunkedAxes, new: ChunkedAxes) -> SplitAxes:
    """Calculate how to split the old chunks on each axis to create the new chunks

    Parameters
    ----------
    old : ChunkedAxes
        Chunks along each axis of the old array
    new : ChunkedAxes
        Chunks along each axis of the new array

    Returns
    -------
    SplitAxes
        Splits along each axis that determine how to slice the input chunks to create
        the new chunks by concatenating the resulting shards.
    """
    from dask.array.rechunk import old_to_new

    _old_to_new = old_to_new(old, new)

    axes = []
    for axis_index, new_axis in enumerate(_old_to_new):
        old_axis: SplitAxis = [[] for _ in old[axis_index]]
        for new_chunk_index, new_chunk in enumerate(new_axis):
            for split_index, (old_chunk_index, slice) in enumerate(new_chunk):
                old_axis[old_chunk_index].append(
                    Split(new_chunk_index, split_index, slice)
                )
        for old_chunk in old_axis:
            old_chunk.sort(key=lambda split: split.slice.start)
        axes.append(old_axis)
    return axes
