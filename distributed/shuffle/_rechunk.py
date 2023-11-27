"""
Utilities for rechunking arrays through p2p shuffles
====================================================

Tensors (or n-D arrays) in dask are split up across the workers as
regular n-D "chunks" or bricks. These bricks are stacked up to form
the global array.

A key algorithm for these tensors is to "rechunk" them. That is to
reassemble the same global representation using differently shaped n-D
bricks.

For example, to take an FFT of an n-D array, one uses a sequence of 1D
FFTs along each axis. The implementation in dask (and indeed almost
all distributed array frameworks) requires that 1D
axis along which the FFT is taken is local to a single brick. So to
perform the global FFT we need to arrange that each axis in turn is
local to bricks.

This can be achieved through all-to-all communication between the
workers to exchange sub-pieces of their individual bricks, given a
"rechunking" scheme.

To perform the redistribution, each input brick is cut up into some
number of smaller pieces, each of which contributes to one of the
output bricks. The mapping from input brick to output bricks
decomposes into the Cartesian product of axis-by-axis mappings. To
see this, consider first a 1D example.

Suppose our array is split up into three equally sized bricks::

    |----0----|----1----|----2----|

And the requested output chunks are::

    |--A--|--B--|----C----|---D---|

So brick 0 contributes to output bricks A and B; brick 1 contributes
to B and C; and brick 2 contributes to C and D.

Now consider a 2D example of the same problem::

    +----0----+----1----+----2----+
    |         |         |         |
    α         |         |         |
    |         |         |         |
    +---------+---------+---------+
    |         |         |         |
    β         |         |         |
    |         |         |         |
    +---------+---------+---------+
    |         |         |         |
    γ         |         |         |
    |         |         |         |
    +---------+---------+---------+

Each brick can be described as the ordered pair of row and column
1D bricks, (0, α), (0, β), ..., (2, γ). Since the rechunking does
not also reshape the array, axes do not "interfere" with one another
when determining output bricks::

    +--A--+--B--+----C----+---D---+
    |     |     |         |       |
    Σ     |     |         |       |
    |     |     |         |       |
    +-----+ ----+---------+-------+
    |     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    Π     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    |     |     |         |       |
    +-----+-----+---------+-------+

Consider the output (B, Σ) brick. This is contributed to by the
input (0, α) and (1, α) bricks. Determination of the subslices is
just done by slicing the the axes separately and combining them.

The key thing to note here is that we never need to create, and
store, the dense 2D mapping, we can instead construct it on the fly
for each output brick in turn as necessary.

The implementation here uses :func:`split_axes` to construct these
1D rechunkings. The output partitioning in
:meth:`~.ArrayRechunkRun.add_partition` then lazily constructs the
subsection of the Cartesian product it needs to determine the slices
of the current input brick.

This approach relies on the generic p2p buffering machinery to
ensure that there are not too many small messages exchanged, since
no special effort is made to minimise messages between workers when
a worker might have two adjacent input bricks that are sliced into
the same output brick.
"""

from __future__ import annotations

import math
import mmap
import os
from collections import defaultdict
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import toolz
from tornado.ioloop import IOLoop

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer
from dask.typing import Key

from distributed.core import PooledRPCCall
from distributed.shuffle._core import (
    NDIndex,
    ShuffleId,
    ShuffleRun,
    ShuffleSpec,
    get_worker_plugin,
    handle_transfer_errors,
    handle_unpack_errors,
)
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._pickle import unpickle_bytestream
from distributed.shuffle._scheduler_plugin import ShuffleSchedulerPlugin
from distributed.shuffle._shuffle import barrier_key, shuffle_barrier
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin
from distributed.sizeof import sizeof

if TYPE_CHECKING:
    import numpy as np
    from typing_extensions import TypeAlias

    import dask.array as da

ChunkedAxis: TypeAlias = tuple[float, ...]  # chunks must either be an int or NaN
ChunkedAxes: TypeAlias = tuple[ChunkedAxis, ...]
NDSlice: TypeAlias = tuple[slice, ...]


def rechunk_transfer(
    input: np.ndarray,
    id: ShuffleId,
    input_chunk: NDIndex,
    new: ChunkedAxes,
    old: ChunkedAxes,
    ndslice: NDSlice,
    disk: bool,
) -> int:
    with handle_transfer_errors(id):
        return get_worker_plugin().add_partition(
            input,
            partition_id=input_chunk,
            spec=ArrayRechunkSpec(id=id, new=new, old=old, disk=disk),
            ndslice=ndslice,
        )


def rechunk_unpack(
    id: ShuffleId, output_chunk: NDIndex, barrier_run_id: int
) -> np.ndarray:
    with handle_unpack_errors(id):
        return get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_chunk
        )


@dataclass
class Partial:
    """Information used to perform a partial rechunk along a single axis"""

    #: Slice of the old chunks along this axis that belong to the partial
    old: slice
    #: Slice of the new chunks along this axis that belong to the partial
    new: slice
    #: Index of the first value of the left-most old chunk along this axis
    #: to include in this partial. Everything left to this index belongs to
    #: the previous partial.
    left_start: int
    #: Index of the first value of the right-most old chunk along this axis
    #: to exclude from this partial.
    #: This corresponds to `left_start` of the subsequent partial.
    right_stop: int


NDPartial: TypeAlias = tuple[Partial, ...]


def rechunk_p2p(x: da.Array, chunks: ChunkedAxes) -> da.Array:
    import dask.array as da

    if x.size == 0:
        # Special case for empty array, as the algorithm below does not behave correctly
        return da.empty(x.shape, chunks=chunks, dtype=x.dtype)

    partial_axes = _compute_partials(x, chunks)

    ndpartials = product(*partial_axes)

    dsk = {}
    token = tokenize(x, chunks)
    name = f"rechunk-p2p-{token}"
    for ndpartial in ndpartials:
        p = partial_rechunk(
            x,
            chunks=chunks,
            ndpartial=ndpartial,
            name=name,
        )
        dsk.update(p)

    layer = MaterializedLayer(dsk)
    graph = HighLevelGraph.from_collections(name, layer, dependencies=[x])
    arr = da.Array(graph, name, chunks, meta=x)
    return arr


def _compute_partials(
    x: da.Array, chunks: ChunkedAxes
) -> tuple[tuple[Partial, ...], ...]:
    """Compute the individual partial rechunks that can be performed on each axis."""
    from dask.array.rechunk import old_to_new

    chunked_shape = tuple(len(axis) for axis in chunks)
    _old_to_new = old_to_new(x.chunks, chunks)

    sliced_axes = _partial_slices(_old_to_new, chunked_shape)

    partial_axes = []
    for axis_index, slices in enumerate(sliced_axes):
        partials = []
        for slice_ in slices:
            last_old_chunk: int
            first_old_chunk, first_old_slice = _old_to_new[axis_index][slice_.start][0]
            last_old_chunk, last_old_slice = _old_to_new[axis_index][slice_.stop - 1][
                -1
            ]
            partials.append(
                Partial(
                    old=slice(first_old_chunk, last_old_chunk + 1),
                    new=slice_,
                    left_start=first_old_slice.start,
                    right_stop=last_old_slice.stop,
                )
            )
        partial_axes.append(tuple(partials))
    return tuple(partial_axes)


def _partial_slices(
    old_to_new: list[list[list[tuple[int, slice]]]], chunked_shape: NDIndex
) -> tuple[tuple[slice, ...], ...]:
    sliced_axes = []

    for axis_index, old_to_new_axis in enumerate(old_to_new):
        # Two consecutive output chunks A and B belong to the same partial rechunk
        # if B is fully included in the right-most input chunk of A, i.e.,
        # separating A and B would not allow us to cull more input tasks.
        last_old_chunk: int | None = (
            None  # Index of the last input chunk of this partial rechunk
        )
        partial_splits = [0]
        recipe: list[tuple[int, slice]]
        for new_chunk_index, recipe in enumerate(old_to_new_axis):
            if len(recipe) == 0:
                continue
            current_last_old_chunk, old_slice = recipe[-1]
            if last_old_chunk is None:
                last_old_chunk = current_last_old_chunk
            elif last_old_chunk != current_last_old_chunk:
                partial_splits.append(new_chunk_index)
                last_old_chunk = current_last_old_chunk
        partial_splits.append(chunked_shape[axis_index])
        sliced_axes.append(
            tuple(slice(a, b) for a, b in toolz.sliding_window(2, partial_splits))
        )
    return tuple(sliced_axes)


def _partial_ndindex(ndslice: NDSlice) -> np.ndindex:
    import numpy as np

    return np.ndindex(tuple(slice.stop - slice.start for slice in ndslice))


def _global_index(partial_index: NDIndex, partial_offset: NDIndex) -> NDIndex:
    return tuple(index + offset for index, offset in zip(partial_index, partial_offset))


def partial_rechunk(
    x: da.Array,
    chunks: ChunkedAxes,
    ndpartial: NDPartial,
    name: str,
) -> Any:
    old_ndslice = tuple(partial.old for partial in ndpartial)
    new_ndslice = tuple(partial.new for partial in ndpartial)
    left_starts = tuple(partial.left_start for partial in ndpartial)
    right_stops = tuple(partial.right_stop for partial in ndpartial)

    dsk: dict = {}

    old_partial_offset = tuple(slice_.start for slice_ in old_ndslice)
    n_new_chunks = math.prod(slc.stop - slc.start for slc in new_ndslice)

    # The shuffle is a concatenation and produces only a single output
    if n_new_chunks == 1:
        input_tasks = []
        for partial_index in _partial_ndindex(old_ndslice):
            global_index = _global_index(partial_index, old_partial_offset)
            input_tasks.append((x.name,) + global_index)
        global_index = tuple(int(axis.start) for axis in new_ndslice)
        shape = tuple(slc.stop - slc.start for slc in old_ndslice)
        dsk[(name,) + global_index] = (
            concatenate,
            shape,
            left_starts,
            right_stops,
            *input_tasks,
        )
        return dsk

    partial_token = tokenize(x, chunks, new_ndslice)
    _barrier_key = barrier_key(ShuffleId(partial_token))
    transfer_name = f"rechunk-transfer-{partial_token}"
    disk: bool = dask.config.get("distributed.p2p.disk")

    ndim = len(x.shape)

    _partial_old = []
    for axis_index in range(ndim):
        partial = ndpartial[axis_index]
        c: list[int] = []
        c = list(x.chunks[axis_index][partial.old])
        c[0] = c[0] - partial.left_start
        if (stop := partial.right_stop) is not None:
            c[-1] = stop
        _partial_old.append(tuple(c))
    partial_old = tuple(_partial_old)

    partial_new: ChunkedAxes = tuple(
        chunks[axis_index][ndpartial[axis_index].new] for axis_index in range(ndim)
    )

    shape = tuple(partial.old.stop - partial.old.start for partial in ndpartial)
    transfer_keys = []
    for partial_index in _partial_ndindex(old_ndslice):
        ndslice = ndslice_for(partial_index, shape, left_starts, right_stops)

        global_index = _global_index(partial_index, old_partial_offset)
        transfer_keys.append((transfer_name,) + global_index)
        dsk[(transfer_name,) + global_index] = (
            rechunk_transfer,
            (x.name,) + global_index,
            partial_token,
            partial_index,
            partial_new,
            partial_old,
            ndslice,
            disk,
        )

    dsk[_barrier_key] = (shuffle_barrier, partial_token, transfer_keys)

    new_partial_offset = tuple(axis.start for axis in new_ndslice)
    for partial_index in _partial_ndindex(new_ndslice):
        global_index = _global_index(partial_index, new_partial_offset)
        dsk[(name,) + global_index] = (
            rechunk_unpack,
            partial_token,
            partial_index,
            _barrier_key,
        )
    return dsk


def ndslice_for(
    partial_index: NDIndex, shape: NDIndex, left_starts: NDIndex, right_stops: NDIndex
) -> NDSlice:
    slices = []
    for axis in range(len(shape)):
        index = partial_index[axis]
        start = left_starts[axis] if index == 0 else 0
        stop = right_stops[axis] if index == shape[axis] - 1 else None
        slices.append(slice(start, stop))
    return tuple(slices)


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


def concatenate(
    shape: tuple[int, ...],
    left_starts: NDIndex,
    right_stops: NDIndex,
    *shards: np.ndarray,
) -> np.ndarray:
    import numpy as np

    from dask.array.core import concatenate3

    rec_cat_arg = np.empty(shape, dtype="O")
    for shard_index, shard in zip(np.ndindex(*shape), shards):
        ndslice = ndslice_for(shard_index, shape, left_starts, right_stops)
        rec_cat_arg[shard_index] = shard[ndslice]
    arrs = rec_cat_arg.tolist()

    return concatenate3(arrs)


def convert_chunk(shards: list[list[tuple[NDIndex, np.ndarray]]]) -> np.ndarray:
    import numpy as np

    from dask.array.core import concatenate3

    indexed: dict[NDIndex, np.ndarray] = {}
    for sublist in shards:
        for index, shard in sublist:
            indexed[index] = shard

    subshape = [max(dim) + 1 for dim in zip(*indexed.keys())]
    assert len(indexed) == np.prod(subshape)

    rec_cat_arg = np.empty(subshape, dtype="O")
    for index, shard in indexed.items():
        rec_cat_arg[tuple(index)] = shard
    arrs = rec_cat_arg.tolist()

    # This may block for several seconds, as it physically reads the memory-mapped
    # buffers from disk
    return concatenate3(arrs)


class ArrayRechunkRun(ShuffleRun[NDIndex, "np.ndarray"]):
    """State for a single active rechunk execution

    This object is responsible for splitting, sending, receiving and combining
    data shards.

    It is entirely agnostic to the distributed system and can perform a rechunk
    with other run instances using `rpc``.

    The user of this needs to guarantee that only `ArrayRechunkRun`s of the same unique
    `ShuffleID` and `run_id` interact.

    Parameters
    ----------
    worker_for:
        A mapping partition_id -> worker_address.
    old:
        Existing chunking of the array per dimension.
    new:
        Desired chunking of the array per dimension.
    id:
        A unique `ShuffleID` this belongs to.
    run_id:
        A unique identifier of the specific execution of the shuffle this belongs to.
    local_address:
        The local address this Shuffle can be contacted by using `rpc`.
    directory:
        The scratch directory to buffer data in.
    executor:
        Thread pool to use for offloading compute.
    rpc:
        A callable returning a PooledRPCCall to contact other Shuffle instances.
        Typically a ConnectionPool.
    scheduler:
        A PooledRPCCall to to contact the scheduler.
    memory_limiter_disk:
    memory_limiter_comm:
        A ``ResourceLimiter`` limiting the total amount of memory used in either
        buffer.
    """

    def __init__(
        self,
        worker_for: dict[NDIndex, str],
        old: ChunkedAxes,
        new: ChunkedAxes,
        id: ShuffleId,
        run_id: int,
        local_address: str,
        directory: str,
        executor: ThreadPoolExecutor,
        rpc: Callable[[str], PooledRPCCall],
        scheduler: PooledRPCCall,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
        disk: bool,
        loop: IOLoop,
    ):
        super().__init__(
            id=id,
            run_id=run_id,
            local_address=local_address,
            directory=directory,
            executor=executor,
            rpc=rpc,
            scheduler=scheduler,
            memory_limiter_comms=memory_limiter_comms,
            memory_limiter_disk=memory_limiter_disk,
            disk=disk,
            loop=loop,
        )
        self.old = old
        self.new = new
        partitions_of = defaultdict(list)
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = worker_for
        self.split_axes = split_axes(old, new)

    async def _receive(
        self,
        data: list[tuple[NDIndex, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]]],
    ) -> None:
        self.raise_if_closed()

        # Repartition shards and filter out already received ones
        shards = defaultdict(list)
        for d in data:
            id1, payload = d
            if id1 in self.received:
                continue
            self.received.add(id1)
            for id2, shard in payload:
                shards[id2].append(shard)
            self.total_recvd += sizeof(d)
        del data
        if not shards:
            return

        try:
            await self._write_to_disk(shards)
        except Exception as e:
            self._exception = e
            raise

    def _shard_partition(
        self,
        data: np.ndarray,
        partition_id: NDIndex,
        ndslice: NDSlice | None = None,
        **kwargs: Any,
    ) -> dict[str, tuple[NDIndex, Any]]:
        assert ndslice is not None
        out: dict[str, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]] = defaultdict(
            list
        )
        data = data[ndslice]

        pass
        ndsplits = product(*(axis[i] for axis, i in zip(self.split_axes, partition_id)))

        for ndsplit in ndsplits:
            chunk_index, shard_index, shard_slice = zip(*ndsplit)

            shard = data[shard_slice]
            # Don't wait until all shards have been transferred over the network
            # before data can be released
            if shard.base is not None:
                shard = shard.copy()

            out[self.worker_for[chunk_index]].append(
                (chunk_index, (shard_index, shard))
            )
        return {k: (partition_id, v) for k, v in out.items()}

    def _get_output_partition(
        self, partition_id: NDIndex, key: Key, **kwargs: Any
    ) -> np.ndarray:
        # Quickly read metadata from disk.
        # This is a bunch of seek()'s interleaved with short reads.
        data = self._read_from_disk(partition_id)
        # Copy the memory-mapped buffers from disk into memory.
        # This is where we'll spend most time.
        with self._disk_buffer.time("read"):
            return convert_chunk(data)

    def deserialize(self, buffer: Any) -> Any:
        return buffer

    def read(self, path: Path) -> tuple[list[list[tuple[NDIndex, np.ndarray]]], int]:
        """Open a memory-mapped file descriptor to disk, read all metadata, and unpickle
        all arrays. This is a fast sequence of short reads interleaved with seeks.
        Do not read in memory the actual data; the arrays' buffers will point to the
        memory-mapped area.

        The file descriptor will be automatically closed by the kernel when all the
        returned arrays are dereferenced, which will happen after the call to
        concatenate3.
        """
        with path.open(mode="r+b") as fh:
            buffer = memoryview(mmap.mmap(fh.fileno(), 0))

        # The file descriptor has *not* been closed!
        shards = list(unpickle_bytestream(buffer))
        return shards, buffer.nbytes

    def _get_assigned_worker(self, id: NDIndex) -> str:
        return self.worker_for[id]


@dataclass(frozen=True)
class ArrayRechunkSpec(ShuffleSpec[NDIndex]):
    new: ChunkedAxes
    old: ChunkedAxes

    def _pin_output_workers(self, plugin: ShuffleSchedulerPlugin) -> dict[NDIndex, str]:
        parts_out = product(*(range(len(c)) for c in self.new))
        return plugin._pin_output_workers(
            self.id, parts_out, _get_worker_for_hash_sharding
        )

    def create_run_on_worker(
        self,
        run_id: int,
        worker_for: dict[NDIndex, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        return ArrayRechunkRun(
            worker_for=worker_for,
            old=self.old,
            new=self.new,
            id=self.id,
            run_id=run_id,
            directory=os.path.join(
                plugin.worker.local_directory,
                f"shuffle-{self.id}-{run_id}",
            ),
            executor=plugin._executor,
            local_address=plugin.worker.address,
            rpc=plugin.worker.rpc,
            scheduler=plugin.worker.scheduler,
            memory_limiter_disk=plugin.memory_limiter_disk,
            memory_limiter_comms=plugin.memory_limiter_comms,
            disk=self.disk,
            loop=plugin.worker.loop,
        )


def _get_worker_for_hash_sharding(
    output_partition: NDIndex, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using hash sharding"""
    i = hash(output_partition) % len(workers)
    return workers[i]
