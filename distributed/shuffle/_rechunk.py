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

import os
import pickle
from collections import defaultdict
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

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
    disk: bool,
) -> int:
    with handle_transfer_errors(id):
        return get_worker_plugin().add_partition(
            input,
            partition_id=input_chunk,
            spec=ArrayRechunkSpec(id=id, new=new, old=old, disk=disk),
        )


def rechunk_unpack(
    id: ShuffleId, output_chunk: NDIndex, barrier_run_id: int
) -> np.ndarray:
    with handle_unpack_errors(id):
        return get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_chunk
        )


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
    disk: bool = dask.config.get("distributed.p2p.disk")
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
            disk,
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


def convert_chunk(shards: list[list[tuple[NDIndex, np.ndarray]]]) -> np.ndarray:
    import numpy as np

    from dask.array.core import concatenate3

    indexed: dict[NDIndex, np.ndarray] = {}
    for sublist in shards:
        for index, shard in sublist:
            indexed[index] = shard
    del shards

    subshape = [max(dim) + 1 for dim in zip(*indexed.keys())]
    assert len(indexed) == np.prod(subshape)

    rec_cat_arg = np.empty(subshape, dtype="O")
    for index, shard in indexed.items():
        rec_cat_arg[tuple(index)] = shard
    arrs = rec_cat_arg.tolist()
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
        )
        self.old = old
        self.new = new
        partitions_of = defaultdict(list)
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = worker_for
        self.split_axes = split_axes(old, new)

    async def _receive(self, data: list[tuple[NDIndex, bytes]]) -> None:
        self.raise_if_closed()

        filtered = []
        for d in data:
            id, payload = d
            if id in self.received:
                continue
            filtered.append(payload)
            self.received.add(id)
            self.total_recvd += sizeof(d)
        del data
        if not filtered:
            return
        try:
            shards = await self.offload(self._repartition_shards, filtered)
            del filtered
            await self._write_to_disk(shards)
        except Exception as e:
            self._exception = e
            raise

    def _repartition_shards(self, data: list[bytes]) -> dict[NDIndex, bytes]:
        repartitioned: defaultdict[
            NDIndex, list[tuple[NDIndex, np.ndarray]]
        ] = defaultdict(list)
        for buffer in data:
            for id, shard in pickle.loads(buffer):
                repartitioned[id].append(shard)
        return {k: pickle.dumps(v) for k, v in repartitioned.items()}

    async def _add_partition(
        self, data: np.ndarray, partition_id: NDIndex, **kwargs: Any
    ) -> int:
        def _() -> dict[str, tuple[NDIndex, bytes]]:
            """Return a mapping of worker addresses to a tuple of input partition
            IDs and shard data.


            TODO: Overhaul!
            As shard data, we serialize the payload together with the sub-index of the
            slice within the new chunk. To assemble the new chunk from its shards, it
            needs the sub-index to know where each shard belongs within the chunk.
            Adding the sub-index into the serialized payload on the sender allows us to
            write the serialized payload directly to disk on the receiver.
            """
            out: dict[
                str, list[tuple[NDIndex, tuple[NDIndex, np.ndarray]]]
            ] = defaultdict(list)
            from itertools import product

            ndsplits = product(
                *(axis[i] for axis, i in zip(self.split_axes, partition_id))
            )

            for ndsplit in ndsplits:
                chunk_index, shard_index, ndslice = zip(*ndsplit)
                out[self.worker_for[chunk_index]].append(
                    (chunk_index, (shard_index, data[ndslice]))
                )
            return {k: (partition_id, pickle.dumps(v)) for k, v in out.items()}

        out = await self.offload(_)
        await self._write_to_comm(out)
        return self.run_id

    async def _get_output_partition(
        self, partition_id: NDIndex, key: str, **kwargs: Any
    ) -> np.ndarray:
        def _(partition_id: NDIndex) -> np.ndarray:
            data = self._read_from_disk(partition_id)
            return convert_chunk(data)

        return await self.offload(_, partition_id)

    def deserialize(self, buffer: bytes) -> Any:
        result = pickle.loads(buffer)
        return result

    def read(self, path: Path) -> tuple[Any, int]:
        shards: list[list[tuple[NDIndex, np.ndarray]]] = []
        with path.open(mode="rb") as f:
            size = f.seek(0, os.SEEK_END)
            f.seek(0)
            while f.tell() < size:
                shards.append(pickle.load(f))
        return shards, size

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
        )


def _get_worker_for_hash_sharding(
    output_partition: NDIndex, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using hash sharding"""
    i = hash(output_partition) % len(workers)
    return workers[i]
