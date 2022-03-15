from __future__ import annotations

import asyncio
import functools
import math
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, NewType

import toolz
import zict

from distributed.protocol import to_serialize
from distributed.shuffle.multi_comm import MultiComm
from distributed.shuffle.multi_file import MultiFile
from distributed.utils import offload, sync

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from distributed.worker import Worker

ShuffleId = NewType("ShuffleId", str)


# NOTE: we use these dataclasses primarily for type-checking benefits.
# They take the place of positional arguments to `shuffle_init`,
# which the type-checker can't validate when it's called as an RPC.


@dataclass(frozen=True, eq=False)
class NewShuffleMetadata:
    "Metadata to create a shuffle"
    id: ShuffleId
    empty: pd.DataFrame
    column: str
    npartitions: int


@dataclass(frozen=True, eq=False)
class ShuffleMetadata(NewShuffleMetadata):
    """
    Metadata every worker needs to share about a shuffle.

    A `ShuffleMetadata` is created with a task and sent to all workers
    over the `ShuffleWorkerExtension.shuffle_init` RPC.
    """

    workers: list[str]

    def worker_for(self, output_partition: int) -> str:
        "Get the address of the worker which should hold this output partition number"
        assert output_partition >= 0, f"Negative output partition: {output_partition}"
        if output_partition >= self.npartitions:
            raise IndexError(
                f"Output partition {output_partition} does not exist in a shuffle producing {self.npartitions} partitions"
            )
        return worker_for(output_partition, self.workers, self.npartitions)

    def _partition_range(self, worker: str) -> tuple[int, int]:
        "Get the output partition numbers (inclusive) that a worker will hold"
        i = self.workers.index(worker)
        first = math.ceil(self.npartitions * i / len(self.workers))
        last = math.ceil(self.npartitions * (i + 1) / len(self.workers)) - 1
        return first, last

    def npartitions_for(self, worker: str) -> int:
        "Get the number of output partitions a worker will hold"
        first, last = self._partition_range(worker)
        return last - first + 1


class Shuffle:
    "State for a single active shuffle"

    def __init__(
        self, metadata: ShuffleMetadata, worker: Worker, file_cache=None
    ) -> None:
        self.metadata = metadata
        self.worker = worker

        import pyarrow as pa

        self.multi_file = MultiFile(
            dump=dump_arrow,
            load=load_arrow,
            directory=os.path.join(self.worker.local_directory, str(self.metadata.id)),
            memory_limit="200 MiB",  # TODO: lift this up to the global ShuffleExtension
            file_cache=file_cache,
            concurrent_files=1,
            join=pa.concat_tables,  # pd.concat
        )
        self.multi_comm = MultiComm(
            memory_limit="200 MiB",  # TODO
            rpc=worker.rpc,
            shuffle_id=self.metadata.id,
            sizeof=lambda L: sum(map(len, L)),
            join=functools.partial(sum, start=[]),
        )
        self.worker.loop.add_callback(self.multi_comm.communicate)
        self.worker.loop.add_callback(self.multi_file.communicate)

        self.output_partitions_left = metadata.npartitions_for(worker.address)
        self.transferred = False
        self.total_recvd = 0

    async def receive(self, data: list[pa.Buffer]) -> None:
        assert not self.transferred, "`receive` called after barrier task"
        from dask.utils import format_bytes

        print("recved", format_bytes(sum(map(len, data))))
        self.total_recvd += sum(map(len, data))
        # An ugly way of turning these batches back into an arrow table
        import io

        import pyarrow as pa

        bio = io.BytesIO()
        bio.write(pa.Schema.from_pandas(self.metadata.empty).serialize())
        for batch in data:
            bio.write(batch)
        bio.seek(0)
        sr = pa.RecordBatchStreamReader(bio)
        data = sr.read_all()

        groups = await offload(split_by_partition, data, self.metadata.column)

        assert len(data) == sum(map(len, groups.values()))
        await self.multi_file.put(groups)

    def add_partition(self, data: pd.DataFrame) -> None:
        out = split_by_worker(
            data, self.metadata.column, self.metadata.npartitions, self.metadata.workers
        )
        assert len(data) == sum(map(len, out.values()))
        out = {
            k: [b.serialize().to_pybytes() for b in t.to_batches()]
            for k, t in out.items()
        }
        self.multi_comm.put(out)

    def get_output_partition(self, i: int) -> pd.DataFrame:
        assert self.transferred, "`get_output_partition` called before barrier task"

        assert self.metadata.worker_for(i) == self.worker.address, (
            f"Output partition {i} belongs on {self.metadata.worker_for(i)}, "
            f"not {self.worker.address}. {self.metadata!r}"
        )
        # ^ NOTE: this check isn't necessary, just a nice validation to prevent incorrect
        # data in the case something has gone very wrong

        assert (
            self.output_partitions_left > 0
        ), f"No outputs remaining, but requested output partition {i} on {self.worker.address}."
        self.output_partitions_left -= 1

        try:
            df = self.multi_file.read(i)
            return df.to_pandas()
        except KeyError:
            return self.metadata.empty.head(0)

    def inputs_done(self) -> None:
        assert not self.transferred, "`inputs_done` called multiple times"
        self.transferred = True

    def done(self) -> bool:
        return self.transferred and self.output_partitions_left == 0


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"

    def __init__(self, worker: Worker) -> None:
        # Attach to worker
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_init"] = self.shuffle_init
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done
        worker.extensions["shuffle"] = self

        # Initialize
        self.worker: Worker = worker
        self.shuffles: dict[ShuffleId, Shuffle] = {}
        self.file_cache = zict.LRU(256, dict(), on_evict=lambda k, v: v.close())

    # Handlers
    ##########
    # NOTE: handlers are not threadsafe, but they're called from async comms, so that's okay

    def shuffle_init(self, comm: object, metadata: ShuffleMetadata) -> None:
        """
        Hander: Register a new shuffle that is about to begin.
        Using a shuffle with an already-known ID is an error.
        """
        if metadata.id in self.shuffles:
            raise ValueError(
                f"Shuffle {metadata.id!r} is already registered on worker {self.worker.address}"
            )
        self.shuffles[metadata.id] = Shuffle(
            metadata, self.worker, file_cache=self.file_cache
        )

    async def shuffle_receive(
        self,
        comm: object,
        shuffle_id: ShuffleId,
        data: pd.DataFrame,
    ) -> None:
        """
        Hander: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        await self._get_shuffle(shuffle_id).receive(data)

    async def shuffle_inputs_done(self, comm: object, shuffle_id: ShuffleId) -> None:
        """
        Hander: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle = self._get_shuffle(shuffle_id)
        await shuffle.multi_comm.flush()
        await asyncio.sleep(1)  # TODO
        await shuffle.multi_file.flush()
        shuffle.inputs_done()
        if shuffle.done():
            # If the shuffle has no output partitions, remove it now;
            # `get_output_partition` will never be called.
            # This happens when there are fewer output partitions than workers.
            del self.shuffles[shuffle_id]

    # Tasks
    #######

    def create_shuffle(self, new_metadata: NewShuffleMetadata) -> ShuffleMetadata:
        return sync(self.worker.loop, self._create_shuffle, new_metadata)  # type: ignore

    async def _create_shuffle(
        self, new_metadata: NewShuffleMetadata
    ) -> ShuffleMetadata:
        """
        Task: Create a new shuffle and broadcast it to all workers.
        """
        # TODO would be nice to not have to have the RPC in this method, and have shuffles started implicitly
        # by the first `receive`/`add_partition`. To do that, shuffle metadata would be passed into
        # every task, and from there into the extension (rather than stored within a `Shuffle`),
        # However:
        # 1. It makes scheduling much harder, since it's a widely-shared common dep
        #    (https://github.com/dask/distributed/pull/5325)
        # 2. Passing in metadata everywhere feels contrived when it would be so easy to store
        # 3. The metadata may not be _that_ small (1000s of columns + 1000s of workers);
        #    serializing and transferring it repeatedly adds overhead.
        if new_metadata.id in self.shuffles:
            raise ValueError(
                f"Shuffle {new_metadata.id!r} is already registered on worker {self.worker.address}"
            )

        identity = await self.worker.scheduler.identity()

        workers = list(identity["workers"])
        metadata = ShuffleMetadata(
            new_metadata.id,
            new_metadata.empty,
            new_metadata.column,
            new_metadata.npartitions,
            workers,
        )

        # Start the shuffle on all peers
        # Note that this will call `shuffle_init` on our own worker as well
        await asyncio.gather(
            *(
                self.worker.rpc(addr).shuffle_init(metadata=to_serialize(metadata))
                for addr in metadata.workers
            ),
        )
        # TODO handle errors from peers, and cancellation.
        # If any peers can't start the shuffle, tell successful peers to cancel it.

        return metadata  # NOTE: unused in tasks, just handy for tests

    def add_partition(self, data: pd.DataFrame, shuffle_id: ShuffleId) -> None:
        self._get_shuffle(shuffle_id).add_partition(data=data)

    def barrier(self, shuffle_id: ShuffleId) -> None:
        sync(self.worker.loop, self._barrier, shuffle_id)

    async def _barrier(self, shuffle_id: ShuffleId) -> None:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error. Calling this before all partitions have been
        added is undefined.
        """
        # NOTE: in this basic shuffle implementation, doing things during the barrier
        # is mostly unnecessary. We only need it to inform workers that don't receive
        # any output partitions that they can clean up.
        # (Otherwise, they'd have no way to know if they needed to keep the `Shuffle` around
        # for more input partitions, which might come at some point. Workers that _do_ receive
        # output partitions could infer this, since once `get_output_partition` gets called the
        # first time, they can assume there are no more inputs.)
        #
        # Technically right now, we could call the `shuffle_inputs_done` RPC only on workers
        # where `metadata.npartitions_for(worker) == 0`.
        # However, when we have buffering, this barrier step will become important for
        # all workers, since they'll use it to flush their buffers and send any leftover shards
        # to their peers.

        metadata = self._get_shuffle(shuffle_id).metadata

        # Set worker restrictions for unpack tasks

        # Could do this during `create_shuffle`, but we might as well overlap it with the time
        # workers will be flushing buffers to each other.
        name = "shuffle-unpack-" + metadata.id  # TODO single-source task name

        # FIXME TODO XXX what about when culling means not all of the output tasks actually exist??!
        # - these restrictions are invalid
        # - get_output_partition won't be called enough times, so cleanup won't happen
        # - also, we're transferring data we don't need to transfer
        restrictions = {
            f"('{name}', {i})": [metadata.worker_for(i)]
            for i in range(metadata.npartitions)
        }

        # Tell all peers that we've reached the barrier

        # Note that this will call `shuffle_inputs_done` on our own worker as well
        await asyncio.gather(
            *(
                self.worker.rpc(worker).shuffle_inputs_done(shuffle_id=shuffle_id)
                for worker in metadata.workers
            ),
            self.worker.scheduler.set_restrictions(worker=restrictions),
        )
        # TODO handle errors from workers and scheduler, and cancellation.

    def get_output_partition(
        self, shuffle_id: ShuffleId, output_partition: int
    ) -> pd.DataFrame:
        """
        Task: Retrieve a shuffled output partition from the ShuffleExtension.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        shuffle = self._get_shuffle(shuffle_id)
        output = shuffle.get_output_partition(output_partition)
        if shuffle.done():
            self.shuffles.pop(shuffle_id, None)
            # key missing if another thread got to it first
        return output

    def _get_shuffle(self, shuffle_id: ShuffleId) -> Shuffle:
        "Get a shuffle by ID; raise ValueError if it's not registered."
        try:
            return self.shuffles[shuffle_id]
        except KeyError:
            raise ValueError(
                f"Shuffle {shuffle_id!r} is not registered on worker {self.worker.address}"
            ) from None


def split_by_worker(
    df: pd.DataFrame, column: str, npartitions: int, workers: list[str]
) -> dict:
    """
    Split data into many arrow batches, partitioned by destination worker
    """
    import numpy as np
    import pyarrow as pa

    grouper = (len(workers) * df[column] // npartitions).astype(df[column].dtype).values

    t = pa.Table.from_pandas(df)
    del df
    t = t.add_column(len(t.columns), "_worker", [grouper])
    t = t.sort_by("_worker")

    worker = np.asarray(t.select(["_worker"]))[0]
    t = t.drop(["_worker"])
    splits = np.where(worker[1:] != worker[:-1])[0] + 1
    splits = np.concatenate([[0], splits])

    shards = [
        t.slice(offset=a, length=b - a) for a, b in toolz.sliding_window(2, splits)
    ]
    shards.append(t.slice(offset=splits[-1], length=None))

    w = np.unique(grouper)
    w.sort()

    return {workers[w]: shard for w, shard in zip(w, shards)}


def split_by_partition(
    t: pa.Table,
    column: str,
) -> dict:
    """
    Split data into many arrow batches, partitioned by destination worker
    """
    import numpy as np

    partitions = np.unique(np.asarray(t.select([column]))[0])
    partitions.sort()
    t = t.sort_by(column)

    partition = np.asarray(t.select([column]))[0]
    splits = np.where(partition[1:] != partition[:-1])[0] + 1
    splits = np.concatenate([[0], splits])

    shards = [
        t.slice(offset=a, length=b - a) for a, b in toolz.sliding_window(2, splits)
    ]
    shards.append(t.slice(offset=splits[-1], length=None))
    assert len(t) == sum(map(len, shards))
    assert len(partitions) == len(shards)
    return dict(zip(partitions, shards))


def dump_arrow(t: pa.Table, file):
    if file.tell() == 0:
        file.write(t.schema.serialize())
    for batch in t.to_batches():
        file.write(batch.serialize())


def load_arrow(file):
    import pyarrow as pa

    try:
        sr = pa.RecordBatchStreamReader(file)
        return sr.read_all()
    except Exception:
        raise EOFError


def worker_for(output_partition: int, workers: list[str], npartitions: int) -> str:
    "Get the address of the worker which should hold this output partition number"
    i = len(workers) * output_partition // npartitions
    return workers[i]
