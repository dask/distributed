from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, NewType

from distributed.protocol import to_serialize
from distributed.utils import sync

if TYPE_CHECKING:
    import pandas as pd

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
        i = len(self.workers) * output_partition // self.npartitions
        return self.workers[i]

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

    def __init__(self, metadata: ShuffleMetadata, worker: Worker) -> None:
        self.metadata = metadata
        self.worker = worker
        self.output_partitions: defaultdict[int, list[pd.DataFrame]] = defaultdict(list)
        self.output_partitions_left = metadata.npartitions_for(worker.address)
        self.transferred = False

    def receive(self, output_partition: int, data: pd.DataFrame) -> None:
        assert not self.transferred, "`receive` called after barrier task"
        self.output_partitions[output_partition].append(data)

    async def add_partition(self, data: pd.DataFrame) -> None:
        assert not self.transferred, "`add_partition` called after barrier task"
        tasks = []
        # NOTE: `groupby` blocks the event loop, but it also holds the GIL,
        # so we don't bother offloading to a thread. See bpo-7946.
        for output_partition, data in data.groupby(self.metadata.column):
            # NOTE: `column` must refer to an integer column, which is the output partition number for the row.
            # This is always `_partitions`, added by `dask/dataframe/shuffle.py::shuffle`.
            addr = self.metadata.worker_for(int(output_partition))
            task = asyncio.create_task(
                self.worker.rpc(addr).shuffle_receive(
                    shuffle_id=self.metadata.id,
                    output_partition=output_partition,
                    data=to_serialize(data),
                )
            )
            tasks.append(task)

        # TODO Once RerunGroup logic exists (https://github.com/dask/distributed/issues/5403),
        # handle errors and cancellation here in a way that lets other workers cancel & clean up their shuffles.
        # Without it, letting errors kill the task is all we can do.
        await asyncio.gather(*tasks)

    def get_output_partition(self, i: int) -> pd.DataFrame:
        import pandas as pd

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
            parts = self.output_partitions.pop(i)
        except KeyError:
            return self.metadata.empty

        assert parts, f"Empty entry for output partition {i}"
        return pd.concat(parts, copy=False)

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
        self.shuffles[metadata.id] = Shuffle(metadata, self.worker)

    def shuffle_receive(
        self,
        comm: object,
        shuffle_id: ShuffleId,
        output_partition: int,
        data: pd.DataFrame,
    ) -> None:
        """
        Hander: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        self._get_shuffle(shuffle_id).receive(output_partition, data)

    def shuffle_inputs_done(self, comm: object, shuffle_id: ShuffleId) -> None:
        """
        Hander: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle = self._get_shuffle(shuffle_id)
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
        sync(self.worker.loop, self._add_partition, data, shuffle_id)

    async def _add_partition(self, data: pd.DataFrame, shuffle_id: ShuffleId) -> None:
        """
        Task: Hand off an input partition to the ShuffleExtension.

        This will block until the extension is ready to receive another input partition.

        Using an unknown ``shuffle_id`` is an error.
        """
        await self._get_shuffle(shuffle_id).add_partition(data)

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
            # key missing if another thread got to it first
            self.shuffles.pop(shuffle_id, None)
        return output

    def _get_shuffle(self, shuffle_id: ShuffleId) -> Shuffle:
        "Get a shuffle by ID; raise ValueError if it's not registered."
        try:
            return self.shuffles[shuffle_id]
        except KeyError:
            raise ValueError(
                f"Shuffle {shuffle_id!r} is not registered on worker {self.worker.address}"
            ) from None
