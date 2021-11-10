from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, NewType

import pandas as pd

from distributed.protocol import to_serialize
from distributed.utils import sync

if TYPE_CHECKING:
    from distributed.worker import Worker

ShuffleId = NewType("ShuffleId", str)


# NOTE: we use these dataclasses purely for type-checking benefits.
# They take the place of positional arguments to `shuffle_init`,
# which the type-checker can't validate when it's called as an RPC.


@dataclass(frozen=True)
class NewShuffleMetadata:
    "Metadata to create a shuffle"
    id: ShuffleId
    empty: pd.DataFrame
    column: str
    npartitions: int


@dataclass(frozen=True)
class ShuffleMetadata(NewShuffleMetadata):
    """
    Metadata every worker needs to share about a shuffle.

    A `ShuffleMetadata` is created with a task and sent to all workers
    over the `ShuffleWorkerExtension.shuffle_init` RPC.
    """

    workers: list[str]

    def worker_for(self, output_partition: int) -> str:
        "Get the address of the worker which should hold this output partition number"
        if output_partition >= self.npartitions:
            raise IndexError(
                f"Output partition {output_partition} does not exist in a shuffle producing {self.npartitions} partitions"
            )
        i = output_partition * len(self.workers) // self.npartitions
        return self.workers[i]


class Shuffle:
    "State for a single active shuffle"

    def __init__(self, metadata: ShuffleMetadata, worker: Worker) -> None:
        self.metadata = metadata
        self.worker = worker
        self.output_partitions: defaultdict[int, list[pd.DataFrame]] = defaultdict(list)

    def receive(self, output_partition: int, data: pd.DataFrame) -> None:
        self.output_partitions[output_partition].append(data)

    async def add_partition(self, data: pd.DataFrame) -> None:
        tasks = []
        # TODO grouping is blocking, should it be offloaded to a thread?
        # It mostly doesn't release the GIL though, so may not make much difference.
        for output_partition, data in data.groupby(self.metadata.column):
            addr = self.metadata.worker_for(int(output_partition))
            task = asyncio.create_task(
                self.worker.rpc(addr).shuffle_receive(
                    shuffle_id=self.metadata.id,
                    output_partition=output_partition,
                    data=to_serialize(data),
                )
            )
            tasks.append(task)

        # TODO handle errors and cancellation here
        await asyncio.gather(*tasks)

    def get_output_partition(self, i: int) -> pd.DataFrame:
        parts = self.output_partitions.pop(i)
        if parts:
            return pd.concat(parts, copy=False)
        return self.metadata.empty


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"

    def __init__(self, worker: Worker) -> None:
        # Attach to worker

        add_handler(worker.handlers, self.shuffle_receive)
        add_handler(worker.handlers, self.shuffle_init)

        existing_extension = worker.extensions.setdefault("shuffle", self)
        if existing_extension is not self:
            raise RuntimeError(
                f"Worker {worker} already has a 'shuffle' extension registered: {existing_extension}"
            )

        # Initialize
        self.worker: Worker = worker
        self.shuffles: dict[ShuffleId, Shuffle] = {}

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
        # NOTE: in the future, this method will likely be async
        self._get_shuffle(shuffle_id).receive(output_partition, data)

    def create_shuffle(self, new_metadata: NewShuffleMetadata) -> None:
        sync(self.worker.loop, self._create_shuffle, new_metadata)

    async def _create_shuffle(self, new_metadata: NewShuffleMetadata) -> None:
        """
        Task: Create a new shuffle and broadcast it to all workers.
        """
        # TODO would be nice to not have to have this method, and have shuffles started implicitly
        # by the first `receive`/`add_partition`, and have shuffle metadata be passed into
        # tasks and from there into the extension (rather than stored within a `Shuffle`),
        # since this would mean the setup task returns meaningful data, and isn't just
        # side effects. However:
        # 1. passing in metadata everywhere feels contrived when it would be so easy to store
        # 2. it makes scheduling much harder, since it's a widely-shared common dep
        #    (https://github.com/dask/distributed/pull/5325)
        if new_metadata.id in self.shuffles:
            raise ValueError(
                f"Shuffle {id!r} is already registered on worker {self.worker.address}"
            )

        client = self.worker.client
        assert (
            client.loop is self.worker.loop
        ), f"Worker client is not using the worker's event loop: {client.loop} vs {self.worker.loop}"
        # NOTE: `Client.scheduler_info()` doesn't actually do anything when the client is async,
        # manually call into the method for now.
        await client._update_scheduler_info()
        identity = client._scheduler_identity

        # TODO worker restrictions, etc!
        workers = list(identity["workers"])
        metadata = ShuffleMetadata(
            new_metadata.id,
            new_metadata.empty,
            new_metadata.column,
            new_metadata.npartitions,
            workers,
        )

        # TODO handle errors from peers, and cancellation.
        # If any peers can't start the shuffle, tell other peers to cancel it.
        await asyncio.gather(
            *(
                self.worker.rpc(addr).shuffle_init(metadata=to_serialize(metadata))
                for addr in metadata.workers
            )
        )
        # Note that this will call `shuffle_init` on our own worker as well

    def add_partition(self, data: pd.DataFrame, shuffle_id: ShuffleId) -> None:
        sync(self.worker.loop, self._add_partition, data, shuffle_id)

    async def _add_partition(self, data: pd.DataFrame, shuffle_id: ShuffleId) -> None:
        """
        Task: Hand off an input partition to the ShuffleExtension.

        This will block until the extension is ready to receive another input partition.

        Using an unknown ``shuffle_id`` is an error.
        """
        await self._get_shuffle(shuffle_id).add_partition(data)

    def get_output_partition(
        self, shuffle_id: ShuffleId, output_partition: int
    ) -> pd.DataFrame:
        """
        Task: Retrieve a shuffled output partition from the ShuffleExtension.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        return self._get_shuffle(shuffle_id).get_output_partition(output_partition)

    def _get_shuffle(self, shuffle_id: ShuffleId) -> Shuffle:
        "Get a shuffle by ID; raise ValueError if it's not registered."
        try:
            return self.shuffles[shuffle_id]
        except KeyError:
            raise ValueError(
                f"Shuffle {shuffle_id!r} is not registered on worker {self.worker.address}"
            ) from None


def add_handler(handlers: dict, handler: Callable) -> None:
    "Add a handler to a worker's handlers dict, checking if it already exists."
    existing_handler = handlers.setdefault(handler.__name__, handler)
    if existing_handler is not handler:
        raise RuntimeError(
            f"Worker already has a {handler.__name__!r} handler registered "
            f"from a different shuffle extension: {existing_handler}"
        )
