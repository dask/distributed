from __future__ import annotations

import asyncio
import dataclasses
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, NewType

import pandas as pd

from distributed.utils import sync

if TYPE_CHECKING:
    from distributed.worker import Worker

ShuffleId = NewType("ShuffleId", str)


@dataclasses.dataclass(frozen=True)
class ShuffleMetadata:
    """
    Metadata every worker needs to share about a shuffle.

    A `ShuffleMetadata` is created with a task and sent to all workers
    over the `ShuffleWorkerExtension.shuffle_init` RPC.
    """

    # NOTE: instead of passing these fields as arguments to `shuffle_init`,
    # we use this dataclass purely for type-checking benefits. (The type-checker
    # cannot not understand RPCs over comms.)
    id: ShuffleId
    workers: list[str]
    empty: pd.DataFrame
    column: str
    npartitions: int

    def worker_for(self, output_partition: int) -> str:
        # i = math.floor((output_partition / self.npartitions) * len(self.workers))
        i = output_partition * len(self.workers) // self.npartitions
        return self.workers[i]


class Shuffle:
    "State for a single active shuffle"

    def __init__(self, metadata: ShuffleMetadata, worker: Worker) -> None:
        self.metadata = metadata
        self.worker = worker
        self.output_partitions: defaultdict[int, list[pd.DataFrame]] = defaultdict(list)

    def receive(self, output_partition: int, data: pd.DataFrame) -> None:
        assert data, f"Shuffle {self.metadata.id!r} received empty DataFrame"
        self.output_partitions[output_partition].append(data)

    def add_partition(self, data: pd.DataFrame) -> None:
        for output_partition, data in data.groupby(self.metadata.column):
            addr = self.metadata.worker_for(int(output_partition))
            # TODO this is exceptionally serial; move the sync-async boundary to the extension
            sync(
                self.worker.loop,
                self.worker.rpc(addr).shuffle_receive,
                shuffle_id=self.metadata.id,
                output_partition=output_partition,
                data=data,
            )

    def get_output_partition(self, i: int) -> pd.DataFrame:
        parts = self.output_partitions.pop(i)
        if parts:
            return pd.concat(parts, copy=False)
        return self.metadata.empty


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"

    def __init__(self, worker: Worker) -> None:
        # Attach to worker

        # TODO: use a stream handler or a normal handler?
        # Note that I don't think you can return responses from stream handlers,
        # which we will probably need to do for backpressure.
        add_handler(worker.stream_handlers, self.shuffle_receive)
        add_handler(worker.handlers, self.shuffle_init)

        existing_extension = worker.extensions.setdefault("shuffle", self)
        if existing_extension is not self:
            raise RuntimeError(
                f"Worker {worker} already has a 'shuffle' extension registered: {existing_extension}"
            )

        # Initialize
        self.worker: Worker = worker
        self.shuffles: dict[ShuffleId, Shuffle] = {}

    def shuffle_init(self, metadata: ShuffleMetadata) -> None:
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
        self, shuffle_id: ShuffleId, output_partition: int, data: pd.DataFrame
    ) -> None:
        """
        Hander: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        # NOTE: in the future, this method will likely be async
        self._get_shuffle(shuffle_id).receive(output_partition, data)

    def create_shuffle(self, metadata: ShuffleMetadata) -> None:
        """
        Task: Create a new shuffle and broadcast it to all workers.

        Note: ``metadata`` is mutated. The ``workers`` field on ``metadata`` will be ignored.
        """
        # TODO would be nice to not have to have this, and have shuffles started implicitly
        # by the first `receive`/`add_partition`, and have shuffle metadata be passed into
        # tasks and from there into the extension (rather than stored within a `Shuffle`),
        # since this would mean the setup task returns meaningful data, and isn't just
        # side effects. However:
        # 1. passing in metadata everywhere feels contrived when it would be so easy to store
        # 2. it makes scheduling much harder, since it's a widely-shared common dep
        #    (https://github.com/dask/distributed/pull/5325)
        if metadata.id in self.shuffles:
            raise ValueError(
                f"Shuffle {id!r} is already registered on worker {self.worker.address}"
            )

        # TODO the client could be sync or async. Fix things so this is more graceful?
        # OR, just come up with a different way to get peer addresses from the scheduler.
        # 1. passing `asynchronous=False` to an async client won't make it sync
        # 2. the behavior of `client.scheduler_info()` is different depending on `client.asynchronous`
        client = self.worker.client
        sync(client.loop, client._update_scheduler_info)
        identity = client._scheduler_identity

        # TODO worker restrictions, etc!
        workers = list(identity["workers"])
        metadata = dataclasses.replace(metadata, workers=workers)

        sync(
            self.worker.loop,
            asyncio.gather,
            *[
                self.worker.rpc(addr).shuffle_init(metadata=metadata)
                for addr in metadata.workers
            ],
        )
        # Note that this will call `shuffle_init` on our own worker as well

    def add_partition(self, data: pd.DataFrame, shuffle_id: ShuffleId) -> None:
        """
        Task: Hand off an input partition to the ShuffleExtension.

        This will block until the extension is ready to receive another input partition.

        Using an unknown ``shuffle_id`` is an error.
        """
        self._get_shuffle(shuffle_id).add_partition(data)

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
