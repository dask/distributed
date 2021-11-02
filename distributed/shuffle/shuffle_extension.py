from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, NewType

from distributed.worker import Worker

if TYPE_CHECKING:
    import pandas as pd

ShuffleId = NewType("ShuffleId", str)


@dataclass
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


class Shuffle:
    "State for a single active shuffle"

    def __init__(self, structure: ShuffleMetadata) -> None:
        ...

    def receive(self, data: pd.DataFrame) -> None:
        ...

    def add_partition(self, data: pd.DataFrame) -> None:
        ...

    def get_output_partition(self) -> pd.DataFrame:
        ...


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"

    def __init__(self, worker: Worker) -> None:
        # Attach to worker

        # TODO: use a stream handler or a normal handler?
        # Note that I don't think you can return responses from stream handlers,
        # which we will probably need to do for backpressure.
        add_handler(worker.stream_handlers, self.shuffle_receive, "shuffle-receive")
        add_handler(worker.handlers, self.shuffle_init, "shuffle-init")

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
        Register a new shuffle that is about to begin.
        Using a shuffle with an already-known ID is an error.
        """
        ...

    def shuffle_receive(self, shuffle_id: ShuffleId, data: pd.DataFrame) -> None:
        """
        Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        # NOTE: in the future, this method will likely be async
        ...

    def add_partition(self, shuffle_id: ShuffleId, data: pd.DataFrame) -> None:
        """
        Hand off an input partition to the ShuffleExtension.

        Called from tasks running in the worker's threadpool.
        This will block until the extension is ready to receive another input partition.

        Using an unknown ``shuffle_id`` is an error.
        """
        ...

    def get_output_partition(self, shuffle_id: ShuffleId) -> pd.DataFrame:
        """
        Retrieve a shuffled output partition from the ShuffleExtension.

        Called from tasks running in the worker's threadpool.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        ...


def add_handler(handlers: dict, handler: Callable, key: str) -> None:
    "Add a handler to a worker's handlers dict, checking if it already exists."
    existing_handler = handlers.setdefault(key, handler)
    if existing_handler is not handler:
        raise RuntimeError(
            f"Worker already has a {key!r} handler registered "
            f"from a different shuffle extension: {existing_handler}"
        )
