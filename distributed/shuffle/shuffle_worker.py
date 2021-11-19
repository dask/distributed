from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Coroutine, TypeVar

import pandas as pd

from distributed.protocol import to_serialize

from .common import ShuffleId, npartitions_for, worker_for

if TYPE_CHECKING:
    from distributed import Worker

T = TypeVar("T")


@dataclass
class ShuffleState:
    workers: list[str]
    npartitions: int
    out_parts_left: int
    barrier_reached: bool = False


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"
    worker: Worker
    shuffles: dict[ShuffleId, ShuffleState]
    waiting_for_metadata: dict[ShuffleId, asyncio.Event]
    output_data: defaultdict[ShuffleId, defaultdict[int, list[pd.DataFrame]]]

    def __init__(self, worker: Worker) -> None:
        # Attach to worker
        worker.extensions["shuffle"] = self
        worker.stream_handlers["shuffle_init"] = self.shuffle_init
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done

        # Initialize
        self.worker: Worker = worker
        self.shuffles = {}
        self.waiting_for_metadata = {}
        self.output_data = defaultdict(lambda: defaultdict(list))

    # Handlers
    ##########

    def shuffle_init(self, id: ShuffleId, workers: list[str], n_out_tasks: int) -> None:
        """
        Handler: initialize a shuffle. Called by scheduler on all workers.

        Must be called exactly once per ID.
        """
        if id in self.shuffles:
            raise ValueError(
                f"Shuffle {id!r} is already registered on worker {self.worker.address}"
            )
        self.shuffles[id] = ShuffleState(
            workers,
            n_out_tasks,
            npartitions_for(self.worker.address, n_out_tasks, workers),
        )
        try:
            # Invariant: if `waiting_for_metadata` event is set, key is already in `shuffles`
            self.waiting_for_metadata[id].set()
        except KeyError:
            pass

    def shuffle_receive(
        self,
        comm: object,
        id: ShuffleId,
        output_partition: int,
        data: pd.DataFrame,
    ) -> None:
        """
        Handler: receive data from a peer.

        The shuffle ID can be unknown.
        Calling after the barrier task is an error.
        """
        try:
            state = self.shuffles[id]
        except KeyError:
            # NOTE: `receive` could be called before `init`, if some other worker
            # processed their `init` faster than us and then sent us data.
            # That's why we keep `output_data` separate from `shuffles`.
            pass
        else:
            assert not state.barrier_reached, f"`receive` called after barrier for {id}"
            receiver = worker_for(output_partition, state.npartitions, state.workers)
            assert receiver == self.worker.address, (
                f"{self.worker.address} received output partition {output_partition} "
                f"for shuffle {id}, which was expected to go to {receiver}."
            )

        self.output_data[id][output_partition].append(data)

    async def shuffle_inputs_done(self, comm: object, id: ShuffleId) -> None:
        """
        Handler: note that the barrier task has been reached. Called by a peer.

        The shuffle will be removed if this worker holds no output partitions for it.

        Must be called exactly once per ID. Blocks until `shuffle_init` has been called.
        """
        state = await self.get_shuffle(id)
        assert not state.barrier_reached, f"`inputs_done` called again for {id}"
        state.barrier_reached = True

        if not state.out_parts_left:
            # No output partitions, remove shuffle now: `get_output_partition` will never be called.
            # This happens when there are fewer output partitions than workers.
            self.remove(id)

    # Tasks
    #######

    async def add_partition(
        self, data: pd.DataFrame, id: ShuffleId, npartitions: int, column: str
    ) -> None:
        """
        Task: Hand off an input partition to the extension.

        This will block until the extension is ready to receive another input partition.
        Also blocks until `shuffle_init` has been called.

        Using an unknown ``shuffle_id`` is an error.
        Calling after the barrier task is an error.
        """
        # Block until scheduler has called init
        state = await self.get_shuffle(id)
        assert not state.barrier_reached, f"`add_partition` for {id} after barrier"

        if npartitions != state.npartitions:
            raise NotImplementedError(
                f"Expected shuffle {id} to produce {npartitions} output tasks, "
                f"but it only has {state.npartitions}. Did you sub-select from the "
                "shuffled DataFrame, like `df.set_index(...).loc['foo':'bar']`?\n"
                "This is not yet supported for peer-to-peer shuffles. Either remove "
                "the sub-selection or use `shuffle='tasks'` for now."
            )
        # Group and send data
        await self.send_partition(data, column, id, npartitions, state.workers)

    async def barrier(self, id: ShuffleId) -> None:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error.
        Must be called exactly once per ID.
        Blocks until `shuffle_init` has been called (on all workers).
        Calling this before all partitions have been added will cause `add_partition` to fail.
        """
        state = await self.get_shuffle(id)
        assert not state.barrier_reached, f"`barrier` for {id} called multiple times"

        # Call `shuffle_inputs_done` on peers.
        # Note that this will call `shuffle_inputs_done` on our own worker as well.
        # Concurrently, the scheduler is setting worker restrictions on its own.
        await asyncio.gather(
            *(
                self.worker.rpc(worker).shuffle_inputs_done(id=id)
                for worker in state.workers
            ),
        )

    async def get_output_partition(
        self, id: ShuffleId, i: int, empty: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Task: Retrieve a shuffled output partition from the extension.

        After calling on the final output partition remaining on this worker, the shuffle will be cleaned up.

        Using an unknown ``shuffle_id`` is an error.
        Requesting a partition which doesn't belong on this worker, or has already been retrieved, is an error.
        """
        state = await self.get_shuffle(id)  # should never have to wait
        assert state.barrier_reached, f"`get_output_partition` for {id} before barrier"
        assert (
            state.out_parts_left > 0
        ), f"No outputs remaining, but requested output partition {i} on {self.worker.address} for {id}."
        # ^ Note: impossible with our cleanup-on-empty

        worker = worker_for(i, state.npartitions, state.workers)
        assert worker == self.worker.address, (
            f"{self.worker.address} received output partition {i} "
            f"for shuffle {id}, which was expected to go to {worker}."
        )

        try:
            parts = self.output_data[id].pop(i)
        except KeyError:
            result = empty
        else:
            result = pd.concat(parts, copy=False)

        state.out_parts_left -= 1
        if not state.out_parts_left:
            # Shuffle is done. Yay!
            self.remove(id)

        return result

    # Helpers
    #########

    def remove(self, id: ShuffleId) -> None:
        "Remove state for this shuffle. The shuffle must be complete and in a valid state."
        state = self.shuffles.pop(id)
        assert state.barrier_reached, f"Removed {id} before barrier"
        assert (
            not state.out_parts_left
        ), f"Removed {id} with {state.out_parts_left} outputs left"

        event = self.waiting_for_metadata.pop(id, None)
        if event:
            assert event.is_set(), f"Removed {id} while still waiting for metadata"

        data = self.output_data.pop(id, None)
        assert (
            not data
        ), f"Removed {id}, which still has data for output partitions {list(data)}"

    async def get_shuffle(self, id: ShuffleId) -> ShuffleState:
        "Get the `ShuffleState`, blocking until it's been received from the scheduler."
        try:
            return self.shuffles[id]
        except KeyError:
            event = self.waiting_for_metadata.setdefault(id, asyncio.Event())
            try:
                await asyncio.wait_for(event.wait(), timeout=5)  # TODO config
            except TimeoutError:
                raise TimeoutError(
                    f"Timed out waiting for scheduler to start shuffle {id}"
                ) from None
            # Invariant: once `waiting_for_metadata` event is set, key is already in `shuffles`.
            # And once key is in `shuffles`, no `get_shuffle` will create a new event.
            # So we can safely remove the event now.
            self.waiting_for_metadata.pop(id, None)
            return self.shuffles[id]

    async def send_partition(
        self,
        data: pd.DataFrame,
        column: str,
        id: ShuffleId,
        npartitions: int,
        workers: list[str],
    ) -> None:
        "Split up an input partition and send its parts to peers."
        tasks = []
        # NOTE: `groupby` blocks the event loop, but it also holds the GIL,
        # so we don't bother offloading to a thread. See bpo-7946.
        for output_partition, data in data.groupby(column):
            # NOTE: `column` must refer to an integer column, which is the output partition number for the row.
            # This is always `_partitions`, added by `dask/dataframe/shuffle.py::shuffle`.
            addr = worker_for(int(output_partition), npartitions, workers)
            task = asyncio.create_task(
                self.worker.rpc(addr).shuffle_receive(
                    id=id,
                    output_partition=output_partition,
                    data=to_serialize(data),
                )
            )
            tasks.append(task)

        # TODO Once RerunGroup logic exists (https://github.com/dask/distributed/issues/5403),
        # handle errors and cancellation here in a way that lets other workers cancel & clean up their shuffles.
        # Without it, letting errors kill the task is all we can do.
        await asyncio.gather(*tasks)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        "The asyncio event loop for the worker"
        return self.worker.loop.asyncio_loop  # type: ignore

    def sync(self, coro: Coroutine[object, object, T]) -> T:
        "Run an async function on the worker's event loop, synchronously from another thread."
        # Is it a bad idea not to use `distributed.utils.sync`?
        # It's much nicer to use asyncio, because among other things it gives us typechecking.
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result()
