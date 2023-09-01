from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, overload

from dask.context import thread_state
from dask.utils import parse_bytes

from distributed.diagnostics.plugin import WorkerPlugin
from distributed.protocol.serialize import ToPickle
from distributed.shuffle._core import (
    NDIndex,
    ShuffleId,
    ShuffleRun,
    ShuffleRunSpec,
    ShuffleSpec,
)
from distributed.shuffle._exceptions import ShuffleClosedError
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors, sync

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    import pandas as pd

    from distributed.worker import Worker


logger = logging.getLogger(__name__)


class ShuffleWorkerPlugin(WorkerPlugin):
    """Interface between a Worker and a Shuffle.

    This extension is responsible for

    - Lifecycle of Shuffle instances
    - ensuring connectivity between remote shuffle instances
    - ensuring connectivity and integration with the scheduler
    - routing concurrent calls to the appropriate `Shuffle` based on its `ShuffleID`
    - collecting instrumentation of ongoing shuffles and route to scheduler/worker
    """

    worker: Worker
    shuffles: dict[ShuffleId, ShuffleRun]
    _runs: set[ShuffleRun]
    _runs_cleanup_condition: asyncio.Condition
    memory_limiter_comms: ResourceLimiter
    memory_limiter_disk: ResourceLimiter
    closed: bool

    def setup(self, worker: Worker) -> None:
        # Attach to worker
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done
        worker.stream_handlers["shuffle-fail"] = self.shuffle_fail
        worker.extensions["shuffle"] = self

        # Initialize
        self.worker = worker
        self.shuffles = {}
        self._runs = set()
        self._runs_cleanup_condition = asyncio.Condition()
        self.memory_limiter_comms = ResourceLimiter(parse_bytes("100 MiB"))
        self.memory_limiter_disk = ResourceLimiter(parse_bytes("1 GiB"))
        self.closed = False
        self._executor = ThreadPoolExecutor(self.worker.state.nthreads)

    def __str__(self) -> str:
        return f"ShuffleWorkerPlugin on {self.worker.address}"

    def __repr__(self) -> str:
        return f"<ShuffleWorkerPlugin, worker={self.worker.address_safe!r}, closed={self.closed}>"

    # Handlers
    ##########
    # NOTE: handlers are not threadsafe, but they're called from async comms, so that's okay

    def heartbeat(self) -> dict:
        return {id: shuffle.heartbeat() for id, shuffle in self.shuffles.items()}

    async def shuffle_receive(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
        data: list[tuple[int, bytes]],
    ) -> None:
        """
        Handler: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle = await self._get_shuffle_run(shuffle_id, run_id)
        await shuffle.receive(data)

    async def shuffle_inputs_done(self, shuffle_id: ShuffleId, run_id: int) -> None:
        """
        Handler: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        with log_errors():
            shuffle = await self._get_shuffle_run(shuffle_id, run_id)
            await shuffle.inputs_done()

    async def _close_shuffle_run(self, shuffle: ShuffleRun) -> None:
        await shuffle.close()
        async with self._runs_cleanup_condition:
            self._runs.remove(shuffle)
            self._runs_cleanup_condition.notify_all()

    def shuffle_fail(self, shuffle_id: ShuffleId, run_id: int, message: str) -> None:
        """Fails the shuffle run with the message as exception and triggers cleanup.

        .. warning::
            To guarantee the correct order of operations, shuffle_fail must be
            synchronous. See
            https://github.com/dask/distributed/pull/7486#discussion_r1088857185
            for more details.
        """
        shuffle = self.shuffles.get(shuffle_id, None)
        if shuffle is None or shuffle.run_id != run_id:
            return
        self.shuffles.pop(shuffle_id)
        exception = RuntimeError(message)
        shuffle.fail(exception)

        self.worker._ongoing_background_tasks.call_soon(
            self._close_shuffle_run, shuffle
        )

    def add_partition(
        self,
        data: Any,
        partition_id: int | NDIndex,
        spec: ShuffleSpec,
        **kwargs: Any,
    ) -> int:
        shuffle = self.get_or_create_shuffle(spec)
        return sync(
            self.worker.loop,
            shuffle.add_partition,
            data=data,
            partition_id=partition_id,
            **kwargs,
        )

    async def _barrier(self, shuffle_id: ShuffleId, run_ids: list[int]) -> int:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error. Calling this before all partitions have been
        added is undefined.
        """
        run_id = run_ids[0]
        # Assert that all input data has been shuffled using the same run_id
        if any(run_id != id for id in run_ids):
            raise RuntimeError(f"Expected all run IDs to match: {run_ids=}")
        # Tell all peers that we've reached the barrier
        # Note that this will call `shuffle_inputs_done` on our own worker as well
        shuffle = await self._get_shuffle_run(shuffle_id, run_id)
        await shuffle.barrier()
        return run_id

    async def _get_shuffle_run(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
    ) -> ShuffleRun:
        """Get or create the shuffle matching the ID and run ID.

        Parameters
        ----------
        shuffle_id
            Unique identifier of the shuffle
        run_id
            Unique identifier of the shuffle run

        Raises
        ------
        KeyError
            If the shuffle does not exist
        RuntimeError
            If the run_id is stale
        """
        shuffle = self.shuffles.get(shuffle_id, None)
        if shuffle is None or shuffle.run_id < run_id:
            shuffle = await self._refresh_shuffle(
                shuffle_id=shuffle_id,
            )

        if shuffle.run_id > run_id:
            raise RuntimeError(f"{run_id=} stale, got {shuffle}")
        elif shuffle.run_id < run_id:
            raise RuntimeError(f"{run_id=} invalid, got {shuffle}")

        if shuffle._exception:
            raise shuffle._exception
        return shuffle

    async def _get_or_create_shuffle(
        self,
        spec: ShuffleSpec,
        key: str,
    ) -> ShuffleRun:
        """Get or create a shuffle matching the ID and data spec.

        Parameters
        ----------
        shuffle_id
            Unique identifier of the shuffle
        type:
            Type of the shuffle operation
        key:
            Task key triggering the function
        """
        shuffle = self.shuffles.get(spec.id, None)
        if shuffle is None:
            shuffle = await self._refresh_shuffle(
                shuffle_id=spec.id,
                spec=spec,
                key=key,
            )

        if self.closed:
            raise ShuffleClosedError(f"{self} has already been closed")
        if shuffle._exception:
            raise shuffle._exception
        return shuffle

    @overload
    async def _refresh_shuffle(
        self,
        shuffle_id: ShuffleId,
    ) -> ShuffleRun:
        ...

    @overload
    async def _refresh_shuffle(
        self,
        shuffle_id: ShuffleId,
        spec: ShuffleSpec,
        key: str,
    ) -> ShuffleRun:
        ...

    async def _refresh_shuffle(
        self,
        shuffle_id: ShuffleId,
        spec: ShuffleSpec | None = None,
        key: str | None = None,
    ) -> ShuffleRun:
        # FIXME: This should never be ToPickle[ShuffleRunSpec]
        result: ShuffleRunSpec | ToPickle[ShuffleRunSpec]
        if spec is None:
            result = await self.worker.scheduler.shuffle_get(
                id=shuffle_id,
                worker=self.worker.address,
            )
        else:
            result = await self.worker.scheduler.shuffle_get_or_create(
                spec=ToPickle(spec),
                key=key,
                worker=self.worker.address,
            )
        if isinstance(result, ToPickle):
            result = result.data
        if self.closed:
            raise ShuffleClosedError(f"{self} has already been closed")
        if shuffle_id in self.shuffles:
            existing = self.shuffles[shuffle_id]
            if existing.run_id >= result.run_id:
                return existing
            else:
                self.shuffles.pop(shuffle_id)
                existing.fail(
                    RuntimeError("{existing!r} stale, expected run_id=={run_id}")
                )

                async def _(
                    extension: ShuffleWorkerPlugin, shuffle: ShuffleRun
                ) -> None:
                    await shuffle.close()
                    async with extension._runs_cleanup_condition:
                        extension._runs.remove(shuffle)
                        extension._runs_cleanup_condition.notify_all()

                self.worker._ongoing_background_tasks.call_soon(_, self, existing)
        shuffle: ShuffleRun = result.spec.create_run_on_worker(
            result.run_id, result.worker_for, self
        )
        self.shuffles[shuffle_id] = shuffle
        self._runs.add(shuffle)
        return shuffle

    async def teardown(self, worker: Worker) -> None:
        assert not self.closed

        self.closed = True

        while self.shuffles:
            _, shuffle = self.shuffles.popitem()
            self.worker._ongoing_background_tasks.call_soon(
                self._close_shuffle_run, shuffle
            )

        async with self._runs_cleanup_condition:
            await self._runs_cleanup_condition.wait_for(lambda: not self._runs)

        try:
            self._executor.shutdown(cancel_futures=True)
        except Exception:  # pragma: no cover
            self._executor.shutdown()

    #############################
    # Methods for worker thread #
    #############################

    def barrier(self, shuffle_id: ShuffleId, run_ids: list[int]) -> int:
        result = sync(self.worker.loop, self._barrier, shuffle_id, run_ids)
        return result

    def get_shuffle_run(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
    ) -> ShuffleRun:
        return sync(
            self.worker.loop,
            self._get_shuffle_run,
            shuffle_id,
            run_id,
        )

    def get_or_create_shuffle(
        self,
        spec: ShuffleSpec,
    ) -> ShuffleRun:
        key = thread_state.key
        return sync(
            self.worker.loop,
            self._get_or_create_shuffle,
            spec,
            key,
        )

    def get_output_partition(
        self,
        shuffle_id: ShuffleId,
        run_id: int,
        partition_id: int | NDIndex,
        meta: pd.DataFrame | None = None,
    ) -> Any:
        """
        Task: Retrieve a shuffled output partition from the ShuffleWorkerPlugin.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        shuffle = self.get_shuffle_run(shuffle_id, run_id)
        key = thread_state.key
        return sync(
            self.worker.loop,
            shuffle.get_output_partition,
            partition_id=partition_id,
            key=key,
            meta=meta,
        )
