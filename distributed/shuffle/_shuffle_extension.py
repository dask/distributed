from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import time
from collections import defaultdict
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, BinaryIO, NewType, TypeVar, overload

import toolz

from dask.utils import parse_bytes

from distributed.core import PooledRPCCall
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.protocol import to_serialize
from distributed.shuffle._arrow import (
    deserialize_schema,
    dump_batch,
    list_of_buffers_to_table,
    load_arrow,
)
from distributed.shuffle._comms import CommShardsBuffer
from distributed.shuffle._disk import DiskShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors, sync

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from distributed.scheduler import Recs, Scheduler, TaskStateState, WorkerState
    from distributed.worker import Worker

ShuffleId = NewType("ShuffleId", str)
T = TypeVar("T")

logger = logging.getLogger(__name__)


class ShuffleClosedError(RuntimeError):
    pass


class Shuffle:
    """State for a single active shuffle

    This object is responsible for splitting, sending, receiving and combining
    data shards.

    It is entirely agnostic to the distributed system and can perform a shuffle
    with other `Shuffle` instances using `rpc` and `broadcast`.

    The user of this needs to guarantee that only `Shuffle`s of the same unique
    `ShuffleID` interact.

    Parameters
    ----------
    worker_for:
        A mapping partition_id -> worker_address.
    output_workers:
        A set of all participating worker (addresses).
    column:
        The data column we split the input partition by.
    schema:
        The schema of the payload data.
    id:
        A unique `ShuffleID` this belongs to.
    local_address:
        The local address this Shuffle can be contacted by using `rpc`.
    directory:
        The scratch directory to buffer data in.
    nthreads:
        How many background threads to use for compute.
    loop:
        The event loop.
    rpc:
        A callable returning a PooledRPCCall to contact other Shuffle instances.
        Typically a ConnectionPool.
    broadcast:
        A function that ensures a RPC is evaluated on all `Shuffle` instances of
        a given `ShuffleID`.
    memory_limiter_disk:
    memory_limiter_comm:
        A ``ResourceLimiter`` limiting the total amount of memory used in either
        buffer.
    """

    def __init__(
        self,
        worker_for: dict[int, str],
        output_workers: set,
        column: str,
        schema: pa.Schema,
        id: ShuffleId,
        local_address: str,
        directory: str,
        nthreads: int,
        rpc: Callable[[str], PooledRPCCall],
        broadcast: Callable,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
    ):

        import pandas as pd

        self.broadcast = broadcast
        self.rpc = rpc
        self.column = column
        self.id = id
        self.schema = schema
        self.output_workers = output_workers
        self.executor = ThreadPoolExecutor(nthreads)
        partitions_of = defaultdict(list)
        self.local_address = local_address
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = pd.Series(worker_for, name="_workers").astype("category")
        self.closed = False

        def _dump_batch(batch: pa.Buffer, file: BinaryIO) -> None:
            return dump_batch(batch, file, self.schema)

        self._disk_buffer = DiskShardsBuffer(
            dump=_dump_batch,
            load=load_arrow,
            directory=directory,
            memory_limiter=memory_limiter_disk,
        )

        self._comm_buffer = CommShardsBuffer(
            send=self.send, memory_limiter=memory_limiter_comms
        )
        # TODO: reduce number of connections to number of workers
        # MultiComm.max_connections = min(10, n_workers)

        self.diagnostics: dict[str, float] = defaultdict(float)
        self.output_partitions_left = len(self.partitions_of.get(local_address, ()))
        self.transferred = False
        self.total_recvd = 0
        self.start_time = time.time()
        self._exception: Exception | None = None
        self._closed_event = asyncio.Event()

    def __repr__(self) -> str:
        return f"<Shuffle id: {self.id} on {self.local_address}>"

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start

    async def barrier(self) -> None:
        self.raise_if_closed()
        # TODO: Consider broadcast pinging once when the shuffle starts to warm
        # up the comm pool on scheduler side
        await self.broadcast(msg={"op": "shuffle_inputs_done", "shuffle_id": self.id})

    async def send(self, address: str, shards: list[bytes]) -> None:
        self.raise_if_closed()
        return await self.rpc(address).shuffle_receive(
            data=to_serialize(shards),
            shuffle_id=self.id,
        )

    async def offload(self, func: Callable[..., T], *args: Any) -> T:
        self.raise_if_closed()
        with self.time("cpu"):
            return await asyncio.get_running_loop().run_in_executor(
                self.executor,
                func,
                *args,
            )

    def heartbeat(self) -> dict[str, Any]:
        comm_heartbeat = self._comm_buffer.heartbeat()
        comm_heartbeat["read"] = self.total_recvd
        return {
            "disk": self._disk_buffer.heartbeat(),
            "comm": comm_heartbeat,
            "diagnostics": self.diagnostics,
            "start": self.start_time,
        }

    async def receive(self, data: list[bytes]) -> None:
        await self._receive(data)

    async def _receive(self, data: list[bytes]) -> None:
        self.raise_if_closed()

        try:
            self.total_recvd += sum(map(len, data))
            groups = await self.offload(self._repartition_buffers, data)
            await self._write_to_disk(groups)
        except Exception as e:
            self._exception = e
            raise

    def _repartition_buffers(self, data: list[bytes]) -> dict[str, list[bytes]]:
        table = list_of_buffers_to_table(data, self.schema)
        groups = split_by_partition(table, self.column)
        assert len(table) == sum(map(len, groups.values()))
        del data
        return {
            k: [batch.serialize() for batch in v.to_batches()]
            for k, v in groups.items()
        }

    async def _write_to_disk(self, data: dict[str, list[bytes]]) -> None:
        self.raise_if_closed()
        await self._disk_buffer.write(data)

    def raise_if_closed(self) -> None:
        if self.closed:
            if self._exception:
                raise self._exception
            raise ShuffleClosedError(
                f"Shuffle {self.id} has been closed on {self.local_address}"
            )

    async def add_partition(self, data: pd.DataFrame) -> None:
        self.raise_if_closed()
        if self.transferred:
            raise RuntimeError(f"Cannot add more partitions to shuffle {self}")

        def _() -> dict[str, list[bytes]]:
            out = split_by_worker(
                data,
                self.column,
                self.worker_for,
            )
            out = {
                k: [b.serialize().to_pybytes() for b in t.to_batches()]
                for k, t in out.items()
            }
            return out

        out = await self.offload(_)
        await self._write_to_comm(out)

    async def _write_to_comm(self, data: dict[str, list[bytes]]) -> None:
        self.raise_if_closed()
        await self._comm_buffer.write(data)

    async def get_output_partition(self, i: int) -> pd.DataFrame:
        self.raise_if_closed()
        assert self.transferred, "`get_output_partition` called before barrier task"

        assert self.worker_for[i] == self.local_address, (
            f"Output partition {i} belongs on {self.worker_for[i]}, "
            f"not {self.local_address}. "
        )
        # ^ NOTE: this check isn't necessary, just a nice validation to prevent incorrect
        # data in the case something has gone very wrong

        assert (
            self.output_partitions_left > 0
        ), f"No outputs remaining, but requested output partition {i} on {self.local_address}."
        await self.flush_receive()
        try:
            df = self._read_from_disk(i)
            with self.time("cpu"):
                out = df.to_pandas()
        except KeyError:
            out = self.schema.empty_table().to_pandas()
        self.output_partitions_left -= 1
        return out

    def _read_from_disk(self, id: int | str) -> pa.Table:
        self.raise_if_closed()
        return self._disk_buffer.read(id)

    async def inputs_done(self) -> None:
        self.raise_if_closed()
        assert not self.transferred, "`inputs_done` called multiple times"
        self.transferred = True
        await self._flush_comm()
        try:
            self._comm_buffer.raise_on_exception()
        except Exception as e:
            self._exception = e
            raise

    async def _flush_comm(self) -> None:
        self.raise_if_closed()
        await self._comm_buffer.flush()

    def done(self) -> bool:
        return self.transferred and self.output_partitions_left == 0

    async def flush_receive(self) -> None:
        self.raise_if_closed()
        await self._disk_buffer.flush()

    async def close(self) -> None:
        if self.closed:
            await self._closed_event.wait()
            return

        self.closed = True
        await self._comm_buffer.close()
        await self._disk_buffer.close()
        try:
            self.executor.shutdown(cancel_futures=True)
        except Exception:
            self.executor.shutdown()
        self._closed_event.set()

    def fail(self, exception: Exception) -> None:
        if not self.closed:
            self._exception = exception


class ShuffleWorkerExtension:
    """Interface between a Worker and a Shuffle.

    This extension is responsible for

    - Lifecycle of Shuffle instances
    - ensuring connectivity between remote shuffle instances
    - ensuring connectivity and integration with the scheduler
    - routing concurrent calls to the appropriate `Shuffle` based on its `ShuffleID`
    - collecting instrumentation of ongoing shuffles and route to scheduler/worker
    """

    worker: Worker
    shuffles: dict[ShuffleId, Shuffle]
    memory_limiter_comms: ResourceLimiter
    memory_limiter_disk: ResourceLimiter
    closed: bool

    def __init__(self, worker: Worker) -> None:
        # Attach to worker
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done
        worker.handlers["shuffle_fail"] = self.shuffle_fail
        worker.stream_handlers["shuffle-fail"] = self.shuffle_fail
        worker.extensions["shuffle"] = self

        # Initialize
        self.worker = worker
        self.shuffles = {}
        self.memory_limiter_comms = ResourceLimiter(parse_bytes("100 MiB"))
        self.memory_limiter_disk = ResourceLimiter(parse_bytes("1 GiB"))
        self.closed = False

    # Handlers
    ##########
    # NOTE: handlers are not threadsafe, but they're called from async comms, so that's okay

    def heartbeat(self) -> dict:
        return {id: shuffle.heartbeat() for id, shuffle in self.shuffles.items()}

    async def shuffle_receive(
        self,
        shuffle_id: ShuffleId,
        data: list[bytes],
    ) -> None:
        """
        Handler: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle = await self._get_shuffle(shuffle_id)
        await shuffle.receive(data)

    async def shuffle_inputs_done(self, shuffle_id: ShuffleId) -> None:
        """
        Handler: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        with log_errors():
            shuffle = await self._get_shuffle(shuffle_id)
            await shuffle.inputs_done()
            if shuffle.done():
                # If the shuffle has no output partitions, remove it now;
                # `get_output_partition` will never be called.
                # This happens when there are fewer output partitions than workers.
                assert shuffle._disk_buffer.empty
                logger.info(f"Shuffle inputs done {shuffle}")
                await self._register_complete(shuffle)
                del self.shuffles[shuffle_id]

    async def shuffle_fail(self, shuffle_id: ShuffleId, message: str) -> None:
        try:
            shuffle = self.shuffles[shuffle_id]
        except KeyError:
            return
        exception = RuntimeError(message)
        shuffle.fail(exception)
        await shuffle.close()
        del self.shuffles[shuffle_id]

    def add_partition(
        self,
        data: pd.DataFrame,
        shuffle_id: ShuffleId,
        npartitions: int,
        column: str,
    ) -> None:
        shuffle = self.get_shuffle(
            shuffle_id, empty=data, npartitions=npartitions, column=column
        )
        sync(self.worker.loop, shuffle.add_partition, data=data)

    async def _barrier(self, shuffle_id: ShuffleId) -> None:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error. Calling this before all partitions have been
        added is undefined.
        """
        # Tell all peers that we've reached the barrier
        # Note that this will call `shuffle_inputs_done` on our own worker as well
        shuffle = await self._get_shuffle(shuffle_id)
        await shuffle.barrier()

    async def _register_complete(self, shuffle: Shuffle) -> None:
        await shuffle.close()
        # All the relevant work has already succeeded if we reached this point,
        # so we do not need to check if the extension is closed.
        await self.worker.scheduler.shuffle_register_complete(
            id=shuffle.id,
            worker=self.worker.address,
        )

    @overload
    async def _get_shuffle(
        self,
        shuffle_id: ShuffleId,
    ) -> Shuffle:
        ...

    @overload
    async def _get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame,
        column: str,
        npartitions: int,
    ) -> Shuffle:
        ...

    async def _get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame | None = None,
        column: str | None = None,
        npartitions: int | None = None,
    ) -> Shuffle:
        "Get a shuffle by ID; raise ValueError if it's not registered."
        import pyarrow as pa

        try:
            shuffle = self.shuffles[shuffle_id]
        except KeyError:
            try:
                result = await self.worker.scheduler.shuffle_get(
                    id=shuffle_id,
                    schema=pa.Schema.from_pandas(empty).serialize().to_pybytes()
                    if empty is not None
                    else None,
                    npartitions=npartitions,
                    column=column,
                    worker=self.worker.address,
                )
                if result["status"] == "ERROR":
                    raise RuntimeError(result["message"])
                assert result["status"] == "OK"
            except KeyError:
                # Even the scheduler doesn't know about this shuffle
                # Let's hand this back to the scheduler and let it figure
                # things out
                logger.info(
                    "Worker Shuffle unable to get information from scheduler, rescheduling"
                )
                from distributed.worker import Reschedule

                raise Reschedule()
            else:
                if self.closed:
                    raise ShuffleClosedError(
                        f"{self.__class__.__name__} already closed on {self.worker.address}"
                    )
                if shuffle_id not in self.shuffles:
                    shuffle = Shuffle(
                        column=result["column"],
                        worker_for=result["worker_for"],
                        output_workers=result["output_workers"],
                        schema=deserialize_schema(result["schema"]),
                        id=shuffle_id,
                        directory=os.path.join(
                            self.worker.local_directory, f"shuffle-{shuffle_id}"
                        ),
                        nthreads=self.worker.state.nthreads,
                        local_address=self.worker.address,
                        rpc=self.worker.rpc,
                        broadcast=functools.partial(
                            self._broadcast_to_participants, shuffle_id
                        ),
                        memory_limiter_disk=self.memory_limiter_disk,
                        memory_limiter_comms=self.memory_limiter_comms,
                    )
                    self.shuffles[shuffle_id] = shuffle
                return self.shuffles[shuffle_id]
        else:
            if shuffle._exception:
                raise shuffle._exception
            return shuffle

    async def _broadcast_to_participants(self, id: ShuffleId, msg: dict) -> dict:
        participating_workers = (
            await self.worker.scheduler.shuffle_get_participating_workers(id=id)
        )
        return await self.worker.scheduler.broadcast(
            msg=msg, workers=participating_workers
        )

    async def close(self) -> None:
        assert not self.closed

        self.closed = True
        while self.shuffles:
            _, shuffle = self.shuffles.popitem()
            await shuffle.close()

    #############################
    # Methods for worker thread #
    #############################

    def barrier(self, shuffle_id: ShuffleId) -> None:
        sync(self.worker.loop, self._barrier, shuffle_id)

    @overload
    def get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame,
        column: str,
        npartitions: int,
    ) -> Shuffle:
        ...

    @overload
    def get_shuffle(
        self,
        shuffle_id: ShuffleId,
    ) -> Shuffle:
        ...

    def get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame | None = None,
        column: str | None = None,
        npartitions: int | None = None,
    ) -> Shuffle:
        return sync(
            self.worker.loop,
            self._get_shuffle,
            shuffle_id,
            empty,
            column,
            npartitions,
        )

    def get_output_partition(
        self, shuffle_id: ShuffleId, output_partition: int
    ) -> pd.DataFrame:
        """
        Task: Retrieve a shuffled output partition from the ShuffleExtension.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        shuffle = self.get_shuffle(shuffle_id)
        output = sync(self.worker.loop, shuffle.get_output_partition, output_partition)
        # key missing if another thread got to it first
        if shuffle.done() and shuffle_id in self.shuffles:
            shuffle = self.shuffles.pop(shuffle_id)
            sync(self.worker.loop, self._register_complete, shuffle)
        return output


class ShuffleSchedulerExtension(SchedulerPlugin):
    """
    Shuffle extension for the scheduler

    Today this mostly just collects heartbeat messages for the dashboard,
    but in the future it may be responsible for more

    See Also
    --------
    ShuffleWorkerExtension
    """

    scheduler: Scheduler
    worker_for: dict[ShuffleId, dict[int, str]]
    heartbeats: defaultdict[ShuffleId, dict]
    schemas: dict[ShuffleId, bytes]
    columns: dict[ShuffleId, str]
    output_workers: dict[ShuffleId, set[str]]
    completed_workers: dict[ShuffleId, set[str]]
    participating_workers: dict[ShuffleId, set[str]]
    tombstones: set[ShuffleId]
    erred_shuffles: dict[ShuffleId, Exception]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers.update(
            {
                "shuffle_get": self.get,
                "shuffle_get_participating_workers": self.get_participating_workers,
                "shuffle_register_complete": self.register_complete,
            }
        )
        self.heartbeats = defaultdict(lambda: defaultdict(dict))
        self.worker_for = {}
        self.schemas = {}
        self.columns = {}
        self.output_workers = {}
        self.completed_workers = {}
        self.participating_workers = {}
        self.tombstones = set()
        self.erred_shuffles = {}
        self.scheduler.add_plugin(self)

    def shuffle_ids(self) -> set[ShuffleId]:
        return set(self.worker_for)

    def heartbeat(self, ws: WorkerState, data: dict) -> None:
        for shuffle_id, d in data.items():
            if shuffle_id in self.output_workers:
                self.heartbeats[shuffle_id][ws.address].update(d)

    @classmethod
    def barrier_key(cls, shuffle_id: ShuffleId) -> str:
        return "shuffle-barrier-" + shuffle_id

    @classmethod
    def id_from_key(cls, key: str) -> ShuffleId:
        assert key.startswith("shuffle-barrier-")
        return ShuffleId(key.replace("shuffle-barrier-", ""))

    def get(
        self,
        id: ShuffleId,
        schema: bytes | None,
        column: str | None,
        npartitions: int | None,
        worker: str,
    ) -> dict:

        if id in self.tombstones:
            return {
                "status": "ERROR",
                "message": f"Shuffle {id} has already been forgotten",
            }
        if exception := self.erred_shuffles.get(id):
            return {"status": "ERROR", "message": str(exception)}

        if id not in self.worker_for:
            assert schema is not None
            assert column is not None
            assert npartitions is not None
            workers = list(self.scheduler.workers)
            output_workers = set()

            name = self.barrier_key(id)
            mapping = {}

            for ts in self.scheduler.tasks[name].dependents:
                part = ts.annotations["shuffle"]
                if ts.worker_restrictions:
                    output_worker = list(ts.worker_restrictions)[0]
                else:
                    output_worker = get_worker_for(part, workers, npartitions)
                mapping[part] = output_worker
                output_workers.add(output_worker)
                self.scheduler.set_restrictions({ts.key: {output_worker}})

            self.worker_for[id] = mapping
            self.schemas[id] = schema
            self.columns[id] = column
            self.output_workers[id] = output_workers
            self.completed_workers[id] = set()
            self.participating_workers[id] = output_workers.copy()

        self.participating_workers[id].add(worker)
        return {
            "status": "OK",
            "worker_for": self.worker_for[id],
            "column": self.columns[id],
            "schema": self.schemas[id],
            "output_workers": self.output_workers[id],
        }

    def get_participating_workers(self, id: ShuffleId) -> list[str]:
        return list(self.participating_workers[id])

    async def remove_worker(self, scheduler: Scheduler, worker: str) -> None:
        affected_shuffles = set()
        broadcasts = []
        from time import time

        recs: Recs = {}
        stimulus_id = f"shuffle-failed-worker-left-{time()}"
        barriers = []
        for shuffle_id, shuffle_workers in self.participating_workers.items():
            if worker not in shuffle_workers:
                continue
            exception = RuntimeError(
                f"Worker {worker} left during active shuffle {shuffle_id}"
            )
            self.erred_shuffles[shuffle_id] = exception
            contact_workers = shuffle_workers.copy()
            contact_workers.discard(worker)
            affected_shuffles.add(shuffle_id)
            name = self.barrier_key(shuffle_id)
            barrier_task = self.scheduler.tasks.get(name)
            if barrier_task:
                barriers.append(barrier_task)
                broadcasts.append(
                    scheduler.broadcast(
                        msg={
                            "op": "shuffle_fail",
                            "message": str(exception),
                            "shuffle_id": shuffle_id,
                        },
                        workers=list(contact_workers),
                    )
                )

        results = await asyncio.gather(*broadcasts, return_exceptions=True)
        for barrier_task in barriers:
            if barrier_task.state == "memory":
                for dt in barrier_task.dependents:
                    if worker not in dt.worker_restrictions:
                        continue
                    dt.worker_restrictions.clear()
                    recs.update({dt.key: "waiting"})
            # TODO: Do we need to handle other states?
        self.scheduler.transitions(recs, stimulus_id=stimulus_id)

        # Assumption: No new shuffle tasks scheduled on the worker
        # + no existing tasks anymore
        # All task-finished/task-errer are queued up in batched stream

        exceptions = [result for result in results if isinstance(result, Exception)]
        if exceptions:
            # TODO: Do we need to handle errors here?
            raise RuntimeError(exceptions)

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if finish != "forgotten":
            return
        if not key.startswith("shuffle-barrier-"):
            return
        shuffle_id = self.id_from_key(key)
        if shuffle_id not in self.worker_for:
            return

        participating_workers = self.participating_workers[shuffle_id]
        worker_msgs = {
            worker: [
                {
                    "op": "shuffle-fail",
                    "shuffle_id": shuffle_id,
                    "message": f"Shuffle {shuffle_id} forgotten",
                }
            ]
            for worker in participating_workers
        }
        self._clean_on_scheduler(shuffle_id)
        self.scheduler.send_all({}, worker_msgs)

    def register_complete(self, id: ShuffleId, worker: str) -> None:
        """Learn from a worker that it has completed all reads of a shuffle"""
        if exception := self.erred_shuffles.get(id):
            raise exception
        if id not in self.completed_workers:
            logger.info("Worker shuffle reported complete after shuffle was removed")
            return
        self.completed_workers[id].add(worker)

    def _clean_on_scheduler(self, id: ShuffleId) -> None:
        self.tombstones.add(id)
        del self.worker_for[id]
        del self.schemas[id]
        del self.columns[id]
        del self.output_workers[id]
        del self.completed_workers[id]
        del self.participating_workers[id]
        self.erred_shuffles.pop(id, None)
        with contextlib.suppress(KeyError):
            del self.heartbeats[id]


def get_worker_for(output_partition: int, workers: list[str], npartitions: int) -> str:
    "Get the address of the worker which should hold this output partition number"
    i = len(workers) * output_partition // npartitions
    return workers[i]


def split_by_worker(
    df: pd.DataFrame,
    column: str,
    worker_for: pd.Series,
) -> dict[Any, pa.Table]:
    """
    Split data into many arrow batches, partitioned by destination worker
    """
    import numpy as np
    import pyarrow as pa

    df = df.merge(
        right=worker_for.cat.codes.rename("_worker"),
        left_on=column,
        right_index=True,
        how="inner",
    )
    nrows = len(df)
    if not nrows:
        return {}
    # assert len(df) == nrows  # Not true if some outputs aren't wanted
    # FIXME: If we do not preserve the index something is corrupting the
    # bytestream such that it cannot be deserialized anymore
    t = pa.Table.from_pandas(df, preserve_index=True)
    t = t.sort_by("_worker")
    codes = np.asarray(t.select(["_worker"]))[0]
    t = t.drop(["_worker"])
    del df

    splits = np.where(codes[1:] != codes[:-1])[0] + 1
    splits = np.concatenate([[0], splits])

    shards = [
        t.slice(offset=a, length=b - a) for a, b in toolz.sliding_window(2, splits)
    ]
    shards.append(t.slice(offset=splits[-1], length=None))

    unique_codes = codes[splits]
    out = {
        # FIXME https://github.com/pandas-dev/pandas-stubs/issues/43
        worker_for.cat.categories[code]: shard
        for code, shard in zip(unique_codes, shards)
    }
    assert sum(map(len, out.values())) == nrows
    return out


def split_by_partition(t: pa.Table, column: str) -> dict[Any, pa.Table]:
    """
    Split data into many arrow batches, partitioned by final partition
    """
    import numpy as np

    partitions = t.select([column]).to_pandas()[column].unique()
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
