from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, NewType

import toolz

from distributed.protocol import to_serialize
from distributed.shuffle.arrow import (
    deserialize_schema,
    dump_batch,
    list_of_buffers_to_table,
    load_arrow,
)
from distributed.shuffle.multi_comm import MultiComm
from distributed.shuffle.multi_file import MultiFile
from distributed.utils import log_errors, sync

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from distributed.worker import Worker

ShuffleId = NewType("ShuffleId", str)


logger = logging.getLogger(__name__)


class Shuffle:
    """State for a single active shuffle"""

    def __init__(
        self,
        column,
        worker_for: dict[int, str],
        output_workers: set,
        schema: pa.Schema,
        id: ShuffleId,
        worker: Worker,
        executor: ThreadPoolExecutor,
    ) -> None:

        import pandas as pd

        self.column = column
        self.id = id
        self.schema = schema
        self.worker = worker
        self.output_workers = output_workers
        self.executor = executor

        partitions_of = defaultdict(list)
        for part, address in worker_for.items():
            partitions_of[address].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = pd.Series(worker_for, name="_workers").astype("category")

        self.multi_file = MultiFile(
            dump=functools.partial(
                dump_batch,
                schema=self.schema,
            ),
            load=load_arrow,
            directory=os.path.join(self.worker.local_directory, "shuffle-%s" % self.id),
            sizeof=lambda L: sum(map(len, L)),
            loop=self.worker.io_loop,
        )

        async def send(address: str, shards: list[bytes]) -> None:
            return await self.worker.rpc(address).shuffle_receive(
                data=to_serialize(shards),
                shuffle_id=self.id,
            )

        self.multi_comm = MultiComm(
            send=send,
            loop=self.worker.io_loop,
        )
        # TODO: reduce number of connections to number of workers
        # MultiComm.max_connections = min(10, n_workers)

        self.diagnostics: dict[str, float] = defaultdict(float)
        self.output_partitions_left = len(
            self.partitions_of.get(self.worker.address, ())
        )
        self.transferred = False
        self.total_recvd = 0
        self.start_time = time.time()
        self._exception: Exception | None = None

    @contextlib.contextmanager
    def time(self, name: str):
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start

    async def offload(self, func, *args):
        # return func(*args)
        return await asyncio.get_event_loop().run_in_executor(
            self.executor,
            func,
            *args,
        )

    def heartbeat(self):
        return {
            "disk": {
                "memory": self.multi_file.total_size,
                "buckets": len(self.multi_file.shards),
                "written": self.multi_file.bytes_written,
                "read": self.multi_file.bytes_read,
                "active": 0,
                "diagnostics": self.multi_file.diagnostics,
                "memory_limit": self.multi_file.memory_limit,
            },
            "comms": {
                "memory": self.multi_comm.total_size,
                "buckets": len(self.multi_comm.shards),
                "written": self.multi_comm.total_moved,
                "read": self.total_recvd,
                "active": self.multi_comm.queue.qsize(),
                "diagnostics": self.multi_comm.diagnostics,
                "memory_limit": self.multi_comm.memory_limit,
            },
            "diagnostics": self.diagnostics,
            "start": self.start_time,
        }

    async def receive(self, data: list[pa.Buffer]) -> None:
        # This is actually ok.  Our local barrier might have finished,
        # but barriers on other workers might still be running and sending us
        # data
        # assert not self.transferred, "`receive` called after barrier task"
        if self._exception:
            raise self._exception

        self.total_recvd += sum(map(len, data))
        # An ugly way of turning these batches back into an arrow table
        with self.time("cpu"):
            data = await self.offload(
                list_of_buffers_to_table,
                data,
                self.schema,
            )

            groups = await self.offload(split_by_partition, data, self.column)

        assert len(data) == sum(map(len, groups.values()))
        del data

        with self.time("cpu"):
            groups = await self.offload(
                lambda: {
                    k: [batch.serialize() for batch in v.to_batches()]
                    for k, v in groups.items()
                }
            )
        try:
            await self.multi_file.put(groups)
        except Exception as e:
            self._exception = e

    def add_partition(self, data: pd.DataFrame) -> None:
        with self.time("cpu"):
            out = split_by_worker(
                data,
                self.column,
                self.worker_for,
            )
            out = {
                k: [b.serialize().to_pybytes() for b in t.to_batches()]
                for k, t in out.items()
            }
        self.multi_comm.put(out)

    def get_output_partition(self, i: int) -> pd.DataFrame:
        assert self.transferred, "`get_output_partition` called before barrier task"

        assert self.worker_for[i] == self.worker.address, (
            f"Output partition {i} belongs on {self.worker_for[i]}, "
            f"not {self.worker.address}. "
        )
        # ^ NOTE: this check isn't necessary, just a nice validation to prevent incorrect
        # data in the case something has gone very wrong

        assert (
            self.output_partitions_left > 0
        ), f"No outputs remaining, but requested output partition {i} on {self.worker.address}."

        sync(self.worker.loop, self.multi_file.flush)
        try:
            df = self.multi_file.read(i)
            with self.time("cpu"):
                out = df.to_pandas()
        except KeyError:
            out = self.schema.empty_table().to_pandas()
        self.output_partitions_left -= 1
        return out

    def inputs_done(self) -> None:
        assert not self.transferred, "`inputs_done` called multiple times"
        self.transferred = True

    def done(self) -> bool:
        return self.transferred and self.output_partitions_left == 0

    def close(self):
        self.multi_file.close()


class ShuffleWorkerExtension:
    "Extend the Worker with routes and state for peer-to-peer shuffles"

    def __init__(self, worker: Worker) -> None:
        # Attach to worker
        worker.handlers["shuffle_receive"] = self.shuffle_receive
        worker.handlers["shuffle_inputs_done"] = self.shuffle_inputs_done
        worker.extensions["shuffle"] = self

        # Initialize
        self.worker: Worker = worker
        self.shuffles: dict[ShuffleId, Shuffle] = {}
        self.executor = ThreadPoolExecutor(worker.nthreads)

    # Handlers
    ##########
    # NOTE: handlers are not threadsafe, but they're called from async comms, so that's okay

    def heartbeat(self):
        return {id: shuffle.heartbeat() for id, shuffle in self.shuffles.items()}

    async def shuffle_receive(
        self,
        comm: object,
        shuffle_id: ShuffleId,
        data: list[bytes],
    ) -> None:
        """
        Hander: Receive an incoming shard of data from a peer worker.
        Using an unknown ``shuffle_id`` is an error.
        """
        shuffle = await self._get_shuffle(shuffle_id)
        task = asyncio.create_task(shuffle.receive(data))
        if (
            shuffle.multi_file.total_size + sum(map(len, data))
            > shuffle.multi_file.memory_limit
        ):
            await task  # backpressure

    async def shuffle_inputs_done(self, comm: object, shuffle_id: ShuffleId) -> None:
        """
        Hander: Inform the extension that all input partitions have been handed off to extensions.
        Using an unknown ``shuffle_id`` is an error.
        """
        with log_errors():
            shuffle = await self._get_shuffle(shuffle_id)
            await shuffle.multi_comm.flush()
            shuffle.inputs_done()
            if shuffle.done():
                # If the shuffle has no output partitions, remove it now;
                # `get_output_partition` will never be called.
                # This happens when there are fewer output partitions than workers.
                assert not shuffle.multi_file.shards
                await shuffle.multi_file.flush()
                del self.shuffles[shuffle_id]
                shuffle.close()

    def add_partition(
        self,
        data: pd.DataFrame,
        shuffle_id: ShuffleId,
        npartitions: int = None,
        column=None,
    ) -> None:
        shuffle = self.get_shuffle(
            shuffle_id, empty=data, npartitions=npartitions, column=column
        )
        shuffle.add_partition(data=data)

    def barrier(self, shuffle_id: ShuffleId) -> None:
        sync(self.worker.loop, self._barrier, shuffle_id)

    async def _barrier(self, shuffle_id: ShuffleId) -> None:
        """
        Task: Note that the barrier task has been reached (`add_partition` called for all input partitions)

        Using an unknown ``shuffle_id`` is an error. Calling this before all partitions have been
        added is undefined.
        """
        # Tell all peers that we've reached the barrier
        # Note that this will call `shuffle_inputs_done` on our own worker as well
        shuffle = await self._get_shuffle(shuffle_id)
        out = await self.worker.scheduler.broadcast(
            msg={"op": "shuffle_inputs_done", "shuffle_id": shuffle_id}
        )
        if not shuffle.output_workers.issubset(set(out)):
            raise ValueError(
                "Some critical workers have left",
                set(shuffle.output_workers) - set(out),
            )
        # TODO handle errors from workers and scheduler, and cancellation.

    def get_output_partition(
        self, shuffle_id: ShuffleId, output_partition: int
    ) -> pd.DataFrame:
        """
        Task: Retrieve a shuffled output partition from the ShuffleExtension.

        Calling this for a ``shuffle_id`` which is unknown or incomplete is an error.
        """
        shuffle = self.get_shuffle(shuffle_id)
        output = shuffle.get_output_partition(output_partition)
        if shuffle.done():
            shuffle = self.shuffles.pop(shuffle_id, None)
            # key missing if another thread got to it first
            if shuffle:
                shuffle.close()
                sync(
                    self.worker.loop,
                    self.worker.scheduler.shuffle_register_complete,
                    id=shuffle_id,
                    worker=self.worker.address,
                )
        return output

    async def _get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame | None = None,
        column=None,
        npartitions: int = None,
    ) -> Shuffle:
        "Get a shuffle by ID; raise ValueError if it's not registered."
        import pyarrow as pa

        try:
            return self.shuffles[shuffle_id]
        except KeyError:
            try:
                result = await self.worker.scheduler.shuffle_get(
                    id=shuffle_id,
                    schema=pa.Schema.from_pandas(empty).serialize().to_pybytes()
                    if empty is not None
                    else None,
                    npartitions=npartitions,
                    column=column,
                )
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
                if shuffle_id not in self.shuffles:
                    shuffle = Shuffle(
                        column=result["column"],
                        worker_for=result["worker_for"],
                        output_workers=result["output_workers"],
                        worker=self.worker,
                        schema=deserialize_schema(result["schema"]),
                        id=shuffle_id,
                        executor=self.executor,
                    )
                    self.shuffles[shuffle_id] = shuffle
                return self.shuffles[shuffle_id]

    def get_shuffle(
        self,
        shuffle_id: ShuffleId,
        empty: pd.DataFrame | None = None,
        column=None,
        npartitions: int = None,
    ):
        return sync(
            self.worker.loop, self._get_shuffle, shuffle_id, empty, column, npartitions
        )

    def close(self):
        self.executor.shutdown()
        while self.shuffles:
            _, shuffle = self.shuffles.popitem()
            shuffle.close()


class ShuffleSchedulerExtension:
    """
    Shuffle extension for the scheduler

    Today this mostly just collects heartbeat messages for the dashboard,
    but in the future it may be responsible for more

    See Also
    --------
    ShuffleWorkerExtension
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers.update(
            {
                "shuffle_get": self.get,
                "shuffle_register_complete": self.register_complete,
            }
        )
        self.heartbeats = defaultdict(lambda: defaultdict(dict))
        self.worker_for = dict()
        self.schemas = dict()
        self.columns = dict()
        self.output_workers = dict()
        self.completed_workers = dict()

    def heartbeat(self, ws, data):
        for shuffle_id, d in data.items():
            self.heartbeats[shuffle_id][ws.address].update(d)

    def get(
        self, id: ShuffleId, schema: bytes | None, column, npartitions: int | None
    ) -> dict:
        if id not in self.worker_for:
            assert schema is not None
            assert column is not None
            assert npartitions is not None
            workers = list(self.scheduler.workers)
            output_workers = set()

            name = "shuffle-barrier-" + id  # TODO single-source task name
            mapping = {}

            for ts in self.scheduler.tasks[name].dependents:
                part = ts.annotations["shuffle"]
                if ts.worker_restrictions:
                    worker = list(ts.worker_restrictions)[0]
                else:
                    worker = worker_for(part, workers, npartitions)
                mapping[part] = worker
                output_workers.add(worker)
                self.scheduler.set_restrictions({ts.key: {worker}})
                # ts.worker_restrictions = {worker}  # TODO: once cython is
                # gone

            self.worker_for[id] = mapping
            self.schemas[id] = schema
            self.columns[id] = column
            self.output_workers[id] = output_workers
            self.completed_workers[id] = set()

        return {
            "worker_for": self.worker_for[id],
            "column": self.columns[id],
            "schema": self.schemas[id],
            "output_workers": self.output_workers[id],
        }

    def register_complete(self, id: ShuffleId, worker: str):
        """Learn from a worker that it has completed all reads of a shuffle"""
        if id not in self.completed_workers:
            logger.info("Worker shuffle reported complete after shuffle was removed")
            return
        self.completed_workers[id].add(worker)

        if self.completed_workers[id] == self.output_workers[id]:
            del self.worker_for[id]
            del self.schemas[id]
            del self.columns[id]
            del self.output_workers[id]
            del self.completed_workers[id]
            with contextlib.suppress(KeyError):
                del self.heartbeats[id]


def worker_for(output_partition: int, workers: list[str], npartitions: int) -> str:
    "Get the address of the worker which should hold this output partition number"
    i = len(workers) * output_partition // npartitions
    return workers[i]


def split_by_worker(
    df: pd.DataFrame,
    column: str,
    worker_for: pd.Series,
) -> dict:
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
    t = pa.Table.from_pandas(df)
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
        worker_for.cat.categories[code]: shard
        for code, shard in zip(unique_codes, shards)
    }
    assert sum(map(len, out.values())) == nrows
    return out


def split_by_partition(
    t: pa.Table,
    column: str,
) -> dict:
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
