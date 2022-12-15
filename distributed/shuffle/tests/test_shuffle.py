from __future__ import annotations

import asyncio
import io
import os
import random
import shutil
from collections import defaultdict
from itertools import chain
from typing import Any, Mapping
from unittest import mock

import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.distributed import Event, Nanny, Worker
from dask.utils import stringify

from distributed.core import PooledRPCCall
from distributed.scheduler import Scheduler
from distributed.scheduler import TaskState as SchedulerTaskState
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._shuffle_extension import (
    Shuffle,
    ShuffleId,
    ShuffleWorkerExtension,
    dump_batch,
    get_worker_for,
    list_of_buffers_to_table,
    load_arrow,
    split_by_partition,
    split_by_worker,
)
from distributed.utils import Deadline
from distributed.utils_test import gen_cluster, gen_test, wait_for_state
from distributed.worker_state_machine import TaskState as WorkerTaskState

pa = pytest.importorskip("pyarrow")


async def clean_worker(
    worker: Worker, interval: float = 0.01, timeout: int | None = None
) -> None:
    """Assert that the worker has no shuffle state"""
    deadline = Deadline.after(timeout)
    extension = worker.extensions["shuffle"]

    while extension.shuffles and not deadline.expired:
        await asyncio.sleep(interval)
    for dirpath, dirnames, filenames in os.walk(worker.local_directory):
        assert "shuffle" not in dirpath
        for fn in dirnames + filenames:
            assert "shuffle" not in fn


async def clean_scheduler(
    scheduler: Scheduler, interval: float = 0.01, timeout: int | None = None
) -> None:
    """Assert that the scheduler has no shuffle state"""
    deadline = Deadline.after(timeout)
    extension = scheduler.extensions["shuffle"]
    while extension.output_workers and not deadline.expired:
        await asyncio.sleep(interval)
    assert not extension.worker_for
    assert not extension.heartbeats
    assert not extension.schemas
    assert not extension.columns
    assert not extension.output_workers
    assert not extension.completed_workers
    assert not extension.participating_workers


@gen_cluster(client=True)
async def test_basic_integration(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    x, y = c.compute([df.x.size, out.x.size])
    x = await x
    y = await y
    assert x == y

    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_concurrent(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    y = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    x, y = c.compute([x.x.size, y.y.size])
    x = await x
    y = await y
    assert x == y

    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_bad_disk(c, s, a, b):

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_id = await get_shuffle_id(s)
    while not a.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    shutil.rmtree(a.local_directory)

    while not b.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    shutil.rmtree(b.local_directory)
    with pytest.raises(RuntimeError, match=f"shuffle_transfer failed .* {shuffle_id}"):
        out = await c.compute(out)

    await c.close()
    # await clean_worker(a)
    # await clean_worker(b)
    # await clean_scheduler(s)


async def wait_until_worker_has_tasks(
    prefix: str, worker: str, count: int, scheduler: Scheduler, interval: float = 0.01
) -> None:
    ws = scheduler.workers[worker]
    while (
        len(
            [
                key
                for key, ts in scheduler.tasks.items()
                if prefix in key and ts.state == "memory" and ws in ts.who_has
            ]
        )
        < count
    ):
        await asyncio.sleep(interval)


async def wait_for_tasks_in_state(
    prefix: str,
    state: str,
    count: int,
    dask_worker: Worker | Scheduler,
    interval: float = 0.01,
) -> None:
    tasks: Mapping[str, SchedulerTaskState | WorkerTaskState]

    if isinstance(dask_worker, Worker):
        tasks = dask_worker.state.tasks
    elif isinstance(dask_worker, Scheduler):
        tasks = dask_worker.tasks
    else:
        raise TypeError(dask_worker)

    while (
        len([key for key, ts in tasks.items() if prefix in key and ts.state == state])
        < count
    ):
        await asyncio.sleep(interval)


async def get_shuffle_id(scheduler: Scheduler) -> ShuffleId:
    scheduler_extension = scheduler.extensions["shuffle"]
    while not scheduler_extension.shuffle_ids():
        await asyncio.sleep(0.01)
    shuffle_ids = scheduler_extension.shuffle_ids()
    assert len(shuffle_ids) == 1
    return next(iter(shuffle_ids))


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_transfer(c, s, a, b):

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)
    await b.close()

    with pytest.raises(RuntimeError):
        out = await c.compute(out)

    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_during_transfer(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        killed_worker_address = n.worker_address
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
        await wait_until_worker_has_tasks(
            "shuffle-transfer", killed_worker_address, 1, s
        )
        await n.process.process.kill()

        with pytest.raises(RuntimeError):
            out = await c.compute(out)

        await c.close()
        await clean_worker(a)
        await clean_scheduler(s)


# TODO: Deduplicate instead of failing: distributed#7324
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_input_only_worker_during_transfer(c, s, a, b):
    def mock_get_worker_for(
        output_partition: int, workers: list[str], npartitions: int
    ) -> str:
        return a.address

    with mock.patch(
        "distributed.shuffle._shuffle_extension.get_worker_for", mock_get_worker_for
    ):
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-05-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
        await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b, 0.001)
        await b.close()

        with pytest.raises(RuntimeError):
            out = await c.compute(out)

        await c.close()
        await clean_worker(a)
        await clean_worker(b)
        await clean_scheduler(s)


# TODO: Deduplicate instead of failing: distributed#7324
@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)], clean_kwargs={"processes": False})
async def test_crashed_input_only_worker_during_transfer(c, s, a):
    def mock_get_worker_for(
        output_partition: int, workers: list[str], npartitions: int
    ) -> str:
        return a.address

    with mock.patch(
        "distributed.shuffle._shuffle_extension.get_worker_for", mock_get_worker_for
    ):
        async with Nanny(s.address, nthreads=1) as n:
            killed_worker_address = n.worker_address
            df = dask.datasets.timeseries(
                start="2000-01-01",
                end="2000-03-01",
                dtypes={"x": float, "y": float},
                freq="10 s",
            )
            out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
            out = out.persist()
            await wait_until_worker_has_tasks(
                "shuffle-transfer", n.worker_address, 1, s
            )
            await n.process.process.kill()

            with pytest.raises(RuntimeError):
                out = await c.compute(out)

            await c.close()
            await clean_worker(a)
            await clean_scheduler(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_closed_bystanding_worker_during_shuffle(c, s, w1, w2, w3):
    with dask.annotate(workers=[w1.address, w2.address], allow_other_workers=False):
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-02-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, w1)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, w2)
    await w3.close()

    await c.compute(out)
    del out

    await clean_worker(w1)
    await clean_worker(w2)
    await clean_worker(w3)
    await clean_scheduler(s)


class BlockedInputsDoneShuffle(Shuffle):
    def __init__(
        self,
        worker_for,
        output_workers,
        column,
        schema,
        id,
        local_address,
        directory,
        nthreads,
        rpc,
        broadcast,
        memory_limiter_disk,
        memory_limiter_comms,
    ):
        super().__init__(
            worker_for,
            output_workers,
            column,
            schema,
            id,
            local_address,
            directory,
            nthreads,
            rpc,
            broadcast,
            memory_limiter_disk,
            memory_limiter_comms,
        )
        self.in_inputs_done = asyncio.Event()
        self.block_inputs_done = asyncio.Event()

    async def inputs_done(self) -> None:
        self.in_inputs_done.set()
        await self.block_inputs_done.wait()
        await super().inputs_done()


@mock.patch("distributed.shuffle._shuffle_extension.Shuffle", BlockedInputsDoneShuffle)
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_barrier(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_id = await get_shuffle_id(s)
    barrier_key = s.extensions["shuffle"].barrier_key(shuffle_id)
    await wait_for_state(barrier_key, "processing", s)
    shuffleA = a.extensions["shuffle"].shuffles[shuffle_id]
    shuffleB = b.extensions["shuffle"].shuffles[shuffle_id]
    await shuffleA.in_inputs_done.wait()
    await shuffleB.in_inputs_done.wait()

    ts = s.tasks[barrier_key]
    processing_worker = a if ts.processing_on.address == a.address else b
    if processing_worker == a:
        close_worker = a
        alive_shuffle = shuffleB

    else:
        close_worker, alive_worker = b, a
        alive_shuffle = shuffleA
    await close_worker.close()

    alive_shuffle.block_inputs_done.set()

    with pytest.raises(RuntimeError):
        out = await c.compute(out)

    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@mock.patch("distributed.shuffle._shuffle_extension.Shuffle", BlockedInputsDoneShuffle)
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_other_worker_during_barrier(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_id = await get_shuffle_id(s)

    barrier_key = s.extensions["shuffle"].barrier_key(shuffle_id)
    await wait_for_state(barrier_key, "processing", s, interval=0)

    shuffleA = a.extensions["shuffle"].shuffles[shuffle_id]
    shuffleB = b.extensions["shuffle"].shuffles[shuffle_id]
    await shuffleA.in_inputs_done.wait()
    await shuffleB.in_inputs_done.wait()

    ts = s.tasks[barrier_key]
    processing_worker = a if ts.processing_on.address == a.address else b
    if processing_worker == a:
        close_worker = b
        alive_shuffle = shuffleA

    else:
        close_worker = a
        alive_shuffle = shuffleB
    await close_worker.close()

    alive_shuffle.block_inputs_done.set()

    with pytest.raises(RuntimeError, match="shuffle_barrier failed"):
        out = await c.compute(out)

    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@pytest.mark.slow
@mock.patch("distributed.shuffle._shuffle_extension.Shuffle", BlockedInputsDoneShuffle)
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_other_worker_during_barrier(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
        shuffle_id = await get_shuffle_id(s)
        barrier_key = s.extensions["shuffle"].barrier_key(shuffle_id)
        # Ensure that barrier is not executed on the nanny
        s.set_restrictions({barrier_key: {a.address}})
        await wait_for_state(barrier_key, "processing", s, interval=0)
        shuffle = a.extensions["shuffle"].shuffles[shuffle_id]
        await shuffle.in_inputs_done.wait()
        await n.process.process.kill()
        shuffle.block_inputs_done.set()

        with pytest.raises(RuntimeError, match="shuffle"):
            out = await c.compute(out)

        await c.close()
        await clean_worker(a)
        await clean_scheduler(s)


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_unpack(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    await wait_for_tasks_in_state("shuffle-p2p", "memory", 1, b)
    await b.close()

    with pytest.raises(RuntimeError):
        out = await c.compute(out)

    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_during_unpack(c, s, a):
    async with Nanny(s.address, nthreads=2) as n:
        killed_worker_address = n.worker_address
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
        await wait_until_worker_has_tasks("shuffle-p2p", killed_worker_address, 1, s)
        await n.process.process.kill()
        with pytest.raises(
            RuntimeError,
        ):
            out = await c.compute(out)

        await c.close()
        await clean_worker(a)
        await clean_scheduler(s)


class BlockedRegisterCompleteShuffleWorkerExtension(ShuffleWorkerExtension):
    def __init__(self, worker: Worker) -> None:
        super().__init__(worker)
        self.in_register_complete = asyncio.Event()
        self.block_register_complete = asyncio.Event()

    async def _register_complete(self, shuffle: Shuffle) -> None:
        self.in_register_complete.set()
        await super()._register_complete(shuffle)
        await self.block_register_complete.wait()


@pytest.mark.parametrize("kill_barrier", [True, False])
@gen_cluster(
    client=True,
    worker_kwargs={
        "extensions": {"shuffle": BlockedRegisterCompleteShuffleWorkerExtension}
    },
    nthreads=[("", 1)] * 2,
)
async def test_closed_worker_during_final_register_complete(c, s, a, b, kill_barrier):

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_ext_a = a.extensions["shuffle"]
    shuffle_ext_b = b.extensions["shuffle"]
    await shuffle_ext_a.in_register_complete.wait()
    await shuffle_ext_b.in_register_complete.wait()

    shuffle_id = await get_shuffle_id(s)
    barrier_key = s.extensions["shuffle"].barrier_key(shuffle_id)
    # TODO: properly parametrize over kill_barrier
    if barrier_key in b.state.tasks:
        shuffle_ext_a.block_register_complete.set()
        while a.state.executing:
            await asyncio.sleep(0.01)
        b.batched_stream.abort()
    else:
        shuffle_ext_b.block_register_complete.set()
        while b.state.executing:
            await asyncio.sleep(0.01)
        a.batched_stream.abort()

    with pytest.raises(RuntimeError, match="shuffle_unpack failed"):
        out = await c.compute(out)

    shuffle_ext_b.block_register_complete.set()

    # something is holding on to refs of out s.t. we cannot release the futures.
    # The shuffle will only be cleaned up once the tasks area released
    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(
    client=True,
    worker_kwargs={
        "extensions": {"shuffle": BlockedRegisterCompleteShuffleWorkerExtension}
    },
    nthreads=[("", 1)] * 2,
)
async def test_closed_other_worker_during_final_register_complete(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_ext_a = a.extensions["shuffle"]
    shuffle_ext_b = b.extensions["shuffle"]
    await shuffle_ext_a.in_register_complete.wait()
    await shuffle_ext_b.in_register_complete.wait()

    shuffle_ext_b.block_register_complete.set()
    while b.state.executing:
        await asyncio.sleep(0.01)
    await b.close()

    shuffle_ext_a.block_register_complete.set()
    with pytest.raises(RuntimeError):
        out = await c.compute(out)

    await c.close()
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_heartbeat(c, s, a, b):
    await a.heartbeat()
    await clean_scheduler(s)
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    while not s.extensions["shuffle"].heartbeats:
        await asyncio.sleep(0.001)
        await a.heartbeat()

    assert s.extensions["shuffle"].heartbeats.values()
    await out

    await clean_worker(a)
    await clean_worker(b)
    del out
    await clean_scheduler(s)


def test_processing_chain():
    """
    This is a serial version of the entire compute chain

    In practice this takes place on many different workers.
    Here we verify its accuracy in a single threaded situation.
    """
    workers = ["a", "b", "c"]
    npartitions = 5
    df = pd.DataFrame(
        {"x": range(100), "y": range(100), "z": chain(range(50), range(50))}
    )
    df["z"] = df["z"].astype("category")
    df["_partitions"] = df.x % npartitions
    schema = pa.Schema.from_pandas(df)
    worker_for = {i: random.choice(workers) for i in list(range(npartitions))}
    worker_for = pd.Series(worker_for, name="_worker").astype("category")

    data = split_by_worker(df, "_partitions", worker_for=worker_for)
    assert set(data) == set(worker_for.cat.categories)
    assert sum(map(len, data.values())) == len(df)

    batches = {
        worker: [b.serialize().to_pybytes() for b in t.to_batches()]
        for worker, t in data.items()
    }

    # Typically we communicate to different workers at this stage
    # We then receive them back and reconstute them

    by_worker = {
        worker: list_of_buffers_to_table(list_of_batches, schema)
        for worker, list_of_batches in batches.items()
    }
    assert sum(map(len, by_worker.values())) == len(df)

    # We split them again, and then dump them down to disk

    splits_by_worker = {
        worker: split_by_partition(t, "_partitions") for worker, t in by_worker.items()
    }

    splits_by_worker = {
        worker: {
            partition: [batch.serialize() for batch in t.to_batches()]
            for partition, t in d.items()
        }
        for worker, d in splits_by_worker.items()
    }

    # No two workers share data from any partition
    assert not any(
        set(a) & set(b)
        for w1, a in splits_by_worker.items()
        for w2, b in splits_by_worker.items()
        if w1 is not w2
    )

    # Our simple file system

    filesystem = defaultdict(io.BytesIO)

    for partitions in splits_by_worker.values():
        for partition, batches in partitions.items():
            for batch in batches:
                dump_batch(batch, filesystem[partition], schema)

    out = {}
    for k, bio in filesystem.items():
        bio.seek(0)
        out[k] = load_arrow(bio)

    assert sum(map(len, out.values())) == len(df)


@gen_cluster(client=True)
async def test_head(c, s, a, b):
    a_files = list(os.walk(a.local_directory))
    b_files = list(os.walk(b.local_directory))

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = await out.head(compute=False).persist()  # Only ask for one key

    assert list(os.walk(a.local_directory)) == a_files  # cleaned up files?
    assert list(os.walk(b.local_directory)) == b_files

    await clean_worker(a)
    await clean_worker(b)
    del out
    await clean_scheduler(s)


def test_split_by_worker():
    workers = ["a", "b", "c"]
    npartitions = 5
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["_partitions"] = df.x % npartitions
    worker_for = {i: random.choice(workers) for i in range(npartitions)}
    s = pd.Series(worker_for, name="_worker").astype("category")


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_clean_after_forgotten_early(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, a)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)
    del out
    await clean_worker(a, timeout=2)
    await clean_worker(b, timeout=2)
    await clean_scheduler(s, timeout=2)


@gen_cluster(client=True)
async def test_tail(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="1 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    full = await x.persist()
    ntasks_full = len(s.tasks)
    del full
    while s.tasks:
        await asyncio.sleep(0)
    partial = await x.tail(compute=False).persist()  # Only ask for one key

    assert len(s.tasks) < ntasks_full
    del partial

    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@pytest.mark.xfail(reason="Tombstone prohibits multiple calls to head")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 4)] * 2)
async def test_repeat(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    await c.compute(out.head(compute=False))

    await clean_worker(a, timeout=2)
    await clean_worker(b, timeout=2)
    await clean_scheduler(s, timeout=2)

    await c.compute(out.tail(compute=False))

    await clean_worker(a, timeout=2)
    await clean_worker(b, timeout=2)
    await clean_scheduler(s, timeout=2)

    await c.compute(out.head(compute=False))

    await clean_worker(a, timeout=2)
    await clean_worker(b, timeout=2)
    await clean_scheduler(s, timeout=2)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_after_shuffle(c, s, a):
    in_event = Event()
    block_event = Event()

    @dask.delayed
    def block(df, in_event, block_event):
        in_event.set()
        block_event.wait()
        return df

    async with Nanny(s.address, nthreads=1) as n:
        df = df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="100 s",
            seed=42,
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        in_event = Event()
        block_event = Event()
        with dask.annotate(workers=[n.worker_address], allow_other_workers=True):
            out = block(out, in_event, block_event)
        fut = c.compute(out)

        await in_event.wait()
        await n.process.process.kill()
        block_event.set()
        with pytest.raises(RuntimeError):
            await fut

        await c.close()
        await clean_worker(a)
        await clean_scheduler(s)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_after_shuffle_persisted(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        df = df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
            seed=42,
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()
        await out

        await n.process.process.kill()

        with pytest.raises(RuntimeError):
            await c.compute(out.sum())

        await c.close()
        await clean_worker(a)
        await clean_scheduler(s)


@pytest.mark.xfail(reason="Tombstone prohibits multiple calls to head")
@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_closed_worker_between_repeats(c, s, w1, w2, w3):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
        seed=42,
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    await c.compute(out.head(compute=False))

    await clean_worker(w1)
    await clean_worker(w2)
    await clean_worker(w3)
    await clean_scheduler(s)

    await w3.close()
    await c.compute(out.tail(compute=False))

    await clean_worker(w1)
    await clean_worker(w2)
    await clean_scheduler(s)

    await w2.close()
    await c.compute(out.head(compute=False))
    await clean_worker(w1)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_new_worker(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-20",
        dtypes={"x": float, "y": float},
        freq="1 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    persisted = shuffled.persist()
    while not s.extensions["shuffle"].worker_for:
        await asyncio.sleep(0.001)

    async with Worker(s.address) as w:

        await c.compute(persisted)

        await clean_worker(a)
        await clean_worker(b)
        await clean_worker(w)
        del persisted
        await clean_scheduler(s)


@gen_cluster(client=True)
async def test_multi(c, s, a, b):
    left = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-20",
        freq="10s",
        dtypes={"id": float, "x": float},
    )
    right = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        freq="10s",
        dtypes={"id": float, "y": float},
    )
    left["id"] = (left["id"] * 1000000).astype(int)
    right["id"] = (right["id"] * 1000000).astype(int)

    out = left.merge(right, on="id", shuffle="p2p")
    out = await c.compute(out.size)
    assert out

    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_restrictions(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    ).persist(workers=a.address)
    await df
    assert a.data
    assert not b.data

    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    x = x.persist(workers=b.address)
    y = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    y = y.persist(workers=a.address)

    await x
    assert all(stringify(key) in b.data for key in x.__dask_keys__())

    await y
    assert all(stringify(key) in a.data for key in y.__dask_keys__())


@pytest.mark.skip(reason="Fails on CI with unknown cause")
@gen_cluster(client=True)
async def test_delete_some_results(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p").persist()
    while not s.tasks or not any(ts.state == "memory" for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    x = x.partitions[: x.npartitions // 2].persist()

    await c.compute(x.size)
    del x
    await clean_worker(a)
    await clean_worker(b)
    await clean_scheduler(s)


@gen_cluster(client=True)
async def test_add_some_results(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    y = x.partitions[: x.npartitions // 2].persist()

    while not s.tasks or not any(ts.state == "memory" for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    x = x.persist()

    await c.compute(x.size)

    await clean_worker(a)
    await clean_worker(b)
    del x
    del y
    await clean_scheduler(s)


@pytest.mark.slow
@gen_cluster(client=True)
async def test_clean_after_close(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p").persist()

    while not s.tasks or not any(ts.state == "memory" for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    await a.close()
    await clean_worker(a)


class PooledRPCShuffle(PooledRPCCall):
    def __init__(self, shuffle: Shuffle):
        self.shuffle = shuffle

    def __getattr__(self, key):
        async def _(**kwargs):
            from distributed.protocol.serialize import nested_deserialize

            method_name = key.replace("shuffle_", "")
            kwargs.pop("shuffle_id", None)
            # TODO: This is a bit awkward. At some point the arguments are
            # already getting wrapped with a `Serialize`. We only want to unwrap
            # here.
            kwargs = nested_deserialize(kwargs)
            meth = getattr(self.shuffle, method_name)
            return await meth(**kwargs)

        return _


class ShuffleTestPool:
    def __init__(self, *args, **kwargs):
        self.shuffles = {}
        super().__init__(*args, **kwargs)

    def __call__(self, addr: str, *args: Any, **kwargs: Any) -> PooledRPCShuffle:
        return PooledRPCShuffle(self.shuffles[addr])

    async def fake_broadcast(self, msg):

        op = msg.pop("op").replace("shuffle_", "")
        out = {}
        for addr, s in self.shuffles.items():
            out[addr] = await getattr(s, op)()
        return out

    def new_shuffle(
        self, name, worker_for_mapping, schema, directory, loop, Shuffle=Shuffle
    ):
        s = Shuffle(
            column="_partition",
            worker_for=worker_for_mapping,
            # FIXME: Is output_workers redundant with worker_for?
            output_workers=set(worker_for_mapping.values()),
            schema=schema,
            directory=directory / name,
            id=ShuffleId(name),
            local_address=name,
            nthreads=2,
            rpc=self,
            broadcast=self.fake_broadcast,
            memory_limiter_disk=ResourceLimiter(10000000),
            memory_limiter_comms=ResourceLimiter(10000000),
        )
        self.shuffles[name] = s
        return s


# 36 parametrizations
# Runtime each ~0.1s
@pytest.mark.parametrize("n_workers", [1, 10])
@pytest.mark.parametrize("n_input_partitions", [1, 2, 10])
@pytest.mark.parametrize("npartitions", [1, 20])
@pytest.mark.parametrize("barrier_first_worker", [True, False])
@gen_test()
async def test_basic_lowlevel_shuffle(
    tmpdir,
    loop_in_thread,
    n_workers,
    n_input_partitions,
    npartitions,
    barrier_first_worker,
):
    dfs = []
    rows_per_df = 10
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)

    workers = list("abcdefghijklmn")[:n_workers]

    worker_for_mapping = {}

    for part in range(npartitions):
        worker_for_mapping[part] = get_worker_for(part, workers, npartitions)
    assert len(set(worker_for_mapping.values())) == min(n_workers, npartitions)
    schema = pa.Schema.from_pandas(dfs[0])

    local_shuffle_pool = ShuffleTestPool()
    shuffles = []
    for ix in range(n_workers):
        shuffles.append(
            local_shuffle_pool.new_shuffle(
                name=workers[ix],
                worker_for_mapping=worker_for_mapping,
                schema=schema,
                directory=tmpdir,
                loop=loop_in_thread,
            )
        )
    random.seed(42)
    if barrier_first_worker:
        barrier_worker = shuffles[0]
    else:
        barrier_worker = random.sample(shuffles, k=1)[0]

    try:
        for ix, df in enumerate(dfs):
            s = shuffles[ix % len(shuffles)]
            await s.add_partition(df)

        await barrier_worker.barrier()

        total_bytes_sent = 0
        total_bytes_recvd = 0
        total_bytes_recvd_shuffle = 0
        for s in shuffles:
            metrics = s.heartbeat()
            assert metrics["comm"]["total"] == metrics["comm"]["written"]
            total_bytes_sent += metrics["comm"]["written"]
            total_bytes_recvd += metrics["disk"]["total"]
            total_bytes_recvd_shuffle += s.total_recvd

        assert total_bytes_recvd_shuffle == total_bytes_sent

        def _done():
            return [s.done() for s in shuffles]

        assert sum(_done()) == max(0, n_workers - npartitions)

        all_parts = []
        for part, worker in worker_for_mapping.items():
            s = local_shuffle_pool.shuffles[worker]
            all_parts.append(s.get_output_partition(part))

        all_parts = await asyncio.gather(*all_parts)

        df_after = pd.concat(all_parts)
        assert all(_done())
    finally:
        await asyncio.gather(*[s.close() for s in shuffles])
    assert len(df_after) == len(pd.concat(dfs))


@gen_test()
async def test_error_offload(tmpdir, loop_in_thread):
    dfs = []
    rows_per_df = 10
    n_input_partitions = 2
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)

    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = get_worker_for(part, workers, npartitions)
        partitions_for_worker[w].append(part)
    schema = pa.Schema.from_pandas(dfs[0])

    local_shuffle_pool = ShuffleTestPool()

    class ErrorOffload(Shuffle):
        async def offload(self, func, *args):
            raise RuntimeError("Error during deserialization")

    sA = local_shuffle_pool.new_shuffle(
        name="A",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
        Shuffle=ErrorOffload,
    )
    sB = local_shuffle_pool.new_shuffle(
        name="B",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
    )
    try:
        await sB.add_partition(dfs[0])
        with pytest.raises(RuntimeError, match="Error during deserialization"):
            await sB.add_partition(dfs[1])
            await sB.barrier()
    finally:
        await asyncio.gather(*[s.close() for s in [sA, sB]])


@gen_test()
async def test_error_send(tmpdir, loop_in_thread):
    dfs = []
    rows_per_df = 10
    n_input_partitions = 1
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)

    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = get_worker_for(part, workers, npartitions)
        partitions_for_worker[w].append(part)
    schema = pa.Schema.from_pandas(dfs[0])

    local_shuffle_pool = ShuffleTestPool()

    class ErrorSend(Shuffle):
        async def send(self, address: str, shards: list[bytes]) -> None:
            raise RuntimeError("Error during send")

    sA = local_shuffle_pool.new_shuffle(
        name="A",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
        Shuffle=ErrorSend,
    )
    sB = local_shuffle_pool.new_shuffle(
        name="B",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
    )
    try:
        await sA.add_partition(dfs[0])
        with pytest.raises(RuntimeError, match="Error during send"):
            await sA.barrier()
    finally:
        await asyncio.gather(*[s.close() for s in [sA, sB]])


@gen_test()
async def test_error_receive(tmpdir, loop_in_thread):
    dfs = []
    rows_per_df = 10
    n_input_partitions = 1
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)

    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = get_worker_for(part, workers, npartitions)
        partitions_for_worker[w].append(part)
    schema = pa.Schema.from_pandas(dfs[0])

    local_shuffle_pool = ShuffleTestPool()

    class ErrorReceive(Shuffle):
        async def receive(self, data: list[bytes]) -> None:
            raise RuntimeError("Error during receive")

    sA = local_shuffle_pool.new_shuffle(
        name="A",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
        Shuffle=ErrorReceive,
    )
    sB = local_shuffle_pool.new_shuffle(
        name="B",
        worker_for_mapping=worker_for_mapping,
        schema=schema,
        directory=tmpdir,
        loop=loop_in_thread,
    )
    try:
        await sB.add_partition(dfs[0])
        with pytest.raises(RuntimeError, match="Error during receive"):
            await sB.barrier()
    finally:
        await asyncio.gather(*[s.close() for s in [sA, sB]])
