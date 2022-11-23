from __future__ import annotations

import asyncio
import io
import os
import random
import shutil
from collections import defaultdict
from typing import Any

import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.distributed import Worker
from dask.utils import stringify

from distributed.core import PooledRPCCall
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._shuffle_extension import (
    Shuffle,
    ShuffleId,
    dump_batch,
    get_worker_for,
    list_of_buffers_to_table,
    load_arrow,
    split_by_partition,
    split_by_worker,
)
from distributed.utils_test import gen_cluster, gen_test

pa = pytest.importorskip("pyarrow")


def clean_worker(worker):
    """Assert that the worker has no shuffle state"""
    assert not worker.extensions["shuffle"].shuffles
    for dirpath, dirnames, filenames in os.walk(worker.local_directory):
        assert "shuffle" not in dirpath
        for fn in dirnames + filenames:
            assert "shuffle" not in fn


def clean_scheduler(scheduler):
    """Assert that the scheduler has no shuffle state"""
    assert not scheduler.extensions["shuffle"].worker_for
    assert not scheduler.extensions["shuffle"].heartbeats
    assert not scheduler.extensions["shuffle"].schemas
    assert not scheduler.extensions["shuffle"].columns
    assert not scheduler.extensions["shuffle"].output_workers
    assert not scheduler.extensions["shuffle"].completed_workers


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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
    while not a.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    shutil.rmtree(a.local_directory)

    while not b.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    shutil.rmtree(b.local_directory)
    with pytest.raises(FileNotFoundError) as e:
        out = await c.compute(out)

    assert os.path.split(a.local_directory)[-1] in str(e.value) or os.path.split(
        b.local_directory
    )[-1] in str(e.value)

    # clean_worker(a)  # TODO: clean up on exception
    # clean_worker(b)  # TODO: clean up on exception
    # clean_scheduler(s)


@pytest.mark.skip
@pytest.mark.slow
@gen_cluster(client=True)
async def test_crashed_worker(c, s, a, b):

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    while (
        len(
            [
                ts
                for ts in s.tasks.values()
                if "shuffle_transfer" in ts.key and ts.state == "memory"
            ]
        )
        < 3
    ):
        await asyncio.sleep(0.01)
    await b.close()

    with pytest.raises(Exception) as e:
        out = await c.compute(out)

    assert b.address in str(e.value)

    # clean_worker(a)  # TODO: clean up on exception
    # clean_worker(b)
    # clean_scheduler(s)


@gen_cluster(client=True)
async def test_heartbeat(c, s, a, b):
    await a.heartbeat()
    clean_scheduler(s)
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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


def test_processing_chain():
    """
    This is a serial version of the entire compute chain

    In practice this takes place on many different workers.
    Here we verify its accuracy in a single threaded situation.
    """
    workers = ["a", "b", "c"]
    npartitions = 5
    df = pd.DataFrame({"x": range(100), "y": range(100)})
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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


def test_split_by_worker():
    workers = ["a", "b", "c"]
    npartitions = 5
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["_partitions"] = df.x % npartitions
    worker_for = {i: random.choice(workers) for i in range(npartitions)}
    s = pd.Series(worker_for, name="_worker").astype("category")


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)

    await c.compute(out.tail(compute=False))

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)

    await c.compute(out.head(compute=False))

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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

        out = await c.compute(persisted)

        clean_worker(a)
        clean_worker(b)
        clean_worker(w)
        clean_scheduler(s)


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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


@pytest.mark.xfail(reason="Don't clean up forgotten shuffles")
@gen_cluster(client=True)
async def test_delete_some_results(c, s, a, b):
    # FIXME: This works but not reliably. It fails every ~25% of runs
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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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

    clean_worker(a)
    clean_worker(b)
    clean_scheduler(s)


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
    clean_worker(a)

    # clean_scheduler(s)  # TODO
    # clean_worker(b)  # TODO


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
