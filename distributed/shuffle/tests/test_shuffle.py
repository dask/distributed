from __future__ import annotations

import asyncio
import io
import os
import random
import shutil
from collections import defaultdict

import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow")

import dask
import dask.dataframe as dd
from dask.distributed import Worker
from dask.utils import stringify

from distributed.shuffle._shuffle_extension import (
    dump_batch,
    list_of_buffers_to_table,
    load_arrow,
    split_by_partition,
    split_by_worker,
)
from distributed.utils_test import gen_cluster


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
async def test_basic(c, s, a, b):
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


@pytest.mark.xfail
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
