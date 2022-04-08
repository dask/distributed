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

from distributed.shuffle.shuffle_extension import (
    dump_batch,
    list_of_buffers_to_table,
    load_arrow,
    split_by_partition,
    split_by_worker,
)
from distributed.utils_test import gen_cluster


@gen_cluster(client=True, timeout=1000000)
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


@gen_cluster(client=True, timeout=1000000)
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

    assert a.local_directory in str(e.value) or b.local_directory in str(e.value)


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

    assert a.address in str(e.value) or b.address in str(e.value)


@gen_cluster(client=True)
async def test_heartbeat(c, s, a, b):
    await a.heartbeat()
    assert not s.extensions["shuffle"].heartbeats
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

    [s] = s.extensions["shuffle"].heartbeats.values()
    await out


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

    for worker, partitions in splits_by_worker.items():
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

    assert not a.extensions["shuffle"].shuffles  # cleaned up metadata?
    assert not b.extensions["shuffle"].shuffles
    assert not s.extensions["shuffle"].worker_for  # cleaned up metadata?

    assert list(os.walk(a.local_directory)) == a_files  # cleaned up files?
    assert list(os.walk(b.local_directory)) == b_files


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
        end="2000-01-30",
        dtypes={"x": float, "y": float},
        freq="1 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p").tail(compute=False)
    persisted = await shuffled.persist()  # Only ask for one key

    assert len(s.tasks) < df.npartitions * 2


@gen_cluster(client=True)
async def test_repeat(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    await c.compute(out.head(compute=False))

    assert not a.extensions["shuffle"].shuffles
    assert not s.extensions["shuffle"].worker_for

    await c.compute(out.tail(compute=False))

    assert not s.extensions["shuffle"].worker_for
    assert not a.extensions["shuffle"].shuffles

    await c.compute(out.head(compute=False))
