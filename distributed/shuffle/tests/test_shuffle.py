import asyncio
import io
import os
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pytest

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
    original_stat = os.stat(a.local_directory).st_mode
    while not a.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    os.chmod(a.local_directory, 0o444)
    while not b.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.01)
    os.chmod(b.local_directory, 0o444)
    try:
        with pytest.raises(PermissionError) as e:
            out = await c.compute(out)

        assert a.local_directory in str(e.value) or b.local_directory in str(e.value)
    finally:
        os.chmod(a.local_directory, original_stat)
        os.chmod(b.local_directory, original_stat)


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
    assert not s.extensions["shuffle"].shuffles
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    while not s.extensions["shuffle"].shuffles:
        await asyncio.sleep(0.001)
        await a.heartbeat()

    [s] = s.extensions["shuffle"].shuffles.values()


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

    data = split_by_worker(df, "_partitions", npartitions, workers)
    assert set(data) == set(workers)

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
