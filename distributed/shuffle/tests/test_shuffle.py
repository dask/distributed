import io
from collections import defaultdict

import pandas as pd
import pyarrow as pa

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
        end="2000-06-02",
        freq="100ms",
        dtypes={"x": int, "y": float, "a": int, "b": float},
    )
    df = dask.datasets.timeseries()
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    x, y = c.compute([df.x.size, out.x.size])
    x = await x
    y = await y
    assert x == y


@gen_cluster(client=True, timeout=1000000)
async def test_concurrent(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-06-02",
        freq="100ms",
        dtypes={"x": int, "y": float, "a": int, "b": float},
    )
    df = dask.datasets.timeseries()
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    y = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    x, y = c.compute([x.x.size, y.y.size])
    x = await x
    y = await y
    assert x == y


@gen_cluster(client=True)
async def test_heartbeat(c, s, a, b):
    await a.heartbeat()
    assert not s.extensions["shuffle"].shuffles
    df = dask.datasets.timeseries(
        dtypes={"x": float, "y": float},
    )
    df = dask.datasets.timeseries()
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    await out.persist()

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
