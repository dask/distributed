from __future__ import annotations

import pytest

import dask.datasets

from distributed import Scheduler
from distributed.utils_test import gen_cluster

da = pytest.importorskip("dask.array")
dd = pytest.importorskip("dask.dataframe")
from distributed.shuffle.tests.utils import UNPACK_PREFIX


def assert_metrics(s: Scheduler, *keys: tuple[str, ...]) -> None:
    metrics = dict(s.cumulative_worker_metrics)
    span = s.extensions["spans"].spans_search_by_name["default",][0]
    span_metrics = dict(span.cumulative_worker_metrics)

    for key in keys:
        value = metrics.pop(key)
        assert span_metrics.pop(key) == pytest.approx(value)
        if key[0] == "execute":
            key2 = ("p2p", "foreground", key[2].replace("p2p-", ""), key[3])
            assert metrics.pop(key2) == pytest.approx(value)
            assert span_metrics.pop(key2) == pytest.approx(value)

    # Check that the test doesn't omit any metrics
    assert [key for key in metrics if key[0] == "p2p"] == []
    assert [key for key in span_metrics if key[0] == "p2p"] == []


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk(c, s, a, b):
    x = da.random.random((10, 10), chunks=(-1, 1))
    x = x.rechunk((1, -1), method="p2p")
    await c.compute(x)
    await a.heartbeat()
    await b.heartbeat()

    assert_metrics(
        s,
        ("execute", "rechunk-transfer", "p2p-shard-partition-cpu", "seconds"),
        ("execute", "rechunk-transfer", "p2p-shard-partition-noncpu", "seconds"),
        ("execute", "rechunk-transfer", "p2p-shards", "bytes"),
        ("execute", "rechunk-transfer", "p2p-shards", "count"),
        ("execute", "rechunk-transfer", "p2p-comms-limiter", "count"),
        ("execute", "rechunk", "p2p-disk-read", "bytes"),
        ("execute", "rechunk", "p2p-disk-read", "count"),
        ("execute", "rechunk", "p2p-get-output-cpu", "seconds"),
        ("execute", "rechunk", "p2p-get-output-noncpu", "seconds"),
        ("p2p", "background-comms", "compress", "seconds"),
        ("p2p", "background-comms", "idle", "seconds"),
        ("p2p", "background-comms", "process", "bytes"),
        ("p2p", "background-comms", "process", "count"),
        ("p2p", "background-comms", "process", "seconds"),
        ("p2p", "background-comms", "send", "seconds"),
        ("p2p", "background-comms", "serialize", "seconds"),
        ("p2p", "background-disk", "disk-write", "bytes"),
        ("p2p", "background-disk", "disk-write", "count"),
        ("p2p", "background-disk", "disk-write", "seconds"),
        ("p2p", "background-disk", "idle", "seconds"),
        ("p2p", "background-disk", "process", "bytes"),
        ("p2p", "background-disk", "process", "count"),
        ("p2p", "background-disk", "process", "seconds"),
        ("p2p", "background-disk", "serialize", "seconds"),
    )


@gen_cluster(client=True)
async def test_dataframe(c, s, a, b):
    """Metrics are *almost* agnostic in dataframe shuffle vs. array rechunk.
    The only exception is the 'p2p-shards' metric, which is implemented separately.
    """
    dd = pytest.importorskip("dask.dataframe")

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        shuffled = df.shuffle("x", npartitions=20)
    await c.compute(shuffled)
    await a.heartbeat()
    await b.heartbeat()

    assert_metrics(
        s,
        ("execute", "shuffle-transfer", "p2p-shard-partition-cpu", "seconds"),
        ("execute", "shuffle-transfer", "p2p-shard-partition-noncpu", "seconds"),
        ("execute", "shuffle-transfer", "p2p-shards-overhead", "bytes"),
        ("execute", "shuffle-transfer", "p2p-shards-buffers", "bytes"),
        ("execute", "shuffle-transfer", "p2p-shards-buffers", "count"),
        ("execute", "shuffle-transfer", "p2p-shards", "count"),
        ("execute", "shuffle-transfer", "p2p-comms-limiter", "count"),
        ("execute", UNPACK_PREFIX, "p2p-disk-read", "bytes"),
        ("execute", UNPACK_PREFIX, "p2p-disk-read", "count"),
        ("execute", UNPACK_PREFIX, "p2p-get-output-cpu", "seconds"),
        ("execute", UNPACK_PREFIX, "p2p-get-output-noncpu", "seconds"),
        ("p2p", "background-comms", "compress", "seconds"),
        ("p2p", "background-comms", "idle", "seconds"),
        ("p2p", "background-comms", "process", "bytes"),
        ("p2p", "background-comms", "process", "count"),
        ("p2p", "background-comms", "process", "seconds"),
        ("p2p", "background-comms", "send", "seconds"),
        ("p2p", "background-comms", "serialize", "seconds"),
        ("p2p", "background-disk", "disk-write", "bytes"),
        ("p2p", "background-disk", "disk-write", "count"),
        ("p2p", "background-disk", "disk-write", "seconds"),
        ("p2p", "background-disk", "idle", "seconds"),
        ("p2p", "background-disk", "process", "bytes"),
        ("p2p", "background-disk", "process", "count"),
        ("p2p", "background-disk", "process", "seconds"),
    )
