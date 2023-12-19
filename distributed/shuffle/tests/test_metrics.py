from __future__ import annotations

import pytest

import dask.datasets

from distributed import Scheduler
from distributed.utils_test import gen_cluster

da = pytest.importorskip("dask.array")


def assert_metrics(s: Scheduler, *keys: tuple[str, ...]) -> None:
    for key in keys:
        assert key in s.cumulative_worker_metrics
        value = s.cumulative_worker_metrics[key]
        if key[-1] == "seconds":
            assert value >= 0
        else:  # count or bytes
            assert value > 0


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
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p", npartitions=20)
    await c.compute(shuffled)
    await a.heartbeat()
    await b.heartbeat()

    assert_metrics(
        s,
        ("execute", "shuffle-transfer", "p2p-shard-partition-cpu", "seconds"),
        ("execute", "shuffle-transfer", "p2p-shard-partition-noncpu", "seconds"),
        ("execute", "shuffle-transfer", "p2p-shards", "bytes"),
        ("execute", "shuffle-transfer", "p2p-shards", "count"),
        ("execute", "shuffle-transfer", "p2p-comms-limiter", "count"),
        ("execute", "shuffle_p2p", "p2p-disk-read", "bytes"),
        ("execute", "shuffle_p2p", "p2p-disk-read", "count"),
        ("execute", "shuffle_p2p", "p2p-get-output-cpu", "seconds"),
        ("execute", "shuffle_p2p", "p2p-get-output-noncpu", "seconds"),
    )
