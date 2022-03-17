import dask
import dask.dataframe as dd

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
