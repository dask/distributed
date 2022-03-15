import dask
import dask.dataframe as dd

from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_basic(c, s, a, b):
    df = dask.datasets.timeseries()
    # df = dask.datasets.timeseries(
    #     start="2000-01-01",
    #     end="2000-06-02",
    #     freq="100ms",
    #     dtypes={"x": int, "y": float, "a": int, "b": float},
    # )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    a, b = c.compute([df.x.size, out.x.size])
    a = await a
    b = await b
    assert a == b
