import dask
import dask.dataframe as dd

from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_basic(c, s, a, b):
    df = dask.datasets.timeseries()
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    a, b = c.compute([df.x.size, out.x.size])
    a = await a
    b = await b
    assert a == b
