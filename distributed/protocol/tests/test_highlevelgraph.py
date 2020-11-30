import pytest

import dask.array as da
import dask.dataframe as dd

from distributed.utils_test import gen_cluster

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")


@gen_cluster(client=True)
async def test_combo_of_layer_types(c, s, a, b):
    """Check pack/unpack of a HLG that has everything!"""

    def add(x, y, z, extra_arg):
        return x + y + z + extra_arg

    y = c.submit(lambda x: x, 2)
    z = c.submit(lambda x: x, 3)
    x = da.blockwise(
        add,
        "x",
        da.zeros((3,), chunks=(1,)),
        "x",
        da.ones((3,), chunks=(1,)),
        "x",
        y,
        None,
        concatenate=False,
        dtype=int,
        extra_arg=z,
    )

    df = dd.from_pandas(pd.DataFrame({"a": np.arange(3)}), npartitions=3)
    df = df.shuffle("a", shuffle="tasks")
    df = df["a"].to_dask_array()

    res = x.sum() + df.sum()
    res = await c.compute(res, optimize_graph=False)
    assert res == 21


@gen_cluster(client=True)
async def test_blockwise(c, s, a, b):
    """Check pack/unpack of blockwise layer"""

    def add(x, y, z, extra_arg):
        return x + y + z + extra_arg

    y = c.submit(lambda x: x, 10)
    z = c.submit(lambda x: x, 3)
    x = da.blockwise(
        add,
        "x",
        da.zeros((3,), chunks=(1,)),
        "x",
        da.ones((3,), chunks=(1,)),
        "x",
        y,
        None,
        concatenate=False,
        dtype=int,
        extra_arg=z,
    )
    res = await c.compute(x.sum(), optimize_graph=False)
    assert res == 42


@gen_cluster(client=True)
async def test_shuffle(c, s, a, b):
    """Check pack/unpack of a shuffled dataframe"""

    df = dd.from_pandas(
        pd.DataFrame(
            {"a": np.arange(10, dtype=int), "b": np.arange(10, 0, -1, dtype=float)}
        ),
        npartitions=5,
    )
    df = df.shuffle("a", shuffle="tasks", max_branch=2)
    df = df["a"] + df["b"]
    res = await c.compute(df, optimize_graph=False)
    assert res.dtypes == np.float64
    assert (res == 10.0).all()
