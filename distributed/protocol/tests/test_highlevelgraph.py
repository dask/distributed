import ast

import dask
from dask.highlevelgraph import BasicLayer
from dask.blockwise import Blockwise
from dask.core import flatten

import dask.array as da
import dask.dataframe as dd

from distributed.utils_test import gen_cluster
from distributed.diagnostics import SchedulerPlugin

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from numpy.testing import assert_array_equal


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


@gen_cluster(client=True)
async def test_annotations(c, s, a, b):
    def fn(k):
        return k[1] * 5 + k[2]

    class TestPlugin(SchedulerPlugin):
        def __init__(self):
            self.correct_priorities = 0
            self.correct_resources = 0
            self.correct_retries = 0

        def update_graph(
            self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs
        ):
            for k, a in kwargs["annotations"].items():
                if "priority" in a:
                    p = fn(ast.literal_eval(k))
                    self.correct_priorities += int(p == a["priority"])

                if "resource" in a:
                    self.correct_resources += int("widget" == a["resource"])

                if "retries" in a:
                    self.correct_retries += int(a["retries"] == 5)

    plugin = TestPlugin()
    s.add_plugin(plugin)

    assert plugin in s.plugins

    with dask.annotate(priority=fn):
        A = da.ones((10, 10), chunks=(2, 2))

    with dask.annotate(resource="widget"):
        B = A + 1

    # TODO: replace with client.compute when it correctly supports HLG transmission
    ret = c.get(B.__dask_graph__(), list(flatten(B.__dask_keys__())), sync=False)

    for r in await c.gather(ret):
        assert_array_equal(r, 2)

    assert isinstance(B.__dask_graph__().layers[A.name], BasicLayer)
    assert isinstance(B.__dask_graph__().layers[B.name], Blockwise)

    assert plugin.correct_priorities == 25
    assert plugin.correct_resources == 25

    df = dd.from_pandas(
        pd.DataFrame(
            {"a": np.arange(10, dtype=int), "b": np.arange(10, 0, -1, dtype=float)}
        ),
        npartitions=5,
    )
    df = df.shuffle("a", shuffle="tasks", max_branch=2)
    acol = df["a"]
    bcol = df["b"]

    with dask.annotate(retries=5):
        df = acol + bcol

    res = c.get(
        df.__dask_graph__(),
        list(flatten(df.__dask_keys__())),
        optimize_graph=False,
        sync=False,
    )

    res = await c.gather(res)
    assert res[0].dtypes == np.float64
    assert (res[0] == 10.0).all()

    assert plugin.correct_retries == 5
