import ast

import pytest

import dask
import dask.array as da
import dask.dataframe as dd

from distributed.diagnostics import SchedulerPlugin
from distributed.utils_test import gen_cluster

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


class ExampleAnnotationPlugin(SchedulerPlugin):
    def __init__(self, priority_fn=None, qux="", resource="", retries=0):
        self.priority_fn = priority_fn or (lambda k: 0)
        self.qux = qux
        self.resource = resource
        self.retries = retries

        self.priority_matches = 0
        self.resource_matches = 0
        self.retry_matches = 0
        self.qux_matches = 0

    def update_graph(self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs):
        annots = kwargs["annotations"]

        if "priority" in annots:
            self.priority_matches = sum(
                int(self.priority_fn(ast.literal_eval(k)) == p)
                for k, p in annots["priority"].items()
            )

        if "qux" in annots:
            self.qux_matches = sum(int(self.qux == v) for v in annots["qux"].values())

        if "custom_resource" in annots:
            self.resource_matches = sum(
                int(self.resource == v) for v in annots["custom_resource"].values()
            )

        if "retries" in annots:
            self.retry_matches = sum(
                int(self.retries == v) for v in annots["retries"].values()
            )


@gen_cluster(client=True)
async def test_array_annotations(c, s, a, b):
    def fn(k):
        return k[1] * 5 + k[2]

    qux = "baz"
    resource = "widget"

    plugin = ExampleAnnotationPlugin(priority_fn=fn, qux=qux, resource=resource)
    s.add_plugin(plugin)

    assert plugin in s.plugins

    with dask.annotate(priority=fn, qux=qux):
        A = da.ones((10, 10), chunks=(2, 2))

    with dask.annotate(custom_resource=resource):
        B = A + 1

    with dask.config.set(optimization__fuse__active=False):
        result = await c.compute(B)

    assert_array_equal(result, 2)

    # There are annotation matches per array chunk (i.e. task)
    assert plugin.qux_matches == A.npartitions
    assert plugin.priority_matches == A.npartitions
    assert plugin.resource_matches == B.npartitions


@gen_cluster(client=True)
async def test_dataframe_annotations(c, s, a, b):
    retries = 5
    plugin = ExampleAnnotationPlugin(retries=retries)
    s.add_plugin(plugin)

    assert plugin in s.plugins

    df = dd.from_pandas(
        pd.DataFrame(
            {"a": np.arange(10, dtype=int), "b": np.arange(10, 0, -1, dtype=float)}
        ),
        npartitions=5,
    )
    df = df.shuffle("a", shuffle="tasks", max_branch=2)
    acol = df["a"]
    bcol = df["b"]

    with dask.annotate(retries=retries):
        df = acol + bcol

    with dask.config.set(optimization__fuse__active=False):
        rdf = await c.compute(df)

    assert rdf.dtypes == np.float64
    assert (rdf == 10.0).all()

    # There is an annotation match per partition (i.e. task)
    assert plugin.retry_matches == df.npartitions
