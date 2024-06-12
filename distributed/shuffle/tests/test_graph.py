from __future__ import annotations

import asyncio

import pytest

pd = pytest.importorskip("pandas")
dd = pytest.importorskip("dask.dataframe")
pytest.importorskip("pyarrow")

import dask
from dask.blockwise import Blockwise
from dask.utils_test import hlg_layer_topological

from distributed.utils_test import gen_cluster


@pytest.mark.skipif(condition=dd._dask_expr_enabled(), reason="no HLG")
def test_basic(client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df["name"] = df["name"].astype("string[python]")
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        p2p_shuffled = df.shuffle("id")

    (opt,) = dask.optimize(p2p_shuffled)
    assert isinstance(hlg_layer_topological(opt.dask, 0), Blockwise)
    # blockwise -> barrier -> unpack -> drop_by_shallow_copy


@pytest.mark.parametrize("dtype", ["csingle", "cdouble", "clongdouble"])
def test_raise_on_complex_numbers(dtype):
    df = dd.from_pandas(
        pd.DataFrame({"x": pd.array(range(10), dtype=dtype)}), npartitions=5
    )
    with pytest.raises(
        TypeError, match=f"p2p does not support data of type '{df.x.dtype}'"
    ), dask.config.set({"dataframe.shuffle.method": "p2p"}):
        df.shuffle("x")


@pytest.mark.xfail(
    reason="Ordinary string columns are also objects and we can't distinguish them from custom objects from meta alone."
)
def test_raise_on_custom_objects(c, s, a, b):
    class Stub:
        def __init__(self, value: int) -> None:
            self.value = value

    df = dd.from_pandas(
        pd.DataFrame({"x": pd.array([Stub(i) for i in range(10)], dtype="object")}),
        npartitions=5,
    )
    with pytest.raises(
        TypeError, match="p2p does not support custom objects"
    ), dask.config.set({"dataframe.shuffle.method": "p2p"}):
        df.shuffle("x")


def test_raise_on_sparse_data():
    df = dd.from_pandas(
        pd.DataFrame({"x": pd.array(range(10), dtype="Sparse[float64]")}), npartitions=5
    )
    with pytest.raises(
        TypeError, match="p2p does not support sparse data"
    ), dask.config.set({"dataframe.shuffle.method": "p2p"}):
        df.shuffle("x")


def test_raise_on_non_string_column_name():
    df = dd.from_pandas(pd.DataFrame({"a": range(10), 1: range(10)}), npartitions=5)
    with pytest.raises(
        TypeError, match="p2p requires all column names to be str"
    ), dask.config.set({"dataframe.shuffle.method": "p2p"}):
        df.shuffle("a")


def test_does_not_raise_on_stringified_numeric_column_name():
    df = dd.from_pandas(pd.DataFrame({"a": range(10), "1": range(10)}), npartitions=5)
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        df.shuffle("a")


@gen_cluster([("", 2)] * 4, client=True)
async def test_basic_state(c, s, *workers):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df["name"] = df["name"].astype("string[python]")
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        shuffled = df.shuffle("id")

    exts = [w.extensions["shuffle"] for w in workers]
    for ext in exts:
        assert not ext.shuffle_runs._active_runs

    f = c.compute(shuffled)
    # TODO this is a bad/pointless test. the `f.done()` is necessary in case the shuffle is really fast.
    # To test state more thoroughly, we'd need a way to 'stop the world' at various stages. Like have the
    # scheduler pause everything when the barrier is reached. Not sure yet how to implement that.
    while (
        not all(len(ext.shuffle_runs._active_runs) == 1 for ext in exts)
        and not f.done()
    ):
        await asyncio.sleep(0.1)

    await f
    assert all(not ext.shuffle_runs._active_runs for ext in exts)


def test_multiple_linear(client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df["name"] = df["name"].astype("string[python]")
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        s1 = df.shuffle("id")
    s1["x"] = s1["x"] + 1
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        s2 = s1.shuffle("x")

    with dask.config.set({"dataframe.shuffle.method": "tasks"}):
        expected = df.assign(x=lambda df: df.x + 1).shuffle("x")
    # TODO eventually test for fusion between s1's unpacks, the `+1`, and s2's `transfer`s

    dd.utils.assert_eq(
        s2,
        expected,
        scheduler=client,
    )
