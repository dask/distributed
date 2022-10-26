from __future__ import annotations

import asyncio

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("dask.dataframe")
pytest.importorskip("pyarrow")

import dask
import dask.dataframe as dd
from dask.blockwise import Blockwise
from dask.utils_test import hlg_layer_topological

from distributed.shuffle._shuffle_extension import ShuffleWorkerExtension
from distributed.utils_test import gen_cluster


def test_basic(client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df["name"] = df["name"].astype("string[python]")
    shuffled = df.shuffle("id", shuffle="p2p")

    (opt,) = dask.optimize(shuffled)
    assert isinstance(hlg_layer_topological(opt.dask, 0), Blockwise)
    # blockwise -> barrier -> unpack -> drop_by_shallow_copy

    dd.utils.assert_eq(shuffled, df.shuffle("id", shuffle="tasks"), scheduler=client)
    # ^ NOTE: this works because `assert_eq` sorts the rows before comparing


@gen_cluster([("", 2)] * 4, client=True)
async def test_basic_state(c, s, *workers):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = df.shuffle("id", shuffle="p2p")

    exts: list[ShuffleWorkerExtension] = [w.extensions["shuffle"] for w in workers]
    for ext in exts:
        assert not ext.shuffles

    f = c.compute(shuffled)
    # TODO this is a bad/pointless test. the `f.done()` is necessary in case the shuffle is really fast.
    # To test state more thoroughly, we'd need a way to 'stop the world' at various stages. Like have the
    # scheduler pause everything when the barrier is reached. Not sure yet how to implement that.
    while not all(len(ext.shuffles) == 1 for ext in exts) and not f.done():
        await asyncio.sleep(0.1)

    await f
    assert all(not ext.shuffles for ext in exts)


def test_multiple_linear(client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df["name"] = df["name"].astype("string[python]")
    s1 = df.shuffle("id", shuffle="p2p")
    s1["x"] = s1["x"] + 1
    s2 = s1.shuffle("x", shuffle="p2p")

    # TODO eventually test for fusion between s1's unpacks, the `+1`, and s2's `transfer`s

    dd.utils.assert_eq(
        s2,
        df.assign(x=lambda df: df.x + 1).shuffle("x", shuffle="tasks"),
        scheduler=client,
    )
