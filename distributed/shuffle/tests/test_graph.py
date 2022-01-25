from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import dask
import dask.dataframe as dd
from dask.blockwise import Blockwise
from dask.dataframe.shuffle import partitioning_index, rearrange_by_column_tasks
from dask.utils_test import hlg_layer_topological

from distributed.utils_test import gen_cluster

from ..shuffle import rearrange_by_column_p2p
from ..shuffle_extension import ShuffleWorkerExtension

if TYPE_CHECKING:
    from distributed import Client, Scheduler, Worker


def shuffle(
    df: dd.DataFrame, on: str, rearrange=rearrange_by_column_p2p
) -> dd.DataFrame:
    "Simple version of `DataFrame.shuffle`, so we don't need dask to know about 'p2p'"
    return (
        df.assign(
            partition=lambda df: df[on].map_partitions(
                partitioning_index, df.npartitions, transform_divisions=False
            )
        )
        .pipe(rearrange, "partition")
        .drop("partition", axis=1)
    )


def test_shuffle_helper(client: Client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffle_helper = shuffle(df, "id", rearrange=rearrange_by_column_tasks)
    dask_shuffle = df.shuffle("id", shuffle="tasks")
    dd.utils.assert_eq(shuffle_helper, dask_shuffle, scheduler=client)


def test_basic(client: Client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = shuffle(df, "id")

    (opt,) = dask.optimize(shuffled)
    assert isinstance(hlg_layer_topological(opt.dask, 1), Blockwise)
    # setup -> blockwise -> barrier -> unpack -> drop_by_shallow_copy
    assert len(opt.dask.layers) == 5

    dd.utils.assert_eq(shuffled, df.shuffle("id", shuffle="tasks"), scheduler=client)
    # ^ NOTE: this works because `assert_eq` sorts the rows before comparing


@gen_cluster([("", 2)] * 4, client=True)
async def test_basic_state(c: Client, s: Scheduler, *workers: Worker):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = shuffle(df, "id")

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


def test_multiple_linear(client: Client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    s1 = shuffle(df, "id")
    s1["x"] = s1["x"] + 1
    s2 = shuffle(s1, "x")

    # TODO eventually test for fusion between s1's unpacks, the `+1`, and s2's `transfer`s

    dd.utils.assert_eq(
        s2,
        df.assign(x=lambda df: df.x + 1).shuffle("x", shuffle="tasks"),
        scheduler=client,
    )
