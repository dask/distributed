from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pandas as pd

import dask
import dask.dataframe as dd
from dask.blockwise import Blockwise
from dask.dataframe.shuffle import partitioning_index, rearrange_by_column_tasks
from dask.utils_test import hlg_layer_topological

from distributed.utils_test import gen_cluster

from .. import ShuffleWorkerExtension, rearrange_by_column_p2p
from ..shuffle_scheduler import TASK_PREFIX, ShuffleSchedulerPlugin, parse_key

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
    dd.utils.assert_eq(shuffle_helper, dask_shuffle)


def test_graph():
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = shuffle(df, "id")
    shuffled.dask.validate()

    # Check graph optimizes correctly
    (opt,) = dask.optimize(shuffled)
    opt.dask.validate()

    assert len(opt.dask.layers) == 3
    # create+transfer -> barrier -> unpack+drop_by_shallow_copy
    transfer_layer = hlg_layer_topological(opt.dask, 0)
    assert isinstance(transfer_layer, Blockwise)
    shuffle_id = transfer_layer.indices[0][0]
    # ^ don't ask why it's in position 0; some oddity of blockwise fusion.
    # Don't be surprised if this breaks unexpectedly.
    assert isinstance(hlg_layer_topological(opt.dask, -1), Blockwise)

    # Check that task names contain the shuffle ID itself.
    # This is how the scheduler plugin infers the shuffle ID.
    for key in opt.dask.to_dict():
        key = str(key)
        if "transfer" in key or "barrier" in key:
            try:
                parts = parse_key(key)
                assert parts
                prefix, group, id = parts
            except Exception:
                print(key)
                raise
            assert prefix == TASK_PREFIX
            assert id == shuffle_id


def test_basic(client: Client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = shuffle(df, "id")

    dd.utils.assert_eq(shuffled, df.shuffle("id", shuffle="tasks"))
    # ^ NOTE: this works because `assert_eq` sorts the rows before comparing


@gen_cluster([("", 2)] * 4, client=True)
async def test_basic_state(c: Client, s: Scheduler, *workers: Worker):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    shuffled = shuffle(df, "id")

    exts: list[ShuffleWorkerExtension] = [w.extensions["shuffle"] for w in workers]
    for ext in exts:
        assert not ext.shuffles
        assert not ext.output_data

    plugin = s.plugins[ShuffleSchedulerPlugin.name]
    assert isinstance(plugin, ShuffleSchedulerPlugin)
    assert not plugin.shuffles
    assert not plugin.output_keys

    f = c.compute(shuffled)
    # TODO this is a bad/pointless test. the `f.done()` is necessary in case the shuffle is really fast.
    # To test state more thoroughly, we'd need a way to 'stop the world' at various stages. Like have the
    # scheduler pause everything when the barrier is reached. Not sure yet how to implement that.
    while (
        not all(len(ext.shuffles) == 1 for ext in exts)
        and len(plugin.shuffles) == 1
        and not f.done()
    ):
        await asyncio.sleep(0.1)

    await f
    assert all(not ext.shuffles for ext in exts)
    assert not plugin.shuffles
    assert not plugin.output_keys
    assert not any(ts.worker_restrictions for ts in s.tasks.values())


def test_multiple_linear(client: Client):
    df = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    s1 = shuffle(df, "id")
    s1["x"] = s1["x"] + 1
    s2 = shuffle(s1, "x")

    (opt,) = dask.optimize(s2)
    assert len(opt.dask.layers) == 5
    # create+transfer -> barrier -> unpack+transfer -> barrier -> unpack

    dd.utils.assert_eq(
        s2, df.assign(x=lambda df: df.x + 1).shuffle("x", shuffle="tasks")
    )


def test_multiple_concurrent(client: Client):
    df1 = dd.demo.make_timeseries(freq="15D", partition_freq="30D")
    df2 = dd.demo.make_timeseries(
        start="2001-01-01", end="2001-12-31", freq="15D", partition_freq="30D"
    )
    s1 = shuffle(df1, "id")
    s2 = shuffle(df2, "id")
    assert s1._name != s2._name

    merged = dd.map_partitions(
        lambda p1, p2: pd.merge(p1, p2, on="id"), s1, s2, align_dataframes=False
    )

    # TODO this fails because blockwise merges the two `unpack` layers together like
    #      X
    #    /  \      -->   X
    #   X    X

    # So the HLG structure is
    #
    #      Actual:                  Expected:
    #
    #                                   merge
    #                                 /       \
    #    unpack+merge              unpack   unpack
    #      /      \                  |         |
    #  barrier   barrier          barrier   barrier
    #    |          |                |         |
    #  xfer        xfer             xfer      xfer

    # And in the scheduler plugin's barrier, we check that the dependents
    # of a `barrier` depend only on that one barrier.
    # But here, they depend on _both_ barriers.
    # This check is probably overly restrictive, because with blockwise fusion
    # after the unpack, it's in fact quite likely that other dependencies would
    # appear.
    #
    # This is probably solveable, but tricky.
    # We'd have to confirm that:
    # 1. The other dependencies aren't barriers
    # 2. If the other dependencies are barriers:
    #    - that shuffle has the same number of partitions _and_ the same set of workers
    #
    # Otherwise, these tasks just cannot be fused, because their data is going to
    # different places. Yet another thing we might need to deal with at the optimization level.

    # _Or_, could the scheduler plugin look for this situation in advance before starting the
    # shuffle, and if it sees that multiple shuffles feed into the output tasks of the one
    # it's starting, ensure that their worker assignments all line up? (This would mean ensuring
    # they all have the same list of workers; in order to be fused they must already have the
    # same number output partitions.)

    dd.utils.assert_eq(
        merged, dd.merge(df1, df2, on="id", shuffle="tasks"), check_index=False
    )
