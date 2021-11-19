from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from distributed.utils_test import gen_cluster

from ..common import ShuffleId, npartitions_for, worker_for
from ..shuffle_worker import ShuffleState, ShuffleWorkerExtension

if TYPE_CHECKING:
    from distributed import Client, Scheduler, Worker


@gen_cluster([("", 1)])
async def test_installation(s: Scheduler, worker: Worker):
    ext = worker.extensions["shuffle"]
    assert isinstance(ext, ShuffleWorkerExtension)
    assert worker.stream_handlers["shuffle_init"] == ext.shuffle_init
    assert worker.handlers["shuffle_receive"] == ext.shuffle_receive
    assert worker.handlers["shuffle_inputs_done"] == ext.shuffle_inputs_done

    assert ext.worker is worker
    assert not ext.shuffles
    assert not ext.output_data


@gen_cluster([("", 1)])
async def test_init(s: Scheduler, worker: Worker):
    ext: ShuffleWorkerExtension = worker.extensions["shuffle"]
    assert not ext.shuffles

    id = ShuffleId("foo")
    workers = [worker.address, "tcp://foo"]
    npartitions = 4

    ext.shuffle_init(id, workers, npartitions)
    assert ext.shuffles == {
        id: ShuffleState(workers, npartitions, 2, barrier_reached=False)
    }

    with pytest.raises(ValueError, match="already registered"):
        ext.shuffle_init(id, [], 0)

    # Unchanged after trying to re-register
    assert list(ext.shuffles) == [id]


@gen_cluster([("", 1)] * 4)
async def test_add_partition(s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    id = ShuffleId("foo")
    npartitions = 8
    addrs = list(exts)
    column = "partition"

    for ext in exts.values():
        ext.shuffle_init(id, addrs, npartitions)

    partition = pd.DataFrame(
        {
            "A": ["a", "b", "c", "d", "e", "f", "g", "h"],
            column: [0, 1, 2, 3, 4, 5, 6, 7],
        }
    )

    ext = exts[addrs[0]]
    await ext.add_partition(partition, id, npartitions, column)

    for i, data in partition.groupby(column):
        i = int(i)
        addr = worker_for(i, npartitions, addrs)
        ext = exts[addr]
        received = ext.output_data[id][i]
        assert len(received) == 1
        assert_frame_equal(data, received[0])

    with pytest.raises(ValueError, match="not registered"):
        await ext.add_partition(partition, ShuffleId("bar"), npartitions, column)

    # TODO (resilience stage) test failed sends


@gen_cluster([("", 1)] * 4, client=True)
async def test_barrier(c: Client, s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    id = ShuffleId("foo")
    npartitions = 3
    addrs = list(exts)
    column = "partition"

    for ext in exts.values():
        ext.shuffle_init(id, addrs, npartitions)

    partition = pd.DataFrame(
        {
            "A": ["a", "b", "c"],
            column: [0, 1, 2],
        }
    )
    first_ext = exts[addrs[0]]
    await first_ext.add_partition(partition, id, npartitions, column)

    await first_ext.barrier(id)

    # Check all workers have been informed of the barrier
    for addr, ext in exts.items():
        if npartitions_for(addr, npartitions, addrs):
            assert ext.shuffles[id].barrier_reached
        else:
            # No output partitions on this worker; shuffle already cleaned up
            assert not ext.shuffles
            assert not ext.output_data

    # Test check on self
    with pytest.raises(AssertionError, match="called multiple times"):
        await first_ext.barrier(id)

    first_ext.shuffles[id].barrier_reached = False

    # RPC to other workers fails
    with pytest.raises(AssertionError, match="`inputs_done` called again"):
        await first_ext.barrier(id)


@gen_cluster([("", 1)] * 4, client=True)
async def test_get_partition(c: Client, s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    id = ShuffleId("foo")
    npartitions = 8
    addrs = list(exts)
    column = "partition"

    for ext in exts.values():
        ext.shuffle_init(id, addrs, npartitions)

    p1 = pd.DataFrame(
        {
            "A": ["a", "b", "c", "d", "e", "f", "g", "h"],
            "partition": [0, 1, 2, 3, 4, 5, 6, 6],
        }
    )
    p2 = pd.DataFrame(
        {
            "A": ["a", "b", "c", "d", "e", "f", "g", "h"],
            "partition": [0, 1, 2, 3, 0, 0, 2, 3],
        }
    )

    first_ext = exts[addrs[0]]
    await asyncio.gather(
        first_ext.add_partition(p1, id, npartitions, column),
        first_ext.add_partition(p2, id, npartitions, column),
    )
    await first_ext.barrier(id)

    empty = pd.DataFrame({"A": [], column: []})

    with pytest.raises(AssertionError, match="was expected to go"):
        first_ext.get_output_partition(id, 7, empty)

    full = pd.concat([p1, p2])
    expected_groups = full.groupby("partition")
    for output_i in range(npartitions):
        addr = worker_for(output_i, npartitions, addrs)
        ext = exts[addr]
        shuffle = ext.shuffles[id]
        parts_left_before = shuffle.out_parts_left

        result = ext.get_output_partition(id, output_i, empty)

        try:
            expected = expected_groups.get_group(output_i)
        except KeyError:
            expected = empty
        assert_frame_equal(expected, result)
        assert shuffle.out_parts_left == parts_left_before - 1

    # Once all partitions are retrieved, shuffles are cleaned up
    for ext in exts.values():
        assert not ext.shuffles

    with pytest.raises(ValueError, match="not registered"):
        first_ext.get_output_partition(id, 0, empty)
