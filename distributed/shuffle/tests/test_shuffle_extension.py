from __future__ import annotations

import asyncio
import string
from collections import Counter
from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from distributed.utils_test import gen_cluster

from ..shuffle_extension import (
    NewShuffleMetadata,
    ShuffleId,
    ShuffleMetadata,
    ShuffleWorkerExtension,
)

if TYPE_CHECKING:
    from distributed import Client, Future, Scheduler, Worker


@pytest.mark.parametrize("npartitions", [1, 2, 3, 5])
@pytest.mark.parametrize("n_workers", [1, 2, 3, 5])
def test_worker_for_distribution(npartitions: int, n_workers: int):
    "Test that `worker_for` distributes evenly"
    metadata = ShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": []}),
        "A",
        npartitions,
        list(string.ascii_lowercase[:n_workers]),
    )

    with pytest.raises(AssertionError, match="Negative"):
        metadata.worker_for(-1)

    assignments = [metadata.worker_for(i) for i in range(metadata.npartitions)]

    # Test internal `_partition_range` method
    for w in metadata.workers:
        first, last = metadata._partition_range(w)
        assert all(
            [
                first <= p_i <= last if a == w else p_i < first or p_i > last
                for p_i, a in enumerate(assignments)
            ]
        )

    counter = Counter(assignments)
    assert len(counter) == min(npartitions, n_workers)

    # Test `npartitions_for`
    calculated_counter = {w: metadata.npartitions_for(w) for w in metadata.workers}
    assert counter == {
        w: count for w, count in calculated_counter.items() if count != 0
    }
    assert calculated_counter.keys() == set(metadata.workers)
    # ^ this also checks that workers receiving 0 output partitions were calculated properly

    # Test the distribution of worker assignments.
    # All workers should be assigned the same number of partitions, or if
    # there's an odd number, some workers will be assigned only one extra partition.
    counts = set(counter.values())
    assert len(counts) <= 2
    if len(counts) == 2:
        lo, hi = sorted(counts)
        assert lo == hi - 1

    with pytest.raises(IndexError, match="does not exist"):
        metadata.worker_for(npartitions)


@gen_cluster([("", 1)])
async def test_installation(s: Scheduler, worker: Worker):
    ext = worker.extensions["shuffle"]
    assert isinstance(ext, ShuffleWorkerExtension)
    assert worker.handlers["shuffle_receive"] == ext.shuffle_receive
    assert worker.handlers["shuffle_init"] == ext.shuffle_init
    assert worker.handlers["shuffle_inputs_done"] == ext.shuffle_inputs_done


@gen_cluster([("", 1)])
async def test_init(s: Scheduler, worker: Worker):
    ext: ShuffleWorkerExtension = worker.extensions["shuffle"]
    assert not ext.shuffles
    metadata = ShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": []}),
        "A",
        5,
        [worker.address],
    )

    ext.shuffle_init(None, metadata)
    assert list(ext.shuffles) == [metadata.id]

    with pytest.raises(ValueError, match="already registered"):
        ext.shuffle_init(None, metadata)

    assert list(ext.shuffles) == [metadata.id]


async def add_dummy_unpack_keys(
    new_metadata: NewShuffleMetadata, client: Client
) -> dict[str, Future]:
    """
    Add dummy keys to the scheduler, so setting worker restrictions during `barrier` succeeds.

    Note: you must hang onto the Futures returned by this function, so they don't get released prematurely.
    """
    # NOTE: `scatter` is just used as an easy way to create keys on the scheduler that won't actually
    # be scheduled. It would be reasonable if this stops working in the future, if some validation is
    # added preventing worker restrictions on scattered data (since it makes no sense).
    fs = await client.scatter(
        {
            str(("shuffle-unpack-" + new_metadata.id, i)): None
            for i in range(new_metadata.npartitions)
        }
    )  # type: ignore
    await asyncio.gather(*fs.values())
    return fs


@gen_cluster([("", 1)] * 4)
async def test_create(s: Scheduler, *workers: Worker):
    exts: list[ShuffleWorkerExtension] = [w.extensions["shuffle"] for w in workers]

    new_metadata = NewShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": []}),
        "A",
        5,
    )

    metadata = await exts[0]._create_shuffle(new_metadata)

    # Check shuffle was created on all workers
    for ext in exts:
        assert len(ext.shuffles) == 1
        shuffle = ext.shuffles[new_metadata.id]
        assert sorted(shuffle.metadata.workers) == sorted(w.address for w in workers)

    # TODO (resilience stage) what happens if some workers already have
    # the ID registered, but others don't?

    with pytest.raises(ValueError, match="already registered"):
        await exts[0]._create_shuffle(new_metadata)


@gen_cluster([("", 1)] * 4)
async def test_add_partition(s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    new_metadata = NewShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": [], "partition": []}),
        "partition",
        8,
    )

    ext = next(iter(exts.values()))
    metadata = await ext._create_shuffle(new_metadata)
    partition = pd.DataFrame(
        {
            "A": ["a", "b", "c", "d", "e", "f", "g", "h"],
            "partition": [0, 1, 2, 3, 4, 5, 6, 7],
        }
    )
    await ext._add_partition(partition, new_metadata.id)

    with pytest.raises(ValueError, match="not registered"):
        await ext._add_partition(partition, ShuffleId("bar"))

    for i, data in partition.groupby(new_metadata.column):
        addr = metadata.worker_for(int(i))
        ext = exts[addr]
        received = ext.shuffles[metadata.id].output_partitions[int(i)]
        assert len(received) == 1
        assert_frame_equal(data, received[0])

    # TODO (resilience stage) test failed sends


@gen_cluster([("", 1)] * 4, client=True)
async def test_barrier(c: Client, s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    new_metadata = NewShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": [], "partition": []}),
        "partition",
        4,
    )
    fs = await add_dummy_unpack_keys(new_metadata, c)

    ext = next(iter(exts.values()))
    metadata = await ext._create_shuffle(new_metadata)
    partition = pd.DataFrame(
        {
            "A": ["a", "b", "c"],
            "partition": [0, 1, 2],
        }
    )
    await ext._add_partition(partition, metadata.id)

    await ext._barrier(metadata.id)

    # Check scheduler restrictions were set for unpack tasks
    for key, i in zip(fs, range(metadata.npartitions)):
        assert s.tasks[key].worker_restrictions == {metadata.worker_for(i)}

    # Check all workers have been informed of the barrier
    for addr, ext in exts.items():
        if metadata.npartitions_for(addr):
            shuffle = ext.shuffles[metadata.id]
            assert shuffle.transferred
            assert not shuffle.done()
        else:
            # No output partitions on this worker; shuffle already cleaned up
            assert not ext.shuffles


@gen_cluster([("", 1)] * 4, client=True)
async def test_get_partition(c: Client, s: Scheduler, *workers: Worker):
    exts: dict[str, ShuffleWorkerExtension] = {
        w.address: w.extensions["shuffle"] for w in workers
    }

    new_metadata = NewShuffleMetadata(
        ShuffleId("foo"),
        pd.DataFrame({"A": [], "partition": []}),
        "partition",
        8,
    )
    _ = await add_dummy_unpack_keys(new_metadata, c)

    ext = next(iter(exts.values()))
    metadata = await ext._create_shuffle(new_metadata)
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
    await asyncio.gather(
        ext._add_partition(p1, metadata.id), ext._add_partition(p2, metadata.id)
    )
    await ext._barrier(metadata.id)

    with pytest.raises(AssertionError, match="belongs on"):
        ext.get_output_partition(metadata.id, 7)

    full = pd.concat([p1, p2])
    expected_groups = full.groupby("partition")
    for output_i in range(metadata.npartitions):
        addr = metadata.worker_for(output_i)
        ext = exts[addr]
        result = ext.get_output_partition(metadata.id, output_i)
        try:
            expected = expected_groups.get_group(output_i)
        except KeyError:
            expected = metadata.empty
        assert_frame_equal(expected, result)

    # Once all partitions are retrieved, shuffles are cleaned up
    for ext in exts.values():
        assert not ext.shuffles

    with pytest.raises(ValueError, match="not registered"):
        ext.get_output_partition(metadata.id, 0)
