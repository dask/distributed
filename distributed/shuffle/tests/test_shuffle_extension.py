from __future__ import annotations

import asyncio
import string
from collections import Counter
from typing import TYPE_CHECKING

import pytest

pd = pytest.importorskip("pandas")
dd = pytest.importorskip("dask.dataframe")

from distributed.shuffle.shuffle_extension import (
    NewShuffleMetadata,
    ShuffleId,
    ShuffleMetadata,
    ShuffleWorkerExtension,
    split_by_partition,
    split_by_worker,
    worker_for,
)
from distributed.utils_test import gen_cluster

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
    assert sorted(metadata.workers) == sorted(w.address for w in workers)

    # Check shuffle was created on all workers
    for ext in exts:
        assert len(ext.shuffles) == 1
        shuffle = ext.shuffles[new_metadata.id]
        assert shuffle.metadata.workers == metadata.workers

    # TODO (resilience stage) what happens if some workers already have
    # the ID registered, but others don't?

    with pytest.raises(ValueError, match="already registered"):
        await exts[0]._create_shuffle(new_metadata)


def test_split_by_worker():
    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [0, 1, 2, 0, 1],
        }
    )
    workers = ["alice", "bob"]
    npartitions = 3

    out = split_by_worker(df, "_partition", npartitions, workers)
    assert set(out) == {"alice", "bob"}
    assert out["alice"].column_names == list(df.columns)

    assert sum(map(len, out.values())) == len(df)


def test_split_by_worker_many_workers():
    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [5, 7, 5, 0, 1],
        }
    )
    workers = ["a", "b", "c", "d", "e", "f", "g", "h"]
    npartitions = 10

    out = split_by_worker(df, "_partition", npartitions, workers)
    assert worker_for(5, workers, npartitions) in out
    assert worker_for(0, workers, npartitions) in out
    assert worker_for(7, workers, npartitions) in out
    assert worker_for(1, workers, npartitions) in out

    assert sum(map(len, out.values())) == len(df)


def test_split_by_partition():
    import pyarrow as pa

    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [3, 1, 2, 3, 1],
        }
    )
    t = pa.Table.from_pandas(df)

    out = split_by_partition(t, "_partition")
    assert set(out) == {1, 2, 3}
    assert out[1].column_names == list(df.columns)
    assert sum(map(len, out.values())) == len(df)
