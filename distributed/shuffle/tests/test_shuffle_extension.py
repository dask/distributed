from __future__ import annotations

import string
from collections import Counter
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from distributed.utils_test import gen_cluster

from ..shuffle_extension import ShuffleId, ShuffleMetadata, ShuffleWorkerExtension

if TYPE_CHECKING:
    from distributed.scheduler import Scheduler
    from distributed.worker import Worker


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

    assignments = [metadata.worker_for(i) for i in range(metadata.npartitions)]
    counter = Counter(assignments)
    assert len(counter) == min(npartitions, n_workers)
    # All workers should be assigned the same number of partitions, or if
    # there's an odd number, one worker will be assigned one extra partition.
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
    assert worker.stream_handlers["shuffle_receive"] == ext.shuffle_receive
    assert worker.handlers["shuffle_init"] == ext.shuffle_init

    with pytest.raises(RuntimeError, match="from a different shuffle extension"):
        ShuffleWorkerExtension(worker)

    # Nothing was changed by double-installation
    assert worker.extensions["shuffle"] is ext
    assert worker.stream_handlers["shuffle_receive"] == ext.shuffle_receive
    assert worker.handlers["shuffle_init"] == ext.shuffle_init


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

    ext.shuffle_init(metadata)
    assert list(ext.shuffles) == [metadata.id]

    with pytest.raises(ValueError, match="already registered"):
        ext.shuffle_init(metadata)

    assert list(ext.shuffles) == [metadata.id]
