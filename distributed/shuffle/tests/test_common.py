import string
from collections import Counter

import pytest

from ..common import npartitions_for, partition_range, worker_for


@pytest.mark.parametrize("npartitions", [1, 2, 3, 5])
@pytest.mark.parametrize("n_workers", [1, 2, 3, 5])
def test_worker_for_distribution(npartitions: int, n_workers: int):
    "Test that `worker_for` distributes evenly"
    workers = list(string.ascii_lowercase[:n_workers])

    with pytest.raises(IndexError, match="Negative"):
        worker_for(-1, npartitions, workers)

    assignments = [worker_for(i, npartitions, workers) for i in range(npartitions)]

    # Test `partition_range`
    for w in workers:
        first, last = partition_range(w, npartitions, workers)
        assert all(
            [
                first <= p_i <= last if a == w else p_i < first or p_i > last
                for p_i, a in enumerate(assignments)
            ]
        )

    counter = Counter(assignments)
    assert len(counter) == min(npartitions, n_workers)

    # Test `npartitions_for`
    calculated_counter = {w: npartitions_for(w, npartitions, workers) for w in workers}
    assert counter == {
        w: count for w, count in calculated_counter.items() if count != 0
    }
    assert calculated_counter.keys() == set(workers)
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
        worker_for(npartitions, npartitions, workers)
