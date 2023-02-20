from __future__ import annotations

from asyncio import iscoroutinefunction

import pytest

pd = pytest.importorskip("pandas")
dd = pytest.importorskip("dask.dataframe")

from distributed.shuffle._scheduler_extension import (
    ShuffleSchedulerExtension,
    get_worker_for_range_sharding,
)
from distributed.shuffle._worker_extension import (
    ShuffleWorkerExtension,
    rechunk_slicing,
    split_by_partition,
    split_by_worker,
)
from distributed.utils_test import gen_cluster


@gen_cluster([("", 1)])
async def test_installation_on_worker(s, a):
    ext = a.extensions["shuffle"]
    assert isinstance(ext, ShuffleWorkerExtension)
    assert a.handlers["shuffle_receive"] == ext.shuffle_receive
    assert a.handlers["shuffle_inputs_done"] == ext.shuffle_inputs_done
    assert a.stream_handlers["shuffle-fail"] == ext.shuffle_fail
    # To guarantee the correct order of operations, shuffle_fail must be synchronous.
    # See also https://github.com/dask/distributed/pull/7486#discussion_r1088857185.
    assert not iscoroutinefunction(ext.shuffle_fail)


@gen_cluster([("", 1)])
async def test_installation_on_scheduler(s, a):
    ext = s.extensions["shuffle"]
    assert isinstance(ext, ShuffleSchedulerExtension)
    assert s.handlers["shuffle_barrier"] == ext.barrier
    assert s.handlers["shuffle_get"] == ext.get


def test_split_by_worker():
    pytest.importorskip("pyarrow")

    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [0, 1, 2, 0, 1],
        }
    )

    workers = ["alice", "bob"]
    worker_for_mapping = {}
    npartitions = 3
    for part in range(npartitions):
        worker_for_mapping[part] = get_worker_for_range_sharding(
            part, workers, npartitions
        )
    worker_for = pd.Series(worker_for_mapping, name="_workers").astype("category")
    out = split_by_worker(df, "_partition", worker_for)
    assert set(out) == {"alice", "bob"}
    assert list(out["alice"].to_pandas().columns) == list(df.columns)

    assert sum(map(len, out.values())) == len(df)


def test_split_by_worker_empty():
    pytest.importorskip("pyarrow")

    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [0, 1, 2, 0, 1],
        }
    )
    worker_for = pd.Series({5: "chuck"}, name="_workers").astype("category")
    out = split_by_worker(df, "_partition", worker_for)
    assert out == {}


def test_split_by_worker_many_workers():
    pytest.importorskip("pyarrow")

    df = pd.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "_partition": [5, 7, 5, 0, 1],
        }
    )
    workers = ["a", "b", "c", "d", "e", "f", "g", "h"]
    npartitions = 10
    worker_for_mapping = {}
    for part in range(npartitions):
        worker_for_mapping[part] = get_worker_for_range_sharding(
            part, workers, npartitions
        )
    worker_for = pd.Series(worker_for_mapping, name="_workers").astype("category")
    out = split_by_worker(df, "_partition", worker_for)
    assert get_worker_for_range_sharding(5, workers, npartitions) in out
    assert get_worker_for_range_sharding(0, workers, npartitions) in out
    assert get_worker_for_range_sharding(7, workers, npartitions) in out
    assert get_worker_for_range_sharding(1, workers, npartitions) in out

    assert sum(map(len, out.values())) == len(df)


def test_split_by_partition():
    pa = pytest.importorskip("pyarrow")

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


def test_rechunk_slicing_1():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_1
    """
    old = ((10, 10, 10, 10, 10),)
    new = ((25, 5, 20),)
    result = rechunk_slicing(old, new)
    expected = {
        (0,): [((0,), (0,), (slice(0, 10, None),))],
        (1,): [((0,), (1,), (slice(0, 10, None),))],
        (2,): [((0,), (2,), (slice(0, 5, None),)), ((1,), (0,), (slice(5, 10, None),))],
        (3,): [((2,), (0,), (slice(0, 10, None),))],
        (4,): [((2,), (1,), (slice(0, 10, None),))],
    }
    assert result == expected


def test_rechunk_slicing_2():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_2
    """
    old = ((20, 20, 20, 20, 20),)
    new = ((58, 4, 20, 18),)
    result = rechunk_slicing(old, new)
    expected = {
        (0,): [((0,), (0,), (slice(0, 20, None),))],
        (1,): [((0,), (1,), (slice(0, 20, None),))],
        (2,): [
            ((0,), (2,), (slice(0, 18, None),)),
            ((1,), (0,), (slice(18, 20, None),)),
        ],
        (3,): [((1,), (1,), (slice(0, 2, None),)), ((2,), (0,), (slice(2, 20, None),))],
        (4,): [((2,), (1,), (slice(0, 2, None),)), ((3,), (0,), (slice(2, 20, None),))],
    }
    assert result == expected


def test_intersect_nan():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan
    """
    old_chunks = ((float("nan"), float("nan")), (8,))
    new_chunks = ((float("nan"), float("nan")), (4, 4))
    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, None, None), slice(0, 4, None))),
            ((0, 1), (0, 0), (slice(0, None, None), slice(4, 8, None))),
        ],
        (1, 0): [
            ((1, 0), (0, 0), (slice(0, None, None), slice(0, 4, None))),
            ((1, 1), (0, 0), (slice(0, None, None), slice(4, 8, None))),
        ],
    }
    assert result == expected


def test_intersect_nan_single():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan_single
    """
    old_chunks = ((float("nan"),), (10,))
    new_chunks = ((float("nan"),), (5, 5))

    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, None, None), slice(0, 5, None))),
            ((0, 1), (0, 0), (slice(0, None, None), slice(5, 10, None))),
        ],
    }
    assert result == expected


def test_intersect_nan_long():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan_long
    """
    old_chunks = (tuple([float("nan")] * 4), (10,))
    new_chunks = (tuple([float("nan")] * 4), (5, 5))
    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, None, None), slice(0, 5, None))),
            ((0, 1), (0, 0), (slice(0, None, None), slice(5, 10, None))),
        ],
        (1, 0): [
            ((1, 0), (0, 0), (slice(0, None, None), slice(0, 5, None))),
            ((1, 1), (0, 0), (slice(0, None, None), slice(5, 10, None))),
        ],
        (2, 0): [
            ((2, 0), (0, 0), (slice(0, None, None), slice(0, 5, None))),
            ((2, 1), (0, 0), (slice(0, None, None), slice(5, 10, None))),
        ],
        (3, 0): [
            ((3, 0), (0, 0), (slice(0, None, None), slice(0, 5, None))),
            ((3, 1), (0, 0), (slice(0, None, None), slice(5, 10, None))),
        ],
    }
    assert result == expected


def test_intersect_chunks_with_nonzero():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_chunks_with_nonzero
    """
    old = ((4, 4), (2,))
    new = ((8,), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((0, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            ((0, 0), (1, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((0, 1), (1, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }
    assert result == expected


def test_intersect_chunks_with_zero():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_chunks_with_zero
    """
    old = ((4, 4), (2,))
    new = ((4, 0, 0, 4), (1, 1))
    result = rechunk_slicing(old, new)

    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((0, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            # FIXME: We should probably filter these out to avoid sending empty shards
            ((1, 0), (0, 0), (slice(0, 0, None), slice(0, 1, None))),
            ((1, 1), (0, 0), (slice(0, 0, None), slice(1, 2, None))),
            ((2, 0), (0, 0), (slice(0, 0, None), slice(0, 1, None))),
            ((2, 1), (0, 0), (slice(0, 0, None), slice(1, 2, None))),
            ((3, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((3, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected

    old = ((4, 0, 0, 4), (1, 1))
    new = ((4, 4), (2,))
    result = rechunk_slicing(old, new)

    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (0, 1): [
            ((0, 0), (0, 1), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (3, 0): [
            ((1, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (3, 1): [
            ((1, 0), (0, 1), (slice(0, 4, None), slice(0, 1, None))),
        ],
    }

    assert result == expected

    old = ((4, 4), (2,))
    new = ((2, 0, 0, 2, 4), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            ((0, 0), (0, 0), (slice(0, 2, None), slice(0, 1, None))),
            ((0, 1), (0, 0), (slice(0, 2, None), slice(1, 2, None))),
            # FIXME: We should probably filter these out to avoid sending empty shards
            ((1, 0), (0, 0), (slice(2, 2, None), slice(0, 1, None))),
            ((1, 1), (0, 0), (slice(2, 2, None), slice(1, 2, None))),
            ((2, 0), (0, 0), (slice(2, 2, None), slice(0, 1, None))),
            ((2, 1), (0, 0), (slice(2, 2, None), slice(1, 2, None))),
            ((3, 0), (0, 0), (slice(2, 4, None), slice(0, 1, None))),
            ((3, 1), (0, 0), (slice(2, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            ((4, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((4, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected

    old = ((4, 4), (2,))
    new = ((0, 0, 4, 4), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            # FIXME: We should probably filter these out to avoid sending empty shards
            ((0, 0), (0, 0), (slice(0, 0, None), slice(0, 1, None))),
            ((0, 1), (0, 0), (slice(0, 0, None), slice(1, 2, None))),
            ((1, 0), (0, 0), (slice(0, 0, None), slice(0, 1, None))),
            ((1, 1), (0, 0), (slice(0, 0, None), slice(1, 2, None))),
            ((2, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((2, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            ((3, 0), (0, 0), (slice(0, 4, None), slice(0, 1, None))),
            ((3, 1), (0, 0), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected
