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
