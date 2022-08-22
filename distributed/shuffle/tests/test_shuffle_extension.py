from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
dd = pytest.importorskip("dask.dataframe")

from distributed.shuffle.shuffle_extension import (
    ShuffleWorkerExtension,
    split_by_partition,
    split_by_worker,
    worker_for,
)
from distributed.utils_test import gen_cluster


@gen_cluster([("", 1)])
async def test_installation(s, a):
    ext = a.extensions["shuffle"]
    assert isinstance(ext, ShuffleWorkerExtension)
    assert a.handlers["shuffle_receive"] == ext.shuffle_receive
    assert a.handlers["shuffle_inputs_done"] == ext.shuffle_inputs_done


@pytest.mark.skip
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


@pytest.mark.skip
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
