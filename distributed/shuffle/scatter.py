from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from dask.base import tokenize
from dask.dataframe import DataFrame
from dask.dataframe.core import _concat
from dask.dataframe.shuffle import shuffle_group
from dask.highlevelgraph import HighLevelGraph
from dask.sizeof import sizeof

from distributed import Future, get_client

if TYPE_CHECKING:
    import pandas as pd


T = TypeVar("T")


class QuickSizeof(Generic[T]):
    "Wrapper to bypass slow `sizeof` calls"

    def __init__(self, obj: T, size: int) -> None:
        self.obj = obj
        self.size = size

    def __sizeof__(self) -> int:
        return self.size


def split(
    df: pd.DataFrame,
    column: str,
    npartitions_output: int,
    ignore_index: bool,
    name: str,
    row_size_estimate: int,
    partition_info: dict[str, int] = None,
) -> dict[int, Future]:
    "Split input partition into shards per output group; scatter shards and return Futures referencing them."
    assert isinstance(partition_info, dict), "partition_info is not a dict"
    client = get_client()

    shards: dict[int, pd.DataFrame] = shuffle_group(
        df,
        cols=column,
        stage=0,
        k=npartitions_output,
        npartitions=npartitions_output,
        ignore_index=ignore_index,
        nfinal=npartitions_output,
    )
    input_partition_i = partition_info["number"]
    # Change keys to be unique among all tasks---the dict keys here end up being
    # the task keys on the scheduler.
    # Also wrap in `QuickSizeof` to significantly speed up the worker storing each
    # shard in its zict buffer.
    shards_rekeyed = {
        # NOTE: this name is optimized to be easy for `key_split` to process
        f"{name}-{input_partition_i}-{output_partition_i}": QuickSizeof(
            shard, len(shard) * row_size_estimate
        )
        for output_partition_i, shard in shards.items()
    }
    # NOTE: `scatter` called within a task has very different (undocumented) behavior:
    # it writes the keys directly to the current worker, then informs the scheduler
    # that these keys exist on the current worker. No communications to other workers ever.
    futures: dict[str, Future] = client.scatter(shards_rekeyed)
    # Switch keys back to output partition numbers so they're easier to select
    return dict(zip(shards, futures.values()))


def gather_regroup(i: int, all_futures: list[dict[int, Future]]) -> pd.DataFrame:
    "Given Futures for all shards, select Futures for this output partition, gather them, and concat."
    client = get_client()
    futures = [fs[i] for fs in all_futures if i in fs]
    shards: list[QuickSizeof[pd.DataFrame]] = client.gather(futures, direct=True)
    # Since every worker holds a reference to all futures until the very last task completes,
    # forcibly cancel these futures now to allow memory to be released eagerly.
    # This is safe because we're only cancelling futures for this output partition,
    # and there's exactly one task for each output partition.
    client.cancel(futures, force=True)

    return _concat([s.obj for s in shards])


def rearrange_by_column_scatter(
    df: DataFrame, column: str, npartitions=None, ignore_index=False
) -> DataFrame:
    token = tokenize(df, column)

    npartitions = npartitions or df.npartitions
    row_size_estimate = sizeof(df._meta_nonempty) // len(df._meta_nonempty)
    splits = df.map_partitions(
        split,
        column,
        npartitions,
        ignore_index,
        f"shuffle-shards-{token}",
        row_size_estimate,
        meta=df,
        enforce_metadata=False,
        transform_divisions=False,
    )

    all_futures = splits.__dask_keys__()
    name = f"shuffle-regroup-{token}"
    dsk = {(name, i): (gather_regroup, i, all_futures) for i in range(npartitions)}
    return DataFrame(
        HighLevelGraph.from_collections(name, dsk, [splits]),
        name,
        df._meta,
        [None] * (npartitions + 1),
    )
