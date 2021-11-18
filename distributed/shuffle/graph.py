from __future__ import annotations

from typing import TYPE_CHECKING

from dask.base import tokenize
from dask.blockwise import BlockwiseDepDict, blockwise
from dask.dataframe import DataFrame
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph

from .common import ShuffleId
from .shuffle_worker import ShuffleWorkerExtension

if TYPE_CHECKING:
    import pandas as pd


def get_shuffle_extension() -> ShuffleWorkerExtension:
    from distributed import get_worker

    return get_worker().extensions["shuffle"]


def shuffle_transfer(
    data: pd.DataFrame, id: ShuffleId, npartitions: int, column: str
) -> None:
    ext = get_shuffle_extension()
    ext.sync(ext.add_partition(data, id, npartitions, column))


def shuffle_unpack(
    id: ShuffleId, i: int, empty: pd.DataFrame, barrier=None
) -> pd.DataFrame:
    ext = get_shuffle_extension()
    return ext.sync(ext.get_output_partition(id, i, empty))


def shuffle_barrier(id: ShuffleId, transfers: list[None]) -> None:
    ext = get_shuffle_extension()
    ext.sync(ext.barrier(id))


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
):
    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    transferred = df.map_partitions(
        shuffle_transfer,
        token,
        npartitions,
        column,
        meta=df,
        enforce_metadata=False,
        transform_divisions=False,
    )

    barrier_key = "shuffle-barrier-" + token
    barrier_dsk = {barrier_key: (shuffle_barrier, token, transferred.__dask_keys__())}
    barrier = Delayed(
        barrier_key,
        HighLevelGraph.from_collections(
            barrier_key, barrier_dsk, dependencies=[transferred]
        ),
    )

    name = "shuffle-unpack-" + token
    dsk = blockwise(
        shuffle_unpack,
        name,
        "i",
        token,
        None,
        BlockwiseDepDict({(i,): i for i in range(npartitions)}),
        "i",
        df._meta,
        None,
        barrier_key,
        None,
        numblocks={},
    )

    return DataFrame(
        HighLevelGraph.from_collections(name, dsk, [barrier]),
        name,
        df._meta,
        [None] * (npartitions + 1),
    )
