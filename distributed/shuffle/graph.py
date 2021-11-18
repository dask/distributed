from __future__ import annotations

from typing import TYPE_CHECKING

from dask.base import tokenize
from dask.blockwise import BlockwiseDepDict, blockwise
from dask.dataframe import DataFrame
from dask.dataframe.core import partitionwise_graph
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

    # We use `partitionwise_graph` instead of `map_partitions` so we can pass in our own key.
    # The scheduler needs the task key to contain the shuffle ID; it's the only way it knows
    # what shuffle a task belongs to.
    # (Yes, this is rather brittle.)
    transfer_name = "shuffle-transfer-" + token
    transfer_dsk = partitionwise_graph(
        shuffle_transfer, transfer_name, df, token, npartitions, column
    )

    barrier_name = "shuffle-barrier-" + token
    barrier_dsk = {
        barrier_name: (
            shuffle_barrier,
            token,
            [(transfer_name, i) for i in range(df.npartitions)],
        )
    }

    unpack_name = "shuffle-unpack-" + token
    unpack_dsk = blockwise(
        shuffle_unpack,
        unpack_name,
        "i",
        token,
        None,
        BlockwiseDepDict({(i,): i for i in range(npartitions)}),
        "i",
        df._meta,
        None,
        barrier_name,
        None,
        numblocks={},
    )

    hlg = HighLevelGraph(
        {
            transfer_name: transfer_dsk,
            barrier_name: barrier_dsk,
            unpack_name: unpack_dsk,
            **df.dask.layers,
        },
        {
            transfer_name: set(df.__dask_layers__()),
            barrier_name: {transfer_name},
            unpack_name: {barrier_name},
            **df.dask.dependencies,
        },
    )

    return DataFrame(
        hlg,
        unpack_name,
        df._meta,
        [None] * (npartitions + 1),
    )
