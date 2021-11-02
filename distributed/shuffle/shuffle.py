from __future__ import annotations

from typing import TYPE_CHECKING

from dask.base import tokenize
from dask.dataframe import DataFrame
from dask.delayed import Delayed, delayed
from dask.highlevelgraph import HighLevelGraph

from distributed import get_worker

from .shuffle_extension import ShuffleId, ShuffleMetadata, ShuffleWorkerExtension

if TYPE_CHECKING:
    import pandas as pd


def shuffle_setup(metadata: ShuffleMetadata) -> None:
    worker = get_worker()
    extension: ShuffleWorkerExtension | None = worker.extensions.get("shuffle")
    if not extension:
        raise RuntimeError(
            f"The worker {worker.address} does not have a ShuffleExtension. "
            "Is pandas installed on the worker?"
        )
    extension.create_shuffle(metadata)


def shuffle_transfer(input: pd.DataFrame, id: ShuffleId, setup=None) -> None:
    extension: ShuffleWorkerExtension = get_worker().extensions["shuffle"]
    extension.add_partition(input, id)


def shuffle_unpack(id: ShuffleId, output_partition: int, barrier=None) -> pd.DataFrame:
    extension: ShuffleWorkerExtension = get_worker().extensions["shuffle"]
    return extension.get_output_partition(id, output_partition)


def shuffle_barrier(transfers: list[None]) -> None:
    pass


def rearrange_by_column_service(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
):
    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    setup = delayed(shuffle_setup)(
        ShuffleMetadata(
            ShuffleId(token),
            [],
            df._meta,
            column,
            npartitions,
        )
    )

    transferred = df.map_partitions(
        shuffle_transfer,
        token,
        setup,
        meta=df,
        enforce_metadata=False,
        transform_divisions=False,
    )

    barrier_key = "shuffle-barrier-" + token
    barrier_dsk = {barrier_key: (shuffle_barrier, transferred.__dask_keys__())}
    barrier = Delayed(
        barrier_key,
        HighLevelGraph.from_collections(
            barrier_key, barrier_dsk, dependencies=[transferred]
        ),
    )

    # TODO blockwise this part (not yet supported by dataframe IO interface)
    name = "shuffle-unpack-" + token
    dsk = {
        (name, i): (shuffle_unpack, token, i, barrier_key) for i in range(npartitions)
    }
    return DataFrame(
        HighLevelGraph.from_collections(name, dsk, [barrier]),
        name,
        df._meta,
        [None] * (npartitions + 1),
    )
