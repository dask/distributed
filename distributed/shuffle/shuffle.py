from __future__ import annotations

from typing import TYPE_CHECKING

from dask.base import tokenize
from dask.delayed import Delayed, delayed
from dask.highlevelgraph import HighLevelGraph

from distributed.shuffle.shuffle_extension import (
    NewShuffleMetadata,
    ShuffleId,
    ShuffleWorkerExtension,
)

if TYPE_CHECKING:
    import pandas as pd

    from dask.dataframe import DataFrame


def get_ext() -> ShuffleWorkerExtension:
    from distributed import get_worker

    try:
        worker = get_worker()
    except ValueError as e:
        raise RuntimeError(
            "`shuffle='p2p'` requires Dask's distributed scheduler. This task is not running on a Worker; "
            "please confirm that you've created a distributed Client and are submitting this computation through it."
        ) from e
    extension: ShuffleWorkerExtension | None = worker.extensions.get("shuffle")
    if not extension:
        raise RuntimeError(
            f"The worker {worker.address} does not have a ShuffleExtension. "
            "Is pandas installed on the worker?"
        )
    return extension


def shuffle_setup(metadata: NewShuffleMetadata) -> None:
    get_ext().create_shuffle(metadata)


def shuffle_transfer(input: pd.DataFrame, id: ShuffleId, setup=None) -> None:
    get_ext().add_partition(input, id)


def shuffle_unpack(id: ShuffleId, output_partition: int, barrier=None) -> pd.DataFrame:
    return get_ext().get_output_partition(id, output_partition)


def shuffle_barrier(id: ShuffleId, transfers: list[None]) -> None:
    get_ext().barrier(id)


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
):
    from dask.dataframe import DataFrame

    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    setup = delayed(shuffle_setup, pure=True)(
        NewShuffleMetadata(
            ShuffleId(token),
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
    barrier_dsk = {barrier_key: (shuffle_barrier, token, transferred.__dask_keys__())}
    barrier = Delayed(
        barrier_key,
        HighLevelGraph.from_collections(
            barrier_key, barrier_dsk, dependencies=[transferred]
        ),
    )

    name = "shuffle-unpack-" + token
    dsk = {
        (name, i): (shuffle_unpack, token, i, barrier_key) for i in range(npartitions)
    }
    # TODO: update to use blockwise.
    # Changes task names, so breaks setting worker restrictions at the moment.
    # Also maybe would be nice if the `DataFrameIOLayer` interface supported this?
    # dsk = blockwise(
    #     shuffle_unpack,
    #     name,
    #     "i",
    #     token,
    #     None,
    #     BlockwiseDepDict({(i,): i for i in range(npartitions)}),
    #     "i",
    #     barrier_key,
    #     None,
    #     numblocks={},
    # )

    return DataFrame(
        HighLevelGraph.from_collections(name, dsk, [barrier]),
        name,
        df._meta,
        [None] * (npartitions + 1),
    )
