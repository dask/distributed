from __future__ import annotations

from typing import TYPE_CHECKING

from dask.base import tokenize
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.shuffle.shuffle_extension import ShuffleId, ShuffleWorkerExtension

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


def shuffle_transfer(
    input: pd.DataFrame, id: ShuffleId, npartitions: int = None, column=None
) -> None:
    get_ext().add_partition(input, id, npartitions=npartitions, column=column)


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

    empty = df._meta.copy()
    for c, dt in empty.dtypes.items():
        if dt == object:
            empty[c] = empty[c].astype(
                "string"
            )  # TODO: we fail at non-string object dtypes
    empty[column] = empty[column].astype("int64")  # TODO: this shouldn't be necesssary

    transferred = df.map_partitions(
        shuffle_transfer,
        id=token,
        npartitions=npartitions,
        column=column,
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
    layer = MaterializedLayer(dsk, annotations={"shuffle": lambda key: key[1]})
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
        HighLevelGraph.from_collections(name, layer, [barrier]),
        name,
        df._meta,
        [None] * (npartitions + 1),
    )
