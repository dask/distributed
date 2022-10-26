from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import SimpleShuffleLayer

from distributed.shuffle._shuffle_extension import ShuffleId, ShuffleWorkerExtension

if TYPE_CHECKING:
    import pandas as pd

    from dask.dataframe import DataFrame


def _get_worker_extension() -> ShuffleWorkerExtension:
    from distributed import get_worker

    try:
        worker = get_worker()
    except ValueError as e:
        raise RuntimeError(
            "`shuffle='p2p'` requires Dask's distributed scheduler. This task is not running on a Worker; "
            "please confirm that you've created a distributed Client and are submitting this computation through it."
        ) from e
    extension: ShuffleWorkerExtension | None = worker.extensions.get("shuffle")
    if extension is None:
        raise RuntimeError(
            f"The worker {worker.address} does not have a ShuffleExtension. "
            "Is pandas installed on the worker?"
        )
    return extension


def shuffle_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    npartitions: int,
    column: str,
) -> None:
    _get_worker_extension().add_partition(
        input, id, npartitions=npartitions, column=column
    )


def shuffle_unpack(
    id: ShuffleId, output_partition: int, barrier: object
) -> pd.DataFrame:
    return _get_worker_extension().get_output_partition(id, output_partition)


def shuffle_barrier(id: ShuffleId, transfers: list[None]) -> None:
    return _get_worker_extension().barrier(id)


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
) -> DataFrame:
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

    name = f"shuffle-p2p-{token}"
    layer = P2PShuffleLayer(
        name,
        column,
        npartitions,
        npartitions_input=df.npartitions,
        ignore_index=True,
        name_input=df._name,
        meta_input=empty,
    )
    return DataFrame(
        HighLevelGraph.from_collections(name, layer, [df]),
        name,
        empty,
        [None] * (npartitions + 1),
    )


class P2PShuffleLayer(SimpleShuffleLayer):
    def __init__(
        self,
        name: str,
        column: str,
        npartitions: int,
        npartitions_input: int,
        ignore_index: bool,
        name_input: str,
        meta_input: pd.DataFrame,
        parts_out: list | None = None,
        annotations: dict | None = None,
    ):
        annotations = annotations or {}
        annotations.update({"shuffle": lambda key: key[1]})
        super().__init__(
            name,
            column,
            npartitions,
            npartitions_input,
            ignore_index,
            name_input,
            meta_input,
            parts_out,
            annotations=annotations,
        )

    def get_split_keys(self) -> list:
        # TODO: This is doing some funky stuff to set priorities but we don't need this
        return []

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<name='{self.name}', npartitions={self.npartitions}>"
        )

    def _cull(self, parts_out: list) -> P2PShuffleLayer:
        return P2PShuffleLayer(
            self.name,
            self.column,
            self.npartitions,
            self.npartitions_input,
            self.ignore_index,
            self.name_input,
            self.meta_input,
            parts_out=parts_out,
        )

    def _construct_graph(self, deserializing: Any = None) -> dict[tuple | str, tuple]:
        token = tokenize(self.name_input, self.column, self.npartitions, self.parts_out)
        dsk: dict[tuple | str, tuple] = {}
        barrier_key = "shuffle-barrier-" + token
        name = "shuffle-transfer-" + token
        tranfer_keys = list()
        for i in range(self.npartitions_input):
            tranfer_keys.append((name, i))
            dsk[(name, i)] = (
                shuffle_transfer,
                (self.name_input, i),
                token,
                self.npartitions,
                self.column,
            )

        dsk[barrier_key] = (shuffle_barrier, token, tranfer_keys)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (shuffle_unpack, token, part_out, barrier_key)
        return dsk
