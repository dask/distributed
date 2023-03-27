from __future__ import annotations

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, NewType

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import SimpleShuffleLayer

from distributed.shuffle._arrow import check_dtype_support, check_minimal_arrow_version

logger = logging.getLogger("distributed.shuffle")
if TYPE_CHECKING:
    import pandas as pd

    from dask.dataframe import DataFrame

    # circular dependency
    from distributed.shuffle._worker_extension import ShuffleWorkerExtension

ShuffleId = NewType("ShuffleId", str)


class ShuffleType(Enum):
    DATAFRAME = "DataFrameShuffle"
    ARRAY_RECHUNK = "ArrayRechunk"


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
    input_partition: int,
    npartitions: int,
    column: str,
) -> int:
    try:
        return _get_worker_extension().add_partition(
            input,
            shuffle_id=id,
            type=ShuffleType.DATAFRAME,
            input_partition=input_partition,
            npartitions=npartitions,
            column=column,
        )
    except Exception:
        msg = f"shuffle_transfer failed during shuffle {id}"
        # FIXME: Use exception chaining instead of logging the traceback.
        #  This has previously led to spurious recursion errors
        logger.error(msg, exc_info=True)
        raise RuntimeError(msg)


def shuffle_unpack(
    id: ShuffleId, output_partition: int, barrier_run_id: int
) -> pd.DataFrame:
    try:
        return _get_worker_extension().get_output_partition(
            id, barrier_run_id, output_partition
        )
    except Exception:
        msg = f"shuffle_unpack failed during shuffle {id}"
        # FIXME: Use exception chaining instead of logging the traceback.
        #  This has previously led to spurious recursion errors
        logger.error(msg, exc_info=True)
        raise RuntimeError(msg)


def shuffle_barrier(id: ShuffleId, run_ids: list[int]) -> int:
    try:
        return _get_worker_extension().barrier(id, run_ids)
    except Exception:
        msg = f"shuffle_barrier failed during shuffle {id}"
        # FIXME: Use exception chaining instead of logging the traceback.
        #  This has previously led to spurious recursion errors
        logger.error(msg, exc_info=True)
        raise RuntimeError(msg)


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
) -> DataFrame:
    from dask.dataframe import DataFrame

    if dask.config.get("optimization.fuse.active"):
        raise RuntimeError(
            "P2P shuffling requires the fuse optimization to be turned off. "
            "Set the 'optimization.fuse.active' config to False to deactivate."
        )

    check_dtype_support(df._meta)
    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    empty = df._meta.copy()
    if any(not isinstance(c, str) for c in empty.columns):
        unsupported = {c: type(c) for c in empty.columns if not isinstance(c, str)}
        raise TypeError(
            f"p2p requires all column names to be str, found: {unsupported}",
        )

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
        check_minimal_arrow_version()
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
        _barrier_key = barrier_key(ShuffleId(token))
        name = "shuffle-transfer-" + token
        transfer_keys = list()
        for i in range(self.npartitions_input):
            transfer_keys.append((name, i))
            dsk[(name, i)] = (
                shuffle_transfer,
                (self.name_input, i),
                token,
                i,
                self.npartitions,
                self.column,
            )

        dsk[_barrier_key] = (shuffle_barrier, token, transfer_keys)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (shuffle_unpack, token, part_out, _barrier_key)
        return dsk


_BARRIER_PREFIX = "shuffle-barrier-"


def barrier_key(shuffle_id: ShuffleId) -> str:
    return _BARRIER_PREFIX + shuffle_id


def id_from_key(key: str) -> ShuffleId:
    assert key.startswith(_BARRIER_PREFIX)
    return ShuffleId(key.replace(_BARRIER_PREFIX, ""))
