from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from enum import Enum
from typing import TYPE_CHECKING, Any, NewType, Union

from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import Layer

from distributed.exceptions import Reschedule
from distributed.shuffle._arrow import check_dtype_support, check_minimal_arrow_version

logger = logging.getLogger("distributed.shuffle")
if TYPE_CHECKING:
    import pandas as pd

    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

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
    parts_out: set[int],
) -> int:
    try:
        return _get_worker_extension().add_partition(
            input,
            shuffle_id=id,
            type=ShuffleType.DATAFRAME,
            input_partition=input_partition,
            npartitions=npartitions,
            column=column,
            parts_out=parts_out,
        )
    except Exception as e:
        raise RuntimeError(f"shuffle_transfer failed during shuffle {id}") from e


def shuffle_unpack(
    id: ShuffleId, output_partition: int, barrier_run_id: int, meta: pd.DataFrame
) -> pd.DataFrame:
    try:
        return _get_worker_extension().get_output_partition(
            id, barrier_run_id, output_partition, meta=meta
        )
    except Reschedule as e:
        raise e
    except Exception as e:
        raise RuntimeError(f"shuffle_unpack failed during shuffle {id}") from e


def shuffle_barrier(id: ShuffleId, run_ids: list[int]) -> int:
    try:
        return _get_worker_extension().barrier(id, run_ids)
    except Exception as e:
        raise RuntimeError(f"shuffle_barrier failed during shuffle {id}") from e


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
) -> DataFrame:
    from dask.dataframe import DataFrame

    meta = df._meta
    check_dtype_support(meta)
    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    if any(not isinstance(c, str) for c in meta.columns):
        unsupported = {c: type(c) for c in meta.columns if not isinstance(c, str)}
        raise TypeError(
            f"p2p requires all column names to be str, found: {unsupported}",
        )

    name = f"shuffle-p2p-{token}"
    layer = P2PShuffleLayer(
        name,
        column,
        npartitions,
        npartitions_input=df.npartitions,
        name_input=df._name,
        meta_input=meta,
    )
    return DataFrame(
        HighLevelGraph.from_collections(name, layer, [df]),
        name,
        meta,
        [None] * (npartitions + 1),
    )


_T_Key: TypeAlias = Union[tuple[str, int], str]
_T_LowLevelGraph: TypeAlias = dict[_T_Key, tuple]


class P2PShuffleLayer(Layer):
    def __init__(
        self,
        name: str,
        column: str,
        npartitions: int,
        npartitions_input: int,
        name_input: str,
        meta_input: pd.DataFrame,
        parts_out: Iterable | None = None,
        annotations: dict | None = None,
    ):
        check_minimal_arrow_version()
        self.name = name
        self.column = column
        self.npartitions = npartitions
        self.name_input = name_input
        self.meta_input = meta_input
        if parts_out:
            self.parts_out = set(parts_out)
        else:
            self.parts_out = set(range(self.npartitions))
        self.npartitions_input = npartitions_input
        annotations = annotations or {}
        annotations.update({"shuffle": lambda key: key[1]})
        super().__init__(annotations=annotations)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<name='{self.name}', npartitions={self.npartitions}>"
        )

    def get_output_keys(self) -> set[_T_Key]:
        return {(self.name, part) for part in self.parts_out}

    def is_materialized(self) -> bool:
        return hasattr(self, "_cached_dict")

    @property
    def _dict(self) -> _T_LowLevelGraph:
        """Materialize full dict representation"""
        self._cached_dict: _T_LowLevelGraph
        dsk: _T_LowLevelGraph
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key: _T_Key) -> tuple:
        return self._dict[key]

    def __iter__(self) -> Iterator[_T_Key]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def _cull(self, parts_out: Iterable[int]) -> P2PShuffleLayer:
        return P2PShuffleLayer(
            self.name,
            self.column,
            self.npartitions,
            self.npartitions_input,
            self.name_input,
            self.meta_input,
            parts_out=parts_out,
        )

    def _keys_to_parts(self, keys: Iterable[_T_Key]) -> set[int]:
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            if isinstance(key, tuple) and len(key) == 2:
                _name, _part = key
                if _name != self.name:
                    continue
                parts.add(_part)
        return parts

    def cull(
        self, keys: Iterable[_T_Key], all_keys: Any
    ) -> tuple[P2PShuffleLayer, dict]:
        """Cull a P2PShuffleLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indices) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        input_parts = {(self.name_input, i) for i in range(self.npartitions_input)}
        culled_deps = {(self.name, part): input_parts.copy() for part in parts_out}

        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def _construct_graph(self) -> _T_LowLevelGraph:
        token = tokenize(self.name_input, self.column, self.npartitions, self.parts_out)
        dsk: _T_LowLevelGraph = {}
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
                self.parts_out,
            )

        dsk[_barrier_key] = (shuffle_barrier, token, transfer_keys)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (
                shuffle_unpack,
                token,
                part_out,
                _barrier_key,
                self.meta_input,
            )
        return dsk


_BARRIER_PREFIX = "shuffle-barrier-"


def barrier_key(shuffle_id: ShuffleId) -> str:
    return _BARRIER_PREFIX + shuffle_id


def id_from_key(key: str) -> ShuffleId:
    assert key.startswith(_BARRIER_PREFIX)
    return ShuffleId(key.replace(_BARRIER_PREFIX, ""))
