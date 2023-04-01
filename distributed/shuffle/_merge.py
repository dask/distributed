# mypy: ignore-errors
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from dask.base import is_dask_collection, tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import Layer

from distributed.shuffle._shuffle import (
    ShuffleId,
    _get_worker_extension,
    barrier_key,
    shuffle_barrier,
    shuffle_transfer,
)

if TYPE_CHECKING:
    import pandas as pd
    from pandas._typing import IndexLabel, MergeHow, Suffixes

    from dask.dataframe.core import _Frame


_HASH_COLUMN_NAME = "__hash_partition"


def _prepare_index_for_partitioning(df: pd.DataFrame, index: IndexLabel):
    import pandas as pd

    from dask.dataframe.core import _Frame

    list_like = pd.api.types.is_list_like(index) and not is_dask_collection(index)

    if not isinstance(index, _Frame):
        if list_like:
            # Make sure we don't try to select with pd.Series/pd.Index
            index = list(index)
        index = df._select_columns_or_index(index)
    elif hasattr(index, "to_frame"):
        # If this is an index, we should still convert to a
        # DataFrame. Otherwise, the hashed values of a column
        # selection will not match (important when merging).
        index = index.to_frame()
    return index


def _calculate_partitions(df: pd.DataFrame, index: IndexLabel, npartitions: int):
    index = _prepare_index_for_partitioning(df, index)
    from dask.dataframe.shuffle import partitioning_index

    partitions = index.map_partitions(
        partitioning_index,
        npartitions=npartitions or df.npartitions,
        meta=df._meta._constructor_sliced([0]),
        transform_divisions=False,
    )
    df2 = df.assign(**{_HASH_COLUMN_NAME: partitions})
    df2._meta.index.name = df._meta.index.name
    return df2


def hash_join_p2p(
    lhs: _Frame,
    left_on: IndexLabel | None,
    rhs: _Frame,
    right_on: IndexLabel | None,
    how: MergeHow = "inner",
    npartitions: int | None = None,
    suffixes: Suffixes = ("_x", "_y"),
    indicator: bool = False,
):
    from dask.dataframe.core import Index, new_dd_object

    if npartitions is None:
        npartitions = max(lhs.npartitions, rhs.npartitions)

    if isinstance(left_on, Index):
        left_on = None
        left_index = True
    else:
        left_index = False

    if isinstance(right_on, Index):
        right_on = None
        right_index = True
    else:
        right_index = False
    merge_kwargs = dict(
        how=how,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        suffixes=suffixes,
        indicator=indicator,
    )
    # dummy result
    # Avoid using dummy data for a collection it is empty
    _lhs_meta = lhs._meta_nonempty if len(lhs.columns) else lhs._meta
    _rhs_meta = rhs._meta_nonempty if len(rhs.columns) else rhs._meta
    meta = _lhs_meta.merge(_rhs_meta, **merge_kwargs)
    lhs = _calculate_partitions(lhs, left_on, npartitions)
    rhs = _calculate_partitions(rhs, right_on, npartitions)
    merge_name = "hash-join-" + tokenize(lhs, rhs, **merge_kwargs)
    join_layer = HashJoinP2PLayer(
        name=merge_name,
        name_input_left=lhs._name,
        left_on=left_on,
        n_partitions_left=lhs.npartitions,
        name_input_right=rhs._name,
        right_on=right_on,
        n_partitions_right=rhs.npartitions,
        meta_output=meta,
        how=how,
        npartitions=npartitions,
        suffixes=suffixes,
        indicator=indicator,
        left_index=left_index,
        right_index=right_index,
    )
    graph = HighLevelGraph.from_collections(
        merge_name, join_layer, dependencies=[lhs, rhs]
    )
    return new_dd_object(graph, merge_name, meta, [None] * (npartitions + 1))


hash_join = hash_join_p2p

_HASH_COLUMN_NAME = "__hash_partition"


def merge_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    input_partition: int,
    npartitions: int,
):
    return shuffle_transfer(
        input=input,
        id=id,
        input_partition=input_partition,
        npartitions=npartitions,
        column=_HASH_COLUMN_NAME,
    )


def merge_unpack(
    shuffle_id_left: ShuffleId,
    shuffle_id_right: ShuffleId,
    output_partition: int,
    barrier_left: int,
    barrier_right: int,
    how: MergeHow,
    left_on: IndexLabel,
    right_on: IndexLabel,
    result_meta: pd.DataFrame,
    suffixes: Suffixes,
):
    from dask.dataframe.multi import merge_chunk

    ext = _get_worker_extension()
    left = ext.get_output_partition(
        shuffle_id_left, barrier_left, output_partition
    ).drop(columns=_HASH_COLUMN_NAME)
    right = ext.get_output_partition(
        shuffle_id_right, barrier_right, output_partition
    ).drop(columns=_HASH_COLUMN_NAME)
    return merge_chunk(
        left,
        right,
        how=how,
        result_meta=result_meta,
        left_on=left_on,
        right_on=right_on,
        suffixes=suffixes,
    )


class HashJoinP2PLayer(Layer):
    def __init__(
        self,
        name: str,
        name_input_left: str,
        left_on,
        n_partitions_left: int,
        n_partitions_right: int,
        name_input_right: str,
        right_on,
        meta_output: pd.DataFrame,
        left_index: bool,
        right_index: bool,
        how: MergeHow = "inner",
        npartitions: int | None = None,
        suffixes: Suffixes = ("_x", "_y"),
        indicator: bool = False,
        parts_out: Sequence | None = None,
        annotations: dict | None = None,
    ) -> None:
        self.name = name
        self.name_input_left = name_input_left
        self.left_on = left_on
        self.name_input_right = name_input_right
        self.right_on = right_on
        self.how = how
        self.npartitions = npartitions
        self.suffixes = suffixes
        self.indicator = indicator
        self.meta_output = meta_output
        self.parts_out = parts_out or list(range(npartitions))
        self.n_partitions_left = n_partitions_left
        self.n_partitions_right = n_partitions_right
        self.left_index = left_index
        self.right_index = right_index
        annotations = annotations or {}
        annotations.update({"shuffle": lambda key: key[-1]})
        super().__init__(annotations=annotations)

    def _cull_dependencies(
        self, keys: Iterable[str], parts_out: Iterable[str] | None = None
    ):
        """Determine the necessary dependencies to produce `keys`.

        For a simple shuffle, output partitions always depend on
        all input partitions. This method does not require graph
        materialization.
        """
        deps = defaultdict(set)
        parts_out = parts_out or self._keys_to_parts(keys)
        for part in parts_out:
            deps[(self.name, part)] |= {
                (self.name_input_left, i) for i in range(self.npartitions)
            }
            deps[(self.name, part)] |= {
                (self.name_input_right, i) for i in range(self.npartitions)
            }
        return deps

    def _keys_to_parts(self, keys: Iterable[str]) -> set[str]:
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            try:
                _name, _part = key
            except ValueError:
                continue
            if _name != self.name:
                continue
            parts.add(_part)
        return parts

    def get_output_keys(self):
        return {(self.name, part) for part in self.parts_out}

    def __repr__(self):
        return f"HashJoin<name='{self.name}', npartitions={self.npartitions}>"

    def is_materialized(self):
        return hasattr(self, "_cached_dict")

    def __getitem__(self, key):
        return self._dict[key]

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    @property
    def _dict(self):
        """Materialize full dict representation"""
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def _cull(self, parts_out: Sequence[str]):
        return HashJoinP2PLayer(
            name=self.name,
            name_input_left=self.name_input_left,
            left_on=self.left_on,
            name_input_right=self.name_input_right,
            right_on=self.right_on,
            how=self.how,
            npartitions=self.npartitions,
            suffixes=self.suffixes,
            indicator=self.indicator,
            meta_output=self.meta_output,
            parts_out=parts_out,
            left_index=self.left_index,
            right_index=self.right_index,
            annotations=self.annotations,
            n_partitions_left=self.n_partitions_left,
            n_partitions_right=self.n_partitions_right,
        )

    def cull(self, keys: Iterable[str], all_keys: Any) -> tuple[HashJoinP2PLayer, dict]:
        """Cull a SimpleShuffleLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indices) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        culled_deps = self._cull_dependencies(keys, parts_out=parts_out)
        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def _construct_graph(self) -> dict[tuple | str, tuple]:
        args = (
            self.left_on,
            self.how,
            self.npartitions,
            self.n_partitions_left,
            self.n_partitions_right,
            self.parts_out,
            self.suffixes,
            self.indicator,
        )
        token_left = tokenize(self.name_input_left, *args)
        token_right = tokenize(self.name_input_right, *args)
        dsk: dict[tuple | str, tuple] = {}
        name_left = "hash-join-transfer-" + token_left
        name_right = "hash-join-transfer-" + token_right
        transfer_keys_left = list()
        transfer_keys_right = list()
        for i in range(self.n_partitions_left):
            transfer_keys_left.append((name_left, i))
            dsk[(name_left, i)] = (
                merge_transfer,
                (self.name_input_left, i),
                token_left,
                i,
                self.npartitions,
            )
        for i in range(self.n_partitions_right):
            transfer_keys_right.append((name_right, i))
            dsk[(name_right, i)] = (
                merge_transfer,
                (self.name_input_right, i),
                token_right,
                i,
                self.npartitions,
            )

        _barrier_key_left = barrier_key(ShuffleId(token_left))
        _barrier_key_right = barrier_key(ShuffleId(token_right))
        dsk[_barrier_key_left] = (shuffle_barrier, token_left, transfer_keys_left)
        dsk[_barrier_key_right] = (shuffle_barrier, token_right, transfer_keys_right)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (
                merge_unpack,
                token_left,
                token_right,
                part_out,
                _barrier_key_left,
                _barrier_key_right,
                self.how,
                self.left_on,
                self.right_on,
                self.meta_output,
                self.suffixes,
            )
        return dsk
