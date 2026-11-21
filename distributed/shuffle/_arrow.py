from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from dask.utils import parse_bytes

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


_INPUT_PARTITION_ID_COLUMN = "__input_partition_id__"
_NULL_OBJECT_COLUMNS = b"dask.null_object_columns"


class _NullObjectInfo(TypedDict):
    marker: str
    object_dtype: bool


def check_dtype_support(meta_input: pd.DataFrame) -> None:
    import pandas as pd

    for name in meta_input:
        column = meta_input[name]
        # FIXME: PyArrow does not support complex numbers: https://issues.apache.org/jira/browse/ARROW-638
        if pd.api.types.is_complex_dtype(column):
            raise TypeError(
                f"p2p does not support data of type '{column.dtype}' found in column '{name}'."
            )
        # FIXME: PyArrow does not support sparse data: https://issues.apache.org/jira/browse/ARROW-8679
        if isinstance(column.dtype, pd.SparseDtype):
            raise TypeError("p2p does not support sparse data found in column '{name}'")


def preserve_null_objects(table: pa.Table, df: pd.DataFrame) -> pa.Table:
    """Retain pandas null semantics that Arrow's null type cannot represent."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    from dask.dataframe._compat import PANDAS_GE_300

    if not PANDAS_GE_300 or not isinstance(df, pd.DataFrame):
        return table

    def null_kind(value: object) -> int:
        if value is None:
            return 0
        if value is pd.NA:
            return 2
        if value is pd.NaT:
            return 3
        return 1  # NaN

    # Use the same marker names even if input partitions reorder their columns.
    columns: dict[str, _NullObjectInfo] = {}
    for i, column in enumerate(sorted(df.columns.tolist())):
        series = df[column]
        # NumPy-backed columns already round-trip their null sentinel through
        # Arrow. Extension arrays may need pd.NA restored after object promotion.
        if (
            series.dtype != object
            and getattr(series.dtype, "na_value", None) is not pd.NA
        ):
            continue
        if not table[column].null_count:
            continue
        marker = f"__dask_null_object_{i}__"
        while marker in table.column_names:
            marker += "_"
        mask = series.isna().to_numpy()
        # 255 denotes a non-null value; 0..3 distinguish pandas null sentinels.
        kinds = np.full(len(series), 255, dtype=np.uint8)
        if series.dtype == object:
            values = series.to_numpy()[mask]
            kinds[mask] = [null_kind(value) for value in values]
        else:
            kinds[mask] = 2  # Extension arrays use pd.NA, as checked above.
        table = table.append_column(marker, pa.array(kinds))
        columns[column] = {
            "marker": marker,
            "object_dtype": series.dtype == object
            and pa.types.is_null(table[column].type),
        }
    if columns:
        metadata = dict(table.schema.metadata or {})
        metadata[_NULL_OBJECT_COLUMNS] = json.dumps(columns).encode()
        table = table.replace_schema_metadata(metadata)
    return table


def concat_tables(tables: Iterable[pa.Table]) -> pa.Table:
    import pyarrow as pa

    tables = list(tables)
    null_columns: dict[str, _NullObjectInfo] = {}
    for table in tables:
        for column, info in json.loads(
            (table.schema.metadata or {}).get(_NULL_OBJECT_COLUMNS, b"{}")
        ).items():
            if column in null_columns:
                info["object_dtype"] |= null_columns[column]["object_dtype"]
            null_columns[column] = info
    result = pa.concat_tables(tables, promote_options="permissive")
    if null_columns:
        metadata = dict(result.schema.metadata or {})
        metadata[_NULL_OBJECT_COLUMNS] = json.dumps(null_columns).encode()
        result = result.replace_schema_metadata(metadata)
    return result


def convert_shards(
    shards: list[pa.Table], meta: pd.DataFrame, partition_column: str, drop_column: bool
) -> pd.DataFrame:
    import pandas as pd
    from pandas.core.dtypes.cast import find_common_type  # type: ignore[attr-defined]

    from dask.dataframe.dispatch import from_pyarrow_table_dispatch

    table = concat_tables(shards)
    table = table.sort_by(_INPUT_PARTITION_ID_COLUMN)
    null_columns = json.loads(
        (table.schema.metadata or {}).get(_NULL_OBJECT_COLUMNS, b"{}")
    )
    # Arrow promotes null columns to the other shards' types. Pandas >=3 instead
    # retains object dtype when concatenating all-null object and typed columns.
    null_kinds = {
        column: table[info["marker"]].to_numpy().copy()
        for column, info in null_columns.items()
        if info["object_dtype"]
    }
    table = table.drop(
        [_INPUT_PARTITION_ID_COLUMN]
        + [info["marker"] for info in null_columns.values()]
    )

    if drop_column:
        meta = meta.drop(columns=partition_column)
    kwargs = {"integer_object_nulls": True} if null_kinds else {}
    df = from_pyarrow_table_dispatch(meta, table, self_destruct=True, **kwargs)
    for column, kinds in null_kinds.items():
        df[column] = df[column].astype(object)
        for code, value in enumerate((None, float("nan"), pd.NA, pd.NaT)):
            df.loc[kinds == code, column] = value
    reconciled_dtypes = {}
    for column, dtype in meta.dtypes.items():
        actual = df[column].dtype
        if actual == dtype:
            continue
        # Use the specific string dtype from meta (e.g., string[pyarrow])
        if isinstance(actual, pd.StringDtype) and isinstance(dtype, pd.StringDtype):
            reconciled_dtypes[column] = dtype
            continue
        # meta might not be aware of the actual categories so the two dtype objects are not equal
        # Also, the categories_dtype does not properly roundtrip through Arrow
        if isinstance(actual, pd.CategoricalDtype) and isinstance(
            dtype, pd.CategoricalDtype
        ):
            continue
        reconciled_dtypes[column] = find_common_type([actual, dtype])

    from dask.dataframe._compat import PANDAS_GE_300

    kwargs = {} if PANDAS_GE_300 else {"copy": False}
    return df.astype(reconciled_dtypes, **kwargs)


def buffers_to_table(data: list[tuple[int, bytes]]) -> pa.Table:
    import numpy as np
    import pyarrow as pa

    """Convert a list of arrow buffers and a schema to an Arrow Table"""

    def _create_input_partition_id_array(
        table: pa.Table, input_partition_id: int
    ) -> pa.ChunkedArray:
        arrays = (
            np.full(
                (batch.num_rows,),
                input_partition_id,
                dtype=np.uint32(),
            )
            for batch in table.to_batches()
        )
        return pa.chunked_array(arrays)

    tables = (
        (input_partition_id, deserialize_table(buffer))
        for input_partition_id, buffer in data
    )
    tables = (
        table.append_column(
            _INPUT_PARTITION_ID_COLUMN,
            _create_input_partition_id_array(table, input_partition_id),
        )
        for input_partition_id, table in tables
    )

    return concat_tables(tables)


def serialize_table(table: pa.Table) -> bytes:
    import pyarrow as pa

    stream = pa.BufferOutputStream()
    with pa.ipc.new_stream(stream, table.schema) as writer:
        writer.write_table(table)
    return stream.getvalue().to_pybytes()


def deserialize_table(buffer: bytes) -> pa.Table:
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(buffer)) as reader:
        return reader.read_all()


def read_from_disk(path: Path) -> tuple[list[pa.Table], int]:
    import pyarrow as pa

    batch_size = parse_bytes("1 MiB")
    batch = []
    shards = []

    with pa.OSFile(str(path), mode="rb") as f:
        size = f.seek(0, whence=2)
        f.seek(0)
        prev = 0
        offset = f.tell()
        while offset < size:
            sr = pa.RecordBatchStreamReader(f)
            shard = sr.read_all()
            offset = f.tell()
            batch.append(shard)

            if offset - prev >= batch_size:
                table = concat_tables(batch)
                shards.append(_copy_table(table))
                batch = []
                prev = offset
    if batch:
        table = concat_tables(batch)
        shards.append(_copy_table(table))
    return shards, size


def _copy_table(table: pa.Table) -> pa.Table:
    import pyarrow as pa

    arrs = [pa.concat_arrays(column.chunks) for column in table.columns]
    return pa.table(data=arrs, schema=table.schema)
