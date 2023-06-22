from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

from packaging.version import parse

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


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


def check_minimal_arrow_version() -> None:
    """Verify that the the correct version of pyarrow is installed to support
    the P2P extension.

    Raises a RuntimeError in case pyarrow is not installed or installed version
    is not recent enough.
    """
    # First version to introduce Table.sort_by
    minversion = "7.0.0"
    try:
        import pyarrow as pa
    except ImportError:
        raise RuntimeError(f"P2P shuffling requires pyarrow>={minversion}")

    if parse(pa.__version__) < parse(minversion):
        raise RuntimeError(
            f"P2P shuffling requires pyarrow>={minversion} but only found {pa.__version__}"
        )


def convert_partition(data: bytes, meta: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd
    import pyarrow as pa

    file = BytesIO(data)
    end = len(data)
    shards = []
    while file.tell() < end:
        sr = pa.RecordBatchStreamReader(file)
        shards.append(sr.read_all())
    table = pa.concat_tables(shards, promote=True)
    df = table.to_pandas(self_destruct=True)

    def default_types_mapper(pyarrow_dtype: pa.DataType) -> object:
        # Avoid converting strings from `string[pyarrow]` to `string[python]`
        # if we have *some* `string[pyarrow]`
        if (
            pyarrow_dtype in {pa.large_string(), pa.string()}
            and pd.StringDtype("pyarrow") in meta.dtypes.values
        ):
            return pd.StringDtype("pyarrow")
        return None

    df = table.to_pandas(self_destruct=True, types_mapper=default_types_mapper)
    return df.astype(meta.dtypes, copy=False)


def list_of_buffers_to_table(data: list[bytes]) -> pa.Table:
    """Convert a list of arrow buffers and a schema to an Arrow Table"""
    import pyarrow as pa

    return pa.concat_tables(deserialize_table(buffer) for buffer in data)


def serialize_table(table: pa.Table) -> bytes:
    import io

    import pyarrow as pa

    stream = io.BytesIO()
    with pa.ipc.new_stream(stream, table.schema) as writer:
        writer.write_table(table)
    return stream.getvalue()


def deserialize_table(buffer: bytes) -> pa.Table:
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(buffer)) as reader:
        return reader.read_all()
