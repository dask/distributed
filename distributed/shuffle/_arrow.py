from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    import pyarrow as pa


def dump_table_batch(tables: list[pa.Table], file: BinaryIO) -> None:
    """
    Dump multiple tables to the file

    Note: This function appends to the file and dumps each table as an individual stream.
    This results in multiple end-of-stream signals in the file.

    See Also
    --------
    load_arrow
    """
    import pyarrow as pa

    for table in tables:
        with pa.ipc.new_stream(file, table.schema) as writer:
            writer.write_table(table)


def load_into_table(file: BinaryIO) -> pa.Table:
    """Load batched data written to file back out into a single table

    Example
    -------
    >>> tables = [pa.Table.from_pandas(df), pa.Table.from_pandas(df2)]  # doctest: +SKIP
    >>> with open("myfile", mode="wb") as f:  # doctest: +SKIP
    ...     for table in tables:  # doctest: +SKIP
    ...         dump_table_batch(tables, f, schema=t.schema)  # doctest: +SKIP

    >>> with open("myfile", mode="rb") as f:  # doctest: +SKIP
    ...     t = load_into_table(f)  # doctest: +SKIP

    See Also
    --------
    dump_table_batch
    """
    import pyarrow as pa

    tables = []
    try:
        while True:
            sr = pa.RecordBatchStreamReader(file)
            tables.append(sr.read_all())
    except pa.ArrowInvalid:
        return pa.concat_tables(tables)


def list_of_buffers_to_table(data: list[bytes], schema: pa.Schema) -> pa.Table:
    """Convert a list of arrow buffers and a schema to an Arrow Table"""
    import pyarrow as pa

    assert len(data) == 1
    with pa.ipc.open_stream(pa.py_buffer(data[0])) as reader:
        return reader.read_all()


def deserialize_schema(data: bytes) -> pa.Schema:
    """Deserialize an arrow schema

    Examples
    --------
    >>> b = schema.serialize()  # doctest: +SKIP
    >>> deserialize_schema(b)  # doctest: +SKIP

    See also
    --------
    pa.Schema.serialize
    """
    import io

    import pyarrow as pa

    bio = io.BytesIO()
    bio.write(data)
    bio.seek(0)
    sr = pa.RecordBatchStreamReader(bio)
    table = sr.read_all()
    bio.close()
    return table.schema


def serialize_table(table: pa.Table) -> bytes:
    import io

    import pyarrow as pa

    stream = io.BytesIO()
    with pa.ipc.new_stream(stream, table.schema) as writer:
        writer.write_table(table)
    return stream.getvalue()
