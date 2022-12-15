from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    import pyarrow as pa


def dump_table(table: pa.Table, file: BinaryIO) -> None:
    """
    Dump a table to file

    Note: This function appends to the file and signals end-of-stream when done.
    This results in multiple end-of-stream signals in a stream.

    See Also
    --------
    load_arrow
    """
    import pyarrow as pa

    with pa.ipc.new_stream(file, table.schema) as writer:
        writer.write_table(table)


def load_arrow(file: BinaryIO) -> pa.Table:
    """Load batched data written to file back out into a table again

    Example
    -------
    >>> t = pa.Table.from_pandas(df)  # doctest: +SKIP
    >>> with open("myfile", mode="wb") as f:  # doctest: +SKIP
    ...     for batch in t.to_batches():  # doctest: +SKIP
    ...         dump_batch(batch, f, schema=t.schema)  # doctest: +SKIP

    >>> with open("myfile", mode="rb") as f:  # doctest: +SKIP
    ...     t = load_arrow(f)  # doctest: +SKIP

    See Also
    --------
    dump_batch
    """
    import pyarrow as pa

    try:
        sr = pa.RecordBatchStreamReader(file)
        return sr.read_all()
    except Exception:
        raise EOFError


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
