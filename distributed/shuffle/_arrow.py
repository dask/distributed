from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    import pyarrow as pa


def dump_batch(batch: pa.Buffer, file: BinaryIO, schema: pa.Schema) -> None:
    """
    Dump a batch to file, if we're the first, also write the schema

    This function is with respect to the open file object

    See Also
    --------
    load_arrow
    """
    if file.tell() == 0:
        file.write(schema.serialize())
    file.write(batch)


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
