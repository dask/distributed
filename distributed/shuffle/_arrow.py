from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    import pyarrow as pa


def dump_shards(shards: list[pa.Table], file: BinaryIO) -> None:
    """
    Write multiple shard tables to the file

    Note: This function appends to the file and dumps each table as an individual stream.
    This results in multiple end-of-stream signals in the file.

    See Also
    --------
    load_partition
    """
    import pyarrow as pa

    for table in shards:
        with pa.ipc.new_stream(file, table.schema) as writer:
            writer.write_table(table)


def load_partition(file: BinaryIO) -> pa.Table:
    """Load partition data written to file back out into a single table

    Example
    -------
    >>> tables = [pa.Table.from_pandas(df), pa.Table.from_pandas(df2)]  # doctest: +SKIP
    >>> with open("myfile", mode="wb") as f:  # doctest: +SKIP
    ...     for table in tables:  # doctest: +SKIP
    ...         dump_shards(tables, f)  # doctest: +SKIP

    >>> with open("myfile", mode="rb") as f:  # doctest: +SKIP
    ...     t = load_partition(f)  # doctest: +SKIP

    See Also
    --------
    dump_shards
    """
    import pyarrow as pa

    shards = []
    try:
        while True:
            sr = pa.RecordBatchStreamReader(file)
            shards.append(sr.read_all())
    # Since we write multiple streams to the same file, we have to read until
    # there is nothing to read anymore. At that point, pa.ArrowInvalid is raised
    except pa.ArrowInvalid as e:
        if str(e) == "Tried reading schema message, was null or length 0":
            return pa.concat_tables(shards)
        raise


def list_of_buffers_to_table(data: list[bytes]) -> pa.Table:
    """Convert a list of arrow buffers and a schema to an Arrow Table"""
    import pyarrow as pa

    tables = []
    for buffer in data:
        tables.append(deserialize_table(buffer))
    return pa.concat_tables(tables)


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


def deserialize_table(buffer: bytes) -> pa.Table:
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(buffer)) as reader:
        return reader.read_all()
