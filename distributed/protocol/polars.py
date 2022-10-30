from __future__ import annotations

import io

import polars as pl
import pyarrow

from distributed.protocol.serialize import dask_deserialize, dask_serialize


@dask_serialize.register(pl.DataFrame)
def serialize_pl(df):
    sink = pyarrow.BufferOutputStream()
    df.write_ipc(sink)
    buf = sink.getvalue()
    header = {}
    frames = [buf]
    return header, frames


@dask_deserialize.register(pl.DataFrame)
def deserialize_pl(header, frames):
    blob = frames[0]
    return pl.read_ipc(pyarrow.BufferReader(blob))


@dask_serialize.register(pl.LazyFrame)
def serialize_pl_lazy(ldf):
    with io.BytesIO() as sink:
        ldf.write_json(sink)
        sink.seek(0)
        buf = sink.read()
    header = {}
    frames = [buf]
    return header, frames


@dask_deserialize.register(pl.LazyFrame)
def deserialize_pl_lazy(header, frames):
    blob = str(frames[0], "utf-8")
    return pl.LazyFrame.from_json(blob)
