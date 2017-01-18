from __future__ import print_function, division, absolute_import

from .serialize import register_serialization

import pandas as pd
from io import BytesIO

import pyarrow as pa
import pyarrow.io as io
import pyarrow.ipc as ipc


def serialize_pandas_dataframe(df):
    batch = pa.RecordBatch.from_pandas(df)
    buf = BytesIO()
    writer = ipc.ArrowFileWriter(buf, batch.schema)
    writer.write_record_batch(batch)
    writer.close()
    frames = [buf.getvalue()]  # is there a memoryview we can pass instead?
    header = {}

    return header, frames


def deserialize_pandas_dataframe(header, frames):
    buf = frames[0]
    reader = ipc.ArrowFileReader(buf)
    return [reader.get_record_batch(i).to_pandas()
            for i in range(reader.num_record_batches)][0]  # what is each batch?


register_serialization(pd.DataFrame,
                       serialize_pandas_dataframe,
                       deserialize_pandas_dataframe)


def serialize_pandas_series(s):
    return serialize_pandas_dataframe(s.to_frame())  # unfortunate copy here


def deserialize_pandas_series(header, frames):
    df = deserialize_pandas_dataframe(header, frames)
    return df[df.columns[0]]  # unfortunate copy here


register_serialization(pd.Series,
                       serialize_pandas_series,
                       deserialize_pandas_series)
