from __future__ import print_function, division, absolute_import

import pickle
import sys

import pandas as pd

from .serialize import register_serialization, serialize, deserialize


def serialize_pandas_dataframe(df):
    head = pickle.dumps(df.head(0))
    headers = []
    framess = []
    compression = [None]
    lengths = [len(head)]
    for column in df.columns:
        x = df[column].values
        header, frames = serialize(x)
        headers.append(header)
        framess.append(frames)
        compression.extend(header.pop('compression', [None] * len(frames)))
        lengths.extend(header.pop('lengths', [len(f) for f in frames]))


    # TODO: avoid if trivial index
    x = df.index.values
    index_header, index_frames = serialize(x)
    compression.extend(index_header.pop('compression',
                                        [None] * len(index_frames)))
    lengths.extend(index_header.pop('lengths', [len(f) for f in index_frames]))

    framess.append(index_frames)

    header = {'frame-counts': [len(f) for f in framess],
              'headers': headers,
              'index-header': index_header,
              'lengths': lengths}

    if any(compression):
        header['compression'] = compression

    return header, [head] + sum(framess, [])


def deserialize_pandas_dataframe(header, frames):
    head = pickle.loads(frames[0])
    n = 1
    d = {}
    for column, h, count in zip(head.columns, header['headers'], header['frame-counts']):
        x = deserialize(h, frames[n:n + count])
        n += count
        d[column] = x

    index = deserialize(header['index-header'], frames[n:])

    df = pd.DataFrame(d, columns=head.columns, index=index)
    df.index.name = head.index.name
    return df


def serialize_pandas_series(s):
    value_header, value_frames = serialize(s.values)
    index_header, index_frames = serialize(s.index)

    compression = []
    lengths = []
    for h, f in [(value_header, value_frames), (index_header, index_frames)]:
        compression.extend(h.pop('compression', [None] * len(f)))
        lengths.extend(h.pop('lengths', [len(ff) for ff in f]))

    header = {'name': s.name,
              'value-header': value_header,
              'index-header': index_header,
              'n_value_frames': len(value_frames),
              'index-name': s.index.name,
              'lengths': lengths}

    if any(compression):
        header['compression'] = compression

    return header, value_frames + index_frames


def deserialize_pandas_series(header, frames):
    values = deserialize(header['value-header'], frames[:header['n_value_frames']])
    index = deserialize(header['index-header'], frames[header['n_value_frames']:])

    return pd.Series(values,
                     name=header['name'],
                     index=pd.Index(index, name=header['index-name']))


register_serialization(pd.DataFrame,
                       serialize_pandas_dataframe,
                       deserialize_pandas_dataframe)

register_serialization(pd.Series,
                       serialize_pandas_series,
                       deserialize_pandas_series)
