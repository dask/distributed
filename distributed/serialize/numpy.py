from __future__ import print_function, division, absolute_import

import numpy as np

try:
    import blosc
except ImportError:
    blosc = False

from ..protocol import frame_split_size
from .core import register_serialization


def serialize_numpy_ndarray(x):
    header = {'dtype': x.dtype.str,
              'strides': x.strides,
              'shape': x.shape}

    if blosc:
        frames = frame_split_size([x.data])
        frames = [blosc.compress(frame, typesize=x.dtype.itemsize,
                                 cname='lz4', clevel=5) for frame in frames]
        header['compression'] = ['blosc'] * len(frames)
    else:
        frames = [x.data]

    return header, frames


def deserialize_numpy_ndarray(header, frames):
    assert len(frames) == 1

    dt = header['dtype']
    if dt.startswith('['):
        dt = np.dtype(eval(dt))  # dangerous
    else:
        dt = np.dtype(dt)

    buffer = frames[0]

    x = np.frombuffer(buffer, dt)
    x = np.lib.stride_tricks.as_strided(x, header['shape'], header['strides'])

    return x


register_serialization(np.ndarray, serialize_numpy_ndarray, deserialize_numpy_ndarray)
