from __future__ import print_function, division, absolute_import

import numpy as np

try:
    import blosc
    n = blosc.set_nthreads(2)
except ImportError:
    blosc = False

from ..protocol import frame_split_size
from .core import register_serialization


def serialize_numpy_ndarray(x):
    if x.dtype.kind == 'V':
        dt = x.dtype.descr
    else:
        dt = x.dtype.str

    x = np.ascontiguousarray(x)

    header = {'dtype': dt,
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

    dt = np.dtype(header['dtype'])

    buffer = frames[0]

    x = np.frombuffer(buffer, dt)
    x = np.lib.stride_tricks.as_strided(x, header['shape'], header['strides'])

    return x


register_serialization(np.ndarray, serialize_numpy_ndarray, deserialize_numpy_ndarray)
