"""
Efficient serialization GPU arrays.
"""
import cupy
from .serialize import dask_serialize, dask_deserialize

# Some questions
# 1.Do we need *protocol-dependent* serialization?
#   I assume we want this kind of serialization only when
#   in UCP.
# 2. What does ucp-py need to know about?


@dask_serialize.register(cupy.ndarray)
def serialize_cupy_ndarray(x):
    # TODO: handle non-contiguous
    # shape
    # typestr
    # descr
    # data
    # version
    # strides (noncontiguous-only)
    header = x.__cuda_array_interface__.copy()
    header['device'] = x.device.id
    header['lengths'] = (x.nbytes,)  # one per stride
    header['compression'] = (None,)  # TODO
    # TODO: I don't think ucx-py should have to worry about
    # MemoryPointer. Maybe some thin wrapper.
    return header, [x.data]


@dask_deserialize.register(cupy.ndarray)
def deserialize_cupy_array(header, frames):
    # MemoryPointer { PoolMemory, offset }
    frame, = frames
    arr = cupy.ndarray(header['shape'], dtype=header['typestr'],
                       memptr=frame)
    return arr
