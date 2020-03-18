"""
Efficient serialization GPU arrays.
"""
import copyreg

import cupy

from .cuda import cuda_deserialize, cuda_serialize
from .serialize import dask_deserialize, dask_serialize, register_generic

try:
    from .rmm import dask_deserialize_rmm_device_buffer as dask_deserialize_cuda_buffer
except ImportError:
    from .numba import dask_deserialize_numba_array as dask_deserialize_cuda_buffer


class PatchedCudaArrayInterface:
    """This class does one thing:
        1) Makes sure that the cuda context is active
           when deallocating the base cuda array.
        Notice, this is only needed when the array to deserialize
        isn't a native cupy array.
    """

    def __init__(self, ary):
        self.__cuda_array_interface__ = ary.__cuda_array_interface__
        # Save a ref to ary so it won't go out of scope
        self.base = ary

    def __del__(self):
        # Making sure that the cuda context is active
        # when deallocating the base cuda array
        try:
            import numba.cuda

            numba.cuda.current_context()
        except ImportError:
            pass
        del self.base


@cuda_serialize.register(cupy.ndarray)
def cuda_serialize_cupy_ndarray(x):
    # Making sure `x` is behaving
    if not (x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]):
        x = cupy.array(x, copy=True)

    header = x.__cuda_array_interface__.copy()
    # note: when compression is not set dask may split the frame into muliple chunks
    # dumps() in dask/protocol/core.py
    header["compression"] = False
    header["strides"] = tuple(x.strides)
    frames = [
        cupy.ndarray(
            shape=(x.nbytes,), dtype=cupy.dtype("u1"), memptr=x.data, strides=(1,)
        )
    ]

    return header, frames


@cuda_deserialize.register(cupy.ndarray)
def cuda_deserialize_cupy_ndarray(header, frames):
    (frame,) = frames
    if not isinstance(frame, cupy.ndarray):
        frame = PatchedCudaArrayInterface(frame)
    arr = cupy.ndarray(
        shape=header["shape"],
        dtype=header["typestr"],
        memptr=cupy.asarray(frame).data,
        strides=header["strides"],
    )
    return arr


@dask_serialize.register(cupy.ndarray)
def dask_serialize_cupy_ndarray(x):
    header, frames = cuda_serialize_cupy_ndarray(x)
    frames = [memoryview(cupy.asnumpy(f)) for f in frames]
    return header, frames


@dask_deserialize.register(cupy.ndarray)
def dask_deserialize_cupy_ndarray(header, frames):
    frames = [dask_deserialize_cuda_buffer(header, frames)]
    arr = cuda_deserialize_cupy_ndarray(header, frames)
    return arr


try:
    from cupy.cusparse import MatDescriptor
    from cupyx.scipy.sparse import spmatrix
except ImportError:
    MatDescriptor = None
    spmatrix = None


if MatDescriptor is not None:

    def reduce_matdescriptor(other):
        # Pickling MatDescriptor errors
        # xref: https://github.com/cupy/cupy/issues/3061
        return cupy.cusparse.MatDescriptor.create, ()

    copyreg.pickle(MatDescriptor, reduce_matdescriptor)

    @cuda_serialize.register(MatDescriptor)
    @dask_serialize.register(MatDescriptor)
    def serialize_cupy_matdescriptor(x):
        header, frames = {}, []
        return header, frames

    @cuda_deserialize.register(MatDescriptor)
    @dask_deserialize.register(MatDescriptor)
    def deserialize_cupy_matdescriptor(header, frames):
        return MatDescriptor.create()


if spmatrix is not None:
    for n, s, d in [
        ("cuda", cuda_serialize, cuda_deserialize),
        ("dask", dask_serialize, dask_deserialize),
    ]:
        register_generic(spmatrix, n, s, d)
