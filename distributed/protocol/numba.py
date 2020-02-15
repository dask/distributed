import numba.cuda
import numpy as np

from .cuda import cuda_deserialize, cuda_serialize
from .serialize import dask_deserialize, dask_serialize

try:
    from .rmm import dask_deserialize_rmm_device_buffer
except ImportError:
    dask_deserialize_rmm_device_buffer = None


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def cuda_serialize_numba_ndarray(x):
    # Making sure `x` is behaving
    if not x.is_c_contiguous():
        shape = x.shape
        t = numba.cuda.device_array(shape, dtype=x.dtype)
        t.copy_to_device(x)
        x = t
    header = x.__cuda_array_interface__.copy()
    return header, [x]


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def cuda_deserialize_numba_ndarray(header, frames):
    (frame,) = frames
    shape = header["shape"]
    strides = header["strides"]

    # Starting with __cuda_array_interface__ version 2, strides can be None,
    # meaning the array is C-contiguous, so we have to calculate it.
    if strides is None:
        itemsize = np.dtype(header["typestr"]).itemsize
        strides = tuple((np.cumprod((1,) + shape[:0:-1]) * itemsize).tolist())

    arr = numba.cuda.devicearray.DeviceNDArray(
        shape,
        strides,
        np.dtype(header["typestr"]),
        gpu_data=numba.cuda.as_cuda_array(frame).gpu_data,
    )
    return arr


@dask_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def dask_serialize_numba_ndarray(x):
    header, frames = cuda_serialize_numba_ndarray(x)
    frames = [memoryview(f.copy_to_host()) for f in frames]
    return header, frames


@dask_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def dask_deserialize_numba_array(header, frames):
    if dask_deserialize_rmm_device_buffer:
        frames = [dask_deserialize_rmm_device_buffer(header, frames)]
    else:
        frames = [numba.cuda.to_device(np.asarray(memoryview(f))) for f in frames]

    arr = cuda_deserialize_numba_ndarray(header, frames)
    return arr
