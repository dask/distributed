import numpy as np
import numba.cuda
from .cuda import cuda_serialize, cuda_deserialize


@cuda_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def serialize_numba_ndarray(x):
    header = x.__cuda_array_interface__.copy()

    # Making sure `x` is behaving
    if x.is_c_contiguous():
        x = numba.cuda.devicearray.DeviceNDArray(
            (int(np.product(x.shape)),),
            (x.dtype.itemsize,),
            x.dtype,
            gpu_data=x.gpu_data,
        )
    elif x.is_f_contiguous():
        header["strides"] = header["strides"][::-1]
        x = numba.cuda.devicearray.DeviceNDArray(
            (int(np.product(x.shape)),),
            (x.dtype.itemsize,),
            x.dtype,
            gpu_data=x.gpu_data,
        )
    else:
        shape = x.shape
        t = numba.cuda.device_array(shape, dtype=x.dtype, order="C")
        t.copy_to_device(x)
        x = t
        header["strides"] = tuple(x.strides)

    return header, [x]


@cuda_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def deserialize_numba_ndarray(header, frames):
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
