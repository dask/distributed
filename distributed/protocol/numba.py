import numba.cuda
from .serialize import dask_serialize, dask_deserialize


@dask_serialize.register(numba.cuda.devicearray.DeviceNDArray)
def serialize_numba_ndarray(x):
    # TODO: handle non-contiguous
    # TODO: handle 2d
    # TODO: 0d

    if x.flags['C_CONTIGUOUS'] or x.flags['F_CONTIGUOUS']:
        strides = x.strides
        if x.ndim > 1:
            data = x.ravel()  # order='K'
        else:
            data = x
    else:
        raise ValueError("Array must be contiguous")
        x = numba.ascontiguousarray(x)
        strides = x.strides
        if x.ndim > 1:
            data = x.ravel()
        else:
            data = x

    dtype = (0, x.dtype.str)
    nbytes = data.dtype.itemsize * data.size

    header = x.__cuda_array_interface__.copy()
    header['lengths'] = (nbytes,)  # one per stride
    header['compression'] = (None,)  # TODO
    header['is_cuda'] = 1
    header['dtype'] = dtype
    return header, [data]


@dask_deserialize.register(numba.cuda.devicearray.DeviceNDArray)
def deserialize_numba_ndarray(header, frames):
    frame, = frames
    # TODO: put this in ucx... as a kind of "fixup"
    frame.typestr = header['typestr']
    frame.shape = header['shape']
    arr = numba.cuda.as_cuda_array(frame)
    return arr
