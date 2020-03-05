"""
Efficient serialization GPU arrays.
"""
import pickle
import cupy

from cupyx.scipy.sparse import spmatrix
from cupyx.scipy.sparse import csr_matrix, csc_matrix, coo_matrix, dia_matrix

from .cuda import cuda_deserialize, cuda_serialize
from .serialize import dask_deserialize, dask_serialize


try:
    from .rmm import dask_deserialize_rmm_device_buffer as dask_deserialize_cuda_buffer
except ImportError:
    from .numba import dask_deserialize_numba_array as dask_deserialize_cuda_buffer


# lookup used for pulling out underlying CuPy bufffers
data_lookup = {
    csr_matrix: ["data", "indices", "indptr"],
    coo_matrix: ["data", "row", "col"],
    dia_matrix: ["data", "offsets"],
}
data_lookup[csc_matrix] = data_lookup[csr_matrix]


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


@cuda_serialize.register(spmatrix)
def cuda_serialize_cupy_sparse(_x):
    frames = []
    all_headers = {}
    all_headers["shape"] = _x.shape  # shape of full sparse data
    for typ in data_lookup[type(_x)]:
        x = getattr(_x, typ)
        # Making sure `x` is behaving
        if not (x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]):
            x = cupy.array(x, copy=True)

        header = x.__cuda_array_interface__.copy()
        header["strides"] = tuple(x.strides)
        frames.append(
            cupy.ndarray(
                shape=(x.nbytes,), dtype=cupy.dtype("u1"), memptr=x.data, strides=(1,)
            )
        )
        all_headers[typ] = header

    return all_headers, frames


@cuda_deserialize.register(spmatrix)
def cuda_deserialize_cupy_sparse(headers, frames):
    _sparse_data = []
    obj_typ = pickle.loads(headers["type-serialized"])
    for idx, typ in enumerate(data_lookup[obj_typ]):
        frame = frames[idx]
        header = headers[typ]
        if not isinstance(frame, cupy.ndarray):
            frame = PatchedCudaArrayInterface(frame)

        arr = cupy.ndarray(
            shape=header["shape"],
            dtype=header["typestr"],
            memptr=cupy.asarray(frame).data,
            strides=header["strides"],
        )
        _sparse_data.append(arr)

    if obj_typ is coo_matrix:
        data = _sparse_data[0]
        row = _sparse_data[1]
        col = _sparse_data[2]
        return obj_typ((data, (row, col)))
    elif obj_typ is dia_matrix:
        data = _sparse_data[0]
        offsets = _sparse_data[1]
        shape = headers["shape"]
        return obj_typ((data, offsets), shape=shape)
    else:
        return obj_typ(tuple(_sparse_data))


@dask_serialize.register(spmatrix)
def dask_serialize_cupy_sparse(x):
    header, frames = cuda_serialize_cupy_sparse(x)
    # To convert CuPy sparse matrices to SciPy, use get method of each CuPy
    # sparse matrix class.
    frames = [memoryview(f.get()) for f in frames]
    return header, frames


@dask_deserialize.register(spmatrix)
def dask_deserialize_cupy_sparse(headers, frames):
    _sparse_data = []
    obj_typ = pickle.loads(headers["type-serialized"])
    for idx, typ in enumerate(data_lookup[obj_typ]):
        frame = frames[idx]
        header = headers[typ]
        frame = [dask_deserialize_cuda_buffer(header, [frame])]
        arr = cuda_deserialize_cupy_ndarray(header, frame)
        _sparse_data.append(arr)
    obj_typ = pickle.loads(headers["type-serialized"])
    if obj_typ is coo_matrix:
        data = _sparse_data[0]
        row = _sparse_data[1]
        col = _sparse_data[2]
        return obj_typ((data, (row, col)))
    elif obj_typ is dia_matrix:
        data = _sparse_data[0]
        offsets = _sparse_data[1]
        shape = headers["shape"]
        return obj_typ((data, offsets), shape=shape)
    else:
        return obj_typ(tuple(_sparse_data))


@cuda_serialize.register(cupy.ndarray)
def cuda_serialize_cupy_ndarray(x):
    # Making sure `x` is behaving
    if not (x.flags["C_CONTIGUOUS"] or x.flags["F_CONTIGUOUS"]):
        x = cupy.array(x, copy=True)

    header = x.__cuda_array_interface__.copy()
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
