"""
Efficient serialization GPU arrays.
"""
import cupy
from .cuda import cuda_serialize, cuda_deserialize
from distutils.version import LooseVersion


class PatchedCudaArrayInterface(object):
    # TODO: This class wont be necessary
    #       once Cupy<7.0 is no longer supported
    def __init__(self, ary):
        vsn = LooseVersion(cupy.__version__)
        cai = ary.__cuda_array_interface__
        if vsn < "7.0.0" and cai.get("strides") is None:
            cai.pop("strides")
        self.__cuda_array_interface__ = cai


@cuda_serialize.register(cupy.ndarray)
def serialize_cupy_ndarray(x):
    # Making sure `x` is behaving
    if not x.flags.c_contiguous:
        x = cupy.array(x, copy=True)

    header = x.__cuda_array_interface__.copy()
    return header, [x]


@cuda_deserialize.register(cupy.ndarray)
def deserialize_cupy_array(header, frames):
    (frame,) = frames
    arr = cupy.ndarray(
        header["shape"],
        dtype=header["typestr"],
        memptr=cupy.asarray(PatchedCudaArrayInterface(frame)).data,
    )
    return arr
