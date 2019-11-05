"""
Efficient serialization GPU arrays.
"""
import cupy
from .cuda import cuda_serialize, cuda_deserialize
from distutils.version import LooseVersion


class PatchedDeviceArray(object):
    # TODO: This class wont be necessary
    #       once Cupy<7.0 is no longer supported
    def __init__(self, ary):
        self.parent = ary
        self.vsn = LooseVersion(cupy.__version__)

    def __getattr__(self, name):
        if name == "parent":
            raise AttributeError()
        if self.vsn >= "7.0.0" or name != "__cuda_array_interface__":
            return getattr(self.parent, name)
        else:
            # Cupy<7.0 cannot handle
            # __cuda_array_interface__['strides'] == None
            rtn = self.parent.__cuda_array_interface__
            if rtn.get("strides") is None:
                rtn.pop("strides")
            return rtn


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
        memptr=cupy.asarray(PatchedDeviceArray(frame)).data,
    )
    return arr
