import rmm
from .cuda import cuda_serialize, cuda_deserialize


# Used for RMM 0.11.0+ otherwise Numba serializers used
if hasattr(rmm, "DeviceBuffer"):
    @cuda_serialize.register(rmm.DeviceBuffer)
    def serialize_rmm_device_buffer(x):
        header = x.__cuda_array_interface__.copy()
        frames = [x]
        return header, frames


    @cuda_deserialize.register(rmm.DeviceBuffer)
    def deserialize_rmm_device_buffer(header, frames):
        (arr,) = frames

        # Copy data into new `DeviceBuffer` if needed
        if not isinstance(arr, rmm.DeviceBuffer):
            (ptr, _) = arr.__cuda_array_interface__["data"]
            (size,) = header["shape"]
            arr = rmm.DeviceBuffer(ptr=ptr, size=size)

        return arr
