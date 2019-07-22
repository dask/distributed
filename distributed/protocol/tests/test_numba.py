from distributed.protocol import serialize, deserialize
import numpy as np
import pytest

cuda = pytest.importorskip("numba.cuda")


def test_serialize_numba():
    x = cuda.to_device(np.arange(100))
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    assert type(frames[0]) == cuda.cudadrv.devicearray.DeviceNDArray
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))
    assert type(y) == cuda.cudadrv.devicearray.DeviceNDArray

    assert (x.copy_to_host() == y.copy_to_host()).all()


def test_serialize_numba_host():
    x = cuda.to_device(np.arange(100))
    header, frames = serialize(x, serializers=("cuda_host", "dask", "pickle"))
    assert type(frames[0]) == np.ndarray
    y = deserialize(header, frames, deserializers=("cuda_host", "dask", "pickle", "error"))
    assert type(y) == cuda.cudadrv.devicearray.DeviceNDArray

    assert (x.copy_to_host() == y.copy_to_host()).all()
