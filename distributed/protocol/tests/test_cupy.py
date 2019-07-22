from distributed.protocol import serialize, deserialize
import numpy as np
import pytest

cupy = pytest.importorskip("cupy")


def test_serialize_cupy():
    x = cupy.arange(100)
    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    assert type(frames[0]) == cupy.ndarray
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))
    assert type(y) == cupy.ndarray

    assert (x == y).all()


def test_serialize_cupy_host():
    x = cupy.arange(100)
    header, frames = serialize(x, serializers=("cuda_host", "dask", "pickle"))
    assert type(frames[0]) == np.ndarray
    y = deserialize(header, frames, deserializers=("cuda_host", "dask", "pickle", "error"))
    assert type(y) == cupy.ndarray

    assert (x == y).all()
