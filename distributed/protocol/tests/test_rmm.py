from distributed.protocol import serialize, deserialize
import pickle
import pytest

numpy = pytest.importorskip("numpy")
cuda = pytest.importorskip("numba.cuda")
rmm = pytest.importorskip("rmm")


@pytest.mark.parametrize("size", [0, 3, 10])
def test_serialize_rmm_device_buffer(size):
    DeviceBuffer = getattr(rmm, "DeviceBuffer", None)
    if not DeviceBuffer:
        pytest.skip("RMM pre-0.11.0 does not have DeviceBuffer")

    x_np = numpy.arange(size, dtype="u1")
    x = DeviceBuffer(size=size)
    cuda.to_device(x_np, to=cuda.as_cuda_array(x))

    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))
    y_np = y.copy_to_host()

    assert (x_np == y_np).all()


@pytest.mark.parametrize("size", [0, 3, 10])
def test_serialize_rmm_device_buffer_from_numba_array(size):
    DeviceBuffer = getattr(rmm, "DeviceBuffer", None)
    if not DeviceBuffer:
        pytest.skip("RMM pre-0.11.0 does not have DeviceBuffer")

    x_np = numpy.arange(size, dtype="u1")
    x = cuda.to_device(x_np)

    header, frames = serialize(x, serializers=("cuda", "dask", "pickle"))
    header["type-serialized"] = pickle.dumps(DeviceBuffer)
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))
    y_np = cuda.as_cuda_array(y).copy_to_host()

    assert (x_np == y_np).all()
