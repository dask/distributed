from distributed.protocol import serialize, deserialize
from dask.dataframe.utils import assert_eq
import pytest


def test_serialize_cupy_cupy_tuple():
    cupy = pytest.importorskip("cupy")
    x = cupy.arange(100)
    header, frames = serialize((x, x), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == "cuda"
    assert isinstance(t, tuple)

    assert (t[0] == x).all()
    assert (t[1] == x).all()


def test_serialize_cupy_none_tuple():
    cupy = pytest.importorskip("cupy")
    x = cupy.arange(100)
    header, frames = serialize((x, None), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, tuple)

    assert (t[0] == x).all()
    assert t[1] is None


def test_serialize_cudf_cudf_tuple():
    cudf = pytest.importorskip("cudf")

    df = cudf.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    header, frames = serialize((df, df), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == "cuda"
    assert isinstance(t, tuple)

    assert_eq(t[0], df)
    assert_eq(t[1], df)


def test_serialize_cudf_none_tuple():
    cudf = pytest.importorskip("cudf")

    df = cudf.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    header, frames = serialize((df, None), serializers=("cuda", "dask", "pickle"))
    t = deserialize(header, frames, deserializers=("cuda", "dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "cuda"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, tuple)

    assert_eq(t[0], df)
    assert t[1] is None
