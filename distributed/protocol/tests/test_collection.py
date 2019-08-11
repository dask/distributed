from distributed.protocol import serialize, deserialize
import pandas as pd
import numpy as np
from dask.dataframe.utils import assert_eq


def test_serialize_cupy_cupy_tuple():
    x = np.arange(100)
    header, frames = serialize((x, x), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "dask"
    assert sub_headers[1]["serializer"] == "dask"
    assert isinstance(t, tuple)

    assert (t[0] == x).all()
    assert (t[1] == x).all()


def test_serialize_cupy_none_tuple():
    x = np.arange(100)
    header, frames = serialize((x, None), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "dask"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, tuple)

    assert (t[0] == x).all()
    assert t[1] is None


def test_serialize_cudf_cudf_tuple():
    df = pd.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    header, frames = serialize((df, df), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "pickle"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, tuple)

    assert_eq(t[0], df)
    assert_eq(t[1], df)


def test_serialize_cudf_none_tuple():
    df = pd.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    header, frames = serialize((df, None), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "pickle"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, tuple)

    assert_eq(t[0], df)
    assert t[1] is None
