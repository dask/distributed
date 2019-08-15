import pytest
from distributed.protocol import serialize, deserialize
import pandas as pd
import numpy as np
from dask.dataframe.utils import assert_eq


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize("y,y_serializer", [(np.arange(50), "dask"), (None, "pickle")])
def test_serialize_numpy_numpy(collection, y, y_serializer):
    x = np.arange(100)
    if issubclass(collection, dict):
        header, frames = serialize({"x": x, "y": y}, serializers=("dask", "pickle"))
    else:
        header, frames = serialize((x, y), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle", "error"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "dask"
    assert sub_headers[1]["serializer"] == y_serializer
    assert isinstance(t, collection)

    assert ((t["x"] if isinstance(t, dict) else t[0]) == x).all()
    if y is None:
        assert (t["y"] if isinstance(t, dict) else t[1]) is None
    else:
        assert ((t["y"] if isinstance(t, dict) else t[1]) == y).all()


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize(
    "df2,df2_serializer",
    [
        (pd.DataFrame({"C": ["a", "b", None], "D": [2.5, 3.5, 4.5]}), "pickle"),
        (None, "pickle"),
    ],
)
def test_serialize_pandas_pandas(collection, df2, df2_serializer):
    df1 = pd.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    if issubclass(collection, dict):
        header, frames = serialize(
            {"df1": df1, "df2": df2}, serializers=("dask", "pickle")
        )
    else:
        header, frames = serialize((df1, df2), serializers=("dask", "pickle"))
    t = deserialize(header, frames, deserializers=("dask", "pickle"))

    assert header["is-collection"] is True
    sub_headers = header["sub-headers"]
    assert sub_headers[0]["serializer"] == "pickle"
    assert sub_headers[1]["serializer"] == "pickle"
    assert isinstance(t, collection)

    assert_eq(t["df1"] if isinstance(t, dict) else t[0], df1)
    if df2 is None:
        assert (t["df2"] if isinstance(t, dict) else t[1]) is None
    else:
        assert_eq(t["df2"] if isinstance(t, dict) else t[1], df2)
