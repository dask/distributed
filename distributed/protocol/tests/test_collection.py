import pytest
from distributed.protocol import serialize, deserialize
import pandas as pd
import numpy as np
from dask.dataframe.utils import assert_eq


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize(
    "y,y_serializer",
    [
        (np.arange(50), "dask"),
        (pd.DataFrame({"C": ["a", "b", None], "D": [2.5, 3.5, 4.5]}), "pickle"),
        (None, "pickle"),
    ],
)
def test_serialize_collection(collection, y, y_serializer):
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
        if isinstance(y, pd.DataFrame):
            assert_eq(t["y"] if isinstance(t, dict) else t[1], y)
        else:
            assert ((t["y"] if isinstance(t, dict) else t[1]) == y).all()


def test_large_collections_serialize_simply():
    header, frames = serialize(tuple(range(1000)))
    assert len(frames) == 1


def test_nested_types():
    x = np.ones(5)
    header, frames = serialize([[[x]]])
    assert "dask" in str(header)
    assert len(frames) == 1
    assert x.data in frames
