import pytest

from dask.dataframe.utils import assert_eq

from distributed.protocol import dumps, loads


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize("y,y_serializer", [(50, "cuda"), (None, "pickle")])
def test_serialize_cupy(collection, y, y_serializer):
    cupy = pytest.importorskip("cupy")

    x = cupy.arange(100)
    if y is not None:
        y = cupy.arange(y)
    if issubclass(collection, dict):
        frames = dumps({"x": x, "y": y}, serializers=("cuda", "dask", "pickle"))
    else:
        frames = dumps((x, y), serializers=("cuda", "dask", "pickle"))

    assert any(isinstance(f, cupy.ndarray) for f in frames)

    t = loads(frames, deserializers=("cuda", "dask", "pickle", "error"))
    assert ((t["x"] if isinstance(t, dict) else t[0]) == x).all()
    if y is None:
        assert (t["y"] if isinstance(t, dict) else t[1]) is None
    else:
        assert ((t["y"] if isinstance(t, dict) else t[1]) == y).all()


@pytest.mark.parametrize("collection", [tuple, dict])
@pytest.mark.parametrize(
    "df2,df2_serializer",
    [({"C": [3, 4, 5], "D": [2.5, 3.5, 4.5]}, "cuda"), (None, "pickle")],
)
def test_serialize_pandas_pandas(collection, df2, df2_serializer):
    cudf = pytest.importorskip("cudf")
    pd = pytest.importorskip("pandas")
    df1 = cudf.DataFrame({"A": [1, 2, None], "B": [1.0, 2.0, None]})
    if df2 is not None:
        df2 = cudf.from_pandas(pd.DataFrame(df2))
    if issubclass(collection, dict):
        frames = dumps({"df1": df1, "df2": df2}, serializers=("cuda", "dask", "pickle"))
    else:
        frames = dumps((df1, df2), serializers=("cuda", "dask", "pickle"))
    assert any(isinstance(f, cudf.core.buffer.Buffer) for f in frames)

    t = loads(frames, deserializers=("cuda", "dask", "pickle"))
    assert_eq(t["df1"] if isinstance(t, dict) else t[0], df1)
    if df2 is None:
        assert (t["df2"] if isinstance(t, dict) else t[1]) is None
    else:
        assert_eq(t["df2"] if isinstance(t, dict) else t[1], df2)
