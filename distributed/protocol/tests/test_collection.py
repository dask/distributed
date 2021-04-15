import pytest

from distributed.protocol import deserialize, serialize

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")


@pytest.mark.parametrize("collection", [tuple, dict, list])
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
        header, frames = serialize(collection((x, y)), serializers=("dask", "pickle"))
    frames = tuple(frames)  # verify that no mutation occurs
    t = deserialize(header, frames, deserializers=("dask", "pickle", "error"))
    assert isinstance(t, collection)

    if collection is dict:
        assert (t["x"] == x).all()
        assert str(t["y"]) == str(y)
    else:
        assert (t[0] == x).all()
        assert str(t[1]) == str(y)


def test_large_collections_serialize_simply():
    header, frames = serialize(tuple(range(1000)))
    assert len(frames) == 1
