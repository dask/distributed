import fsspec
import msgpack
import pytest
import yaml

from distributed.cluster_dump import _tuple_to_list, write_state
from distributed.utils_test import gen_test


@pytest.mark.parametrize(
    "input, expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ((1, 2, 3), [1, 2, 3]),
        ({"x": (1, (2,))}, {"x": [1, [2]]}),
        ("foo", "foo"),
    ],
)
def test_tuple_to_list(input, expected):
    assert _tuple_to_list(input) == expected


async def get_state():
    return {"foo": "bar", "list": ["a"], "tuple": (1, "two", 3)}


@gen_test()
async def test_write_state_msgpack(tmp_path):
    path = str(tmp_path / "bar")
    await write_state(get_state(), path, "msgpack")

    with fsspec.open(f"{path}.msgpack.gz", "rb", compression="gzip") as f:
        readback = msgpack.load(f)
        assert readback == _tuple_to_list(await get_state())


@gen_test()
async def test_write_state_yaml(tmp_path):
    path = str(tmp_path / "bar")
    await write_state(get_state(), path, "yaml")

    with open(f"{path}.yaml") as f:
        readback = yaml.safe_load(f)
        assert readback == _tuple_to_list(await get_state())
        f.seek(0)
        assert "!!python/tuple" not in f.read()
