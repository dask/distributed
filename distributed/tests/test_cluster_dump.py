import io

import msgpack
import pytest
import yaml

from distributed.cluster_dump import _tuple_to_list, url_and_writer


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


def test_url_and_writer_msgpack(tmp_path):
    path = str(tmp_path / "bar")
    url, mode, write = url_and_writer(path, "msgpack")
    assert url == f"{path}.msgpack.gz"
    assert mode == "wb"

    state = {"foo": "bar", "list": ["a"], "tuple": (1, "two", 3)}
    buffer = io.BytesIO()
    write(state, buffer)
    buffer.seek(0)
    readback = msgpack.load(buffer)
    assert readback == _tuple_to_list(state)


def test_url_and_writer_yaml(tmp_path):
    path = str(tmp_path / "bar")
    url, mode, write = url_and_writer(path, "yaml")
    assert url == f"{path}.yaml"
    assert mode == "w"

    state = {"foo": "bar", "list": ["a"], "tuple": (1, "two", 3)}
    buffer = io.StringIO()
    write(state, buffer)
    buffer.seek(0)
    readback = yaml.safe_load(buffer)
    assert readback == _tuple_to_list(state)
    assert "!!python/tuple" not in buffer.getvalue()
