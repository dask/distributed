from __future__ import annotations

import sys

import pytest

from dask.sizeof import sizeof

from distributed.compatibility import WINDOWS
from distributed.protocol import dumps, loads, msgpack, to_serialize
from distributed.protocol.cuda import cuda_deserialize, cuda_serialize
from distributed.protocol.serialize import (
    Pickled,
    Serialize,
    Serialized,
    ToPickle,
    dask_deserialize,
    dask_serialize,
    deserialize,
    serialize,
)
from distributed.utils import nbytes


def test_protocol():
    for msg in [1, "a", b"a", {"x": 1}, {b"x": 1}, {"x": b""}, {}]:
        assert loads(dumps(msg)) == msg


def test_small():
    assert sum(map(nbytes, dumps(b""))) < 10
    assert sum(map(nbytes, dumps(1))) < 10


def test_small_and_big():
    d = {"x": (1, 2, 3), "y": b"0" * 10000000}
    L = dumps(d)
    assert loads(L) == d


@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_large_bytes(dtype):
    payload = dtype(b"0" * 1000000)
    msg = {"x": to_serialize(payload), "y": 1}
    frames = dumps(msg)
    msg["x"] = msg["x"].data
    assert loads(frames) == msg
    assert len(frames[0]) < 1000
    assert len(frames[1]) < 1000


def test_large_messages_map():
    x = {i: "mystring_%d" % i for i in range(100_000)}
    b = dumps(x)
    x2 = loads(b)
    assert x == x2


def test_loads_deserialize_False():
    frames = dumps({"data": Serialize(123), "status": "OK"})
    msg = loads(frames)
    assert msg == {"data": 123, "status": "OK"}

    msg = loads(frames, deserialize=False)
    assert msg["status"] == "OK"
    assert isinstance(msg["data"], Serialized)

    result = deserialize(msg["data"].header, msg["data"].frames)
    assert result == 123


def eq_frames(a, b):
    if b"headers" in a:
        return msgpack.loads(a, use_list=False, strict_map_key=False) == msgpack.loads(
            b, use_list=False, strict_map_key=False
        )
    else:
        return a == b


def test_dumps_loads_Serialize():
    msg = {"x": 1, "data": Serialize(123)}
    frames = dumps(msg)
    assert len(frames) > 2
    result = loads(frames)
    assert result == {"x": 1, "data": 123}

    result2 = loads(frames, deserialize=False)
    assert result2["x"] == 1
    assert isinstance(result2["data"], Serialized)
    assert any(a is b for a in result2["data"].frames for b in frames)

    frames2 = dumps(result2)
    assert all(map(eq_frames, frames, frames2))

    result3 = loads(frames2)
    assert result == result3


def test_dumps_loads_Serialized():
    msg = {"x": 1, "data": Serialized(*serialize(123))}
    frames = dumps(msg)
    assert len(frames) > 2
    result = loads(frames)
    assert result == {"x": 1, "data": 123}

    result2 = loads(frames, deserialize=False)
    assert result2 == msg

    frames2 = dumps(result2)
    assert all(map(eq_frames, frames, frames2))

    result3 = loads(frames2)
    assert result == result3


@pytest.mark.parametrize("serializers", [("dask",), ("cuda",)])
def test_preserve_header(serializers):
    """
    Test that a serialization family doesn't overwrite the headers
    of the underlying registered dumps/loads functions.
    """

    class MyObj:
        pass

    @cuda_serialize.register(MyObj)
    @dask_serialize.register(MyObj)
    def _(x):
        return {}, []

    @cuda_deserialize.register(MyObj)
    @dask_deserialize.register(MyObj)
    def _(header, frames):
        assert header == {}
        assert frames == []
        return MyObj()

    header, frames = serialize(MyObj(), serializers=serializers)
    o = deserialize(header, frames)
    assert isinstance(o, MyObj)


@pytest.mark.parametrize(
    "Wrapper, Wrapped",
    [
        (Serialize, Serialized),
        (to_serialize, Serialized),
        (ToPickle, Pickled),
    ],
)
def test_sizeof_serialize(Wrapper, Wrapped):
    size = 100_000
    ser_obj = Wrapper(b"0" * size)
    assert size <= sizeof(ser_obj) < size * 1.05
    serialized = Wrapped(*serialize(ser_obj))
    assert size <= sizeof(serialized) < size * 1.05


@pytest.mark.skipif(WINDOWS, reason="On windows this is triggering a stackoverflow")
def test_deeply_nested_structures():
    # These kind of deeply nested structures are generated in our profiling code
    def gen_deeply_nested(depth):
        msg = {}
        d = msg
        while depth:
            depth -= 1
            d["children"] = d = {}
        return msg

    msg = gen_deeply_nested(sys.getrecursionlimit() - 100)
    with pytest.raises(TypeError, match="Could not serialize object"):
        serialize(msg, on_error="raise")

    msg = gen_deeply_nested(sys.getrecursionlimit() // 4)
    assert isinstance(serialize(msg), tuple)
