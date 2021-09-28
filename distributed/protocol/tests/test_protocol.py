import pytest

from distributed.protocol import dumps, loads, maybe_compress, msgpack, to_serialize
from distributed.protocol.compression import compressions
from distributed.protocol.serialize import Serialize, Serialized, deserialize, serialize
from distributed.system import MEMORY_LIMIT
from distributed.utils import nbytes


def test_protocol():
    for msg in [1, "a", b"a", {"x": 1}, {b"x": 1}, {"x": b""}, {}]:
        assert loads(dumps(msg)) == msg


def test_compression_1():
    pytest.importorskip("lz4")
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)
    frames = dumps({"x": Serialize(x.tobytes())})
    assert sum(map(nbytes, frames)) < x.nbytes
    y = loads(frames)
    assert {"x": x.tobytes()} == y


def test_compression_2():
    pytest.importorskip("lz4")
    np = pytest.importorskip("numpy")
    x = np.random.random(10000)
    msg = dumps(to_serialize(x.tobytes()))
    compression = msgpack.loads(msg[1]).get("compression")
    assert all(c is None for c in compression)


def test_compression_without_deserialization():
    pytest.importorskip("lz4")
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)

    frames = dumps({"x": Serialize(x)})
    assert all(len(frame) < 1000000 for frame in frames)

    msg = loads(frames, deserialize=False)
    assert all(len(frame) < 1000000 for frame in msg["x"].frames)


def test_small():
    assert sum(map(nbytes, dumps(b""))) < 10
    assert sum(map(nbytes, dumps(1))) < 10


def test_small_and_big():
    d = {"x": (1, 2, 3), "y": b"0" * 10000000}
    L = dumps(d)
    assert loads(L) == d
    # assert loads([small_header, small]) == {'x': [1, 2, 3]}
    # assert loads([big_header, big]) == {'y': d['y']}


@pytest.mark.parametrize(
    "lib,compression",
    [(None, None), ("zlib", "zlib"), ("lz4", "lz4"), ("zstandard", "zstd")],
)
def test_maybe_compress(lib, compression):
    if lib:
        pytest.importorskip(lib)

    try_converters = [bytes, memoryview]

    for f in try_converters:
        payload = b"123"
        assert maybe_compress(f(payload), compression=compression) == (None, payload)

        payload = b"0" * 10000
        rc, rd = maybe_compress(f(payload), compression=compression)
        # For some reason compressing memoryviews can force blosc...
        assert rc in (compression, "blosc")
        assert compressions[rc]["decompress"](rd) == payload


def test_maybe_compress_sample():
    np = pytest.importorskip("numpy")
    lz4 = pytest.importorskip("lz4")
    payload = np.random.randint(0, 255, size=10000).astype("u1").tobytes()
    fmt, compressed = maybe_compress(payload)
    assert fmt is None
    assert compressed == payload


def test_large_bytes():
    for tp in (bytes, bytearray):
        msg = {"x": to_serialize(tp(b"0" * 1000000)), "y": 1}
        frames = dumps(msg)
        msg["x"] = msg["x"].data
        assert loads(frames) == msg
        assert len(frames[0]) < 1000
        assert len(frames[1]) < 1000


@pytest.mark.slow
def test_large_messages():
    np = pytest.importorskip("numpy")
    pytest.importorskip("lz4")
    if MEMORY_LIMIT < 8e9:
        pytest.skip("insufficient memory")

    x = np.random.randint(0, 255, size=200000000, dtype="u1")

    msg = {
        "x": [Serialize(x), b"small_bytes"],
        "y": {"a": Serialize(x), "b": b"small_bytes"},
    }

    b = dumps(msg)
    msg2 = loads(b)
    assert msg["x"][1] == msg2["x"][1]
    assert msg["y"]["b"] == msg2["y"]["b"]
    assert (msg["x"][0].data == msg2["x"][0]).all()
    assert (msg["y"]["a"].data == msg2["y"]["a"]).all()


def test_large_messages_map():
    if MEMORY_LIMIT < 8e9:
        pytest.skip("insufficient memory")

    x = {i: "mystring_%d" % i for i in range(100000)}

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


def test_loads_without_deserialization_avoids_compression():
    pytest.importorskip("lz4")
    b = b"0" * 100000

    msg = {"x": 1, "data": to_serialize(b)}
    frames = dumps(msg)

    assert sum(map(nbytes, frames)) < 10000

    msg2 = loads(frames, deserialize=False)
    assert sum(map(nbytes, msg2["data"].frames)) < 10000

    msg3 = dumps(msg2)
    msg4 = loads(msg3)

    assert msg4 == {"x": 1, "data": b"0" * 100000}


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


def test_maybe_compress_memoryviews():
    np = pytest.importorskip("numpy")
    pytest.importorskip("lz4")
    x = np.arange(1000000, dtype="int64")
    compression, payload = maybe_compress(x.data)
    try:
        import blosc  # noqa: F401
    except ImportError:
        assert compression == "lz4"
        assert len(payload) < x.nbytes * 0.75
    else:
        assert compression == "blosc"
        assert len(payload) < x.nbytes / 10
