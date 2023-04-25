from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from dask.sizeof import sizeof

from distributed.compatibility import WINDOWS
from distributed.protocol import dumps, loads, maybe_compress, msgpack, to_serialize
from distributed.protocol.compression import compressions
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
from distributed.system import MEMORY_LIMIT
from distributed.utils import nbytes


def test_protocol():
    for msg in [1, "a", b"a", {"x": 1}, {b"x": 1}, {"x": b""}, {}]:
        assert loads(dumps(msg)) == msg


def test_auto_compression():
    """Test that the 'auto' compression algorithm is lz4 -> snappy -> None.
    If neither is installed, test that we don't fall back to the very slow zlib.
    """
    try:
        import lz4  # noqa: F401

        assert compressions["auto"] is compressions["lz4"]
        return
    except ImportError:
        pass

    try:
        import snappy  # noqa: F401

        assert compressions["auto"] is compressions["snappy"]
    except ImportError:
        assert compressions["auto"].name is None


def test_compression_1():
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)
    b = x.tobytes()
    frames = dumps({"x": Serialize(b)}, context={"compression": "zlib"})
    assert sum(map(nbytes, frames)) < nbytes(b)
    y = loads(frames)
    assert {"x": b} == y


def test_compression_2():
    np = pytest.importorskip("numpy")
    x = np.random.random(10000)
    msg = dumps(to_serialize(x.data), context={"compression": "zlib"})
    compression = msgpack.loads(msg[1]).get("compression")
    assert all(c is None for c in compression)


def test_compression_3():
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)
    frames = dumps({"x": Serialize(x.data)}, context={"compression": "zlib"})
    assert sum(map(nbytes, frames)) < x.nbytes
    y = loads(frames)
    assert {"x": x.data} == y


def test_compression_without_deserialization():
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)

    frames = dumps({"x": Serialize(x)}, context={"compression": "zlib"})
    assert all(len(frame) < 1000000 for frame in frames)

    msg = loads(frames, deserialize=False)
    assert all(len(frame) < 1000000 for frame in msg["x"].frames)


def test_lz4_decompression_avoids_deep_copy():
    """Test that lz4 output is a bytearray, not bytes, so that numpy deserialization is
    not forced to perform a deep copy to obtain a writeable array.
    Note that zlib, zstandard, and snappy don't have this option.
    """
    pytest.importorskip("lz4")
    a = bytearray(1_000_000)
    b = compressions["lz4"].compress(a)
    c = compressions["lz4"].decompress(b)
    assert isinstance(c, bytearray)


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
    [
        (None, None),
        ("zlib", "zlib"),
        ("lz4", "lz4"),
        ("snappy", "snappy"),
        ("zstandard", "zstd"),
    ],
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
        assert rc == compression
        assert compressions[rc].decompress(rd) == payload


@pytest.mark.slow
@pytest.mark.parametrize(
    "lib,compression",
    [
        ("zlib", "zlib"),
        ("lz4", "lz4"),
        ("snappy", "snappy"),
        ("zstandard", "zstd"),
    ],
)
@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_compression_thread_safety(lib, compression, dtype):
    if lib:
        pytest.importorskip(lib)

    barrier = threading.Barrier(4)
    expect = b"0" * 10000
    payload = dtype(expect)

    def compress_decompress():
        barrier.wait()
        for _ in range(2000):
            rc, rd = maybe_compress(payload, compression=compression)
            assert rc == compression
            assert compressions[rc].decompress(rd) == expect

    with ThreadPoolExecutor(4) as ex:
        futures = [ex.submit(compress_decompress) for _ in range(4)]
        for future in futures:
            future.result()


@pytest.mark.parametrize(
    "lib,compression",
    [
        (None, None),
        ("zlib", "zlib"),
        ("lz4", "lz4"),
        ("snappy", "snappy"),
        ("zstandard", "zstd"),
    ],
)
@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_maybe_compress_compression(lib, compression, dtype):
    if lib:
        pytest.importorskip(lib)
    payload = dtype(b"123")
    assert maybe_compress(payload, compression=compression) == (None, payload)

    payload = dtype(b"0" * 10000)
    rc, rd = maybe_compress(payload, compression=compression)
    assert rc == compression
    assert compressions[rc].decompress(rd) == payload


def test_maybe_compress_sample():
    np = pytest.importorskip("numpy")
    payload = np.random.randint(0, 255, size=10000).astype("u1").tobytes()
    fmt, compressed = maybe_compress(payload, compression="zlib")
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


def test_large_messages():
    np = pytest.importorskip("numpy")
    if MEMORY_LIMIT < 8e9:
        pytest.skip("insufficient memory")

    x = np.random.randint(0, 255, size=200000000, dtype="u1")

    msg = {
        "x": [Serialize(x), b"small_bytes"],
        "y": {"a": Serialize(x), "b": b"small_bytes"},
    }

    b = dumps(msg, context={"compression": "zlib"})
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
    b = b"0" * 100000

    msg = {"x": 1, "data": to_serialize(b)}
    frames = dumps(msg, context={"compression": "zlib"})

    assert sum(map(nbytes, frames)) < 10000

    msg2 = loads(frames, deserialize=False)
    assert sum(map(nbytes, msg2["data"].frames)) < 10000

    msg3 = dumps(msg2, context={"compression": "zlib"})
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
    x = np.arange(1000000, dtype="int64")
    compression, payload = maybe_compress(x.data, compression="zlib")
    assert compression == "zlib"
    assert len(payload) < x.nbytes * 0.75


def test_maybe_compress_auto():
    np = pytest.importorskip("numpy")
    x = np.arange(1000000, dtype="int64")
    compression, payload = maybe_compress(x.data)
    assert compression in ("lz4", "snappy", None)
    compression2, payload = maybe_compress(x.data, compression="auto")
    assert compression2 == compression
    assert len(payload) < x.nbytes * 0.75


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
