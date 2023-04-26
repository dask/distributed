from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import dask.config
from dask.sizeof import sizeof

from distributed.compatibility import WINDOWS
from distributed.protocol import dumps, loads, maybe_compress, msgpack, to_serialize
from distributed.protocol.compression import compressions, get_compression_settings
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


@pytest.fixture(params=[None, "zlib", "lz4", "snappy", "zstd"])
def compression(request):
    if request.param == "zstd":
        pytest.importorskip("zstandard")
    elif request.param:
        pytest.importorskip(request.param)
    return request.param


def test_protocol():
    for msg in [1, "a", b"a", {"x": 1}, {b"x": 1}, {"x": b""}, {}]:
        assert loads(dumps(msg)) == msg


def test_get_compression_settings():
    with dask.config.set({"test123": None}):
        assert get_compression_settings("test123") is None
    with dask.config.set({"test123": False}):
        assert get_compression_settings("test123") is None
    with dask.config.set({"test123": "zlib"}):
        assert get_compression_settings("test123") == "zlib"
    with dask.config.set({"test123": "hello"}):
        with pytest.raises(ValueError, match="Invalid.*test123=hello.*zlib"):
            get_compression_settings("test123")


def test_auto_compression():
    """Test that the 'auto' compression algorithm is lz4 -> snappy -> None.
    If neither is installed, test that we don't fall back to the very slow zlib.
    """
    with dask.config.set({"test123": "auto"}):
        try:
            import lz4  # noqa: F401

            assert get_compression_settings("test123") == "lz4"
            return
        except ImportError:
            pass

        try:
            import snappy  # noqa: F401

            assert get_compression_settings("test123") == "snappy"
        except ImportError:
            assert get_compression_settings("test123") is None


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


@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_maybe_compress(compression, dtype):
    payload = dtype(b"123")
    assert maybe_compress(payload, compression=compression) == (None, payload)

    payload = dtype(b"0" * 10000)
    rc, rd = maybe_compress(payload, compression=compression)
    assert rc == compression
    assert compressions[rc].decompress(rd) == payload


def test_maybe_compress_sample(compression):
    np = pytest.importorskip("numpy")
    payload = np.random.randint(0, 255, size=10000).astype("u1").tobytes()
    fmt, compressed = maybe_compress(payload, compression=compression)
    assert fmt is None
    assert compressed == payload


@pytest.mark.slow
@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_compression_thread_safety(compression, dtype):
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


@pytest.mark.parametrize("dtype", [bytes, memoryview])
def test_large_bytes(dtype):
    payload = dtype(b"0" * 1000000)
    msg = {"x": to_serialize(payload), "y": 1}
    frames = dumps(msg)
    msg["x"] = msg["x"].data
    assert loads(frames) == msg
    assert len(frames[0]) < 1000
    assert len(frames[1]) < 1000


@pytest.mark.slow
def test_large_messages(compression):
    x = "x" * 200_000_000

    msg = {
        "x": [to_serialize(x), b"small_bytes"],
        "y": {"a": to_serialize(x), "b": b"small_bytes"},
    }

    b = dumps(msg, context={"compression": compression})
    if compression is not None:
        assert sum(len(frame) for frame in b) < len(x)

    msg2 = loads(b)
    assert msg["x"][1] == msg2["x"][1]
    assert msg["y"]["b"] == msg2["y"]["b"]
    assert msg["x"][0].data == msg2["x"][0]
    assert msg["y"]["a"].data == msg2["y"]["a"]


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


def test_maybe_compress_memoryviews(compression):
    np = pytest.importorskip("numpy")
    x = np.arange(1000000, dtype="int64")
    actual_compression, payload = maybe_compress(x.data, compression=compression)
    assert actual_compression == compression
    if compression:
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
