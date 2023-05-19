from __future__ import annotations

import threading
import zlib
from concurrent.futures import ThreadPoolExecutor

import pytest

import dask.config

from distributed import Worker, wait
from distributed.compatibility import LINUX
from distributed.protocol import dumps, loads, maybe_compress, msgpack, to_serialize
from distributed.protocol.compression import (
    Compression,
    compressions,
    get_compression_settings,
)
from distributed.utils import nbytes
from distributed.utils_test import gen_cluster


@pytest.fixture(params=[None, "zlib", "lz4", "snappy", "zstd"])
def compression(request):
    if request.param == "zstd":
        pytest.importorskip("zstandard")
    elif request.param:
        pytest.importorskip(request.param)
    return request.param


@pytest.fixture(scope="function")
def compression_counters():
    counters = [0, 0]

    def compress(v):
        counters[0] += 1
        return zlib.compress(v)

    def decompress(v):
        counters[1] += 1
        return zlib.decompress(v)

    compressions["dummy"] = Compression("dummy", compress, decompress)
    yield counters
    del compressions["dummy"]


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
    frames = dumps({"x": to_serialize(b)}, context={"compression": "zlib"})
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
    frames = dumps({"x": to_serialize(x.data)}, context={"compression": "zlib"})
    assert sum(map(nbytes, frames)) < x.nbytes
    y = loads(frames)
    assert {"x": x.data} == y


def test_compression_without_deserialization():
    np = pytest.importorskip("numpy")
    x = np.ones(1000000)

    frames = dumps({"x": to_serialize(x)}, context={"compression": "zlib"})
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
    assert compression == compressions["auto"].name
    assert len(payload) < x.nbytes * 0.75


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


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[], config={"distributed.comm.compression": "dummy"})
async def test_compress_remote_comms(c, s, compression_counters):
    # Note: if you swap the IP addresses, the test will fail saying that compression
    # was not used. This is because the auto-generated Comm.local_address for the TCP
    # client is unrelated to the listening address of the worker that spawned it.
    async with Worker(s.address, host="127.0.0.2") as a:
        async with Worker(s.address, host="127.0.0.1") as b:
            x = c.submit(lambda: "x" * 11_000, workers=[a.address])
            y = c.submit(lambda x: None, x, workers=[b.address])
            await wait(y)
            assert compression_counters == [2, 1]


@pytest.mark.parametrize("host", [None, "127.0.0.1", "localhost"])
@gen_cluster(client=True, nthreads=[], config={"distributed.comm.compression": "dummy"})
async def test_disable_compression_on_localhost(c, s, compression_counters, host):
    async with Worker(s.address, host=host) as a:
        async with Worker(s.address, host=host) as b:
            x = c.submit(lambda: "x" * 11_000, workers=[a.address])
            y = c.submit(lambda x: None, x, workers=[b.address])
            await wait(y)
            assert compression_counters == [0, 0]
