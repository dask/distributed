from zlib import crc32

import numpy as np
import pytest

from distributed.protocol import (
    serialize,
    deserialize,
    decompress,
    dumps,
    loads,
    to_serialize,
    msgpack,
)
from distributed.protocol.utils import BIG_BYTES_SHARD_SIZE
from distributed.protocol.numpy import itemsize
from distributed.protocol.pickle import HIGHEST_PROTOCOL
from distributed.protocol.compression import maybe_compress
from distributed.system import MEMORY_LIMIT
from distributed.utils import tmpfile, nbytes
from distributed.utils_test import gen_cluster


def test_serialize():
    x = np.ones((5, 5))
    header, frames = serialize(x)
    assert header["type"]
    assert len(frames) == 1

    if "compression" in header:
        frames = decompress(header, frames)
    result = deserialize(header, frames)
    assert (result == x).all()


@pytest.mark.parametrize(
    "x",
    [
        np.ones(5),
        np.array(5),
        np.random.random((5, 5)),
        np.random.random((5, 5))[::2, :],
        np.random.random((5, 5))[:, ::2],
        np.asfortranarray(np.random.random((5, 5))),
        np.asfortranarray(np.random.random((5, 5)))[::2, :],
        np.asfortranarray(np.random.random((5, 5)))[:, ::2],
        np.random.random(5).astype("f4"),
        np.random.random(5).astype(">i8"),
        np.random.random(5).astype("<i8"),
        np.arange(5).astype("M8[us]"),
        np.arange(5).astype("M8[ms]"),
        np.arange(5).astype("m8"),
        np.arange(5).astype("m8[s]"),
        np.arange(5).astype("c16"),
        np.arange(5).astype("c8"),
        np.array([True, False, True]),
        np.ones(shape=5, dtype=[("a", "i4"), ("b", "M8[us]")]),
        np.array(["abc"], dtype="S3"),
        np.array(["abc"], dtype="U3"),
        np.array(["abc"], dtype=object),
        np.array([np.arange(3), np.arange(4, 6)], dtype=object),
        np.ones(shape=(5,), dtype=("f8", 32)),
        np.ones(shape=(5,), dtype=[("x", "f8", 32)]),
        np.ones(shape=(5,), dtype=np.dtype([("a", "i1"), ("b", "f8")], align=False)),
        np.ones(shape=(5,), dtype=np.dtype([("a", "i1"), ("b", "f8")], align=True)),
        np.ones(shape=(5,), dtype=np.dtype([("a", "m8[us]")], align=False)),
        # this dtype fails unpickling
        np.ones(shape=(5,), dtype=np.dtype([("a", "m8")], align=False)),
        np.array([(1, "abc")], dtype=[("x", "i4"), ("s", object)]),
        np.zeros(5000, dtype=[("x%d" % i, "<f8") for i in range(4)]),
        np.zeros(5000, dtype="S32"),
        np.zeros((1, 1000, 1000)),
        np.arange(12)[::2],  # non-contiguous array
        np.ones(shape=(5, 6)).astype(dtype=[("total", "<f8"), ("n", "<f8")]),
        np.broadcast_to(np.arange(3), shape=(10, 3)),  # zero-strided array
    ],
)
def test_dumps_serialize_numpy(x):
    header, frames = serialize(x)
    if "compression" in header:
        frames = decompress(header, frames)
    for frame in frames:
        assert isinstance(frame, (bytes, memoryview))
    if x.dtype.char == "O" and any(isinstance(e, np.ndarray) for e in x.flat):
        if HIGHEST_PROTOCOL >= 5:
            assert len(frames) > 1
        else:
            assert len(frames) == 1
    y = deserialize(header, frames)

    assert x.shape == y.shape
    assert x.dtype == y.dtype
    if x.flags.c_contiguous or x.flags.f_contiguous:
        assert x.strides == y.strides

    if x.dtype.char == "O":
        for e_x, e_y in zip(x.flat, y.flat):
            np.testing.assert_equal(e_x, e_y)
    else:
        np.testing.assert_equal(x, y)


@pytest.mark.parametrize(
    "x",
    [
        np.ma.masked_array([5, 6], mask=[True, False], fill_value=10, dtype="i4"),
        np.ma.masked_array([5.0, 6.0], mask=[True, False], fill_value=10, dtype="f4"),
        np.ma.masked_array(
            [5.0, 6.0], mask=[True, False], fill_value=np.nan, dtype="f8"
        ),
        np.ma.masked_array(
            [5.0, 6.0], mask=np.ma.nomask, fill_value=np.nan, dtype="f8"
        ),
        np.ma.masked_array(
            [True, False], mask=np.ma.nomask, fill_value=True, dtype="bool"
        ),
        np.ma.masked_array(["a", "b"], mask=[True, False], fill_value="c", dtype="O"),
    ],
)
def test_serialize_numpy_ma_masked_array(x):
    (y,) = loads(dumps([to_serialize(x)]))
    assert x.data.dtype == y.data.dtype
    np.testing.assert_equal(x.data, y.data)
    np.testing.assert_equal(x.mask, y.mask)
    np.testing.assert_equal(x.fill_value, y.fill_value)


def test_serialize_numpy_ma_masked():
    (y,) = loads(dumps([to_serialize(np.ma.masked)]))
    assert y is np.ma.masked


def test_dumps_serialize_numpy_custom_dtype():
    import builtins

    test_rational = pytest.importorskip("numpy.core.test_rational")
    rational = test_rational.rational
    try:
        builtins.rational = (
            rational  # Work around https://github.com/numpy/numpy/issues/9160
        )
        x = np.array([1], dtype=rational)
        header, frames = serialize(x)
        y = deserialize(header, frames)

        np.testing.assert_equal(x, y)
    finally:
        del builtins.rational


def test_memmap():
    with tmpfile("npy") as fn:
        with open(fn, "wb") as f:  # touch file
            pass
        x = np.memmap(fn, shape=(5, 5), dtype="i4", mode="readwrite")
        x[:] = 5

        header, frames = serialize(x)
        if "compression" in header:
            frames = decompress(header, frames)
        y = deserialize(header, frames)

        np.testing.assert_equal(x, y)


@pytest.mark.slow
def test_dumps_serialize_numpy_large():
    if MEMORY_LIMIT < 2e9:
        pytest.skip("insufficient memory")
    x = np.random.random(size=int(BIG_BYTES_SHARD_SIZE * 2 // 8)).view("u1")
    assert x.nbytes == BIG_BYTES_SHARD_SIZE * 2
    frames = dumps([to_serialize(x)])
    dtype, shape = x.dtype, x.shape
    checksum = crc32(x)
    del x
    [y] = loads(frames)

    assert (y.dtype, y.shape) == (dtype, shape)
    assert crc32(y) == checksum, "Arrays are unequal"


@pytest.mark.parametrize(
    "dt,size",
    [
        ("f8", 8),
        ("i4", 4),
        ("c16", 16),
        ("b", 1),
        ("S3", 3),
        ("M8[us]", 8),
        ("M8[s]", 8),
        ("U3", 12),
        ([("a", "i4"), ("b", "f8")], 12),
        (("i4", 100), 4),
        ([("a", "i4", 100)], 8),
        ([("a", "i4", 20), ("b", "f8")], 20 * 4 + 8),
        ([("a", "i4", 200), ("b", "f8")], 8),
    ],
)
def test_itemsize(dt, size):
    assert itemsize(np.dtype(dt)) == size


def test_compress_numpy():
    pytest.importorskip("lz4")
    x = np.ones(10000000, dtype="i4")
    frames = dumps({"x": to_serialize(x)})
    assert sum(map(nbytes, frames)) < x.nbytes

    header = msgpack.loads(frames[2], raw=False, use_list=False, strict_map_key=False)
    try:
        import blosc  # noqa: F401
    except ImportError:
        pass
    else:
        assert all(c == "blosc" for c in header["headers"][("x",)]["compression"])


def test_compress_memoryview():
    mv = memoryview(b"0" * 1000000)
    compression, compressed = maybe_compress(mv)
    if compression:
        assert len(compressed) < len(mv)


@pytest.mark.skip
def test_dont_compress_uncompressable_data():
    blosc = pytest.importorskip("blosc")
    x = np.random.randint(0, 255, size=100000).astype("uint8")
    header, [data] = serialize(x)
    assert "compression" not in header
    assert data == x.data

    x = np.ones(1000000)
    header, [data] = serialize(x)
    assert header["compression"] == ["blosc"]
    assert data != x.data

    x = np.ones(100)
    header, [data] = serialize(x)
    assert "compression" not in header
    if isinstance(data, memoryview):
        assert data.obj.ctypes.data == x.ctypes.data


@gen_cluster(client=True, timeout=60)
async def test_dumps_large_blosc(c, s, a, b):
    x = c.submit(np.ones, BIG_BYTES_SHARD_SIZE * 2, dtype="u1")
    await x


def test_compression_takes_advantage_of_itemsize():
    pytest.importorskip("lz4")
    blosc = pytest.importorskip("blosc")
    x = np.arange(1000000, dtype="i8")

    assert len(blosc.compress(x.data, typesize=8)) < len(
        blosc.compress(x.data, typesize=1)
    )

    _, a = serialize(x)
    aa = [maybe_compress(frame)[1] for frame in a]
    _, b = serialize(x.view("u1"))
    bb = [maybe_compress(frame)[1] for frame in b]

    assert sum(map(nbytes, aa)) < sum(map(nbytes, bb))


def test_large_numpy_array():
    x = np.ones((100000000,), dtype="u4")
    header, frames = serialize(x)
    assert sum(header["lengths"]) == sum(map(nbytes, frames))


@pytest.mark.parametrize(
    "x",
    [
        np.broadcast_to(np.arange(10), (20, 10)),  # Some strides are 0
        np.broadcast_to(1, (3, 4, 2)),  # All strides are 0
        np.broadcast_to(np.arange(100)[:1], 5),  # x.base is larger than x
        np.broadcast_to(np.arange(5), (4, 5))[:, ::-1],
    ],
)
@pytest.mark.parametrize("writeable", [True, False])
def test_zero_strided_numpy_array(x, writeable):
    assert 0 in x.strides
    x.setflags(write=writeable)
    header, frames = serialize(x)
    y = deserialize(header, frames)
    np.testing.assert_equal(x, y)
    # Ensure we transmit fewer bytes than the full array
    assert sum(map(nbytes, frames)) < x.nbytes
    # Ensure both x and y are have same write flag
    assert x.flags.writeable == y.flags.writeable


def test_non_zero_strided_array():
    x = np.arange(10)
    header, frames = serialize(x)
    assert "broadcast_to" not in header
    assert sum(map(nbytes, frames)) == x.nbytes


def test_serialize_writeable_array_readonly_base_object():
    # Regression test for https://github.com/dask/distributed/issues/3252

    x = np.arange(3)
    # Create array which doesn't own it's own memory
    y = np.broadcast_to(x, (3, 3))

    # Make y writeable and it's base object (x) read-only
    y.setflags(write=True)
    x.setflags(write=False)

    # Serialize / deserialize y
    z = deserialize(*serialize(y))
    np.testing.assert_equal(z, y)

    # Ensure z and y have the same flags (including WRITEABLE)
    assert z.flags == y.flags
