from __future__ import annotations

import pytest

from distributed.shuffle._exceptions import DataUnavailable
from distributed.shuffle._memory import MemoryShardsBuffer
from distributed.utils_test import gen_test


def deserialize_bytes(buffer: bytes) -> bytes:
    return buffer


@gen_test()
async def test_basic():
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        await mf.write({"x": b"0" * 1000, "y": b"1" * 500})
        await mf.write({"x": b"0" * 1000, "y": b"1" * 500})

        await mf.flush()

        x = mf.read("x")
        y = mf.read("y")

        with pytest.raises(DataUnavailable):
            mf.read("z")

        assert x == [b"0" * 1000] * 2
        assert y == [b"1" * 500] * 2


@gen_test()
async def test_read_before_flush():
    payload = {"1": b"foo"}
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        with pytest.raises(RuntimeError):
            mf.read("1")

        await mf.write(payload)

        with pytest.raises(RuntimeError):
            mf.read("1")

        await mf.flush()
        assert mf.read("1") == [b"foo"]
        with pytest.raises(DataUnavailable):
            mf.read("2")


@pytest.mark.parametrize("count", [2, 100, 1000])
@gen_test()
async def test_many(count):
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        d = {str(i): str(i).encode() * 100 for i in range(count)}

        for _ in range(10):
            await mf.write(d)

        await mf.flush()

        for i in d:
            out = mf.read(str(i))
            assert out == [str(i).encode() * 100] * 10
