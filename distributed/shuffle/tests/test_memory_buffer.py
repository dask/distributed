from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from dask.sizeof import sizeof

from distributed.shuffle._memory import MemoryShardsBuffer
from distributed.utils_test import gen_test


def read_bytes(path: Path) -> tuple[bytes, int]:
    with path.open("rb") as f:
        data = f.read()
        size = f.tell()
    return data, size


def deserialize_bytes(buffer: bytes) -> tuple[Any, int]:
    return buffer, sizeof(buffer)


@gen_test()
async def test_basic(tmp_path):
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        await mf.write({"x": b"0" * 1000, "y": b"1" * 500})
        await mf.write({"x": b"0" * 1000, "y": b"1" * 500})

        await mf.flush()

        x = mf.read("x")
        y = mf.read("y")

        with pytest.raises(KeyError):
            mf.read("z")

        assert x == b"0" * 2000
        assert y == b"1" * 1000

    assert not os.path.exists(tmp_path)


@gen_test()
async def test_read_before_flush(tmp_path):
    payload = {"1": b"foo"}
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        with pytest.raises(RuntimeError):
            mf.read("1")

        await mf.write(payload)

        with pytest.raises(RuntimeError):
            mf.read("1")

        await mf.flush()
        assert mf.read("1") == b"foo"
        with pytest.raises(KeyError):
            mf.read("2")


@pytest.mark.parametrize("count", [2, 100, 1000])
@gen_test()
async def test_many(tmp_path, count):
    async with MemoryShardsBuffer(deserialize=deserialize_bytes) as mf:
        d = {str(i): str(i).encode() * 100 for i in range(count)}

        for _ in range(10):
            await mf.write(d)

        await mf.flush()

        for i in d:
            out = mf.read(str(i))
            assert out == str(i).encode() * 100 * 10

    assert not os.path.exists(tmp_path)
