from __future__ import annotations

import asyncio
import os

import pytest

from distributed.shuffle._multi_file import MultiFile
from distributed.utils_test import gen_test


def dump(data, f):
    f.write(data)


def load(f):
    out = f.read()
    if not out:
        raise EOFError()
    return out


@gen_test()
async def test_basic(tmp_path):
    with MultiFile(directory=tmp_path, dump=dump, load=load) as mf:
        await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})
        await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

        await mf.flush()

        x = mf.read("x")
        y = mf.read("y")

        assert x == b"0" * 2000
        assert y == b"1" * 1000

    assert not os.path.exists(tmp_path)


@pytest.mark.parametrize("count", [2, 100, 1000])
@gen_test()
async def test_many(tmp_path, count):
    with MultiFile(directory=tmp_path, dump=dump, load=load) as mf:
        d = {i: [str(i).encode() * 100] for i in range(count)}

        for _ in range(10):
            await mf.put(d)

        await mf.flush()

        for i in d:
            out = mf.read(i)
            assert out == str(i).encode() * 100 * 10

    assert not os.path.exists(tmp_path)


@gen_test()
async def test_exceptions(tmp_path):
    def dump(data, f):
        raise Exception(123)

    with MultiFile(directory=tmp_path, dump=dump, load=load) as mf:
        await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

        while not mf._exception:
            await asyncio.sleep(0.1)

        with pytest.raises(Exception, match="123"):
            await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

        with pytest.raises(Exception, match="123"):
            await mf.flush()
