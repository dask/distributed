import os

import pytest

from distributed.shuffle.multi_file import MultiFile


def dump(data, f):
    f.write(data)


def load(f):
    out = f.read()
    if not out:
        raise EOFError()
    return out


@pytest.mark.asyncio
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


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 100, 1000])
async def test_many(tmp_path, count):
    with MultiFile(directory=tmp_path, dump=dump, load=load) as mf:
        d = {i: [str(i).encode() * 100] for i in range(count)}

        for i in range(10):
            await mf.put(d)

        await mf.flush()

        for i in d:
            out = mf.read(i)
            assert out == str(i).encode() * 100 * 10

    assert not os.path.exists(tmp_path)
