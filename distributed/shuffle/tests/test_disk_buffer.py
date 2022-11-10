from __future__ import annotations

import asyncio
import os

import pytest

from distributed.shuffle._disk import DiskShardsBuffer
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
    async with DiskShardsBuffer(directory=tmp_path, dump=dump, load=load) as mf:
        await mf.write({"x": [b"0" * 1000], "y": [b"1" * 500]})
        await mf.write({"x": [b"0" * 1000], "y": [b"1" * 500]})

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
    payload = {"1": [b"foo"]}
    async with DiskShardsBuffer(directory=tmp_path, dump=dump, load=load) as mf:
        with pytest.raises(RuntimeError):
            mf.read(1)

        await mf.write(payload)

        with pytest.raises(RuntimeError):
            mf.read(1)

        await mf.flush()
        assert mf.read("1") == b"foo"
        with pytest.raises(KeyError):
            mf.read(2)


@pytest.mark.parametrize("count", [2, 100, 1000])
@gen_test()
async def test_many(tmp_path, count):
    async with DiskShardsBuffer(directory=tmp_path, dump=dump, load=load) as mf:
        d = {i: [str(i).encode() * 100] for i in range(count)}

        for _ in range(10):
            await mf.write(d)

        await mf.flush()

        for i in d:
            out = mf.read(i)
            assert out == str(i).encode() * 100 * 10

    assert not os.path.exists(tmp_path)


@gen_test()
async def test_exceptions(tmp_path):
    def dump(data, f):
        raise Exception(123)

    async with DiskShardsBuffer(directory=tmp_path, dump=dump, load=load) as mf:
        await mf.write({"x": [b"0" * 1000], "y": [b"1" * 500]})

        while not mf._exception:
            await asyncio.sleep(0.1)

        with pytest.raises(Exception, match="123"):
            await mf.write({"x": [b"0" * 1000], "y": [b"1" * 500]})

        await mf.flush()


@gen_test()
async def test_high_pressure_flush_with_exception(tmp_path):
    counter = 0
    payload = {f"shard-{ix}": [f"shard-{ix}".encode() * 100] for ix in range(100)}

    def dump_broken(data, f):
        nonlocal counter
        # We only want to raise if this was queued up before
        if counter > DiskShardsBuffer.concurrency_limit:
            raise Exception(123)
        counter += 1
        dump(data, f)

    async with DiskShardsBuffer(
        directory=tmp_path,
        dump=dump_broken,
        load=load,
    ) as mf:
        tasks = []
        for _ in range(10):
            tasks.append(asyncio.create_task(mf.write(payload)))

        # Wait until things are actually queued up.
        # This is when there is no slot on the queue available anymore
        # but there are still shards around
        while not mf.shards:
            # Disks are fast, don't give it time to unload the queue...
            # There may only be a few ticks atm so keep this at zero
            await asyncio.sleep(0)

        with pytest.raises(Exception, match="123"):
            await mf.flush()
            mf.raise_on_exception()
