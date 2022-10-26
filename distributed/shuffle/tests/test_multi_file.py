from __future__ import annotations

import asyncio
import os

import pytest
from tornado.ioloop import IOLoop

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
    async with MultiFile(
        directory=tmp_path, dump=dump, load=load, loop=IOLoop.current()
    ) as mf:
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
    async with MultiFile(
        directory=tmp_path, dump=dump, load=load, loop=IOLoop.current()
    ) as mf:
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

    async with MultiFile(
        directory=tmp_path, dump=dump, load=load, loop=IOLoop.current()
    ) as mf:
        await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

        while not mf._exception:
            await asyncio.sleep(0.1)

        with pytest.raises(Exception, match="123"):
            await mf.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

        with pytest.raises(Exception, match="123"):
            await mf.flush()


@gen_test()
async def test_buffer_too_many_concurrent_files(tmp_path):
    pass


@pytest.mark.parametrize(
    "explicit_flush",
    [
        True,
        False,
    ],
)
@gen_test()
async def test_high_pressure_flush_with_exception(tmp_path, explicit_flush):
    counter = 0
    payload = {f"shard-{ix}": [f"shard-{ix}".encode() * 100] for ix in range(100)}

    def dump_broken(data, f):
        nonlocal counter
        if counter > MultiFile.concurrent_files:
            raise Exception(123)
        counter += 1
        dump(data, f)

    # Something here should raise...
    with pytest.raises(Exception, match="123"):
        async with MultiFile(
            directory=tmp_path, dump=dump_broken, load=load, loop=IOLoop.current()
        ) as mf:
            tasks = []
            for _ in range(10):
                tasks.append(asyncio.create_task(mf.put(payload)))

            # Wait until things are actually queued up.
            # This is when there is no slot on the queue available anymore
            # but there are still shards around
            while not (mf.shards and mf.queue.empty()):
                await asyncio.sleep(0)
            if explicit_flush:
                # Flushing while this happens is a bad idea and deadlocks atm
                await mf.flush()
