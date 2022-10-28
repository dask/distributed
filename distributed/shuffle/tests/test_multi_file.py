from __future__ import annotations

import asyncio
import math
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


@gen_test()
async def test_read_before_flush(tmp_path):
    payload = {"1": [b"foo"]}
    async with MultiFile(
        directory=tmp_path, dump=dump, load=load, loop=IOLoop.current()
    ) as mf:
        with pytest.raises(RuntimeError):
            mf.read(1)

        await mf.put(payload)

        with pytest.raises(RuntimeError):
            mf.read(1)

        await mf.flush()
        assert mf.read("1") == b"foo"
        with pytest.raises(KeyError):
            mf.read(2)


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
    # TODO: Concurreny limiting is configured at global levels. This only tests existence of a single instance of MultiFile
    # In concurrent shuffles this is no longer true
    payload = {
        f"shard-{ix}": [f"shard-{ix}".encode()]
        for ix in range(MultiFile.concurrent_files * 10)
    }

    async with MultiFile(
        directory=tmp_path,
        dump=dump,
        load=load,
        loop=IOLoop.current(),
    ) as mf:

        def concurrent_writes():
            return MultiFile.concurrent_files - mf._queue.qsize()

        assert concurrent_writes() == 0
        tasks = []

        for _ in range(MultiFile.concurrent_files * 10):
            tasks.append(asyncio.create_task(mf.put(payload)))

        def assert_below_limit():
            assert 0 <= concurrent_writes() <= MultiFile.concurrent_files

        while not concurrent_writes() == MultiFile.concurrent_files:
            assert_below_limit()
            await asyncio.sleep(0)

        while mf.shards:
            await asyncio.sleep(0)
            assert_below_limit()


@pytest.mark.parametrize(
    "explicit_flush",
    [True, False],
)
@gen_test()
async def test_high_pressure_flush_with_exception(tmp_path, explicit_flush):
    counter = 0
    payload = {f"shard-{ix}": [f"shard-{ix}".encode() * 100] for ix in range(100)}

    def dump_broken(data, f):
        nonlocal counter
        # We only want to raise if this was queued up before
        if counter > MultiFile.concurrent_files:
            raise Exception(123)
        counter += 1
        dump(data, f)

    # Something here should raise...
    with pytest.raises(Exception, match="123"):
        async with MultiFile(
            directory=tmp_path,
            dump=dump_broken,
            load=load,
            loop=IOLoop.current(),
        ) as mf:
            tasks = []
            for _ in range(10):
                tasks.append(asyncio.create_task(mf.put(payload)))

            # Wait until things are actually queued up.
            # This is when there is no slot on the queue available anymore
            # but there are still shards around
            while not (mf.shards and mf._queue.empty()):
                # Disks are fast, don't give it time to unload the queue...
                # There may only be a few ticks atm so keep this at zero
                await asyncio.sleep(0)

            # This toggle makes sense to ensure this code path is triggered even
            # if close does not flush
            if explicit_flush:
                # Flushing while this happens is a bad idea and deadlocks atm
                await mf.flush()


def gen_bytes(percentage: float) -> bytes:
    num_bytes = int(math.floor(percentage * MultiFile.memory_limit))
    return b"0" * num_bytes


@pytest.mark.slow
@gen_test()
async def test_memory_limit(tmp_path):
    # TODO: Memory limit concurrency is defined on interpreter level. Need to
    # test multiple instances

    big_payload = {
        "shard-1": [gen_bytes(2)] * 2,
        "shard-2": [gen_bytes(0.1)] * 10,
        "shard-3": [gen_bytes(1)] * 2,
    }
    small_payload = {
        "shard-4": [gen_bytes(0.1)],
    }
    async with MultiFile(
        directory=tmp_path,
        dump=dump,
        load=load,
        loop=IOLoop.current(),
    ) as mf:
        many_small = [asyncio.create_task(mf.put(small_payload)) for _ in range(9)]
        many_small = asyncio.gather(*many_small)
        # Puts that do not breach the limit do not block
        await asyncio.wait_for(many_small, 0.05)

        many_small = [asyncio.create_task(mf.put(small_payload)) for _ in range(12)]
        many_small = asyncio.gather(*many_small)
        # Puts that do not breach the limit do not block
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(many_small), 0.1)

        big = asyncio.create_task(mf.put(big_payload))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(big), 0.1)
        small = asyncio.create_task(mf.put(small_payload))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(small), 0.1)
        await big

        # Once the big write is through, we can write without blocking again
        await asyncio.wait_for(mf.put(small_payload), 0.05)


@pytest.mark.slow
@gen_test()
async def test_memory_limit_blocked_exception(tmp_path):
    # TODO: Memory limit concurrency is defined on interpreter level. Need to
    # test multiple instances

    def dump_only_bytes(data, f):
        if not isinstance(data, bytes):
            raise TypeError("Wrong type")
        f.write(data)

    big_payload = {
        "shard-1": [gen_bytes(2)] * 5,
    }
    broken_payload = {
        "shard-2": ["not-bytes"],
    }
    small_payload = {
        "shard-2": [b"bytes"],
    }
    async with MultiFile(
        directory=tmp_path,
        dump=dump_only_bytes,
        load=load,
        loop=IOLoop.current(),
    ) as mf:
        big_write = asyncio.create_task(mf.put(big_payload))
        broken_write = asyncio.create_task(mf.put(broken_payload))
        small_write = asyncio.create_task(mf.put(small_payload))

        # The broken write hits the limit and blocks
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(broken_write), 0.1)

        await asyncio.gather(big_write, broken_write, small_write)

        # Make sure exception is not dropped
        with pytest.raises(TypeError, match="Wrong type"):
            await mf.flush()
