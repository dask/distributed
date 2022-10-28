from __future__ import annotations

import asyncio
import concurrent.futures
import math
from collections import defaultdict

import pytest
from tornado.ioloop import IOLoop

from distributed.shuffle._multi_comm import MultiComm
from distributed.utils_test import gen_test


@gen_test()
async def test_basic(tmp_path):
    d = defaultdict(list)

    async def send(address, shards):
        d[address].extend(shards)

    mc = MultiComm(send=send, loop=IOLoop.current())
    mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})
    mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

    await mc.flush()

    assert b"".join(d["x"]) == b"0" * 2000
    assert b"".join(d["y"]) == b"1" * 1000


@gen_test()
async def test_exceptions(tmp_path):
    d = defaultdict(list)

    async def send(address, shards):
        raise Exception(123)

    mc = MultiComm(send=send, loop=IOLoop.current())
    mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

    while not mc._exception:
        await asyncio.sleep(0.1)

    with pytest.raises(Exception, match="123"):
        mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

    with pytest.raises(Exception, match="123"):
        await mc.flush()

    await mc.close()


@gen_test()
async def test_slow_send(tmpdir):
    block_send = asyncio.Event()
    block_send.set()
    sending_first = asyncio.Event()
    d = defaultdict(list)

    async def send(address, shards):
        await block_send.wait()
        d[address].extend(shards)
        sending_first.set()

    mc = MultiComm(send=send, loop=IOLoop.current())
    mc.max_connections = 1
    mc.put({"x": [b"0"], "y": [b"1"]})
    mc.put({"x": [b"0"], "y": [b"1"]})
    flush_task = asyncio.create_task(mc.flush())
    await sending_first.wait()
    block_send.clear()

    with pytest.raises(RuntimeError):
        mc.put({"x": [b"2"], "y": [b"2"]})
        await flush_task

    assert [b"2" not in shard for shard in d["x"]]


def gen_bytes(percentage: float) -> bytes:
    num_bytes = int(math.floor(percentage * MultiComm.memory_limit))
    return b"0" * num_bytes


@pytest.mark.parametrize("explicit_flush", [True, False])
@gen_test()
async def test_concurrent_puts(explicit_flush):
    d = defaultdict(list)

    async def send(address, shards):
        d[address].extend(shards)

    frac = 0.1
    nshards = 10
    nputs = 20
    payload = {x: [gen_bytes(frac)] for x in range(nshards)}
    with concurrent.futures.ThreadPoolExecutor(
        2, thread_name_prefix="test IOLoop"
    ) as tpe:
        async with MultiComm(send=send, loop=IOLoop.current()) as mc:
            loop = asyncio.get_running_loop()
            futs = [loop.run_in_executor(tpe, mc.put, payload) for _ in range(nputs)]

            await asyncio.gather(*futs)
            if explicit_flush:
                await mc.flush()

                assert not mc.shards
                assert not mc.sizes

        assert not mc.shards
        assert not mc.sizes
        assert len(d) == 10
        assert sum(map(len, d[0])) == len(gen_bytes(frac)) * nputs


@gen_test()
async def test_concurrent_puts_error():
    d = defaultdict(list)

    counter = 0

    async def send(address, shards):
        nonlocal counter
        counter += 1
        if counter == 5:
            raise OSError("error during send")
        d[address].extend(shards)

    frac = 0.1
    nshards = 10
    nputs = 20
    payload = {x: [gen_bytes(frac)] for x in range(nshards)}
    with concurrent.futures.ThreadPoolExecutor(
        2, thread_name_prefix="test IOLoop"
    ) as tpe:
        async with MultiComm(send=send, loop=IOLoop.current()) as mc:
            loop = asyncio.get_running_loop()
            futs = [loop.run_in_executor(tpe, mc.put, payload) for _ in range(nputs)]

            with pytest.raises(OSError, match="error during send"):
                await asyncio.gather(*futs)

    assert not mc.shards
    assert not mc.sizes
