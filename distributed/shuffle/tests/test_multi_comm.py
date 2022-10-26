from __future__ import annotations

import asyncio
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
