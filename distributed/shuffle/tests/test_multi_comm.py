from collections import defaultdict

import pytest

from distributed.shuffle.multi_comm import MultiComm


@pytest.mark.asyncio
async def test_basic(tmp_path):
    d = defaultdict(list)

    async def send(address, shards):
        d[address].extend(shards)

    mc = MultiComm(send=send)
    mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})
    mc.put({"x": [b"0" * 1000], "y": [b"1" * 500]})

    await mc.flush()

    assert b"".join(d["x"]) == b"0" * 2000
    assert b"".join(d["y"]) == b"1" * 1000
