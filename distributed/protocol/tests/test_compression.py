from __future__ import annotations

import zlib

import pytest

from distributed import Worker, wait
from distributed.compatibility import LINUX
from distributed.protocol.compression import Compression, compressions
from distributed.utils_test import gen_cluster


@pytest.fixture(scope="function")
def compression_counters():
    counters = [0, 0]

    def compress(v):
        counters[0] += 1
        return zlib.compress(v)

    def decompress(v):
        counters[1] += 1
        return zlib.decompress(v)

    compressions["dummy"] = Compression("dummy", compress, decompress, None)
    yield counters
    del compressions["dummy"]


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[], config={"distributed.comm.compression": "dummy"})
async def test_compress_remote_comms(c, s, compression_counters):
    # Note: if you swap the IP addresses, the test will fail saying that compression
    # was not used. This is because the auto-generated Comm.local_address for the TCP
    # client is unrelated to the listening address of the worker that spawned it.
    async with Worker(s.address, host="127.0.0.2") as a:
        async with Worker(s.address, host="127.0.0.1") as b:
            x = c.submit(lambda: "x" * 11_000, workers=[a.address])
            y = c.submit(lambda x: None, x, workers=[b.address])
            await wait(y)
            assert compression_counters == [2, 1]


@pytest.mark.parametrize("host", [None, "127.0.0.1", "localhost"])
@gen_cluster(client=True, nthreads=[], config={"distributed.comm.compression": "dummy"})
async def test_disable_compression_on_localhost(c, s, compression_counters, host):
    async with Worker(s.address, host=host) as a:
        async with Worker(s.address, host=host) as b:
            x = c.submit(lambda: "x" * 11_000, workers=[a.address])
            y = c.submit(lambda x: None, x, workers=[b.address])
            await wait(y)
            assert compression_counters == [0, 0]
