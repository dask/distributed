from __future__ import annotations

import importlib
import zlib

import pytest

import dask

import distributed.protocol.compression
from distributed import Client, Scheduler, Worker, wait
from distributed.compatibility import LINUX
from distributed.protocol.compression import Compression
from distributed.utils_test import gen_test

ROLES = ["remote-client", "remote-worker", "localhost", "spill"]


@pytest.mark.parametrize("role", ROLES)
def test_config_auto_compression_is_updated(role):
    expect = distributed.protocol.compression.compressions["auto"].name
    assert expect in ("lz4", "snappy", None)
    with dask.config.set({"distributed.comm.compression." + role: "auto"}):
        importlib.reload(distributed.protocol.compression)
        assert dask.config.get("distributed.comm.compression." + role) == expect


@pytest.mark.parametrize("role", ROLES)
def test_config_invalid(role):
    """Test that configuration is validated at module import time"""
    with dask.config.set({"distributed.comm.compression." + role: "dunno"}):
        with pytest.raises(
            ValueError, match=r"distributed\.comm\.compression\..*dunno.*zlib"
        ):
            importlib.reload(distributed.protocol.compression)


@pytest.mark.parametrize("compression", ["auto", None, False, "zlib"])
def test_config_deprecated(compression):
    with dask.config.set({"distributed.comm.compression": compression}):
        with pytest.warns(
            FutureWarning, match="distributed.comm.compression has been split up"
        ):
            importlib.reload(distributed.protocol.compression)
        expect = distributed.protocol.compression.compressions[compression].name
        assert dask.config.get("distributed.comm.compression") == {
            role: expect for role in ROLES
        }


class DummyCompression:
    def __init__(self):
        self.n_compress = 0
        self.n_decompress = 0

    def compress(self, x):
        self.n_compress += 1
        # Perform actual compression; otherwise decompress() would never be called
        return zlib.compress(x)

    def decompress(self, x):
        self.n_decompress += 1
        return zlib.decompress(x)


DUMMY_COMPRESSIONS_CFG = {
    f"distributed.comm.compression.{r}": f"dummy_{r}" for r in ROLES
}


@pytest.fixture(scope="function")
def dummy_compressions():
    """Inject role-specific compressions that are actually zlib plus a call counter"""
    dummies = {r: DummyCompression() for r in ROLES}
    for r in ROLES:
        distributed.protocol.compression.compressions[f"dummy_{r}"] = Compression(
            f"dummy_{r}", dummies[r].compress, dummies[r].decompress
        )

    yield dummies

    for r in ROLES:
        del distributed.protocol.compression.compressions[f"dummy_{r}"]


def assert_use_compression_role(role, n, dummies):
    expect = {r: n if r == role else (0, 0) for r, dummy in dummies.items()}
    actual = {r: (obj.n_compress, obj.n_decompress) for r, obj in dummies.items()}
    for dummy in dummies.values():
        dummy.n_compress = 0
        dummy.n_decompress = 0

    assert actual == expect


@pytest.mark.parametrize("direct", [None, False, True])
@pytest.mark.parametrize("host", [None, "127.0.0.1", "localhost"])
@gen_test(config=DUMMY_COMPRESSIONS_CFG)
async def test_local_client(dummy_compressions, direct, host):
    payload = "x" * 11_000  # larger than min_size parameter of maybe_compress

    async with Scheduler(host=host, port=0, dashboard_address=":0") as s:
        async with Worker(s.address, host=host):
            async with Client(s.address, asynchronous=True) as c:
                x = await c.scatter({"x": payload}, direct=direct)
                assert_use_compression_role("localhost", (2, 1), dummy_compressions)
                assert await c.gather(x, direct=direct) == {"x": payload}
                assert_use_compression_role("localhost", (2, 1), dummy_compressions)


@pytest.mark.parametrize("direct", [None, False, True])
@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_test(config=DUMMY_COMPRESSIONS_CFG)
async def test_remote_client(dummy_compressions, direct):
    payload = "x" * 11_000  # larger than min_size parameter of maybe_compress

    async with Scheduler(host="127.0.0.2", port=0, dashboard_address=":0") as s:
        async with Worker(s.address, host="127.0.0.2"):
            async with Client(s.address, asynchronous=True) as c:
                x = await c.scatter({"x": payload}, direct=direct)
                assert_use_compression_role("remote-client", (2, 1), dummy_compressions)
                assert await c.gather(x, direct=direct) == {"x": payload}
                assert_use_compression_role("remote-client", (2, 1), dummy_compressions)


@pytest.mark.parametrize("host", [None, "127.0.0.1", "localhost"])
@gen_test(config=DUMMY_COMPRESSIONS_CFG)
async def test_local_worker(dummy_compressions, host):
    async with Scheduler(port=0, dashboard_address=":0") as s:
        async with Worker(s.address, host=host) as a:
            async with Worker(s.address, host=host) as b:
                async with Client(s.address, asynchronous=True) as c:
                    x = c.submit(lambda: "x" * 11_000, workers=[a.address])
                    y = c.submit(lambda x: None, x, workers=[b.address])
                    await wait(y)
                    assert_use_compression_role("localhost", (2, 1), dummy_compressions)


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_test(config=DUMMY_COMPRESSIONS_CFG)
async def test_remote_worker(dummy_compressions):
    async with Scheduler(port=0, dashboard_address=":0") as s:
        # Note: if you swap the IP addresses, the test will fail saying that the
        # 'localhost' compression role was used. This is because the auto-generated
        # Comm.local_address for the TCP client is unrelated to the listening address of
        # the worker that spawned it.
        async with Worker(s.address, host="127.0.0.2") as a:
            async with Worker(s.address, host="127.0.0.1") as b:
                async with Client(s.address, asynchronous=True) as c:
                    x = c.submit(lambda: "x" * 11_000, workers=[a.address])
                    y = c.submit(lambda x: None, x, workers=[b.address])
                    await wait(y)
                    assert_use_compression_role(
                        "remote-worker", (2, 1), dummy_compressions
                    )
