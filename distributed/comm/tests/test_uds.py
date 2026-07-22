import asyncio
import os

import pytest

from distributed.comm.addressing import parse_address, unparse_address
from distributed.comm.core import connect
from distributed.comm.registry import backends, get_backend
from distributed.comm.uds import UDSBackend, UDSListener
from distributed.utils_test import gen_test


@pytest.fixture(params=["tornado"])
def uds(monkeypatch, request):
    """Set the TCP backend to either tornado or asyncio"""
    if request.param == "tornado":
        import distributed.comm.uds as uds
    else:
        raise NotImplementedError()
    monkeypatch.setitem(backends, "uds", UDSBackend())
    return uds


def test_registered():
    assert "uds" in backends
    backend = get_backend("uds")
    assert isinstance(backend, UDSBackend)


def test_parse_uds_address():
    addr = "uds:///tmp/dask-test.sock"
    scheme, loc = parse_address(addr)
    assert scheme == "uds"
    assert loc == "/tmp/dask-test.sock"
    assert unparse_address(scheme, loc) == addr


@gen_test()
async def test_uds_specific(uds):
    """
    Test concrete UDS API.
    """

    async def handle_comm(comm):
        assert comm.peer_address == (f"uds://{host}:0")
        assert comm.extra_info == {}
        msg = await comm.read()
        msg["op"] = "pong"
        await comm.write(msg)
        await comm.close()

    listener = await UDSListener("localhost", handle_comm)
    host, port = listener.get_host_port()

    assert host.endswith(".sock")
    assert port == 0  # we fake port 0 when using UDS

    l = []

    async def client_communicate(key, delay=0):
        comm = await connect(listener.contact_address)
        assert comm.peer_address == f"uds://{host}:0"
        assert comm.extra_info == {}
        await comm.write({"op": "ping", "data": key})
        if delay:
            await asyncio.sleep(delay)
        msg = await comm.read()
        assert msg == {"op": "pong", "data": key}
        l.append(key)
        await comm.close()

    await client_communicate(key=1234)

    # Many clients at once
    N = 100
    futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
    await asyncio.gather(*futures)
    assert set(l) == {1234} | set(range(N))

    listener.stop()
    assert not os.path.exists(host)  # assert socket deleted
