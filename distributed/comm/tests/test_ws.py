import warnings
import pytest

from distributed import Client, Scheduler, Worker
from distributed.comm.registry import backends, get_backend
from distributed.comm import ws, listen, connect
from distributed.utils_test import (
    get_client_ssl_context,
    get_server_ssl_context,
    cleanup,
    inc,
    captured_logger,
)  # noqa: F401

from .test_comms import check_tls_extra


def test_registered():
    assert "ws" in backends
    backend = get_backend("ws")
    assert isinstance(backend, ws.WSBackend)


@pytest.mark.asyncio
async def test_listen_connect():
    async def handle_comm(comm):
        while True:
            msg = await comm.read()
            await comm.write(msg)

    async with listen("ws://", handle_comm) as listener:
        comm = await connect(listener.contact_address)
        await comm.write(b"Hello!")
        result = await comm.read()
        assert result == b"Hello!"

        await comm.close()


@pytest.mark.asyncio
async def test_listen_connect_wss():
    async def handle_comm(comm):
        while True:
            msg = await comm.read()
            await comm.write(msg)

    server_ctx = get_server_ssl_context()
    client_ctx = get_client_ssl_context()

    async with listen("wss://", handle_comm, ssl_context=server_ctx) as listener:
        comm = await connect(listener.contact_address, ssl_context=client_ctx)
        assert comm.peer_address.startswith("wss://")
        check_tls_extra(comm.extra_info)
        await comm.write(b"Hello!")
        result = await comm.read()
        assert result == b"Hello!"
        await comm.close()


@pytest.mark.asyncio
async def test_expect_ssl_context():
    server_ctx = get_server_ssl_context()

    async with listen("wss://", lambda comm: comm, ssl_context=server_ctx) as listener:
        with pytest.raises(TypeError):
            comm = await connect(listener.contact_address)


@pytest.mark.asyncio
async def test_roundtrip(cleanup):
    async with Scheduler(protocol="ws://") as s:
        async with Worker(s.address) as w:
            async with Client(s.address, asynchronous=True) as c:
                assert c.scheduler.address.startswith("ws://")
                assert w.address.startswith("ws://")
                future = c.submit(inc, 1)
                result = await future
                assert result == 2


@pytest.mark.asyncio
async def test_collections(cleanup):
    da = pytest.importorskip("dask.array")
    async with Scheduler(protocol="ws://") as s:
        async with Worker(s.address) as a:
            async with Worker(s.address) as b:
                async with Client(s.address, asynchronous=True) as c:
                    x = da.random.random((1000, 1000), chunks=(100, 100))
                    x = x + x.T
                    await x.persist()


@pytest.mark.asyncio
async def test_large_transfer(cleanup):
    np = pytest.importorskip("numpy")
    async with Scheduler(protocol="ws://") as s:
        async with Worker(s.address, protocol="ws://") as w:
            async with Client(s.address, asynchronous=True) as c:
                future = await c.scatter(np.random.random(1000000))


@pytest.mark.asyncio
@pytest.mark.parametrize("dashboard", [True, False])
async def test_same_http_server(cleanup, dashboard):
    async with Scheduler(protocol="ws://", dashboard=dashboard) as s:
        assert s.http_server is s.listener.server
        async with Worker(s.address, protocol="ws://") as w:
            async with Client(s.address, asynchronous=True) as c:
                result = await c.submit(lambda x: x + 1, 10)
                assert result == 11


@pytest.mark.asyncio
async def test_quiet_close(cleanup):
    with warnings.catch_warnings(record=True) as record:
        async with Client(protocol="ws", processes=False, asynchronous=True) as c:
            pass

    # For some reason unrelated @coroutine warnings are showing up
    record = [warning for warning in record if "coroutine" not in str(warning.message)]

    assert not record, record[0].message
