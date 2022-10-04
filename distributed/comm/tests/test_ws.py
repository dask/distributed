from __future__ import annotations

import os
import warnings

import pytest

import dask

from distributed import Client, Scheduler, Worker
from distributed.comm import connect, listen, ws
from distributed.comm.core import FatalCommClosedError
from distributed.comm.registry import backends, get_backend
from distributed.comm.tests.test_comms import check_tls_extra
from distributed.security import Security
from distributed.utils_test import (
    gen_cluster,
    gen_test,
    get_client_ssl_context,
    get_server_ssl_context,
    inc,
    xfail_ssl_issue5601,
)

pytestmark = pytest.mark.flaky(reruns=2)


def test_registered():
    assert "ws" in backends
    backend = get_backend("ws")
    assert isinstance(backend, ws.WSBackend)


@gen_test()
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


@gen_test()
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


@gen_test()
async def test_expect_ssl_context():
    server_ctx = get_server_ssl_context()

    async with listen("wss://", lambda comm: comm, ssl_context=server_ctx) as listener:
        with pytest.raises(FatalCommClosedError, match="TLS expects a `ssl_context` *"):
            comm = await connect(listener.contact_address)


@gen_test()
async def test_expect_scheduler_ssl_when_sharing_server(tmpdir):
    xfail_ssl_issue5601()
    pytest.importorskip("cryptography")
    security = Security.temporary()
    key_path = os.path.join(str(tmpdir), "dask.pem")
    cert_path = os.path.join(str(tmpdir), "dask.crt")
    with open(key_path, "w") as f:
        f.write(security.tls_scheduler_key)
    with open(cert_path, "w") as f:
        f.write(security.tls_scheduler_cert)
    c = {
        "distributed.scheduler.dashboard.tls.key": key_path,
        "distributed.scheduler.dashboard.tls.cert": cert_path,
    }
    with dask.config.set(c):
        with pytest.raises(RuntimeError):
            async with Scheduler(protocol="ws://", dashboard=True, port=8787):
                pass


@gen_cluster(client=True, scheduler_kwargs={"protocol": "ws://"})
async def test_roundtrip(c, s, a, b):
    assert a.address.startswith("ws://")
    assert b.address.startswith("ws://")
    assert c.scheduler.address.startswith("ws://")
    assert await c.submit(inc, 1) == 2


@gen_cluster(client=True, scheduler_kwargs={"protocol": "ws://"})
async def test_collections(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.random.random((1000, 1000), chunks=(100, 100))
    x = x + x.T
    await x.persist()


@gen_cluster(client=True, scheduler_kwargs={"protocol": "ws://"})
async def test_large_transfer(c, s, a, b):
    np = pytest.importorskip("numpy")
    await c.scatter(np.random.random(1_000_000))


@gen_test()
async def test_large_transfer_with_no_compression():
    np = pytest.importorskip("numpy")
    with dask.config.set({"distributed.comm.compression": None}):
        async with Scheduler(protocol="ws://") as s:
            async with Worker(s.address, protocol="ws://"):
                async with Client(s.address, asynchronous=True) as c:
                    await c.scatter(np.random.random(1_500_000))


@pytest.mark.parametrize(
    "dashboard,protocol,security,port",
    [
        (True, "ws://", None, 8787),
        (True, "wss://", True, 8787),
        (False, "ws://", None, 8787),
        (False, "wss://", True, 8787),
        (True, "ws://", None, 8786),
        (True, "wss://", True, 8786),
        (False, "ws://", None, 8786),
        (False, "wss://", True, 8786),
    ],
)
@gen_test()
async def test_http_and_comm_server(dashboard, protocol, security, port):
    if security:
        xfail_ssl_issue5601()
        pytest.importorskip("cryptography")
        security = Security.temporary()
    async with Scheduler(
        protocol=protocol, dashboard=dashboard, port=port, security=security
    ) as s:
        if port == 8787:
            assert s.http_server is s.listener.server
        else:
            assert s.http_server is not s.listener.server
        async with Worker(s.address, protocol=protocol, security=security) as w:
            async with Client(s.address, asynchronous=True, security=security) as c:
                result = await c.submit(lambda x: x + 1, 10)
                assert result == 11


@pytest.mark.parametrize(
    "protocol,sni",
    [("ws://", True), ("ws://", False), ("wss://", True), ("wss://", False)],
)
@gen_test()
async def test_connection_made_with_extra_conn_args(protocol, sni):
    if protocol == "ws://":
        security = Security(
            extra_conn_args={"headers": {"Authorization": "Token abcd"}}
        )
    else:
        xfail_ssl_issue5601()
        pytest.importorskip("cryptography")
        security = Security.temporary(
            extra_conn_args={"headers": {"Authorization": "Token abcd"}}
        )
    async with Scheduler(
        protocol=protocol, security=security, dashboard_address=":0"
    ) as s:
        connection_args = security.get_connection_args("worker")
        if sni:
            connection_args["server_hostname"] = "sni.example.host"
        comm = await connect(s.address, **connection_args)
        assert comm.sock.request.headers.get("Authorization") == "Token abcd"

        await comm.close()


@gen_test()
async def test_connection_made_with_sni():
    xfail_ssl_issue5601()
    pytest.importorskip("cryptography")
    security = Security.temporary()
    async with Scheduler(
        protocol="wss://", security=security, dashboard_address=":0"
    ) as s:
        connection_args = security.get_connection_args("worker")
        connection_args["server_hostname"] = "sni.example.host"
        comm = await connect(s.address, **connection_args)
        assert comm.sock.request.headers.get("Host") == "sni.example.host"

        await comm.close()


@gen_test()
async def test_quiet_close():
    with warnings.catch_warnings(record=True) as record:
        async with Client(
            protocol="ws", processes=False, asynchronous=True, dashboard_address=":0"
        ):
            pass

    assert not record, record[0].message


@gen_cluster(client=True, scheduler_kwargs={"protocol": "ws://"})
async def test_ws_roundtrip(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = np.arange(100)
    future = await c.scatter(x)
    y = await future
    assert (x == y).all()


@gen_test()
async def test_wss_roundtrip():
    np = pytest.importorskip("numpy")
    xfail_ssl_issue5601()
    pytest.importorskip("cryptography")
    security = Security.temporary()
    async with Scheduler(
        protocol="wss://", security=security, dashboard_address=":0"
    ) as s:
        async with Worker(s.address, security=security) as w:
            async with Client(s.address, security=security, asynchronous=True) as c:
                x = np.arange(100)
                future = await c.scatter(x)
                y = await future
                assert (x == y).all()
