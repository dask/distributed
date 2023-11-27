from __future__ import annotations

import asyncio
import os
import sys
import threading
from functools import partial

import pytest
from tornado import ioloop

import dask

from distributed.comm import (
    CommClosedError,
    connect,
    get_address_host,
    get_local_address_for,
    inproc,
    listen,
    parse_address,
    parse_host_port,
    resolve_address,
    unparse_host_port,
)
from distributed.comm.registry import backends, get_backend
from distributed.comm.tcp import get_stream_address
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.metrics import time
from distributed.protocol import Serialized, deserialize, serialize, to_serialize
from distributed.protocol.utils_test import get_host_array
from distributed.utils import get_ip, get_ipv6, get_mp_context, wait_for
from distributed.utils_test import (
    gen_test,
    get_cert,
    get_client_ssl_context,
    get_server_ssl_context,
    has_ipv6,
    requires_ipv6,
)

EXTERNAL_IP4 = get_ip()


@pytest.fixture(params=["tornado"])
def tcp(monkeypatch, request):
    """Set the TCP backend to either tornado or asyncio"""
    if request.param == "tornado":
        import distributed.comm.tcp as tcp
    else:
        raise NotImplementedError()
    monkeypatch.setitem(backends, "tcp", tcp.TCPBackend())
    monkeypatch.setitem(backends, "tls", tcp.TLSBackend())
    return tcp


ca_file = get_cert("tls-ca-cert.pem")

# The Subject field of our test certs
cert_subject = (
    (("countryName", "XY"),),
    (("localityName", "Dask-distributed"),),
    (("organizationName", "Dask"),),
    (("commonName", "localhost"),),
)


def check_tls_extra(info):
    assert isinstance(info, dict)
    assert info["peercert"]["subject"] == cert_subject
    assert "cipher" in info
    cipher_name, proto_name, secret_bits = info["cipher"]
    # Most likely
    assert "AES" in cipher_name
    assert "TLS" in proto_name
    assert secret_bits >= 128


tls_kwargs = dict(
    listen_args={"ssl_context": get_server_ssl_context()},
    connect_args={"ssl_context": get_client_ssl_context()},
)


async def get_comm_pair(listen_addr, listen_args=None, connect_args=None, **kwargs):
    listen_args = listen_args or {}
    connect_args = connect_args or {}
    q = asyncio.Queue()

    async def handle_comm(comm):
        await q.put(comm)

    async with listen(listen_addr, handle_comm, **listen_args, **kwargs) as listener:
        comm = await connect(listener.contact_address, **connect_args, **kwargs)

    serv_comm = await q.get()
    return (comm, serv_comm)


def get_tcp_comm_pair(**kwargs):
    return get_comm_pair("tcp://", **kwargs)


def get_tls_comm_pair(**kwargs):
    kwargs.update(tls_kwargs)
    return get_comm_pair("tls://", **kwargs)


def get_inproc_comm_pair(**kwargs):
    return get_comm_pair("inproc://", **kwargs)


async def debug_loop():
    """
    Debug helper
    """
    while True:
        loop = ioloop.IOLoop.current()
        print(".", loop, loop._handlers)
        await asyncio.sleep(0.5)


#
# Test utility functions
#


def test_parse_host_port(tcp):
    f = parse_host_port

    assert f("localhost:123") == ("localhost", 123)
    assert f("127.0.0.1:456") == ("127.0.0.1", 456)
    assert f("localhost:123", 80) == ("localhost", 123)
    assert f("localhost", 80) == ("localhost", 80)

    with pytest.raises(ValueError):
        f("localhost")

    assert f("[::1]:123") == ("::1", 123)
    assert f("[fe80::1]:123", 80) == ("fe80::1", 123)
    assert f("[::1]", 80) == ("::1", 80)

    with pytest.raises(ValueError):
        f("[::1]")
    with pytest.raises(ValueError):
        f("::1:123")
    with pytest.raises(ValueError):
        f("::1")


def test_unparse_host_port(tcp):
    f = unparse_host_port

    assert f("localhost", 123) == "localhost:123"
    assert f("127.0.0.1", 123) == "127.0.0.1:123"
    assert f("::1", 123) == "[::1]:123"
    assert f("[::1]", 123) == "[::1]:123"

    assert f("127.0.0.1") == "127.0.0.1"
    assert f("127.0.0.1", None) == "127.0.0.1"
    assert f("127.0.0.1", "*") == "127.0.0.1:*"

    assert f("::1") == "[::1]"
    assert f("[::1]") == "[::1]"
    assert f("::1", "*") == "[::1]:*"


def test_get_address_host(tcp):
    f = get_address_host

    assert f("tcp://127.0.0.1:123") == "127.0.0.1"
    assert f("inproc://%s/%d/123" % (get_ip(), os.getpid())) == get_ip()


def test_resolve_address(tcp):
    f = resolve_address

    assert f("tcp://127.0.0.1:123") == "tcp://127.0.0.1:123"
    assert f("127.0.0.2:789") == "tcp://127.0.0.2:789"
    assert f("tcp://0.0.0.0:456") == "tcp://0.0.0.0:456"
    assert f("tcp://0.0.0.0:456") == "tcp://0.0.0.0:456"

    if has_ipv6():
        assert f("tcp://[::1]:123") == "tcp://[::1]:123"
        assert f("tls://[::1]:123") == "tls://[::1]:123"
        # OS X returns '::0.0.0.2' as canonical representation
        assert f("[::2]:789") in ("tcp://[::2]:789", "tcp://[::0.0.0.2]:789")
        assert f("tcp://[::]:123") == "tcp://[::]:123"

    assert f("localhost:123") == "tcp://127.0.0.1:123"
    assert f("tcp://localhost:456") == "tcp://127.0.0.1:456"
    assert f("tls://localhost:456") == "tls://127.0.0.1:456"


def test_get_local_address_for(tcp):
    f = get_local_address_for

    assert f("tcp://127.0.0.1:80") == "tcp://127.0.0.1"
    assert f("tcp://8.8.8.8:4444") == "tcp://" + get_ip()
    if has_ipv6():
        assert f("tcp://[::1]:123") == "tcp://[::1]"

    inproc_arg = "inproc://%s/%d/444" % (get_ip(), os.getpid())
    inproc_res = f(inproc_arg)
    assert inproc_res.startswith("inproc://")
    assert inproc_res != inproc_arg


#
# Test concrete transport APIs
#


@gen_test()
async def test_tcp_listener_does_not_call_handler_on_handshake_error(tcp):
    handle_comm_called = False

    async def handle_comm(comm):
        nonlocal handle_comm_called
        handle_comm_called = True

    with dask.config.set({"distributed.comm.timeouts.connect": 0.01}):
        listener = await tcp.TCPListener("127.0.0.1", handle_comm)
        host, port = listener.get_host_port()
        # connect without handshake:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        # wait a bit to let the listener side hit the timeout on the handshake:
        await asyncio.sleep(0.02)

    assert not handle_comm_called

    writer.close()
    await writer.wait_closed()


@gen_test()
async def test_tcp_specific(tcp):
    """
    Test concrete TCP API.
    """

    async def handle_comm(comm):
        assert comm.peer_address.startswith("tcp://" + host)
        assert comm.extra_info == {}
        msg = await comm.read()
        msg["op"] = "pong"
        await comm.write(msg)
        await comm.close()

    listener = await tcp.TCPListener("127.0.0.1", handle_comm)
    host, port = listener.get_host_port()
    assert host in ("localhost", "127.0.0.1", "::1")
    assert port > 0

    l = []

    async def client_communicate(key, delay=0):
        addr = "%s:%d" % (host, port)
        comm = await connect(listener.contact_address)
        assert comm.peer_address == "tcp://" + addr
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


@pytest.mark.parametrize("sni", [None, "localhost"])
@gen_test()
async def test_tls_specific(tcp, sni):
    """
    Test concrete TLS API.
    """

    async def handle_comm(comm):
        try:
            assert comm.peer_address.startswith("tls://" + host)
            check_tls_extra(comm.extra_info)
            msg = await comm.read()
            msg["op"] = "pong"
            await comm.write(msg)
        finally:
            await comm.close()

    server_ctx = get_server_ssl_context()
    client_ctx = get_client_ssl_context()

    listener = await tcp.TLSListener("127.0.0.1", handle_comm, ssl_context=server_ctx)
    host, port = listener.get_host_port()
    assert host in ("localhost", "127.0.0.1", "::1")
    assert port > 0

    l = []

    async def client_communicate(key, delay=0):
        addr = "%s:%d" % (host, port)
        comm = await connect(
            listener.contact_address, ssl_context=client_ctx, server_hostname=sni
        )
        try:
            assert comm.peer_address == "tls://" + addr
            check_tls_extra(comm.extra_info)
            await comm.write({"op": "ping", "data": key})
            if delay:
                await asyncio.sleep(delay)
            msg = await comm.read()
            assert msg == {"op": "pong", "data": key}
            l.append(key)
        finally:
            await comm.close()

    await client_communicate(key=1234)

    # Many clients at once
    N = 100
    futures = [client_communicate(key=i, delay=0.05) for i in range(N)]
    await asyncio.gather(*futures)
    assert set(l) == {1234} | set(range(N))


@gen_test()
async def test_comm_failure_threading(tcp):
    """
    When we fail to connect, make sure we don't make a lot
    of threads.

    We only assert for PY3, because the thread limit only is
    set for python 3.  See github PR #2403 discussion for info.
    """

    async def sleep_for_60ms():
        max_thread_count = 0
        for _ in range(60):
            await asyncio.sleep(0.001)
            thread_count = threading.active_count()
            if thread_count > max_thread_count:
                max_thread_count = thread_count
        return max_thread_count

    original_thread_count = threading.active_count()

    # tcp.TCPConnector()
    sleep_future = sleep_for_60ms()
    with pytest.raises(IOError):
        await connect("tcp://localhost:28400", 0.052)
    max_thread_count = await sleep_future
    # 2 is the number set by BaseTCPConnector.executor (ThreadPoolExecutor)
    assert max_thread_count <= 2 + original_thread_count

    # tcp.TLSConnector()
    sleep_future = sleep_for_60ms()
    with pytest.raises(IOError):
        await connect(
            "tls://localhost:28400", 0.052, ssl_context=get_client_ssl_context()
        )
    max_thread_count = await sleep_future
    assert max_thread_count <= 2 + original_thread_count


async def check_inproc_specific(run_client):
    """
    Test concrete InProc API.
    """
    listener_addr = inproc.global_manager.new_address()
    addr_head = listener_addr.rpartition("/")[0]

    client_addresses = set()

    N_MSGS = 3

    async def handle_comm(comm):
        try:
            assert comm.peer_address.startswith("inproc://" + addr_head)
            client_addresses.add(comm.peer_address)
            for _ in range(N_MSGS):
                msg = await comm.read()
                msg["op"] = "pong"
                await comm.write(msg)
        finally:
            await comm.close()

    async with inproc.InProcListener(listener_addr, handle_comm) as listener:
        assert (
            listener.listen_address
            == listener.contact_address
            == "inproc://" + listener_addr
        )

        l = []

        async def client_communicate(key, delay=0):
            comm = await connect(listener.contact_address)
            try:
                assert comm.peer_address == "inproc://" + listener_addr
                for _ in range(N_MSGS):
                    await comm.write({"op": "ping", "data": key})
                    if delay:
                        await asyncio.sleep(delay)
                    msg = await comm.read()
                assert msg == {"op": "pong", "data": key}
                l.append(key)
                with pytest.raises(CommClosedError):
                    await comm.read()
            finally:
                await comm.close()

        client_communicate = partial(run_client, client_communicate)

        await client_communicate(key=1234)

        # Many clients at once
        N = 20
        futures = [client_communicate(key=i, delay=0.001) for i in range(N)]
        await asyncio.gather(*futures)
        assert set(l) == {1234} | set(range(N))

        assert len(client_addresses) == N + 1
        assert listener.contact_address not in client_addresses


async def run_coro(func, *args, **kwargs):
    return await func(*args, **kwargs)


async def run_coro_in_thread(func, *args, **kwargs):
    async def run_with_timeout():
        t = asyncio.create_task(func(*args, **kwargs))
        return await wait_for(t, timeout=10)

    return await asyncio.to_thread(
        asyncio_run, run_with_timeout(), loop_factory=get_loop_factory()
    )


@gen_test()
async def test_inproc_specific_same_thread():
    await check_inproc_specific(run_coro)


@gen_test()
async def test_inproc_specific_different_threads():
    await check_inproc_specific(run_coro_in_thread)


@gen_test()
async def test_inproc_continues_listening_after_handshake_error():
    async def handle_comm():
        pass

    async with listen("inproc://", handle_comm) as listener:
        addr = listener.listen_address
        scheme, loc = parse_address(addr)
        connector = get_backend(scheme).get_connector()

        comm = await connector.connect(loc)
        await comm.close()

        comm = await connector.connect(loc)
        await comm.close()


@gen_test()
async def test_inproc_handshakes_concurrently():
    async def handle_comm():
        pass

    async with listen("inproc://", handle_comm) as listener:
        addr = listener.listen_address
        scheme, loc = parse_address(addr)
        connector = get_backend(scheme).get_connector()

        comm1 = await connector.connect(loc)
        comm2 = await connector.connect(loc)
        await comm1.close()
        await comm2.close()


#
# Test communications through the abstract API
#


async def check_client_server(
    addr,
    check_listen_addr=None,
    check_contact_addr=None,
    listen_args=None,
    connect_args=None,
):
    """
    Abstract client / server test.
    """

    async def handle_comm(comm):
        try:
            scheme, loc = parse_address(comm.peer_address)
            assert scheme == bound_scheme

            msg = await comm.read()
            assert msg["op"] == "ping"
            msg["op"] = "pong"
            await comm.write(msg)

            msg = await comm.read()
            assert msg["op"] == "foobar"
        finally:
            await comm.close()

    # Arbitrary connection args should be ignored
    listen_args = listen_args or {"xxx": "bar"}
    connect_args = connect_args or {"xxx": "foo"}

    listener = await listen(addr, handle_comm, **listen_args)

    # Check listener properties
    bound_addr = listener.listen_address
    bound_scheme, bound_loc = parse_address(bound_addr)
    assert bound_scheme in backends
    assert bound_scheme == parse_address(addr)[0]

    if check_listen_addr is not None:
        check_listen_addr(bound_loc)

    contact_addr = listener.contact_address
    contact_scheme, contact_loc = parse_address(contact_addr)
    assert contact_scheme == bound_scheme

    if check_contact_addr is not None:
        check_contact_addr(contact_loc)
    else:
        assert contact_addr == bound_addr

    # Check client <-> server comms
    l = []

    async def client_communicate(key, delay=0):
        comm = await connect(listener.contact_address, **connect_args)
        try:
            assert comm.peer_address == listener.contact_address

            await comm.write({"op": "ping", "data": key})
            await comm.write({"op": "foobar"})
            if delay:
                await asyncio.sleep(delay)
            msg = await comm.read()
            assert msg == {"op": "pong", "data": key}
            l.append(key)
        finally:
            await comm.close()

    await client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    await asyncio.gather(*futures)
    assert set(l) == {1234} | set(range(20))

    listener.stop()


@pytest.mark.gpu
@gen_test()
async def test_ucx_client_server(ucx_loop):
    pytest.importorskip("distributed.comm.ucx")
    ucp = pytest.importorskip("ucp")

    addr = ucp.get_address()
    await check_client_server("ucx://" + addr)


def tcp_eq(expected_host, expected_port=None):
    def checker(loc):
        host, port = parse_host_port(loc)
        assert host == expected_host
        if expected_port is not None:
            assert port == expected_port
        else:
            assert 1023 < port < 65536

    return checker


tls_eq = tcp_eq


def inproc_check():
    expected_ip = get_ip()
    expected_pid = os.getpid()

    def checker(loc):
        ip, pid, suffix = loc.split("/")
        assert ip == expected_ip
        assert int(pid) == expected_pid

    return checker


@gen_test()
async def test_default_client_server_ipv4(tcp):
    # Default scheme is (currently) TCP
    await check_client_server("127.0.0.1", tcp_eq("127.0.0.1"))
    await check_client_server("127.0.0.1:3201", tcp_eq("127.0.0.1", 3201))
    await check_client_server("0.0.0.0", tcp_eq("0.0.0.0"), tcp_eq(EXTERNAL_IP4))
    await check_client_server(
        "0.0.0.0:3202", tcp_eq("0.0.0.0", 3202), tcp_eq(EXTERNAL_IP4, 3202)
    )
    # IPv4 is preferred for the bound address
    await check_client_server("", tcp_eq("0.0.0.0"), tcp_eq(EXTERNAL_IP4))
    await check_client_server(
        ":3203", tcp_eq("0.0.0.0", 3203), tcp_eq(EXTERNAL_IP4, 3203)
    )


@requires_ipv6
@gen_test()
async def test_default_client_server_ipv6(tcp):
    external_ip6 = get_ipv6()
    await check_client_server("[::1]", tcp_eq("::1"))
    await check_client_server("[::1]:3211", tcp_eq("::1", 3211))
    await check_client_server("[::]", tcp_eq("::"), tcp_eq(external_ip6))
    await check_client_server(
        "[::]:3212", tcp_eq("::", 3212), tcp_eq(external_ip6, 3212)
    )


@gen_test()
async def test_tcp_client_server_ipv4(tcp):
    await check_client_server("tcp://127.0.0.1", tcp_eq("127.0.0.1"))
    await check_client_server("tcp://127.0.0.1:3221", tcp_eq("127.0.0.1", 3221))
    await check_client_server("tcp://0.0.0.0", tcp_eq("0.0.0.0"), tcp_eq(EXTERNAL_IP4))
    await check_client_server(
        "tcp://0.0.0.0:3222", tcp_eq("0.0.0.0", 3222), tcp_eq(EXTERNAL_IP4, 3222)
    )
    await check_client_server("tcp://", tcp_eq("0.0.0.0"), tcp_eq(EXTERNAL_IP4))
    await check_client_server(
        "tcp://:3223", tcp_eq("0.0.0.0", 3223), tcp_eq(EXTERNAL_IP4, 3223)
    )


@requires_ipv6
@gen_test()
async def test_tcp_client_server_ipv6(tcp):
    external_ip6 = get_ipv6()
    await check_client_server("tcp://[::1]", tcp_eq("::1"))
    await check_client_server("tcp://[::1]:3231", tcp_eq("::1", 3231))
    await check_client_server("tcp://[::]", tcp_eq("::"), tcp_eq(external_ip6))
    await check_client_server(
        "tcp://[::]:3232", tcp_eq("::", 3232), tcp_eq(external_ip6, 3232)
    )


@gen_test()
async def test_tls_client_server_ipv4(tcp):
    await check_client_server("tls://127.0.0.1", tls_eq("127.0.0.1"), **tls_kwargs)
    await check_client_server(
        "tls://127.0.0.1:3221", tls_eq("127.0.0.1", 3221), **tls_kwargs
    )
    await check_client_server(
        "tls://", tls_eq("0.0.0.0"), tls_eq(EXTERNAL_IP4), **tls_kwargs
    )


@requires_ipv6
@gen_test()
async def test_tls_client_server_ipv6(tcp):
    await check_client_server("tls://[::1]", tls_eq("::1"), **tls_kwargs)


@gen_test()
async def test_inproc_client_server():
    await check_client_server("inproc://", inproc_check())
    await check_client_server(inproc.new_address(), inproc_check())


#
# TLS certificate handling
#


@gen_test()
async def test_tls_reject_certificate(tcp):
    cli_ctx = get_client_ssl_context()
    serv_ctx = get_server_ssl_context()

    # These certs are not signed by our test CA
    bad_cert_key = ("tls-self-signed-cert.pem", "tls-self-signed-key.pem")
    bad_cli_ctx = get_client_ssl_context(*bad_cert_key)
    bad_serv_ctx = get_server_ssl_context(*bad_cert_key)

    async def handle_comm(comm):
        try:
            scheme, loc = parse_address(comm.peer_address)
            assert scheme == "tls"
        finally:
            await comm.close()

    # Listener refuses a connector not signed by the CA
    listener = await listen("tls://", handle_comm, ssl_context=serv_ctx)

    with pytest.raises(EnvironmentError) as excinfo:
        comm = await connect(
            listener.contact_address, timeout=0.5, ssl_context=bad_cli_ctx
        )
        await comm.write({"x": "foo"})  # TODO: why is this necessary in Tornado 6 ?

    if os.name != "nt":
        try:
            # See https://serverfault.com/questions/793260/what-does-tlsv1-alert-unknown-ca-mean
            # assert "unknown ca" in str(excinfo.value)
            pass
        except AssertionError:
            if os.name == "nt":
                assert "An existing connection was forcibly closed" in str(
                    excinfo.value
                )
            else:
                raise

    # Sanity check
    comm = await connect(listener.contact_address, timeout=2, ssl_context=cli_ctx)
    await comm.close()

    # Connector refuses a listener not signed by the CA
    listener = await listen("tls://", handle_comm, ssl_context=bad_serv_ctx)

    with pytest.raises(EnvironmentError) as excinfo:
        await connect(listener.contact_address, timeout=2, ssl_context=cli_ctx)

    # XXX: For asyncio this is just a timeout error
    # assert "certificate verify failed" in str(excinfo.value.__cause__)


#
# Test communication closing
#


async def check_comm_closed_implicit(
    addr, delay=None, listen_args=None, connect_args=None
):
    listen_args = listen_args or {}
    connect_args = connect_args or {}

    async def handle_comm(comm):
        await comm.close()

    async with listen(addr, handle_comm, **listen_args) as listener:
        comm = await connect(listener.contact_address, **connect_args)
        with pytest.raises(CommClosedError):
            await comm.write({})
            await comm.read()

        comm = await connect(listener.contact_address, **connect_args)
        with pytest.raises(CommClosedError):
            await comm.read()


@gen_test()
async def test_tcp_comm_closed_implicit(tcp):
    await check_comm_closed_implicit("tcp://127.0.0.1")


@gen_test()
async def test_tls_comm_closed_implicit(tcp):
    await check_comm_closed_implicit("tls://127.0.0.1", **tls_kwargs)


@gen_test()
async def test_inproc_comm_closed_implicit():
    await check_comm_closed_implicit(inproc.new_address())


async def check_comm_closed_explicit(addr, listen_args=None, connect_args=None):
    listen_args = listen_args or {}
    connect_args = connect_args or {}
    a, b = await get_comm_pair(addr, listen_args=listen_args, connect_args=connect_args)
    a_read = a.read()
    b_read = b.read()
    await a.close()
    # In-flight reads should abort with CommClosedError
    with pytest.raises(CommClosedError):
        await a_read
    with pytest.raises(CommClosedError):
        await b_read
    # New reads as well
    with pytest.raises(CommClosedError):
        await a.read()
    with pytest.raises(CommClosedError):
        await b.read()
    # And writes
    with pytest.raises(CommClosedError):
        await a.write({})
    with pytest.raises(CommClosedError):
        await b.write({})
    await b.close()


@gen_test()
async def test_tcp_comm_closed_explicit(tcp):
    await check_comm_closed_explicit("tcp://127.0.0.1")


@gen_test()
async def test_tls_comm_closed_explicit(tcp):
    await check_comm_closed_explicit("tls://127.0.0.1", **tls_kwargs)


@gen_test()
async def test_inproc_comm_closed_explicit():
    await check_comm_closed_explicit(inproc.new_address())


@gen_test()
async def test_inproc_comm_closed_explicit_2():
    listener_errors = []

    async def handle_comm(comm):
        # Wait
        try:
            await comm.read()
        except CommClosedError:
            assert comm.closed()
            listener_errors.append(True)
        else:
            await comm.close()

    listener = await listen("inproc://", handle_comm)

    comm = await connect(listener.contact_address)
    await comm.close()
    assert comm.closed()
    start = time()
    while len(listener_errors) < 1:
        assert time() < start + 1
        await asyncio.sleep(0.01)
    assert len(listener_errors) == 1

    with pytest.raises(CommClosedError):
        await comm.read()
    with pytest.raises(CommClosedError):
        await comm.write("foo")

    comm = await connect(listener.contact_address)
    await comm.write("foo")
    with pytest.raises(CommClosedError):
        await comm.read()
    with pytest.raises(CommClosedError):
        await comm.write("foo")
    assert comm.closed()

    comm = await connect(listener.contact_address)
    await comm.write("foo")

    start = time()
    while not comm.closed():
        await asyncio.sleep(0.01)
        assert time() < start + 2

    await comm.close()
    await comm.close()


class CustomBase(BaseException):
    # We don't want to interfere with KeyboardInterrupts or CancelledErrors for
    # this test
    ...


@pytest.mark.parametrize("exc_type", [BufferError, CustomBase])
@gen_test()
async def test_comm_closed_on_write_error(tcp, exc_type):
    # Internal errors from comm.stream.write, such as
    # BufferError should lead to the stream being closed
    # and not re-used. See GitHub #4133

    reader, writer = await get_tcp_comm_pair()

    def _write(data):
        raise exc_type()

    writer.stream.write = _write
    with pytest.raises(exc_type):
        await writer.write("x")

    assert writer.closed()

    await reader.close()
    await writer.close()


@gen_test()
async def test_comm_closed_on_read_error(tcp):
    reader, writer = await get_tcp_comm_pair()

    with pytest.raises(asyncio.TimeoutError):
        await wait_for(reader.read(), 0.01)

    assert reader.closed()

    await reader.close()
    await writer.close()


#
# Various stress tests
#


async def echo(comm):
    message = await comm.read()
    await comm.write(message)


@gen_test()
async def test_retry_connect(tcp, monkeypatch):
    async def echo(comm):
        message = await comm.read()
        await comm.write(message)
        await comm.close()

    class UnreliableConnector(tcp.TCPConnector):
        def __init__(self):
            self.num_failures = 2
            self.failures = 0
            super().__init__()

        async def connect(self, address, deserialize=True, **connection_args):
            if self.failures > self.num_failures:
                return await super().connect(address, deserialize, **connection_args)
            else:
                self.failures += 1
                raise OSError()

    class UnreliableBackend(tcp.TCPBackend):
        _connector_class = UnreliableConnector

    monkeypatch.setitem(backends, "tcp", UnreliableBackend())

    listener = await listen("tcp://127.0.0.1:1234", echo)
    try:
        comm = await connect(listener.contact_address)
        await comm.write(b"test")
        msg = await comm.read()
        assert msg == b"test"
        await comm.close()
    finally:
        listener.stop()


@pytest.mark.slow
@gen_test()
async def test_handshake_slow_comm(tcp, monkeypatch):
    class SlowComm(tcp.TCP):
        def __init__(self, *args, delay_in_comm=0.5, **kwargs):
            super().__init__(*args, **kwargs)
            self.delay_in_comm = delay_in_comm

        async def read(self, *args, **kwargs):
            await asyncio.sleep(self.delay_in_comm)
            return await super().read(*args, **kwargs)

        async def write(self, *args, **kwargs):
            await asyncio.sleep(self.delay_in_comm)
            res = await super(type(self), self).write(*args, **kwargs)
            return res

    class SlowConnector(tcp.TCPConnector):
        comm_class = SlowComm

    class SlowBackend(tcp.TCPBackend):
        def get_connector(self):
            return SlowConnector()

    monkeypatch.setitem(backends, "tcp", SlowBackend())

    listener = await listen("tcp://127.0.0.1:1234", echo)
    try:
        comm = await connect(listener.contact_address)
        await comm.write(b"test")
        msg = await comm.read()
        assert msg == b"test"

        import dask

        # The connect itself is fast. Only the handshake is slow
        with dask.config.set({"distributed.comm.timeouts.connect": "500ms"}):
            await connect(listener.contact_address)
    finally:
        listener.stop()


async def check_connect_timeout(addr):
    t1 = time()
    with pytest.raises(IOError):
        await connect(addr, timeout=0.15)
    dt = time() - t1
    assert 1 >= dt >= 0.1


@gen_test()
async def test_tcp_connect_timeout(tcp):
    await check_connect_timeout("tcp://127.0.0.1:44444")


@gen_test()
async def test_inproc_connect_timeout():
    await check_connect_timeout(inproc.new_address())


async def check_many_listeners(addr):
    async def handle_comm(comm):
        pass

    listeners = []
    N = 100

    for _ in range(N):
        listener = await listen(addr, handle_comm)
        listeners.append(listener)

    assert len({l.listen_address for l in listeners}) == N
    assert len({l.contact_address for l in listeners}) == N

    for listener in listeners:
        listener.stop()


@gen_test()
async def test_tcp_many_listeners(tcp):
    await check_many_listeners("tcp://127.0.0.1")
    await check_many_listeners("tcp://0.0.0.0")
    await check_many_listeners("tcp://")


@gen_test()
async def test_inproc_many_listeners():
    await check_many_listeners("inproc://")


#
# Test deserialization
#


async def check_listener_deserialize(addr, deserialize, in_value, check_out):
    q = asyncio.Queue()

    async def handle_comm(comm):
        try:
            msg = await comm.read()
        except Exception as exc:
            q.put_nowait(exc)
        else:
            q.put_nowait(msg)
        finally:
            await comm.close()

    async with listen(addr, handle_comm, deserialize=deserialize) as listener:
        comm = await connect(listener.contact_address)

    await comm.write(in_value)

    out_value = await q.get()
    if isinstance(out_value, Exception):
        raise out_value  # Prevents deadlocks, get actual deserialization exception
    check_out(out_value)
    await comm.close()


async def check_connector_deserialize(addr, deserialize, in_value, check_out):
    done = asyncio.Event()

    async def handle_comm(comm):
        try:
            await comm.write(in_value)
            await done.wait()
        finally:
            await comm.close()

    async with listen(addr, handle_comm) as listener:
        comm = await connect(listener.contact_address, deserialize=deserialize)

    try:
        out_value = await comm.read()
        done.set()
    finally:
        await comm.close()
    check_out(out_value)


async def check_deserialize(addr):
    """
    Check the "deserialize" flag on connect() and listen().
    """
    # Test with Serialize and Serialized objects

    msg = {
        "op": "update",
        "x": b"abc",
        "to_ser": [to_serialize(123)],
        "ser": Serialized(*serialize(456)),
    }
    msg_orig = msg.copy()

    def check_out_false(out_value):
        # Check output with deserialize=False
        out_value = out_value.copy()  # in case transport passed the object as-is
        to_ser = out_value.pop("to_ser")
        ser = out_value.pop("ser")
        expected_msg = msg_orig.copy()
        del expected_msg["ser"]
        del expected_msg["to_ser"]
        assert out_value == expected_msg

        assert isinstance(ser, Serialized)
        assert deserialize(ser.header, ser.frames) == 456

        assert isinstance(to_ser, (tuple, list)) and len(to_ser) == 1
        (to_ser,) = to_ser
        # The to_serialize() value could have been actually serialized
        # or not (it's a transport-specific optimization)
        if isinstance(to_ser, Serialized):
            assert deserialize(to_ser.header, to_ser.frames) == 123
        else:
            assert to_ser == to_serialize(123)

    def check_out_true(out_value):
        # Check output with deserialize=True
        expected_msg = msg.copy()
        expected_msg["ser"] = 456
        expected_msg["to_ser"] = [123]
        # Notice, we allow "to_ser" to be a tuple or a list
        assert list(out_value.pop("to_ser")) == expected_msg.pop("to_ser")
        assert out_value == expected_msg

    await check_listener_deserialize(addr, False, msg, check_out_false)
    await check_connector_deserialize(addr, False, msg, check_out_false)

    await check_listener_deserialize(addr, True, msg, check_out_true)
    await check_connector_deserialize(addr, True, msg, check_out_true)

    # Test with long bytestrings, large enough to be transferred
    # as a separate payload
    # TODO: currently bytestrings are not transferred as a separate payload

    _uncompressible = os.urandom(1024**2) * 4  # end size: 8 MB

    msg = {
        "op": "update",
        "x": _uncompressible,
        "to_ser": (to_serialize(_uncompressible),),
        "ser": Serialized(*serialize(_uncompressible)),
    }
    msg_orig = msg.copy()

    def check_out(deserialize_flag, out_value):
        # Check output with deserialize=False
        assert sorted(out_value) == sorted(msg_orig)
        out_value = out_value.copy()  # in case transport passed the object as-is
        to_ser = out_value.pop("to_ser")
        ser = out_value.pop("ser")
        expected_msg = msg_orig.copy()
        del expected_msg["ser"]
        del expected_msg["to_ser"]
        assert out_value == expected_msg

        if deserialize_flag:
            assert isinstance(ser, (bytes, bytearray))
            assert bytes(ser) == _uncompressible
        else:
            assert isinstance(ser, Serialized)
            assert deserialize(ser.header, ser.frames) == _uncompressible
            assert isinstance(to_ser, tuple) and len(to_ser) == 1
            (to_ser,) = to_ser
            # The to_serialize() value could have been actually serialized
            # or not (it's a transport-specific optimization)
            if isinstance(to_ser, Serialized):
                assert deserialize(to_ser.header, to_ser.frames) == _uncompressible
            else:
                assert to_ser == to_serialize(_uncompressible)

    await check_listener_deserialize(addr, False, msg, partial(check_out, False))
    await check_connector_deserialize(addr, False, msg, partial(check_out, False))

    await check_listener_deserialize(addr, True, msg, partial(check_out, True))
    await check_connector_deserialize(addr, True, msg, partial(check_out, True))


@gen_test()
async def test_tcp_deserialize(tcp):
    await check_deserialize("tcp://")


@gen_test()
async def test_inproc_deserialize():
    await check_deserialize("inproc://")


async def check_deserialize_roundtrip(addr):
    """
    Sanity check round-tripping with "deserialize" on and off.
    """
    # Test with long bytestrings, large enough to be transferred
    # as a separate payload
    _uncompressible = os.urandom(1024**2) * 4  # end size: 4 MB

    msg = {
        "op": "update",
        "x": _uncompressible,
        "to_ser": [to_serialize(_uncompressible)],
        "ser": Serialized(*serialize(_uncompressible)),
    }

    for should_deserialize in (True, False):
        a, b = await get_comm_pair(addr, deserialize=should_deserialize)
        await a.write(msg)
        got = await b.read()
        await b.write(got)
        got = await a.read()

        assert sorted(got) == sorted(msg)
        for k in ("op", "x"):
            assert got[k] == msg[k]
        if should_deserialize:
            assert isinstance(got["to_ser"][0], (bytes, bytearray))
            assert isinstance(got["ser"], (bytes, bytearray))
        else:
            assert isinstance(got["to_ser"][0], (to_serialize, Serialized))
            assert isinstance(got["ser"], Serialized)


@gen_test()
async def test_inproc_deserialize_roundtrip():
    await check_deserialize_roundtrip("inproc://")


@gen_test()
async def test_tcp_deserialize_roundtrip(tcp):
    await check_deserialize_roundtrip("tcp://")


def _raise_eoferror():
    raise EOFError


class _EOFRaising:
    def __reduce__(self):
        return _raise_eoferror, ()


async def check_deserialize_eoferror(addr):
    """
    EOFError when deserializing should close the comm.
    """

    async def handle_comm(comm):
        await comm.write({"data": to_serialize(_EOFRaising())})
        with pytest.raises(CommClosedError):
            await comm.read()

    async with listen(addr, handle_comm) as listener:
        comm = await connect(listener.contact_address, deserialize=deserialize)
        with pytest.raises(CommClosedError):
            await comm.read()


@gen_test()
async def test_tcp_deserialize_eoferror(tcp):
    await check_deserialize_eoferror("tcp://")


#
# Test various properties
#


async def check_repr(a, b):
    assert "closed" not in repr(a)
    assert "closed" not in repr(b)
    await a.close()
    assert "closed" in repr(a)
    assert a.local_address in repr(a)
    assert b.peer_address in repr(a)
    await b.close()
    assert "closed" in repr(b)
    assert a.local_address in repr(b)
    assert b.peer_address in repr(b)


@gen_test()
async def test_tcp_repr(tcp):
    a, b = await get_tcp_comm_pair()
    assert a.local_address in repr(b)
    assert b.local_address in repr(a)
    await check_repr(a, b)


@gen_test()
async def test_tls_repr(tcp):
    a, b = await get_tls_comm_pair()
    assert a.local_address in repr(b)
    assert b.local_address in repr(a)
    await check_repr(a, b)


@gen_test()
async def test_inproc_repr():
    a, b = await get_inproc_comm_pair()
    assert a.local_address in repr(b)
    assert b.local_address in repr(a)
    await check_repr(a, b)


async def check_addresses(a, b):
    assert a.peer_address == b.local_address
    assert a.local_address == b.peer_address
    a.abort()
    b.abort()


@gen_test()
async def test_tcp_adresses(tcp):
    a, b = await get_tcp_comm_pair()
    await check_addresses(a, b)


@gen_test()
async def test_get_stream_address_raises_if_closed():
    a, b = await get_tcp_comm_pair()
    a.abort()
    with pytest.raises(OSError):
        get_stream_address(a)
    b.abort()


@gen_test()
async def test_tls_adresses(tcp):
    a, b = await get_tls_comm_pair()
    await check_addresses(a, b)


@gen_test()
async def test_inproc_adresses():
    a, b = await get_inproc_comm_pair()
    await check_addresses(a, b)


def _get_backend_on_path(path):
    sys.path.append(os.fsdecode(path))
    return get_backend("udp")


def test_register_backend_entrypoint(tmp_path):
    (tmp_path / "dask_udp.py").write_bytes(b"def udp_backend():\n    return 1\n")
    dist_info = tmp_path / "dask_udp-0.0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "entry_points.txt").write_bytes(
        b"[distributed.comm.backends]\nudp = dask_udp:udp_backend\n"
    )
    with get_mp_context().Pool(1) as pool:
        assert pool.apply(_get_backend_on_path, args=(tmp_path,)) == 1
    pool.join()


class OpaqueList(list):
    """Don't let the serialization layer travese this object"""

    pass


@pytest.mark.parametrize(
    "list_cls",
    [
        list,  # Use protocol.numpy.serialize_numpy_array / deserialize_numpy_array
        OpaqueList,  # Use generic pickle.dumps / pickle.loads
    ],
)
@gen_test()
async def test_do_not_share_buffers(tcp, list_cls):
    """Test that two objects with buffer interface in the same message do not share
    their buffer upon deserialization

    See Also
    --------
    test_share_buffer_with_header
    test_ucx.py::test_do_not_share_buffers
    """
    np = pytest.importorskip("numpy")

    async def handle_comm(comm):
        msg = await comm.read()
        msg["data"] = to_serialize(list_cls([np.array([1, 2]), np.array([3, 4])]))
        await comm.write(msg)
        await comm.close()

    listener = await tcp.TCPListener("127.0.0.1", handle_comm)
    comm = await connect(listener.contact_address)

    await comm.write({"op": "ping"})
    msg = await comm.read()
    await comm.close()

    a, b = msg["data"]
    assert get_host_array(a) is not get_host_array(b)


@pytest.mark.parametrize(
    "nbytes_np,nbytes_other,expect_separate_buffer",
    [
        (1, 0, False),  # <2 kiB (including prologue and msgpack header)
        (1, 1800, False),  # <2 kiB
        (1, 2100, True),  # >2 kiB
        (200_000, 9500, False),  # <5% of numpy array
        (200_000, 10500, True),  # >5% of numpy array
        (350_000, 0, False),  # sharded buffer
    ],
)
@gen_test()
async def test_share_buffer_with_header(
    tcp, nbytes_np, nbytes_other, expect_separate_buffer
):
    """Test that a numpy or parquet object shares the buffer with its serialized header
    to improve performance, but only as long as the header is trivial in size.

    See Also
    --------
    test_do_not_share_buffers
    """
    np = pytest.importorskip("numpy")

    async def handle_comm(comm):
        comm.max_shard_size = 250_000
        msg = await comm.read()
        msg["np"] = to_serialize(np.random.randint(0, 256, nbytes_np, dtype="u1"))
        msg["other"] = np.random.bytes(nbytes_other)
        await comm.write(msg)
        await comm.close()

    listener = await tcp.TCPListener("127.0.0.1", handle_comm)
    comm = await connect(listener.contact_address)

    await comm.write({"op": "ping"})
    msg = await comm.read()
    await comm.close()

    a = msg["np"]
    ha = get_host_array(a)
    assert (ha.nbytes == a.nbytes) == expect_separate_buffer
