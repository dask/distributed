from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from functools import partial
import socket

from tornado import gen, ioloop
import pytest

from distributed.core import (
    pingpong, Server, rpc, connect, send_recv,
    coerce_to_address, ConnectionPool, CommClosedError)
from distributed.metrics import time
from distributed.utils import get_ip, get_ipv6
from distributed.utils_test import (
    slow, loop, gen_test, gen_cluster, has_ipv6,
    assert_can_connect, assert_can_connect_from_everywhere_4,
    assert_can_connect_from_everywhere_4_6, assert_can_connect_from_everywhere_6,
    assert_can_connect_locally_4, assert_can_connect_locally_6)


EXTERNAL_IP4 = get_ip()
if has_ipv6():
    EXTERNAL_IP6 = get_ipv6()


def test_server(loop):
    """
    Simple Server test.
    """
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        with pytest.raises(ValueError):
            server.port
        server.listen(8881)
        assert server.port == 8881
        assert server.address == ('tcp://%s:8881' % get_ip())

        for addr in ('127.0.0.1:8881', 'tcp://127.0.0.1:8881', server.address):
            comm = yield connect(addr)

            n = yield comm.write({'op': 'ping'})
            assert isinstance(n, int)
            assert 4 <= n <= 1000

            response = yield comm.read()
            assert response == b'pong'

            yield comm.write({'op': 'ping', 'close': True})
            response = yield comm.read()
            assert response == b'pong'

            yield comm.close()

        server.stop()

    loop.run_sync(f)


class MyServer(Server):
    default_port = 8756


@gen_test()
def test_server_listen():
    """
    Test various Server.listen() arguments and their effect.
    """

    @contextmanager
    def listen_on(cls, *args):
        server = cls({})
        server.listen(*args)
        try:
            yield server
        finally:
            server.stop()

    # Note server.address is the concrete, contactable address

    with listen_on(Server, 8882) as server:
        assert server.port == 8882
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server) as server:
        assert server.port > 0
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(MyServer) as server:
        assert server.port == MyServer.default_port
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server, ('', 2468)) as server:
        assert server.port == 2468
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    with listen_on(Server, 'tcp://:2468') as server:
        assert server.port == 2468
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4_6(server.port)

    # Only IPv4

    with listen_on(Server, ('0.0.0.0', 2468)) as server:
        assert server.port == 2468
        assert server.address == 'tcp://%s:%d' % (EXTERNAL_IP4, server.port)
        yield assert_can_connect(server.address)
        yield assert_can_connect_from_everywhere_4(server.port)

    with listen_on(Server, ('127.0.0.1', 2468)) as server:
        assert server.port == 2468
        assert server.address == 'tcp://127.0.0.1:%d' % server.port
        yield assert_can_connect(server.address)
        yield assert_can_connect_locally_4(server.port)

    with listen_on(Server, 'tcp://127.0.0.1:2468') as server:
        assert server.port == 2468
        assert server.address == 'tcp://127.0.0.1:%d' % server.port
        yield assert_can_connect(server.address)
        yield assert_can_connect_locally_4(server.port)

    # Only IPv6

    if has_ipv6():
        with listen_on(Server, ('::', 2468)) as server:
            assert server.port == 2468
            assert server.address == 'tcp://[%s]:%d' % (EXTERNAL_IP6, server.port)
            yield assert_can_connect(server.address)
            yield assert_can_connect_from_everywhere_6(server.port)

        with listen_on(Server, ('::1', 2468)) as server:
            assert server.port == 2468
            assert server.address == 'tcp://[::1]:%d' % server.port
            yield assert_can_connect(server.address)
            yield assert_can_connect_locally_6(server.port)

        with listen_on(Server, 'tcp://[::1]:2468') as server:
            assert server.port == 2468
            assert server.address == 'tcp://[::1]:%d' % server.port
            yield assert_can_connect(server.address)
            yield assert_can_connect_locally_6(server.port)


def test_rpc(loop):
    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        server.listen(8883)

        with rpc('127.0.0.1:8883') as remote:
            response = yield remote.ping()
            assert response == b'pong'

            assert remote.comms
            assert remote.address == 'tcp://127.0.0.1:8883'

            response = yield remote.ping(close=True)
            assert response == b'pong'

        assert not remote.comms
        assert remote.status == 'closed'

        server.stop()

    loop.run_sync(f)


def test_rpc_inputs():
    L = [rpc('127.0.0.1:8884'),
         rpc(('127.0.0.1', 8884)),
         rpc('tcp://127.0.0.1:8884'),
         ]

    assert all(r.address == 'tcp://127.0.0.1:8884' for r in L), L

    for r in L:
        r.close_rpc()


def test_rpc_with_many_connections(loop):
    remote = rpc(('127.0.0.1', 8885))

    @gen.coroutine
    def g():
        for i in range(10):
            yield remote.ping()

    @gen.coroutine
    def f():
        server = Server({'ping': pingpong})
        server.listen(8885)

        yield [g() for i in range(10)]

        server.stop()

        remote.close_comms()
        assert all(comm.closed() for comm in remote.comms)

    loop.run_sync(f)


def echo(stream, x):
    return x

@slow
def test_large_packets(loop):
    """ tornado has a 100MB cap by default """
    @gen.coroutine
    def f():
        server = Server({'echo': echo})
        server.listen(8886)

        data = b'0' * int(200e6)  # slightly more than 100MB
        conn = rpc('127.0.0.1:8886')
        result = yield conn.echo(x=data)
        assert result == data

        d = {'x': data}
        result = yield conn.echo(x=d)
        assert result == d

        conn.close_comms()
        server.stop()

    loop.run_sync(f)


def test_identity(loop):
    @gen.coroutine
    def f():
        server = Server({})
        server.listen(8887)

        with rpc(('127.0.0.1', 8887)) as remote:
            a = yield remote.identity()
            b = yield remote.identity()
            assert a['type'] == 'Server'
            assert a['id'] == b['id']

        server.stop()

    loop.run_sync(f)


def test_ports(loop):
    port = 9876
    server = Server({}, io_loop=loop)
    server.listen(port)
    try:
        assert server.port == port

        with pytest.raises((OSError, socket.error)):
            server2 = Server({}, io_loop=loop)
            server2.listen(port)
    finally:
        server.stop()

    try:
        server3 = Server({}, io_loop=loop)
        server3.listen(0)
        assert isinstance(server3.port, int)
        assert server3.port > 1024
    finally:
        server3.stop()


def stream_div(stream=None, x=None, y=None):
    return x / y

@gen_test()
def test_errors():
    server = Server({'div': stream_div})
    server.listen(0)

    with rpc(('127.0.0.1', server.port)) as r:
        with pytest.raises(ZeroDivisionError):
            yield r.div(x=1, y=0)


@gen_test()
def test_connect_raises():
    with pytest.raises((gen.TimeoutError, IOError)):
        yield connect('127.0.0.1:58259', timeout=0.01)


@gen_test()
def test_send_recv_args():
    server = Server({'echo': echo})
    server.listen(0)

    addr = '127.0.0.1:%d' % server.port
    addr2 = server.address

    result = yield send_recv(addr=addr, op='echo', x=b'1')
    assert result == b'1'
    result = yield send_recv(addr=addr, op='echo', x=b'2', reply=False)
    assert result == None
    result = yield send_recv(addr=addr2, op='echo', x=b'2')
    assert result == b'2'

    comm = yield connect(addr)
    result = yield send_recv(comm, op='echo', x=b'3')
    assert result == b'3'
    assert not comm.closed()
    result = yield send_recv(comm, op='echo', x=b'4', close=True)
    assert result == b'4'
    assert comm.closed()

    server.stop()


def test_coerce_to_address():
    for arg in ['127.0.0.1:8786',
                ('127.0.0.1', 8786),
                ('127.0.0.1', '8786')]:
        assert coerce_to_address(arg) == 'tcp://127.0.0.1:8786'


@gen_test()
def test_connection_pool():

    @gen.coroutine
    def ping(comm, delay=0.1):
        yield gen.sleep(delay)
        raise gen.Return('pong')

    servers = [Server({'ping': ping}) for i in range(10)]
    for server in servers:
        server.listen(0)

    rpc = ConnectionPool(limit=5)

    # Reuse connections
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    yield [rpc(s.address).ping() for s in servers[:5]]
    yield [rpc('127.0.0.1:%d' % s.port).ping() for s in servers[:5]]
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[:5]]
    assert sum(map(len, rpc.available.values())) == 5
    assert sum(map(len, rpc.occupied.values())) == 0
    assert rpc.active == 0
    assert rpc.open == 5

    # Clear out connections to make room for more
    yield [rpc(ip='127.0.0.1', port=s.port).ping() for s in servers[5:]]
    assert rpc.active == 0
    assert rpc.open == 5

    s = servers[0]
    yield [rpc(ip='127.0.0.1', port=s.port).ping(delay=0.1) for i in range(3)]
    assert len(rpc.available['tcp://127.0.0.1:%d' % s.port]) == 3

    # Explicitly clear out connections
    rpc.collect()
    start = time()
    while any(rpc.available.values()):
        yield gen.sleep(0.01)
        assert time() < start + 2

    rpc.close()


@gen_cluster()
def test_ticks(s, a, b):
    pytest.importorskip('crick')
    yield gen.sleep(0.1)
    c = s.digests['tick-duration']
    assert c.size()
    assert 0.01 < c.components[0].quantile(0.5) < 0.5
