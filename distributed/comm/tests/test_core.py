from __future__ import print_function, division, absolute_import

from functools import partial

import pytest

from tornado import gen, ioloop, queues
from tornado.iostream import StreamClosedError

from distributed.core import pingpong
from distributed.metrics import time
from distributed.utils_test import slow, loop, gen_test, gen_cluster

from distributed.comm import tcp, zmq, connect, listen
from distributed.comm.utils import parse_host_port, unparse_host_port


@gen.coroutine
def debug_loop():
    """
    Debug helper
    """
    while True:
        loop = ioloop.IOLoop.current()
        print('.', loop, loop._handlers)
        yield gen.sleep(0.50)


def test_parse_host_port():
    f = parse_host_port

    assert f('localhost:123') == ('localhost', 123)
    assert f('127.0.0.1:456') == ('127.0.0.1', 456)
    assert f('localhost:123', 80) == ('localhost', 123)
    assert f('localhost', 80) == ('localhost', 80)

    with pytest.raises(ValueError):
        f('localhost')

    assert f('[::1]:123') == ('::1', 123)
    assert f('[fe80::1]:123', 80) == ('fe80::1', 123)
    assert f('[::1]', 80) == ('::1', 80)

    with pytest.raises(ValueError):
        f('[::1]')
    with pytest.raises(ValueError):
        f('::1:123')
    with pytest.raises(ValueError):
        f('::1')


def test_unparse_host_port():
    f = unparse_host_port

    assert f('localhost', 123) == 'localhost:123'
    assert f('127.0.0.1', 123) == '127.0.0.1:123'
    assert f('::1', 123) == '[::1]:123'
    assert f('[::1]', 123) == '[::1]:123'

    assert f('127.0.0.1') == '127.0.0.1'
    assert f('127.0.0.1', 0) == '127.0.0.1'
    assert f('127.0.0.1', None) == '127.0.0.1'
    assert f('127.0.0.1', '*') == '127.0.0.1:*'

    assert f('::1') == '[::1]'
    assert f('[::1]') == '[::1]'
    assert f('::1', '*') == '[::1]:*'


@gen_test()
def test_tcp_specific():
    """
    Test concrete TCP API.
    """
    q = queues.Queue()

    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        q.put(msg)
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = tcp.TCPListener('localhost', handle_comm)
    listener.start()
    host, port = addr = listener.get_host_port()
    assert host in ('localhost', '127.0.0.1', '::1')
    assert port > 0

    connector = tcp.TCPConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connector.connect(addr)
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


@gen_test()
def test_zmq_specific():
    """
    Test concrete ZMQ API.
    """
    debug_loop()
    q = queues.Queue()

    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        q.put(msg)
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = zmq.ZMQListener('127.0.0.1', handle_comm)
    listener.start()
    host, port = addr = listener.get_host_port()
    assert host == '127.0.0.1'
    assert port > 0

    connector = zmq.ZMQConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connector.connect(addr)
        yield comm.write({'op': 'ping', 'data': key})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


@gen.coroutine
def check_client_server(addr):
    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        assert msg['op'] == 'ping'
        msg['op'] = 'pong'
        yield comm.write(msg)

        msg = yield comm.read()
        assert msg['op'] == 'foobar'

        yield comm.close()

    listener = listen(addr, handle_comm)
    listener.start()
    bound_addr = listener.get_address()

    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connect(bound_addr)
        yield comm.write({'op': 'ping', 'data': key})
        yield comm.write({'op': 'foobar'})
        if delay:
            yield gen.sleep(delay)
        msg = yield comm.read()
        assert msg == {'op': 'pong', 'data': key}
        l.append(key)
        yield comm.close()

    yield client_communicate(key=1234)

    # Many clients at once
    futures = [client_communicate(key=i, delay=0.05) for i in range(20)]
    yield futures
    assert set(l) == {1234} | set(range(20))


@gen_test()
def test_default_client_server_ipv4():
    # Default scheme is (currently) TCP
    yield check_client_server('127.0.0.1')

@gen_test()
def test_default_client_server_ipv6():
    yield check_client_server('[::1]')

@gen_test()
def test_tcp_client_server_ipv4():
    yield check_client_server('tcp://127.0.0.1')

@gen_test()
def test_tcp_client_server_ipv6():
    yield check_client_server('tcp://[::1]')

@gen_test()
def test_zmq_client_server_ipv4():
    yield check_client_server('zmq://127.0.0.1')

@gen_test()
def test_zmq_client_server_ipv6():
    yield check_client_server('zmq://[::1]')
