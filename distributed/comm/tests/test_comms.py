from __future__ import print_function, division, absolute_import

from functools import partial

import pytest

from tornado import gen, ioloop, queues

from distributed.core import pingpong
from distributed.metrics import time
from distributed.utils_test import slow, loop, gen_test, gen_cluster, requires_ipv6

from distributed.comm import tcp, zmq, connect, listen, CommClosedError
from distributed.comm.core import parse_address
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
    @gen.coroutine
    def handle_comm(comm):
        assert comm.peer_address.startswith('tcp://' + host)
        msg = yield comm.read()
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = tcp.TCPListener('localhost', handle_comm)
    listener.start()
    host, port = listener.get_host_port()
    assert host in ('localhost', '127.0.0.1', '::1')
    assert port > 0

    connector = tcp.TCPConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        addr = '%s:%d' % (host, port)
        comm = yield connector.connect(addr)
        assert comm.peer_address == 'tcp://' + addr
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

    @gen.coroutine
    def handle_comm(comm):
        msg = yield comm.read()
        msg['op'] = 'pong'
        yield comm.write(msg)
        yield comm.close()

    listener = zmq.ZMQListener('127.0.0.1', handle_comm)
    listener.start()
    host, port = listener.get_host_port()
    assert host == '127.0.0.1'
    assert port > 0

    connector = zmq.ZMQConnector()
    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        addr = '%s:%d' % (host, port)
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
    """
    Abstract client / server test.
    """
    @gen.coroutine
    def handle_comm(comm):
        scheme, loc = parse_address(comm.peer_address)
        assert scheme == bound_scheme

        msg = yield comm.read()
        assert msg['op'] == 'ping'
        msg['op'] = 'pong'
        yield comm.write(msg)

        msg = yield comm.read()
        assert msg['op'] == 'foobar'

        yield comm.close()

    listener = listen(addr, handle_comm)
    listener.start()
    bound_addr = listener.address

    bound_scheme, bound_loc = parse_address(bound_addr)
    assert bound_scheme in ('tcp', 'zmq')
    assert bound_scheme == parse_address(addr)[0]

    l = []

    @gen.coroutine
    def client_communicate(key, delay=0):
        comm = yield connect(bound_addr)
        assert comm.peer_address == bound_addr

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
    yield check_client_server('127.0.0.1:3201')
    yield check_client_server('0.0.0.0')
    yield check_client_server('0.0.0.0:3202')
    yield check_client_server('')
    yield check_client_server(':3203')

@requires_ipv6
@gen_test()
def test_default_client_server_ipv6():
    yield check_client_server('[::1]')
    yield check_client_server('[::1]:3211')

@gen_test()
def test_tcp_client_server_ipv4():
    yield check_client_server('tcp://127.0.0.1')
    yield check_client_server('tcp://127.0.0.1:3221')
    yield check_client_server('tcp://0.0.0.0')
    yield check_client_server('tcp://0.0.0.0:3222')
    yield check_client_server('tcp://')
    yield check_client_server('tcp://:3223')

@requires_ipv6
@gen_test()
def test_tcp_client_server_ipv6():
    yield check_client_server('tcp://[::1]')
    yield check_client_server('tcp://[::1]:3231')
    yield check_client_server('tcp://[::]')
    yield check_client_server('tcp://[::]:3231')

@gen_test()
def test_zmq_client_server_ipv4():
    yield check_client_server('zmq://127.0.0.1')
    yield check_client_server('zmq://127.0.0.1:3241')

@requires_ipv6
@gen_test()
def test_zmq_client_server_ipv6():
    yield check_client_server('zmq://[::1]')
    yield check_client_server('zmq://[::1]:3251')


@gen.coroutine
def check_comm_closed_implicit(addr):
    @gen.coroutine
    def handle_comm(comm):
        yield comm.close()

    listener = listen(addr, handle_comm)
    listener.start()
    bound_addr = listener.address

    comm = yield connect(bound_addr)
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(bound_addr)
    with pytest.raises(CommClosedError):
        yield comm.read()


@gen_test()
def test_tcp_comm_closed_implicit():
    yield check_comm_closed_implicit('tcp://127.0.0.1')

# XXX zmq transport does not detect a connection is closed by peer
#@gen_test()
#def test_zmq_comm_closed():
    #yield check_comm_closed('zmq://127.0.0.1')


@gen.coroutine
def check_comm_closed_explicit(addr):
    @gen.coroutine
    def handle_comm(comm):
        # Wait
        try:
            yield comm.read()
        except CommClosedError:
            pass

    listener = listen(addr, handle_comm)
    listener.start()
    bound_addr = listener.address

    comm = yield connect(bound_addr)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.write({})

    comm = yield connect(bound_addr)
    comm.close()
    with pytest.raises(CommClosedError):
        yield comm.read()

@gen_test()
def test_tcp_comm_closed_explicit():
    yield check_comm_closed_explicit('tcp://127.0.0.1')

@gen_test()
def test_zmq_comm_closed_explicit():
    yield check_comm_closed_explicit('zmq://127.0.0.1')


@gen.coroutine
def check_connect_timeout(addr):
    t1 = time()
    with pytest.raises(IOError):
        yield connect(addr, timeout=0.15)
    dt = time() - t1
    assert 0.3 >= dt >= 0.1


@gen_test()
def test_tcp_connect_timeout():
    yield check_connect_timeout('tcp://127.0.0.1:44444')


def check_many_listeners(addr):
    @gen.coroutine
    def handle_comm(comm):
        pass

    listeners = []
    for i in range(100):
        listener = listen(addr, handle_comm)
        listener.start()
        listeners.append(listener)

    for listener in listeners:
        listener.stop()


def test_tcp_many_listeners():
    check_many_listeners('tcp://127.0.0.1')
    check_many_listeners('tcp://0.0.0.0')
    check_many_listeners('tcp://')
