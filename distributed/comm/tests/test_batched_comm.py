from __future__ import print_function, division, absolute_import

from functools import partial
import os
import ssl
import sys
import threading

import pytest

from tornado import gen, queues
from tornado.ioloop import IOLoop

from distributed.utils_test import (gen_test, get_server_ssl_context,
                                    get_client_ssl_context)
from distributed.comm import connect, listen, CommClosedError
from distributed.comm.batched import BatchedComm


tls_kwargs = dict(listen_args={'ssl_context': get_server_ssl_context()},
                  connect_args={'ssl_context': get_client_ssl_context()})


@gen.coroutine
def get_comm_pair(listen_addr, listen_args=None, connect_args=None):
    q = queues.Queue()

    def handle_comm(comm):
        q.put(comm)

    listener = listen(listen_addr, handle_comm,
                      connection_args=listen_args)
    listener.start()

    comm = yield connect(listener.contact_address,
                         connection_args=connect_args)
    serv_comm = yield q.get()
    raise gen.Return((comm, serv_comm))


@gen.coroutine
def check_write(addr, **kwargs):
    loop = IOLoop.current(instance=False)

    a, b = yield get_comm_pair(addr, **kwargs)
    b = BatchedComm(0.2, b)

    t1 = loop.time()
    for i in range(3):
        b.write(i)

    msg = yield a.read()
    t2 = loop.time()
    assert t2 - t1 >= 0.2
    # Hopefully CI machines are fast enough for the 0.2s interval?
    assert msg == list(range(3))

    N = 100
    for i in range(N):
        yield b.write(i)
        yield gen.sleep(5e-3)  # wait for more than batched interval / N

    got = []
    while len(got) < N:
        got += (yield a.read())

    assert got == list(range(N))

    a.abort()
    b.abort()


@gen_test()
def test_write_tcp():
    yield check_write('tcp://')

@gen_test()
def test_write_inproc():
    yield check_write('inproc://')

@gen_test()
def test_write_tls():
    yield check_write('inproc://', **tls_kwargs)


@gen_test()
def test_late_start():
    a, b = yield get_comm_pair('tcp://')
    batched = BatchedComm(1e-6)
    for i in range(3):
        batched.write(i)

    batched.start(b)
    msg = yield a.read()
    assert msg == list(range(3))

    a.abort()
    batched.abort()


@gen_test()
def test_read():
    a, b = yield get_comm_pair('tcp://')
    a = BatchedComm(0.2, a)
    b = BatchedComm(0.2, b)

    for i in range(3):
        b.write(i)
    msg = yield a.read()
    assert msg == list(range(3))

    a.abort()
    b.abort()


@gen_test()
def test_close():
    a, b = yield get_comm_pair('tcp://')
    b = BatchedComm(0.2, b)

    assert not b.closed()
    assert not b.comm.closed()
    yield b.close()
    assert b.closed()
    assert b.comm.closed()
    a.abort()


@gen_test()
def test_abort():
    a, b = yield get_comm_pair('tcp://')
    b = BatchedComm(0.2, b)

    assert not b.closed()
    assert not b.comm.closed()
    b.abort()
    assert b.closed()
    assert b.comm.closed()
    a.abort()


@gen.coroutine
def check_properties(addr, **kwargs):
    a, b = yield get_comm_pair(addr, **kwargs)
    b = BatchedComm(0.2, b)
    assert b.peer_address == b.comm.peer_address
    assert b.extra_info == b.comm.extra_info

    a.abort()
    b.abort()

@gen_test()
def test_properties_tcp():
    yield check_properties('tcp://')

@gen_test()
def test_properties_tls():
    yield check_properties('tls://', **tls_kwargs)
