from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
#import socket
import struct
import sys

#from toolz import first

from tornado import gen, ioloop
from tornado.iostream import StreamClosedError

import zmq
from zmq.eventloop import ioloop as zmqioloop
from zmq.eventloop.future import Context

from .. import config
from ..metrics import time
from .transports import connectors, listeners, Comm
from .utils import to_frames, from_frames, parse_host_port


logger = logging.getLogger(__name__)


def install():
    ioloop.IOLoop.clear_current()
    ioloop.IOLoop.clear_instance()
    zmqioloop.install()

    inst = ioloop.IOLoop.instance()
    if not isinstance(inst, zmqioloop.IOLoop):
        raise RuntimeError("failed to install ZeroMQ IO Loop, got %r" % (inst,))

install()



NOCOPY_THRESHOLD = 1000 ** 2   # 1 MB

ctx = Context()
# Workaround https://github.com/zeromq/pyzmq/issues/962
ctx.io_loop = None


def set_socket_options(sock):
    """
    Set common options on a ZeroMQ socket.
    """
    # XXX use context default options instead?
    sock.set(zmq.RECONNECT_IVL, -1)  # disable reconnections
    #sock.set(zmq.IPV6, True)


def make_zmq_url(ip, port=0):
    if ':' in ip and not ip.startswith('['):
        ip = '[%s]' % ip   # IPv6
    if port:
        return "tcp://%s:%d" % (ip, port)
    else:
        return "tcp://%s" % (ip,)


class ZMQ(Comm):

    def __init__(self, sock, deserialize=True):
        self.sock = sock
        self.deserialize = deserialize
        # XXX socket timeouts

    @gen.coroutine
    def read(self):
        if self.sock is None:
            raise ValueError("ZMQ closed")

        frames = yield self.sock.recv_multipart(copy=False)
        msg = from_frames([f.buffer for f in frames], deserialize=self.deserialize)
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        if self.sock is None:
            raise ValueError("ZMQ closed")

        frames = to_frames(msg)
        copy = all(len(f) < NOCOPY_THRESHOLD for f in frames)
        yield self.sock.send_multipart(frames, copy=copy)
        raise gen.Return(sum(map(len, frames)))

    @gen.coroutine
    def close(self):
        sock, self.sock = self.sock, None
        if sock is not None and not sock.closed:
            sock.close(linger=5000)   # 5 seconds

    #def abort(self):


class ZMQConnector(object):
    timeout = 6

    @gen.coroutine
    def _do_connect(self, sock, listener_url, deserialize=True):
        sock.connect(listener_url)

        req = {'op': 'zmq-connect'}
        yield sock.send_multipart(to_frames(req))
        frames = yield sock.recv_multipart(copy=True)
        sock.disconnect(listener_url)

        resp = from_frames(frames)
        accept_url = resp['zmq-url']
        sock.connect(accept_url)

        comm = ZMQ(sock, deserialize)
        raise gen.Return(comm)

    @gen.coroutine
    def connect(self, address, deserialize=True):
        listener_url = make_zmq_url(*parse_host_port(address))
        sock = ctx.socket(zmq.DEALER)
        set_socket_options(sock)

        #comm = yield gen.with_timeout(timedelta(seconds=self.timeout),
                                      #self._do_connect(sock, listener_url))
        comm = yield self._do_connect(sock, listener_url)
        raise gen.Return(comm)

    def set_timeout(self, timeout):
        self.timeout = timeout


class ZMQListener(object):

    def __init__(self, address, comm_handler, deserialize=True, default_port=0):
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.sock = None
        self.bound_port = None
        self.please_stop = False

    def start(self):
        self.sock = ctx.socket(zmq.ROUTER)
        set_socket_options(self.sock)
        if self.port == 0:
            self.bound_port = self.sock.bind_to_random_port(make_zmq_url(self.ip))
        else:
            self.bound_port = port
            self.sock.bind(make_zmq_url(self.ip, self.port))
        self._listen()

    @gen.coroutine
    def _listen(self):
        while not self.please_stop:
            frames = yield self.sock.recv_multipart()
            envelope = frames[0]
            req = from_frames(frames[1:])
            assert req['op'] == 'zmq-connect'

            cli_sock = ctx.socket(zmq.DEALER)
            cli_port = cli_sock.bind_to_random_port(make_zmq_url(self.ip))
            set_socket_options(cli_sock)

            resp = {'zmq-url': make_zmq_url(self.ip, cli_port)}
            yield self.sock.send_multipart([envelope] + to_frames(resp))

            comm = ZMQ(cli_sock, self.deserialize)
            self.comm_handler(comm)

    def stop(self):
        sock, self.sock = self.sock, None
        if sock is not None:
            self.please_stop = True
            sock.close()
            # XXX cancel listen future?

    def _check_started(self):
        if self.sock is None:
            raise ValueError("invalid operation on non-started ZMQListener")

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        self._check_started()
        return self.ip, self.bound_port

    def get_address(self):
        """
        The listening address as a string.
        """
        return 'zmq://%s:%d' % self.get_host_port()


#connectors['tcp'] = TCPConnector()
#listeners['tcp'] = TCPListener
