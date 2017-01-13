from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
import struct
import sys

from tornado import gen, ioloop

import zmq
from zmq.eventloop import ioloop as zmqioloop
from zmq.eventloop.future import Context

from .. import config
from .core import connectors, listeners, Comm, CommClosedError, Listener
from .utils import to_frames, from_frames, parse_host_port, unparse_host_port
from . import zmqimpl


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


#async_ctx = Context()
## Workaround https://github.com/zeromq/pyzmq/issues/962
#async_ctx.io_loop = None

#def make_socket(sockty):
    #sock = async_ctx.socket(sockty)
    #return sock

ctx = zmqimpl.Context()

def make_socket(sockty):
    sock = ctx.socket(sockty)
    return sock


def set_socket_options(sock):
    """
    Set common options on a ZeroMQ socket.
    """
    # XXX set context default options instead?
    sock.set(zmq.RECONNECT_IVL, -1)  # disable reconnections
    sock.set(zmq.IPV6, True)


def make_zmq_url(ip, port=0):
    if not port:
        port = '*'
    return "tcp://" + unparse_host_port(ip, port)


def bind_to_random_port(sock, ip):
    sock.bind(make_zmq_url(ip))
    endpoint = sock.get(zmq.LAST_ENDPOINT).decode()
    _, sep, port = endpoint.rpartition(':')
    assert sep
    return int(port)


class ZMQ(Comm):

    def __init__(self, sock, peer_addr, deserialize=True):
        self.sock = sock
        self.deserialize = deserialize
        self._peer_addr = peer_addr
        # XXX socket timeouts

    @property
    def peer_address(self):
        return self._peer_addr

    @gen.coroutine
    def read(self, deserialize=None):
        if self.sock is None:
            raise CommClosedError
        if deserialize is None:
            deserialize = self.deserialize

        frames = yield self.sock.recv_multipart(copy=False)
        msg = from_frames([f.buffer for f in frames], deserialize=deserialize)
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        if self.sock is None:
            raise CommClosedError

        frames = to_frames(msg)
        copy = all(len(f) < NOCOPY_THRESHOLD for f in frames)
        yield self.sock.send_multipart(frames, copy=copy)
        raise gen.Return(sum(map(len, frames)))

    @gen.coroutine
    def close(self):
        sock, self.sock = self.sock, None
        if sock is not None and not sock.closed:
            sock.close(linger=5000)   # 5 seconds

    def abort(self):
        sock, self.sock = self.sock, None
        if sock is not None and not sock.closed:
            sock.close(linger=0)      # no wait

    def closed(self):
        return self.sock is None


class ZMQConnector(object):

    @gen.coroutine
    def _do_connect(self, sock, address, listener_url, deserialize=True):
        sock.connect(listener_url)

        req = {'op': 'zmq-connect'}
        yield sock.send_multipart(to_frames(req))
        frames = yield sock.recv_multipart(copy=True)
        sock.disconnect(listener_url)

        resp = from_frames(frames)
        sock.connect(resp['zmq-url'])

        comm = ZMQ(sock, 'zmq://' + address, deserialize)
        raise gen.Return(comm)

    @gen.coroutine
    def connect(self, address, deserialize=True):
        listener_url = make_zmq_url(*parse_host_port(address))
        sock = make_socket(zmq.DEALER)
        set_socket_options(sock)

        comm = yield self._do_connect(sock, address, listener_url)
        raise gen.Return(comm)


class ZMQListener(Listener):

    def __init__(self, address, comm_handler, deserialize=True, default_port=0):
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.sock = None
        self.bound_port = None
        self.please_stop = False

    def start(self):
        self.sock = make_socket(zmq.ROUTER)
        set_socket_options(self.sock)
        if self.port == 0:
            self.bound_port = bind_to_random_port(self.sock, self.ip)
        else:
            self.bound_port = self.port
            self.sock.bind(make_zmq_url(self.ip, self.port))
        self._listen()

    @gen.coroutine
    def _listen(self):
        while not self.please_stop:
            frames = yield self.sock.recv_multipart()
            envelope = frames[0]
            req = from_frames(frames[1:])
            assert req['op'] == 'zmq-connect'

            cli_sock = make_socket(zmq.DEALER)
            set_socket_options(cli_sock)
            cli_port = bind_to_random_port(cli_sock, self.ip)

            resp = {'zmq-url': make_zmq_url(self.ip, cli_port)}
            yield self.sock.send_multipart([envelope] + to_frames(resp))

            address = 'zmq://<unknown>'  # XXX
            comm = ZMQ(cli_sock, address, self.deserialize)
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

    @property
    def address(self):
        """
        The listening address as a string.
        """
        return 'zmq://'+ unparse_host_port(*self.get_host_port())


connectors['zmq'] = ZMQConnector()
listeners['zmq'] = ZMQListener
