from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
import socket
import struct
import sys

from toolz import first

from tornado import gen
from tornado.iostream import IOStream, StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

from .. import config
from ..metrics import time
from .transports import connectors, listeners, Comm
from .utils import to_frames, from_frames, parse_host_port


logger = logging.getLogger(__name__)


def get_total_physical_memory():
    try:
        import psutil
        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9


MAX_BUFFER_SIZE = get_total_physical_memory()


def set_tcp_timeout(stream):
    """
    Set kernel-level TCP timeout on the stream.
    """
    if stream.closed():
        return

    timeout = int(config.get('tcp-timeout', 30))

    sock = stream.socket

    # Default (unsettable) value on Windows
    # https://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
    nprobes = 10
    assert timeout >= nprobes + 1, "Timeout too low"

    idle = max(2, timeout // 4)
    interval = max(1, (timeout - idle) // nprobes)
    idle = timeout - interval * nprobes
    assert idle > 0

    try:
        if sys.platform.startswith("win"):
            logger.debug("Setting TCP keepalive: idle=%d, interval=%d",
                         idle, interval)
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle * 1000, interval * 1000))
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                TCP_KEEPIDLE = socket.TCP_KEEPIDLE
                TCP_KEEPINTVL = socket.TCP_KEEPINTVL
                TCP_KEEPCNT = socket.TCP_KEEPCNT
            except AttributeError:
                if sys.platform == "darwin":
                    TCP_KEEPIDLE = 0x10  # (named "TCP_KEEPALIVE" in C)
                    TCP_KEEPINTVL = 0x101
                    TCP_KEEPCNT = 0x102
                else:
                    TCP_KEEPIDLE = None

            if TCP_KEEPIDLE is not None:
                logger.debug("Setting TCP keepalive: nprobes=%d, idle=%d, interval=%d",
                             nprobes, idle, interval)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPCNT, nprobes)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, idle)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, interval)

        if sys.platform.startswith("linux"):
            logger.debug("Setting TCP user timeout: %d ms",
                         timeout * 1000)
            TCP_USER_TIMEOUT = 18  # since Linux 2.6.37
            sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, timeout * 1000)
    except EnvironmentError as e:
        logger.warn("Could not set timeout on TCP stream: %s", e)


class TCP(Comm):

    def __init__(self, stream, deserialize=True):
        self.stream = stream
        self.deserialize = deserialize
        stream.set_nodelay(True)
        set_tcp_timeout(stream)

    @gen.coroutine
    def read(self):
        if self.stream is None:
            raise StreamClosedError

        n_frames = yield self.stream.read_bytes(8)
        n_frames = struct.unpack('Q', n_frames)[0]
        lengths = yield self.stream.read_bytes(8 * n_frames)
        lengths = struct.unpack('Q' * n_frames, lengths)

        frames = []
        for length in lengths:
            if length:
                frame = yield self.stream.read_bytes(length)
            else:
                frame = b''
            frames.append(frame)

        msg = from_frames(frames, deserialize=self.deserialize)
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        if self.stream is None:
            raise StreamClosedError

        frames = to_frames(msg)

        lengths = ([struct.pack('Q', len(frames))] +
                   [struct.pack('Q', len(frame)) for frame in frames])
        self.stream.write(b''.join(lengths))

        for frame in frames:
            # Can't wait for the write() Future as it may be lost
            # ("If write is called again before that Future has resolved,
            #   the previous future will be orphaned and will never resolve")
            self.stream.write(frame)

        yield gen.moment
        raise gen.Return(sum(map(len, frames)))

    @gen.coroutine
    def close(self):
        stream, self.stream = self.stream, None
        if stream is not None and not stream.closed():
            try:
                # Flush the stream's write buffer by waiting for a last write.
                if stream.writing():
                    yield stream.write(b'')
            except EnvironmentError:
                pass
            finally:
                stream.close()

    #def abort(self):


class TCPConnector(object):
    timeout = 3

    @gen.coroutine
    def connect(self, address, deserialize=True):
        ip, port = parse_host_port(address)

        client = TCPClient()
        start = time()
        while True:
            future = client.connect(ip, port,
                                    max_buffer_size=MAX_BUFFER_SIZE)
            try:
                stream = yield gen.with_timeout(timedelta(seconds=self.timeout),
                                                future)
            except EnvironmentError:
                if time() - start < timeout:
                    yield gen.sleep(0.01)
                    logger.debug("sleeping on connect")
                else:
                    raise
            except gen.TimeoutError:
                raise IOError("Timed out while connecting to %s:%d" % (ip, port))
            else:
                break

        raise gen.Return(TCP(stream, deserialize))

    def set_timeout(self, timeout):
        self.timeout = timeout


class TCPListener(object):

    def __init__(self, address, comm_handler, deserialize=True, default_port=0):
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.tcp_server = None
        self.bound_address = None

    def start(self):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE)
        self.tcp_server.handle_stream = self.handle_stream
        #print("TCP server listening on:", (self.ip, self.port))
        self.tcp_server.listen(self.port, self.ip)

    def stop(self):
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()

    def _check_started(self):
        if self.tcp_server is None:
            raise ValueError("invalid operation on non-started TCPListener")

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        self._check_started()

        if self.bound_address is None:
            self.bound_address = first(self.tcp_server._sockets.values()).getsockname()
        # IPv6 getsockname() can return more a 4-len tuple
        return self.bound_address[:2]

    def get_address(self):
        """
        The listening address as a string.
        """
        return 'tcp:%s:%d' % self.get_host_port()

    def handle_stream(self, stream, address):
        comm = TCP(stream, self.deserialize)
        self.comm_handler(comm)


connectors['tcp'] = TCPConnector()
listeners['tcp'] = TCPListener
