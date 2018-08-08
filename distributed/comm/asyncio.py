import logging
import socket
import struct

try:
    import ssl
except ImportError:
    ssl = None

import asyncio
from tornado import gen

from weakref import finalize
from ..utils import (ensure_ip, get_ip, get_ipv6, nbytes,
                     shutting_down, log_errors)

from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .utils import to_frames, from_frames, ensure_concrete_host
from ._asyncio_utils import start_server


logger = logging.getLogger(__name__)


def get_total_physical_memory():
    try:
        import psutil
        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9


MAX_BUFFER_SIZE = get_total_physical_memory()


def get_stream_address(stream):
    """
    Get a stream's local address.
    """
    try:
        return unparse_host_port(*stream._transport._sock.getsockname()[:2])
    except EnvironmentError:
        # Probably EBADF
        return "<closed>"


def convert_stream_closed_error(obj, exc):
    """
    Re-raise StreamClosedError as CommClosedError.
    """
    if exc.real_error is not None:
        # The stream was closed because of an underlying OS error
        exc = exc.real_error
        raise CommClosedError("in %s: %s: %s" % (obj, exc.__class__.__name__, exc))
    else:
        raise CommClosedError("in %s: %s" % (obj, exc))


class TCP(Comm):
    """
    An established communication based on an underlying Tornado IOStream.
    """
    def __init__(self,
                 reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter,
                 local_addr: str,
                 peer_addr: str,
                 deserialize=True):
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.reader = reader
        self.writer = writer
        self.deserialize = deserialize
        self._finalizer = finalize(self, self._get_finalizer())
        self._finalizer.atexit = False
        self._extra = {}

        # set_tcp_timeout(stream)
        # self._read_extra()

    def _read_extra(self):
        pass

    def _get_finalizer(self):
        def finalize(reader=self.reader, writer=self.writer, r=repr(self)):
            return
            if not reader.closed():
                logger.warning("Closing dangling stream in %s" % (r,))
                reader.close()
                writer.close()

        return finalize

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    async def read(self, deserializers=None):
        with log_errors():
            reader = self.reader
            if reader is None:
                raise CommClosedError

            try:
                n_frames = await reader.readexactly(8)
                n_frames = struct.unpack('Q', n_frames)[0]
                lengths = await reader.readexactly(8 * n_frames)
                lengths = struct.unpack('Q' * n_frames, lengths)

                frames = []
                for length in lengths:
                    if length:
                        frame = await reader.readexactly(length)
                    else:
                        frame = b''
                    frames.append(frame)
            except asyncio.streams.IncompleteReadError as e:
                self.reader = None
                self.writer = None
                if not shutting_down():
                    raise CommClosedError()
            else:
                try:
                    msg = await from_frames(frames,
                                            deserialize=self.deserialize,
                                            deserializers=deserializers)
                except EOFError:
                    # Frames possibly garbled or truncated by communication error
                    self.abort()
                    raise CommClosedError("aborted stream on truncated data")
                return msg

    async def write(self, msg, serializers=None, on_error='message'):
        with log_errors():
            writer = self.writer
            bytes_since_last_yield = 0
            if writer is None:
                raise CommClosedError

            frames = await to_frames(msg,
                                     serializers=serializers,
                                     on_error=on_error,
                                     context={'sender': self._local_addr,
                                              'recipient': self._peer_addr})

            try:
                lengths = [nbytes(frame) for frame in frames]
                length_bytes = ([struct.pack('Q', len(frames))] +
                                 [struct.pack('Q', length) for length in lengths])
                if sum(lengths) < 2 ** 17:  # 127 kiB
                    writer.write(b''.join(length_bytes + frames))
                else:
                    writer.write(b''.join(length_bytes))
                    for frame in frames:
                        # Can't wait for the write() Future as it may be lost
                        # ("If write is called again before that Future has resolved,
                        #   the previous future will be orphaned and will never resolve")
                        writer.write(frame)
                        bytes_since_last_yield += nbytes(frame)
                        if bytes_since_last_yield > 32e6:
                            await writer.drain()
                            bytes_since_last_yield = 0
            except asyncio.streams.IncompleteReadError as e:
                self.reader = None
                self.writer = None
                raise CommClosedError()
                # convert_stream_closed_error(self, e)

            return sum(map(nbytes, frames))

    @gen.coroutine
    def close(self):
        writer, self.writer = self.writer, None
        if writer is not None and not writer._transport._closing:
            self._finalizer.detach()
            writer.close()
            yield writer.wait_closed()

    def abort(self):
        writer, self.writer = self.writer, None
        if writer is not None and not writer._transport._closing:
            self._finalizer.detach()
            writer.close()
            yield writer.wait_closed()

    def closed(self):
        return self.writer is None or self.writer._transport._closing

    @property
    def extra_info(self):
        return self._extra


class RequireEncryptionMixin(object):

    def _check_encryption(self, address, connection_args):
        if not self.encrypted and connection_args.get('require_encryption'):
            # XXX Should we have a dedicated SecurityError class?
            raise RuntimeError("encryption required by Dask configuration, "
                               "refusing communication from/to %r"
                               % (self.prefix + address,))


class BaseTCPConnector(Connector, RequireEncryptionMixin):

    async def connect(self, address, deserialize=True, **connection_args):
        self._check_encryption(address, connection_args)
        ip, port = parse_host_port(address)
        kwargs = self._get_connect_args(**connection_args)

        reader, writer = await asyncio.open_connection(
                ip, port,
                family=socket.AF_INET,
                **kwargs
        )

        local_address = self.prefix + get_stream_address(reader)
        return self.comm_class(reader, writer,
                               local_address,
                               self.prefix + address,
                               deserialize)


class TCPConnector(BaseTCPConnector):
    prefix = 'tcp://'
    comm_class = TCP
    encrypted = False

    def _get_connect_args(self, **connection_args):
        return {}


class BaseTCPListener(Listener, RequireEncryptionMixin):

    def __init__(self, address, comm_handler, deserialize=True,
                 default_port=0, **connection_args):
        self._check_encryption(address, connection_args)
        self.ip, self.port = parse_host_port(address, default_port)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.server_args = self._get_server_args(**connection_args)
        self.server = None
        self.bound_address = None

    def start(self, loop=None):
        if self.server is not None:
            return
        if loop is None:
            loop = asyncio.get_event_loop()
        self.server = start_server(self._handle_stream,
                                   self.ip, self.port,
                                   family=socket.AF_INET,
                                   **self.server_args)

        # backlog = int(dask.config.get('distributed.comm.socket-backlog'))

    def stop(self):
        server, self.server = self.server, None
        if server is not None:
            server.close()

    def _check_started(self):
        if self.server is None:
            raise ValueError("invalid operation on non-started TCPListener")

    async def _handle_stream(self, reader, writer, *args, **kwargs):
        host, ip = writer.get_extra_info('peername')
        address = self.prefix + unparse_host_port(host, ip)
        reader, writer = await self._prepare_stream(reader, writer, address)
        # if stream is None:
        #     # Preparation failed
        #     return
        logger.debug("Incoming connection from %r to %r",
                     address, self.contact_address)
        local_address = self.prefix + get_stream_address(reader)
        comm = self.comm_class(reader, writer, local_address, address, self.deserialize)
        await self.comm_handler(comm)

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        self._check_started()

        if self.bound_address is None:
            sockets = self.server.sockets
            if not sockets:
                raise RuntimeError("TCP Server %r not started yet?" % (self.server,))

            def _look_for_family(fam):
                for sock in sockets:
                    if sock.family == fam:
                        return sock
                return None

            # If listening on both IPv4 and IPv6, prefer IPv4 as defective IPv6
            # is common (e.g. Travis-CI).
            sock = _look_for_family(socket.AF_INET)
            if sock is None:
                sock = _look_for_family(socket.AF_INET6)
            if sock is None:
                raise RuntimeError("No Internet socket found on TCPServer??")

            self.bound_address = sock.getsockname()

        return self.bound_address[:2]

    @property
    def listen_address(self):
        """
        The listening address as a string.
        """
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        """
        The contact address as a string.
        """
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)
        return self.prefix + unparse_host_port(host, port)


class TCPListener(BaseTCPListener):
    prefix = 'tcp://'
    comm_class = TCP
    encrypted = False

    def _get_server_args(self, **connection_args):
        return {}

    async def _prepare_stream(self, reader, writer, address):
        return reader, writer


class BaseTCPBackend(Backend):

    # I/O

    def get_connector(self):
        return self._connector_class()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return self._listener_class(loc, handle_comm, deserialize, **connection_args)

    # Address handling

    def get_address_host(self, loc):
        return parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ':' in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


class TCPBackend(BaseTCPBackend):
    _connector_class = TCPConnector
    _listener_class = TCPListener


backends['tcp'] = TCPBackend()
