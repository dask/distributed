import asyncio
import logging
import socket
import struct
import weakref

try:
    import ssl
except ImportError:
    ssl = None  # type: ignore

import dask

from ..utils import ensure_ip, get_ip, get_ipv6, nbytes
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, CommClosedError, Connector, Listener
from .registry import Backend
from .utils import ensure_concrete_host, from_frames, to_frames

logger = logging.getLogger(__name__)


_COMM_CLOSED = object()


class DaskCommProtocol(asyncio.BufferedProtocol):
    def __init__(self, on_connection=None, min_read_size=128 * 1024):
        super().__init__()
        self.on_connection = on_connection
        self._exception = None
        self._queue = asyncio.Queue()
        self._transport = None
        self._paused = False
        self._drain_waiter = None
        self._loop = asyncio.get_running_loop()
        self._is_closed = self._loop.create_future()

        # Per-message state
        self._using_default_buffer = True

        self._default_buffer = memoryview(bytearray(min_read_size))
        self._default_len = min_read_size
        self._default_start = 0
        self._default_end = 0

        self._nframes = None
        self._frame_lengths = None
        self._frames = None
        self._frame_index = None
        self._frame_nbytes_needed = 0

    @property
    def local_addr(self):
        if self._transport is None:
            return "<closed>"
        try:
            host, port = self._transport.get_extra_info("socket").getsockname()[:2]
            return unparse_host_port(host, port)
        except Exception:
            return "<closed>"

    @property
    def peer_addr(self):
        if self._transport is None:
            return "<closed>"
        try:
            host, port = self._transport.get_extra_info("peername")
            return unparse_host_port(host, port)
        except Exception:
            return "<closed>"

    @property
    def is_closed(self):
        return self._transport is None

    def _abort(self):
        if self._transport is not None:
            self._transport, transport = None, self._transport
            transport.abort()

    def _close_from_finalizer(self, comm_repr):
        if self._transport is not None:
            logger.warning(f"Closing dangling comm `{comm_repr}`")
            self._abort()

    async def _close(self):
        if self._transport is not None:
            self._transport, transport = None, self._transport
            transport.close()
        await self._is_closed

    def connection_made(self, transport):
        self._transport = transport
        if self.on_connection is not None:
            self.on_connection(self)

    def get_buffer(self, sizehint):
        if self._frames is None or self._frame_nbytes_needed < self._default_len:
            self._using_default_buffer = True
            return self._default_buffer[self._default_end :]
        else:
            self._using_default_buffer = False
            frame = self._frames[self._frame_index]
            return frame[-self._frame_nbytes_needed :]

    def buffer_updated(self, nbytes):
        if nbytes == 0:
            return

        if self._using_default_buffer:
            self._default_end += nbytes
            while self._parse_next():
                pass
            self._reset_default_buffer()
        else:
            self._frame_nbytes_needed -= nbytes
            if not self._frames_check_remaining():
                self._message_completed()

    def _parse_next(self):
        if self._nframes is None:
            if not self._parse_nframes():
                return False
        if len(self._frame_lengths) < self._nframes:
            if not self._parse_frame_lengths():
                return False
        return self._parse_frames()

    def _parse_nframes(self):
        if self._default_end - self._default_start > 8:
            self._nframes = struct.unpack_from(
                "<Q", self._default_buffer, offset=self._default_start
            )[0]
            self._default_start += 8
            self._frame_lengths = []
            return True
        return False

    def _parse_frame_lengths(self):
        needed = self._nframes - len(self._frame_lengths)
        available = (self._default_end - self._default_start) // 8
        n_read = min(available, needed)
        self._frame_lengths.extend(
            struct.unpack_from(
                f"<{n_read}Q", self._default_buffer, offset=self._default_start
            )
        )
        self._default_start += 8 * n_read

        if n_read == needed:
            self._frames = [memoryview(bytearray(n)) for n in self._frame_lengths]
            self._frame_index = 0
            self._frame_nbytes_needed = (
                self._frame_lengths[0] if self._frame_lengths else 0
            )
            return True
        return False

    def _frames_check_remaining(self):
        # Current frame not filled
        if self._frame_nbytes_needed:
            return True
        # Advance until next non-empty frame
        while True:
            self._frame_index += 1
            if self._frame_index < self._nframes:
                self._frame_nbytes_needed = self._frame_lengths[self._frame_index]
                if self._frame_nbytes_needed:
                    return True
            else:
                # No non-empty frames remain
                return False

    def _parse_frames(self):
        while True:
            # Are we out of frames?
            if not self._frames_check_remaining():
                self._message_completed()
                return True
            # Are we out of data?
            available = self._default_end - self._default_start
            if not available:
                return False

            frame = self._frames[self._frame_index]
            n_read = min(self._frame_nbytes_needed, available)
            frame[
                -self._frame_nbytes_needed : (n_read - self._frame_nbytes_needed)
                or None
            ] = self._default_buffer[self._default_start : self._default_start + n_read]
            self._default_start += n_read
            self._frame_nbytes_needed -= n_read

    def _reset_default_buffer(self):
        start = self._default_start
        end = self._default_end

        if start < end and start != 0:
            self._default_buffer[: end - start] = self._default_buffer[start:end]
            self._default_start = 0
            self._default_end = end - start
        elif start == end:
            self._default_start = 0
            self._default_end = 0

    def _message_completed(self):
        self._queue.put_nowait(self._frames)
        self._nframes = None
        self._frames = None
        self._frame_lengths = None
        self._frame_nbytes_remaining = 0

    def connection_lost(self, exc=None):
        if exc is None:
            exc = CommClosedError("Connection closed")
        self._exception = exc
        self._transport = None
        self._is_closed.set_result(None)

        # Unblock read, if any
        self._queue.put_nowait(_COMM_CLOSED)

        # Unblock write, if any
        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(exc)

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def read(self):
        # Even if comm is closed, we still yield all received data before
        # erroring
        if self._queue is not None:
            out = await self._queue.get()
            if out is not _COMM_CLOSED:
                return out
            self._queue = None
        raise self._exception

    async def write(self, frames):
        if self._exception:
            raise self._exception

        nframes = len(frames)
        frames_nbytes = [nbytes(f) for f in frames]
        header = struct.pack(f"{nframes + 1}Q", nframes, *frames_nbytes)
        frames_nbytes_total = sum(frames_nbytes) + nbytes(header)
        frames = [header, *frames]

        if frames_nbytes_total < 2 ** 17:  # 128kiB
            # small enough, send in one go
            frames = [b"".join(frames)]

        if len(frames) > 1:
            self._transport.writelines(frames)
        else:
            self._transport.write(frames[0])
        if self._transport.is_closing():
            await asyncio.sleep(0)
        elif self._paused:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

        return frames_nbytes_total


class TCP(Comm):
    max_shard_size = dask.utils.parse_bytes(dask.config.get("distributed.comm.shard"))

    def __init__(
        self,
        protocol,
        local_addr: str,
        peer_addr: str,
        deserialize: bool = True,
    ):
        self._protocol = protocol
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.deserialize = deserialize
        self._closed = False
        super().__init__()

        # setup a finalizer to close the protocol if the comm was never explicitly closed
        self._finalizer = weakref.finalize(
            self, self._protocol._close_from_finalizer, repr(self)
        )
        self._finalizer.atexit = False

        # Fill in any extra info about this comm
        self._extra_info = self._get_extra_info()

    def _get_extra_info(self):
        return {}

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return self._peer_addr

    async def read(self, deserializers=None):
        frames = await self._protocol.read()
        try:
            return await from_frames(
                frames,
                deserialize=self.deserialize,
                deserializers=deserializers,
                allow_offload=self.allow_offload,
            )
        except EOFError:
            # Frames possibly garbled or truncated by communication error
            self.abort()
            raise CommClosedError("aborted stream on truncated data")

    async def write(self, msg, serializers=None, on_error="message"):
        frames = await to_frames(
            msg,
            allow_offload=self.allow_offload,
            serializers=serializers,
            on_error=on_error,
            context={
                "sender": self.local_info,
                "recipient": self.remote_info,
                **self.handshake_options,
            },
            frame_split_size=self.max_shard_size,
        )
        nbytes = await self._protocol.write(frames)
        return nbytes

    async def close(self):
        """Flush and close the comm"""
        self._finalizer.detach()
        await self._protocol._close()

    def abort(self):
        """Hard close the comm"""
        self._finalizer.detach()
        self._protocol._abort()

    def closed(self):
        return self._protocol.is_closed

    @property
    def extra_info(self):
        return self._extra_info


class TLS(TCP):
    def _get_extra_info(self):
        get = self._protocol._transport.get_extra_info
        return {"peercert": get("peercert"), "cipher": get("cipher")}


def _expect_tls_context(connection_args):
    ctx = connection_args.get("ssl_context")
    if not isinstance(ctx, ssl.SSLContext):
        raise TypeError(
            "TLS expects a `ssl_context` argument of type "
            "ssl.SSLContext (perhaps check your TLS configuration?)"
            "  Instead got %s" % str(ctx)
        )
    return ctx


def _error_if_require_encryption(address, **kwargs):
    if kwargs.get("require_encryption"):
        raise RuntimeError(
            "encryption required by Dask configuration, "
            "refusing communication from/to %r" % ("tcp://" + address,)
        )


class TCPConnector(Connector):
    prefix = "tcp://"
    comm_class = TCP

    async def connect(self, address, deserialize=True, **kwargs):
        loop = asyncio.get_event_loop()
        ip, port = parse_host_port(address)

        kwargs = self._get_extra_kwargs(address, **kwargs)
        transport, protocol = await loop.create_connection(
            DaskCommProtocol, ip, port, **kwargs
        )
        local_addr = self.prefix + protocol.local_addr
        peer_addr = self.prefix + address
        return self.comm_class(protocol, local_addr, peer_addr, deserialize=deserialize)

    def _get_extra_kwargs(self, address, **kwargs):
        _error_if_require_encryption(address, **kwargs)
        return {}


class TLSConnector(TCPConnector):
    prefix = "tls://"
    comm_class = TLS

    def _get_extra_kwargs(self, address, **kwargs):
        ctx = _expect_tls_context(kwargs)
        return {"ssl": ctx}


class TCPListener(Listener):
    prefix = "tcp://"
    comm_class = TCP

    def __init__(
        self,
        address,
        comm_handler,
        deserialize=True,
        allow_offload=True,
        default_host=None,
        default_port=0,
        **kwargs,
    ):
        self.ip, self.port = parse_host_port(address, default_port)
        self.default_host = default_host
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self._extra_kwargs = self._get_extra_kwargs(address, **kwargs)
        self.bound_address = None

    def _get_extra_kwargs(self, address, **kwargs):
        _error_if_require_encryption(address, **kwargs)
        return {}

    def _on_connection(self, protocol):
        logger.debug("Incoming connection")
        comm = self.comm_class(
            protocol,
            local_addr=self.prefix + protocol.local_addr,
            peer_addr=self.prefix + protocol.peer_addr,
            deserialize=self.deserialize,
        )
        comm.allow_offload = self.allow_offload
        asyncio.ensure_future(self._comm_handler(comm))

    async def _comm_handler(self, comm):
        try:
            await self.on_connection(comm)
        except CommClosedError:
            logger.debug("Connection closed before handshake completed")
            return
        await self.comm_handler(comm)

    async def start(self):
        loop = asyncio.get_event_loop()
        self._handle = await loop.create_server(
            lambda: DaskCommProtocol(self._on_connection),
            host=self.ip,
            port=self.port,
            **self._extra_kwargs,
        )

    def stop(self):
        # Stop listening
        self._handle.close()
        # TODO: stop should really be asynchronous
        asyncio.ensure_future(self._handle.wait_closed())

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        if self.bound_address is None:

            def get_socket():
                for family in [socket.AF_INET, socket.AF_INET6]:
                    for sock in self._handle.sockets:
                        if sock.family == socket.AF_INET:
                            return sock
                raise RuntimeError("No active INET socket found?")

            sock = get_socket()
            self.bound_address = sock.getsockname()[:2]
        return self.bound_address

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
        host = ensure_concrete_host(host, default_host=self.default_host)
        return self.prefix + unparse_host_port(host, port)


class TLSListener(TCPListener):
    prefix = "tls://"
    comm_class = TLS

    def _get_extra_kwargs(self, address, **kwargs):
        ctx = _expect_tls_context(kwargs)
        return {"ssl": ctx}


class TCPBackend(Backend):
    _connector_class = TCPConnector
    _listener_class = TCPListener

    def get_connector(self):
        return self._connector_class()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return self._listener_class(loc, handle_comm, deserialize, **connection_args)

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
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


class TLSBackend(TCPBackend):
    _connector_class = TLSConnector
    _listener_class = TLSListener
