from __future__ import annotations

import asyncio
import collections
import logging
import os
import socket
import ssl
import struct
import sys
import weakref
from asyncio.selector_events import _SelectorSocketTransport  # type: ignore
from collections.abc import Callable
from itertools import islice
from typing import Any

import dask

from distributed.comm.addressing import parse_host_port, unparse_host_port
from distributed.comm.core import Comm, CommClosedError, Connector, Listener
from distributed.comm.registry import Backend
from distributed.comm.utils import (
    ensure_concrete_host,
    from_frames,
    host_array,
    to_frames,
)
from distributed.utils import ensure_ip, ensure_memoryview, get_ip, get_ipv6

logger = logging.getLogger(__name__)


_COMM_CLOSED = object()


def coalesce_buffers(
    buffers: list[bytes],
    target_buffer_size: int = 64 * 1024,
    small_buffer_size: int = 2048,
) -> list[bytes]:
    """Given a list of buffers, coalesce them into a new list of buffers that
    minimizes both copying and tiny writes.

    Parameters
    ----------
    buffers : list of bytes_like
    target_buffer_size : int, optional
        The target intermediate buffer size from concatenating small buffers
        together. Coalesced buffers will be no larger than approximately this size.
    small_buffer_size : int, optional
        Buffers <= this size are considered "small" and may be copied.
    """
    # Nothing to do
    if len(buffers) == 1:
        return buffers

    out_buffers: list[bytes] = []
    concat: list[bytes] = []  # A list of buffers to concatenate
    csize = 0  # The total size of the concatenated buffers

    def flush() -> None:
        nonlocal csize
        if concat:
            if len(concat) == 1:
                out_buffers.append(concat[0])
            else:
                out_buffers.append(b"".join(concat))
            concat.clear()
            csize = 0

    for b in buffers:
        size = len(b)
        if size <= small_buffer_size:
            concat.append(b)
            csize += size
            if csize >= target_buffer_size:
                flush()
        else:
            flush()
            out_buffers.append(b)
    flush()

    return out_buffers


class DaskCommProtocol(asyncio.BufferedProtocol):
    """Manages a state machine for parsing the message framing used by dask.

    Parameters
    ----------
    on_connection : callable, optional
        A callback to call on connection, used server side for handling
        incoming connections.
    min_read_size : int, optional
        The minimum buffer size to pass to ``socket.recv_into``. Larger sizes
        will result in fewer recv calls, at the cost of more copying. For
        request-response comms (where only one message may be in the queue at a
        time), a smaller value is likely more performant.
    """

    on_connection: Callable[[DaskCommProtocol], None] | None
    _loop: asyncio.AbstractEventLoop
    #: A queue of received messages
    _queue: asyncio.Queue | None
    #: The corresponding transport, set on `connection_made`
    _transport: asyncio.WriteTransport | _ZeroCopyWriter | None
    #: Is the protocol paused?
    _paused: bool
    #: If the protocol is paused, this holds a future to wait on until it's unpaused.
    _drain_waiter: asyncio.Future | None
    #: A future for waiting until the protocol is actually closed
    _closed_waiter: asyncio.Future

    _using_default_buffer: bool
    _default_len: int
    _default_buffer: memoryview
    #: Index in default_buffer pointing to the first unparsed byte
    _default_start: int
    #: Index in default_buffer pointing to the last written byte
    _default_end: int

    _nframes: int | None
    _frame_lengths: list[int] | None
    _frames: list[memoryview] | None
    #: current frame to parse
    _frame_index: int | None
    #: nbytes left for parsing current frame
    _frame_nbytes_needed: int
    _frame_nbytes_remaining: int

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        on_connection: Callable[[DaskCommProtocol], None] | None = None,
        min_read_size: int = 128 * 1024,
    ):
        super().__init__()
        self.on_connection = on_connection
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()
        self._transport = None
        self._paused = False
        self._drain_waiter = None
        self._closed_waiter = self._loop.create_future()

        # In the interest of reducing the number of `recv` calls, we always
        # want to provide the opportunity to read `min_read_size` bytes from
        # the socket (since memcpy is much faster than recv). Each read event
        # may read into either a default buffer (of size `min_read_size`), or
        # directly into one of the message frames (if the frame size is >
        # `min_read_size`).

        # Per-message state
        self._using_default_buffer = True
        self._default_len = max(min_read_size, 16)  # need at least 16 bytes of buffer
        self._default_buffer = host_array(self._default_len)
        self._default_start = 0
        self._default_end = 0

        # Each message is composed of one or more frames, these attributes
        # are filled in as the message is parsed, and cleared once a message
        # is fully parsed.
        self._nframes = None
        self._frame_lengths = None
        self._frames = None
        self._frame_index = None
        self._frame_nbytes_needed = 0
        self._frame_nbytes_remaining = 0

    @property
    def local_addr(self) -> str:
        if self.is_closed:
            return "<closed>"
        assert self._transport is not None
        sockname = self._transport.get_extra_info("sockname")
        if sockname is not None:
            return unparse_host_port(*sockname[:2])
        return "<unknown>"

    @property
    def peer_addr(self) -> str:
        if self.is_closed:
            return "<closed>"
        assert self._transport is not None
        peername = self._transport.get_extra_info("peername")
        if peername is not None:
            return unparse_host_port(*peername[:2])
        return "<unknown>"

    @property
    def is_closed(self) -> bool:
        return self._transport is None

    def _abort(self) -> None:
        if not self.is_closed:
            self._transport, transport = None, self._transport
            assert transport is not None
            transport.abort()  # type: ignore[unreachable]

    def _close_from_finalizer(self, comm_repr: str) -> None:
        if not self.is_closed:
            logger.warning(f"Closing dangling comm `{comm_repr}`")
            try:
                self._abort()
            except RuntimeError:
                # This happens if the event loop is already closed
                pass

    async def _close(self) -> None:
        if not self.is_closed:
            self._transport, transport = None, self._transport
            assert transport is not None
            transport.close()  # type: ignore[unreachable]
        await self._closed_waiter

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # XXX: When using asyncio, the default builtin transport makes
        # excessive copies when buffering. For the case of TCP on asyncio (no
        # TLS) we patch around that with a wrapper class that handles the write
        # side with minimal copying.
        if type(transport) is _SelectorSocketTransport:
            self._transport = _ZeroCopyWriter(self, transport)
        else:
            assert isinstance(transport, asyncio.WriteTransport)
            self._transport = transport

        # Set the buffer limits to something more optimal for large data transfer
        # (512 KiB).
        self._transport.set_write_buffer_limits(high=512 * 1024)  # type: ignore
        if self.on_connection is not None:
            self.on_connection(self)

    def get_buffer(self, sizehint: object) -> memoryview:
        """Get a buffer to read into for this read event"""
        # Read into the default buffer if there are no frames or the current
        # frame is small. Otherwise, read directly into the current frame.
        if self._frames is None or self._frame_nbytes_needed < self._default_len:
            self._using_default_buffer = True
            return self._default_buffer[self._default_end :]
        else:
            self._using_default_buffer = False
            assert self._frame_index is not None
            frame = self._frames[self._frame_index]
            return frame[-self._frame_nbytes_needed :]

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes == 0:
            return

        if self._using_default_buffer:
            self._default_end += nbytes
            self._parse_default_buffer()
        else:
            self._frame_nbytes_needed -= nbytes
            if not self._frames_check_remaining():
                self._message_completed()

    def _parse_default_buffer(self) -> None:
        """Parse all messages in the default buffer."""
        while True:
            if self._nframes is None:
                if not self._parse_nframes():
                    break
            assert self._nframes is not None
            assert self._frame_lengths is not None
            if len(self._frame_lengths) < self._nframes:
                if not self._parse_frame_lengths():
                    break
            if not self._parse_frames():
                break
        self._reset_default_buffer()

    def _parse_nframes(self) -> bool:
        """Fill in `_nframes` from the default buffer. Returns True if
        successful, False if more data is needed"""
        # TODO: we drop the message total size prefix (sent as part of the
        # tornado-based tcp implementation), as it's not needed. If we ever
        # drop that prefix entirely, we can adjust this code (change 16 -> 8
        # and 8 -> 0).
        if self._default_end - self._default_start >= 16:
            self._nframes = struct.unpack_from(
                "<Q", self._default_buffer, offset=self._default_start + 8
            )[0]
            self._default_start += 16
            self._frame_lengths = []
            return True
        return False

    def _parse_frame_lengths(self) -> bool:
        """Fill in `_frame_lengths` from the default buffer. Returns True if
        successful, False if more data is needed"""
        assert self._nframes is not None
        assert self._frame_lengths is not None
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
            self._frames = [host_array(n) for n in self._frame_lengths]
            self._frame_index = 0
            self._frame_nbytes_needed = (
                self._frame_lengths[0] if self._frame_lengths else 0
            )
            return True
        return False

    def _frames_check_remaining(self) -> bool:
        # Current frame not filled
        if self._frame_nbytes_needed:
            return True
        # Advance until next non-empty frame
        assert self._frame_index is not None
        assert self._nframes is not None
        assert self._frame_lengths is not None
        while True:
            self._frame_index += 1
            if self._frame_index < self._nframes:
                self._frame_nbytes_needed = self._frame_lengths[self._frame_index]
                if self._frame_nbytes_needed:
                    return True
            else:
                # No non-empty frames remain
                return False

    def _parse_frames(self) -> bool:
        """Fill in `_frames` from the default buffer. Returns True if
        successful, False if more data is needed"""
        while True:
            available = self._default_end - self._default_start
            # Are we out of frames?
            if not self._frames_check_remaining():
                self._message_completed()
                return bool(available)
            # Are we out of data?
            if not available:
                return False

            assert self._frames is not None
            assert self._frame_index is not None
            frame = self._frames[self._frame_index]
            n_read = min(self._frame_nbytes_needed, available)
            frame[
                -self._frame_nbytes_needed : (n_read - self._frame_nbytes_needed)
                or None
            ] = self._default_buffer[self._default_start : self._default_start + n_read]
            self._default_start += n_read
            self._frame_nbytes_needed -= n_read

    def _reset_default_buffer(self) -> None:
        """Reset the default buffer for the next read event"""
        start = self._default_start
        end = self._default_end

        if start < end and start != 0:
            # Still some unparsed data, copy it to the front of the buffer
            self._default_buffer[: end - start] = self._default_buffer[start:end]
            self._default_start = 0
            self._default_end = end - start
        elif start == end:
            # All data is parsed, just reset the indices
            self._default_start = 0
            self._default_end = 0

    def _message_completed(self) -> None:
        """Push a completed message to the queue and reset per-message state"""
        assert self._queue is not None
        self._queue.put_nowait(self._frames)
        self._nframes = None
        self._frames = None
        self._frame_lengths = None
        self._frame_nbytes_remaining = 0

    def connection_lost(self, exc: BaseException | None = None) -> None:
        self._transport = None
        self._closed_waiter.set_result(None)

        # Unblock read, if any
        assert self._queue is not None
        self._queue.put_nowait(_COMM_CLOSED)

        # Unblock write, if any
        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(CommClosedError("Connection closed"))

    def pause_writing(self) -> None:
        self._paused = True

    def resume_writing(self) -> None:
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def read(self) -> list[bytes]:
        """Read a single message from the comm."""
        # Even if comm is closed, we still yield all received data before
        # erroring
        if self._queue is not None:
            out = await self._queue.get()
            if out is not _COMM_CLOSED:
                return out
            self._queue = None
        raise CommClosedError("Connection closed")

    async def write(self, frames: list[bytes]) -> int:
        """Write a message to the comm."""
        if self.is_closed:
            raise CommClosedError("Connection closed")
        elif self._paused:
            # Wait until there's room in the write buffer
            drain_waiter = self._drain_waiter = self._loop.create_future()
            await drain_waiter

        # Ensure all memoryviews are in single-byte format
        frames = [
            ensure_memoryview(f) if isinstance(f, memoryview) else f for f in frames
        ]

        nframes = len(frames)
        frames_nbytes = [len(f) for f in frames]
        # TODO: the old TCP comm included an extra `msg_nbytes` prefix that
        # isn't really needed. We include it here for backwards compatibility,
        # but this could be removed if we ever want to make another breaking
        # change to the comms.
        msg_nbytes = sum(frames_nbytes) + (nframes + 1) * 8
        header = struct.pack(f"{nframes + 2}Q", msg_nbytes, nframes, *frames_nbytes)

        if msg_nbytes < 4 * 1024:
            # Always concatenate small messages
            buffers = [b"".join([header, *frames])]
        else:
            buffers = coalesce_buffers([header, *frames])

        assert self._transport is not None
        if len(buffers) > 1:
            self._transport.writelines(buffers)
        else:
            self._transport.write(buffers[0])

        return msg_nbytes


class TCP(Comm):
    max_shard_size = dask.utils.parse_bytes(dask.config.get("distributed.comm.shard"))

    def __init__(
        self,
        protocol: DaskCommProtocol,
        local_addr: str,
        peer_addr: str,
        deserialize: bool = True,
    ):
        self._protocol = protocol
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self._closed = False
        super().__init__(deserialize=deserialize)

        # setup a finalizer to close the protocol if the comm was never explicitly closed
        self._finalizer = weakref.finalize(
            self, self._protocol._close_from_finalizer, repr(self)
        )
        self._finalizer.atexit = False

        # Fill in any extra info about this comm
        self._extra_info = self._get_extra_info()

    def _get_extra_info(self) -> dict[str, Any]:
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

    async def close(self) -> None:
        """Flush and close the comm"""
        await self._protocol._close()
        self._finalizer.detach()

    def abort(self) -> None:
        """Hard close the comm"""
        self._protocol._abort()
        self._finalizer.detach()

    def closed(self) -> bool:
        return self._protocol.is_closed

    @property
    def extra_info(self):
        return self._extra_info


class TLS(TCP):
    def _get_extra_info(self) -> dict[str, Any]:
        assert self._protocol._transport is not None
        get = self._protocol._transport.get_extra_info
        return {"peercert": get("peercert"), "cipher": get("cipher")}


def _expect_tls_context(connection_args):
    ctx = connection_args.get("ssl_context")
    if not isinstance(ctx, ssl.SSLContext):
        raise TypeError(
            "TLS expects a `ssl_context` argument of type "
            "ssl.SSLContext (perhaps check your TLS configuration?)"
            f" Instead got {ctx!r}"
        )
    return ctx


def _error_if_require_encryption(address: str, **kwargs: Any) -> None:
    if kwargs.get("require_encryption"):
        raise RuntimeError(
            "encryption required by Dask configuration, "
            "refusing communication from/to %r" % ("tcp://" + address,)
        )


class TCPConnector(Connector):
    prefix = "tcp://"
    comm_class = TCP

    async def connect(self, address, deserialize=True, **kwargs):
        loop = asyncio.get_running_loop()
        ip, port = parse_host_port(address)

        kwargs = self._get_extra_kwargs(address, **kwargs)
        transport, protocol = await loop.create_connection(
            DaskCommProtocol, ip, port, **kwargs
        )
        local_addr = self.prefix + protocol.local_addr
        peer_addr = self.prefix + address
        return self.comm_class(protocol, local_addr, peer_addr, deserialize=deserialize)

    def _get_extra_kwargs(self, address: str, **kwargs: Any) -> dict[str, Any]:
        _error_if_require_encryption(address, **kwargs)
        return {}


class TLSConnector(TCPConnector):
    prefix = "tls://"
    comm_class = TLS

    def _get_extra_kwargs(self, address: str, **kwargs: Any) -> dict[str, Any]:
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

    def _get_extra_kwargs(self, address: str, **kwargs: Any) -> dict[str, Any]:
        _error_if_require_encryption(address, **kwargs)
        return {}

    def _on_connection(self, protocol):
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

    async def _start_all_interfaces_with_random_port(self):
        """Due to a design decision in asyncio, listening on `("", 0)` will
        result in two different random ports being used (one for IPV4, one for
        IPV6), rather than both interfaces sharing the same random port. We
        work around this here. See https://bugs.python.org/issue45693 for more
        info."""
        loop = asyncio.get_running_loop()
        # Typically resolves to list with length == 2 (one IPV4, one IPV6).
        infos = await loop.getaddrinfo(
            None,
            0,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            flags=socket.AI_PASSIVE,
            proto=0,
        )
        # Sort infos to always bind ipv4 before ipv6
        infos = sorted(infos, key=lambda x: x[0].name)
        # This code is a simplified and modified version of that found in
        # cpython here:
        # https://github.com/python/cpython/blob/401272e6e660445d6556d5cd4db88ed4267a50b3/Lib/asyncio/base_events.py#L1439
        servers = []
        port = None
        try:
            for res in infos:
                af, socktype, proto, canonname, sa = res
                try:
                    sock = socket.socket(af, socktype, proto)
                except OSError:
                    # Assume it's a bad family/type/protocol combination.
                    continue
                # Disable IPv4/IPv6 dual stack support (enabled by
                # default on Linux) which makes a single socket
                # listen on both address families.
                if af == getattr(socket, "AF_INET6", None) and hasattr(
                    socket, "IPPROTO_IPV6"
                ):
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)

                # If random port is already chosen, reuse it
                if port is not None:
                    sa = (sa[0], port, *sa[2:])
                try:
                    sock.bind(sa)
                except OSError as err:
                    raise OSError(
                        err.errno,
                        "error while attempting "
                        "to bind on address %r: %s" % (sa, err.strerror.lower()),
                    ) from None

                # If random port hadn't already been chosen, cache this port to
                # reuse for other interfaces
                if port is None:
                    port = sock.getsockname()[1]

                # Create a new server for the socket
                server = await loop.create_server(
                    lambda: DaskCommProtocol(self._on_connection),
                    sock=sock,
                    **self._extra_kwargs,
                )
                servers.append(server)
                sock = None
        except BaseException:
            # Close all opened servers
            for server in servers:
                server.close()
            # If a socket was already created but not converted to a server
            # yet, close that as well.
            if sock is not None:
                sock.close()
            raise

        return servers

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        if not self.ip and not self.port:
            servers = await self._start_all_interfaces_with_random_port()
        else:
            servers = [
                await loop.create_server(
                    lambda: DaskCommProtocol(self._on_connection),
                    host=self.ip,
                    port=self.port,
                    **self._extra_kwargs,
                )
            ]
        self._servers = servers

    def stop(self) -> None:
        # Stop listening
        for server in self._servers:
            server.close()

    def get_host_port(self):
        """
        The listening address as a (host, port) tuple.
        """
        if self.bound_address is None:

            def get_socket(server):
                for family in [socket.AF_INET, socket.AF_INET6]:
                    for sock in server.sockets:
                        if sock.family == family:
                            return sock
                raise RuntimeError("No active INET socket found?")

            sock = get_socket(self._servers[0])
            self.bound_address = sock.getsockname()[:2]
        return self.bound_address

    @property
    def listen_address(self) -> str:
        """
        The listening address as a string.
        """
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self) -> str:
        """
        The contact address as a string.
        """
        host, port = self.get_host_port()
        host = ensure_concrete_host(host, default_host=self.default_host)
        return self.prefix + unparse_host_port(host, port)


class TLSListener(TCPListener):
    prefix = "tls://"
    comm_class = TLS

    def _get_extra_kwargs(self, address: str, **kwargs: Any) -> dict[str, Any]:
        ctx = _expect_tls_context(kwargs)
        return {"ssl": ctx}


class TCPBackend(Backend):
    _connector_class = TCPConnector
    _listener_class = TCPListener

    def get_connector(self) -> Connector:
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


# This class is based on parts of `asyncio.selector_events._SelectorSocketTransport`
# (https://github.com/python/cpython/blob/dc4a212bd305831cb4b187a2e0cc82666fcb15ca/Lib/asyncio/selector_events.py#L757).
class _ZeroCopyWriter:
    """The builtin socket transport in asyncio makes a bunch of copies, which
    can make sending large amounts of data much slower. This hacks around that.

    Note that this workaround isn't used with the windows ProactorEventLoop or
    uvloop."""

    protocol: DaskCommProtocol
    transport: _SelectorSocketTransport
    _loop: asyncio.AbstractEventLoop
    #: A deque of buffers to send
    _buffers: collections.deque[memoryview]
    #: The total size of all bytes left to send in _buffers
    _size: int
    #: Is the backing protocol paused?
    _protocol_paused: bool

    # We use sendmsg for scatter IO if it's available. Since bookkeeping
    # scatter IO has a small cost, we want to minimize the amount of processing
    # we do for each send call. We assume the system send buffer is < 4 MiB
    # (which would be very large), and set a limit on the number of buffers to
    # pass to sendmsg.
    if hasattr(socket.socket, "sendmsg"):
        # Note: WINDOWS constant doesn't work with `mypy --platform win32`
        if sys.platform == "win32":
            SENDMSG_MAX_COUNT = 16  # No os.sysconf available
        else:
            try:
                SENDMSG_MAX_COUNT = os.sysconf("SC_IOV_MAX")
            except Exception:
                SENDMSG_MAX_COUNT = 16  # Should be supported on all systems
    else:
        SENDMSG_MAX_COUNT = 1  # sendmsg not supported, use send instead

    def __init__(self, protocol: DaskCommProtocol, transport: _SelectorSocketTransport):
        self.protocol = protocol
        self.transport = transport
        self._loop = asyncio.get_running_loop()

        # This class mucks with the builtin asyncio transport's internals.
        # Check that the bits we touch still exist.
        for attr in [
            "_sock",
            "_sock_fd",
            "_fatal_error",
            "_eof",
            "_closing",
            "_conn_lost",
            "_call_connection_lost",
        ]:
            assert hasattr(transport, attr)
        # Likewise, this calls a few internal methods of `loop`, ensure they
        # still exist.
        for attr in ["_add_writer", "_remove_writer"]:
            assert hasattr(self._loop, attr)

        # A deque of buffers to send
        self._buffers = collections.deque()
        # The total size of all bytes left to send in _buffers
        self._size = 0
        # Is the backing protocol paused?
        self._protocol_paused = False
        # Initialize the buffer limits
        self.set_write_buffer_limits()

    def set_write_buffer_limits(
        self,
        high: int | None = None,
        low: int | None = None,
    ) -> None:
        """Set the write buffer limits"""
        # Copied almost verbatim from asyncio.transports._FlowControlMixin
        if high is None:
            if low is None:
                high = 64 * 1024  # 64 KiB
            else:
                high = 4 * low
        if low is None:
            low = high // 4
        self._high_water = high
        self._low_water = low
        self._maybe_pause_protocol()

    def _maybe_pause_protocol(self) -> None:
        """If the high water mark has been reached, pause the protocol"""
        if not self._protocol_paused and self._size > self._high_water:
            self._protocol_paused = True
            self.protocol.pause_writing()

    def _maybe_resume_protocol(self) -> None:
        """If the low water mark has been reached, unpause the protocol"""
        if self._protocol_paused and self._size <= self._low_water:
            self._protocol_paused = False
            self.protocol.resume_writing()

    def _buffer_clear(self) -> None:
        """Clear the send buffer"""
        self._buffers.clear()
        self._size = 0

    def _buffer_append(self, data: bytes) -> None:
        """Append new data to the send buffer"""
        mv = ensure_memoryview(data)
        self._size += len(mv)
        self._buffers.append(mv)

    def _buffer_peek(self) -> list[memoryview]:
        """Get one or more buffers to write to the socket"""
        return list(islice(self._buffers, self.SENDMSG_MAX_COUNT))

    def _buffer_advance(self, size: int) -> None:
        """Advance the buffer index forward by `size`"""
        self._size -= size

        buffers = self._buffers
        while size:
            b = buffers[0]
            b_len = len(b)
            if b_len <= size:
                buffers.popleft()
                size -= b_len
            else:
                buffers[0] = b[size:]
                break

    def write(self, data: bytes) -> None:
        # Copied almost verbatim from asyncio.selector_events._SelectorSocketTransport
        transport = self.transport

        if transport._eof:
            raise RuntimeError("Cannot call write() after write_eof()")
        if not data:
            return
        if transport._conn_lost:
            return

        if not self._buffers:
            try:
                n = transport._sock.send(data)
            except (BlockingIOError, InterruptedError):
                pass
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                transport._fatal_error(exc, "Fatal write error on socket transport")
                return
            else:
                data = data[n:]
                if not data:
                    return
            # Not all was written; register write handler.
            # Don't use the public API add_writer; it makes test_tcp_deserialize fail
            self._loop._add_writer(transport._sock_fd, self._on_write_ready)  # type: ignore[attr-defined]

        # Add it to the buffer.
        self._buffer_append(data)
        self._maybe_pause_protocol()

    def writelines(self, buffers: list[bytes]) -> None:
        # Based on modified version of `write` above
        waiting = bool(self._buffers)
        for b in buffers:
            self._buffer_append(b)
        if not waiting:
            try:
                self._do_bulk_write()
            except (BlockingIOError, InterruptedError):
                pass
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self.transport._fatal_error(
                    exc, "Fatal write error on socket transport"
                )
                return
            if not self._buffers:
                return
            # Not all was written; register write handler.
            # Don't use the public API add_writer; it makes test_tcp_deserialize fail
            self._loop._add_writer(self.transport._sock_fd, self._on_write_ready)  # type: ignore[attr-defined]

        self._maybe_pause_protocol()

    def close(self) -> None:
        self._buffer_clear()
        return self.transport.close()

    def abort(self) -> None:
        self._buffer_clear()
        return self.transport.abort()

    def get_extra_info(self, key: str) -> Any:
        return self.transport.get_extra_info(key)

    def _do_bulk_write(self) -> None:
        buffers = self._buffer_peek()
        if len(buffers) == 1:
            n = self.transport._sock.send(buffers[0])
        else:
            n = self.transport._sock.sendmsg(buffers)
        self._buffer_advance(n)

    def _on_write_ready(self) -> None:
        # Copied almost verbatim from asyncio.selector_events._SelectorSocketTransport
        transport = self.transport
        if transport._conn_lost:
            return
        try:
            self._do_bulk_write()
        except (BlockingIOError, InterruptedError):
            pass
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            # Don't use the public API remove_writer; it makes test_tcp_deserialize fail
            self._loop._remove_writer(transport._sock_fd)  # type: ignore[attr-defined]
            self._buffers.clear()
            transport._fatal_error(exc, "Fatal write error on socket transport")
        else:
            self._maybe_resume_protocol()
            if not self._buffers:
                # Don't use the public API remove_writer; it makes test_tcp_deserialize fail
                self._loop._remove_writer(transport._sock_fd)  # type: ignore[attr-defined]
                if transport._closing:
                    transport._call_connection_lost(None)
                elif transport._eof:
                    transport._sock.shutdown(socket.SHUT_WR)
