import asyncio
import collections
import logging
import socket
import struct
import weakref

try:
    import ssl
except ImportError:
    ssl = None  # type: ignore

import dask

from ..utils import ensure_ip, get_ip, get_ipv6
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, CommClosedError, Connector, Listener
from .registry import Backend
from .utils import ensure_concrete_host, from_frames, to_frames

logger = logging.getLogger(__name__)


_COMM_CLOSED = object()


def coalesce_buffers(buffers, target_buffer_size=64 * 1024, small_buffer_size=2048):
    """Given a list of buffers, coalesce them into a new list of buffers that
    minimizes both copying and tiny writes.

    Parameters
    ----------
    buffers : list of bytes_like
    target_buffer_size : int, optional
        The target intermediate buffer size from concatenating small buffers
        together. Coalesced buffers will be no larger than approximately this size.
    small_buffer_size : int, optional
        Buffers <= this size are considered "small" and may be copied. Buffers
        larger than this may also be copied if the total message length is less
        than ``target_buffer_size``.
    """
    # Nothing to do
    if len(buffers) == 1:
        return buffers

    # If the whole message can be sent in <= target_buffer_size, always concatenate
    if sum(map(len, buffers)) <= target_buffer_size:
        return [b"".join(buffers)]

    out_buffers = []
    concat = []  # A list of buffers to concatenate
    csize = 0  # The total size of the concatenated buffers

    def flush():
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

        self._default_len = max(min_read_size, 16)  # need at least 16 bytes of buffer
        self._default_buffer = memoryview(bytearray(self._default_len))
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
        peername = self._transport.get_extra_info("peername")
        if peername is not None:
            return unparse_host_port(*peername[:2])
        return "<unknown>"

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
            try:
                self._abort()
            except RuntimeError:
                # This happens if the event loop is already closed
                pass

    async def _close(self):
        if self._transport is not None:
            self._transport, transport = None, self._transport
            transport.close()
        await self._is_closed

    def connection_made(self, transport):
        # XXX: When using asyncio, the default builtin transport makes
        # excessive copies when buffering. For the case of TCP on asyncio (no
        # TLS) we patch around that with a wrapper class that handles the write
        # side with minimal copying.
        if type(transport) is asyncio.selector_events._SelectorSocketTransport:
            transport = _ZeroCopyWriter(self, transport)
        self._transport = transport
        # Set the buffer limits to something more optimal for large data transfer.
        self._transport.set_write_buffer_limits(high=512 * 1024)  # 512 KiB
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
            available = self._default_end - self._default_start
            # Are we out of frames?
            if not self._frames_check_remaining():
                self._message_completed()
                return bool(available)
            # Are we out of data?
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
        elif self._paused:
            # Wait until there's room in the write buffer
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter

        # Ensure all memoryviews are in single-byte format
        frames = [f.cast("B") if isinstance(f, memoryview) else f for f in frames]

        nframes = len(frames)
        frames_nbytes = [len(f) for f in frames]
        # TODO: the old TCP comm included an extra `msg_nbytes` prefix that
        # isn't really needed. We include it here for backwards compatibility,
        # but this could be removed if we ever want to make another breaking
        # change to the comms.
        msg_nbytes = sum(frames_nbytes) + (nframes + 1) * 8
        header = struct.pack(f"{nframes + 2}Q", msg_nbytes, nframes, *frames_nbytes)

        buffers = coalesce_buffers([header, *frames])

        if len(buffers) > 1:
            self._transport.writelines(buffers)
        else:
            self._transport.write(buffers[0])

        return msg_nbytes


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
        await self._protocol._close()
        self._finalizer.detach()

    def abort(self):
        """Hard close the comm"""
        self._protocol._abort()
        self._finalizer.detach()

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
        loop = asyncio.get_running_loop()
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

    async def start(self):
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

    def stop(self):
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


class _ZeroCopyWriter:
    """The builtin socket transport in asyncio makes a bunch of copies, which
    can make sending large amounts of data much slower. This hacks around that.

    Note that this workaround isn't used with the windows ProactorEventLoop or
    uvloop."""

    SENDMSG_MAX_SIZE = 1024 * 1024  # 1 MiB
    SENDMSG_MAX_COUNT = 16

    def __init__(self, protocol, transport):
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

        self._buffers = collections.deque()
        self._offset = 0
        self._size = 0
        self._protocol_paused = False
        self.set_write_buffer_limits()

    def set_write_buffer_limits(self, high=None, low=None):
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

    def _maybe_pause_protocol(self):
        if not self._protocol_paused and self._size > self._high_water:
            self._protocol_paused = True
            self.protocol.pause_writing()

    def _maybe_resume_protocol(self):
        if self._protocol_paused and self._size <= self._low_water:
            self._protocol_paused = False
            self.protocol.resume_writing()

    def _buffer_clear(self):
        self._buffers.clear()
        self._size = 0
        self._offset = 0

    def _buffer_append(self, data):
        if not isinstance(data, memoryview):
            data = memoryview(data)
        if data.format != "B":
            data = data.cast("B")
        self._size += len(data)
        self._buffers.append(data)

    def _buffer_peek(self):
        offset = self._offset
        buffers = []
        size = 0
        count = 0
        for b in self._buffers:
            if offset:
                b = b[offset:]
                offset = 0
            buffers.append(b)
            size += len(b)
            count += 1
            if size > self.SENDMSG_MAX_SIZE or count == self.SENDMSG_MAX_COUNT:
                break
        return buffers

    def _buffer_advance(self, size):
        self._size -= size

        offset = self._offset
        buffers = self._buffers
        while size:
            b = buffers[0]
            b_len = len(b) - offset
            if b_len <= size:
                buffers.popleft()
                size -= b_len
                offset = 0
            else:
                offset += size
                break
        self._offset = offset

    def write(self, data):
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
            self._loop._add_writer(transport._sock_fd, self._on_write_ready)

        # Add it to the buffer.
        self._buffer_append(data)
        self._maybe_pause_protocol()

    def writelines(self, buffers):
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
            self._loop._add_writer(self.transport._sock_fd, self._on_write_ready)

        self._maybe_pause_protocol()

    def close(self):
        self._buffer_clear()
        return self.transport.close()

    def abort(self):
        self._buffer_clear()
        return self.transport.abort()

    def get_extra_info(self, key):
        return self.transport.get_extra_info(key)

    def _do_bulk_write(self):
        buffers = self._buffer_peek()
        if len(buffers) == 1:
            n = self.transport._sock.send(buffers[0])
            self._buffer_advance(n)
        else:
            n = self.transport._sock.sendmsg(buffers)
            self._buffer_advance(n)

    def _on_write_ready(self):
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
            self._loop._remove_writer(transport._sock_fd)
            self._buffers.clear()
            transport._fatal_error(exc, "Fatal write error on socket transport")
        else:
            self._maybe_resume_protocol()
            if not self._buffers:
                self._loop._remove_writer(transport._sock_fd)
                if transport._closing:
                    transport._call_connection_lost(None)
                elif transport._eof:
                    transport._sock.shutdown(socket.SHUT_WR)
