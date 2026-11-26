from __future__ import annotations

import logging
import os
import socket
from typing import ClassVar

import tornado.netutil as netutil
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

import dask

from distributed.comm.core import (
    BaseListener,
    CommClosedError,
    Connector,
)
from distributed.comm.registry import Backend
from distributed.comm.tcp import TCP
from distributed.system import MEMORY_LIMIT
from distributed.utils import (
    get_uds_path,
)

logger = logging.getLogger(__name__)

MAX_BUFFER_SIZE = MEMORY_LIMIT / 2


class UDS(TCP):
    """A comm for UDS. Subclasses TCP and overrides methods that don't make sense for UDS."""

    @property
    def local_address(self) -> str:
        if self._local_addr.startswith("unix://"):
            return self._local_addr
        else:
            return f"unix://{self._local_addr}"

    @property
    def peer_address(self) -> str:
        return self.local_address

    @property
    def same_host(self):
        return True

    def _set_tcp_timeout(self, stream):
        return


class UnixSocketResolver(netutil.Resolver):
    """A resolver for Unix Domain Sockets. This is used by tornado to lookup hostnames. For UDS, this should always return socket type and pathname (instead of a real DNS lookup)."""

    async def resolve(
        self, host: str, port: int, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[tuple[int, str]]:
        return [(socket.AF_UNIX, host)]


class UDSListener(BaseListener):
    """A Listener for Unix Domain Sockets, based on the TCPListener class. Ensures the address is an absolute path instead of a hostname:port string."""

    prefix = "unix://"
    comm_class = UDS

    def __init__(
        self,
        address,
        comm_handler,
        deserialize=True,
        allow_offload=True,
        **connection_args,
    ):
        super().__init__()
        self.address = get_uds_path(address)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self.tcp_server = None

    async def _handle_stream(self, stream, address):
        if self.tcp_server is None:
            # stop() was called after the connection was accepted, but before this
            # method could run. abort_handshaking_comms() has already run and won't
            # take care of this comm; if we left the stream dangling, the client
            # would hang forever in the comm handshake, which is deliberately not
            # subject to timeouts (see distributed.comm.core.connect()).
            stream.close()
            return

        # for UDS the remote address is always the same as the local address
        logger.debug(f"Incoming connection to {self.address}")

        comm = self.comm_class(stream, self.address, self.address, self.deserialize)
        comm.allow_offload = self.allow_offload

        try:
            await self.on_connection(comm)
        except CommClosedError:
            logger.info(f"Connection to {address} closed before handshake completed")
            return

        await self.comm_handler(comm)

    async def start(self, **kwargs):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE, **kwargs)
        self.tcp_server.handle_stream = self._handle_stream
        # When shuffling data between workers, there can
        # really be O(cluster size) connection requests
        # on a single worker socket, make sure the backlog
        # is large enough not to lose any.
        backlog = int(dask.config.get("distributed.comm.socket-backlog"))
        socket = netutil.bind_unix_socket(
            self.address,
            mode=0o600,
            backlog=backlog,
        )
        self.tcp_server.add_socket(socket)

    @property
    def listen_address(self):
        """Return the listening address as a string."""
        return self.prefix + self.address

    @property
    def contact_address(self):
        """Return the contact address as a string."""
        return self.listen_address

    def stop(self):
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()
        if os.path.exists(self.address):
            try:
                os.remove(self.address)
            except OSError as e:
                logger.debug(
                    f"Attempted removal of socket at {self.address} failed with error: {e}"
                )


class UDSConnector(Connector):
    client: ClassVar[TCPClient] = TCPClient(resolver=UnixSocketResolver())

    prefix = "unix://"
    comm_class = UDS

    async def connect(self, address, deserialize=True, **kwargs):
        """Connect to a Unix domain socket."""
        try:
            stream = await self.client.connect(
                address, port=0, max_buffer_size=MAX_BUFFER_SIZE
            )
        except StreamClosedError as e:
            # The socket connect() call failed
            raise CommClosedError(f"in {self}: {e}") from e

        local_address = f"{self.prefix}{stream.socket.getpeername()}"
        return self.comm_class(
            stream, local_address, f"{self.prefix}{address}", deserialize
        )


class UDSBackend(Backend):
    """A Backend for Unix Domain Sockets. It overrides the TCP class's functions for parsing addresses, since UDS does not require port numbers."""

    _connector_class = UDSConnector
    _listener_class = UDSListener

    def get_connector(self):
        return self._connector_class()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return self._listener_class(loc, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        path = loc.split("unix://")[-1]
        if os.path.isabs(path):
            return path
        else:
            # something like `unix://127.0.0.1:0` was passed in
            # this happens when a cluster sets protocl to 'unix', but doesn't explicitly override the default host and port
            # in this case, return a new uds socket path
            return get_uds_path(path)

    def resolve_address(self, loc):
        return loc

    def get_local_address_for(self, loc):
        return loc
