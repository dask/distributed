from __future__ import annotations

import logging
import os
import socket
from typing import ClassVar

import tornado.netutil as netutil
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer

import dask

from distributed.comm.tcp import (
    MAX_BUFFER_SIZE,
    TCP,
    BaseTCPBackend,
    TCPConnector,
    TCPListener,
)
from distributed.utils import (
    get_uds_path,
)

logger = logging.getLogger(__name__)


class UnixSocketResolver(netutil.Resolver):
    """A resolver for Unix Domain Sockets. This is used by tornado to lookup hostnames. For UDS, this should always return socket type and pathname (instead of a real DNS lookup)."""

    async def resolve(
        self, host: str, port: int, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[tuple[int, str]]:
        return [(socket.AF_UNIX, host)]


class UDSListener(TCPListener):
    """A Listener for Unix Domain Sockets, based on the TCPListener class. Ensures the address is an absolute path instead of a hostname:port string."""

    prefix = "unix://"
    comm_class = TCP

    def __init__(
        self,
        address,
        *args,
        **kwargs,
    ):
        path = get_uds_path(address)
        if ":" not in path:
            path = f"{path}:0"
        super().__init__(path, *args, **kwargs)  # fake port 0

    def get_host_port(self):
        """
        The listening address as a tuple. Port is always 0.
        """
        self._check_started()

        if self.bound_address is None:
            self.bound_address = self.tcp_server._sockets[0].getsockname()
        return (self.bound_address, 0)  # fake port 0

    async def _handle_stream(self, stream, address):
        """
        We override the super class's _handle_stream to pass in 0 as a fake port (it will be ignored anyway).
        """
        return await super()._handle_stream(stream, (self.bound_address, 0))

    async def start(self):
        self.tcp_server = TCPServer(max_buffer_size=MAX_BUFFER_SIZE, **self.server_args)
        self.tcp_server.handle_stream = self._handle_stream
        # When shuffling data between workers, there can
        # really be O(cluster size) connection requests
        # on a single worker socket, make sure the backlog
        # is large enough not to lose any.
        backlog = int(dask.config.get("distributed.comm.socket-backlog"))
        socket = netutil.bind_unix_socket(
            self.ip,  # self.ip is actually the path to the socket
            mode=0o600,
            backlog=backlog,
        )
        self.tcp_server.add_socket(socket)
        self.bound_address = self.ip  # ip is path to unix socket

    def stop(self):
        super().stop()
        if os.path.exists(self.bound_address):
            try:
                os.remove(self.bound_address)
            except OSError as e:
                logger.debug(
                    f"Attempted removal of socket at {self.bound_address} failed with error: {e}"
                )


class UDSConnector(TCPConnector):
    client: ClassVar[TCPClient] = TCPClient(resolver=UnixSocketResolver())

    prefix = "unix://"
    comm_class = TCP


class UDSBackend(BaseTCPBackend):
    """A Backend for Unix Domain Sockets. It overrides the TCP class's functions for parsing addresses, since UDS does not require port numbers."""

    _connector_class = UDSConnector
    _listener_class = UDSListener

    def get_address_host(self, loc):
        return loc.split(":")[0]

    def resolve_address(self, loc):
        return loc
