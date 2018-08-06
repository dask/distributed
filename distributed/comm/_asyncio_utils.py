"""
Asyncio.start_server is a coroutine, which stops ``TCPListener.start`` from
being a normal function.  It turns out that the only reason it is a coroutine
is to get a bit of information asynchronously::

    async def _ensure_resolved(self, address, *,
                               family=0, type=socket.SOCK_STREAM,
                               proto=0, flags=0, loop):
        host, port = address[:2]
        info = _ipaddr_info(host, port, family, type, proto)
        if info is not None:
            # "host" is already a resolved IP.
            return [info]
        else:
            return await loop.getaddrinfo(host, port, family=family, type=type,
                                          proto=proto, flags=flags)


We replace this with socket.getaddrinfo and make the thing return immediately.
We also remove all sockets that aren't SOCK_STREAM type.
"""



import socket

from asyncio.streams import _DEFAULT_LIMIT
from asyncio.base_events import Server
from asyncio import events, coroutines, protocols
import os
import sys
import itertools

def create_server(self, protocol_factory, host=None, port=None,
                  *,
                  family=socket.AF_UNSPEC,
                  flags=socket.AI_PASSIVE,
                  sock=None,
                  backlog=100,
                  ssl=None,
                  reuse_address=None,
                  reuse_port=None):
    """Create a TCP server.

    The host parameter can be a string, in that case the TCP server is bound
    to host and port.

    The host parameter can also be a sequence of strings and in that case
    the TCP server is bound to all hosts of the sequence. If a host
    appears multiple times (possibly indirectly e.g. when hostnames
    resolve to the same IP address), the server is only bound once to that
    host.

    Return a Server object which can be used to stop the service.

    This method is a coroutine.
    """
    if isinstance(ssl, bool):
        raise TypeError('ssl argument must be an SSLContext or None')
    if host is not None or port is not None:
        if sock is not None:
            raise ValueError(
                'host/port and sock can not be specified at the same time')

        AF_INET6 = getattr(socket, 'AF_INET6', 0)
        if reuse_address is None:
            reuse_address = os.name == 'posix' and sys.platform != 'cygwin'
        sockets = []
        if host == '':
            hosts = [None]
        elif (isinstance(host, str) or
              not isinstance(host, collections.Iterable)):
            hosts = [host]
        else:
            hosts = host

        infos = [socket.getaddrinfo(host, port, family=family, flags=flags) for
                 host in hosts]
        infos = set(itertools.chain.from_iterable(infos))
        infos = [info for info in infos if info[1] == socket.SocketKind.SOCK_STREAM]

        completed = False
        try:
            for res in infos:
                af, socktype, proto, canonname, sa = res
                try:
                    sock = socket.socket(af, socktype, proto)
                except socket.error:
                    # Assume it's a bad family/type/protocol combination.
                    if self._debug:
                        logger.warning('create_server() failed to create '
                                       'socket.socket(%r, %r, %r)',
                                       af, socktype, proto, exc_info=True)
                    continue
                sockets.append(sock)
                if reuse_address:
                    sock.setsockopt(
                        socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                if reuse_port:
                    _set_reuseport(sock)
                # Disable IPv4/IPv6 dual stack support (enabled by
                # default on Linux) which makes a single socket
                # listen on both address families.
                if af == AF_INET6 and hasattr(socket, 'IPPROTO_IPV6'):
                    sock.setsockopt(socket.IPPROTO_IPV6,
                                    socket.IPV6_V6ONLY,
                                    True)
                try:
                    sock.bind(sa)
                except OSError as err:
                    raise OSError(err.errno, 'error while attempting '
                                  'to bind on address %r: %s'
                                  % (sa, err.strerror.lower()))
            completed = True
        finally:
            if not completed:
                for sock in sockets:
                    sock.close()
    else:
        if sock is None:
            raise ValueError('Neither host/port nor sock were specified')
        if not _is_stream_socket(sock):
            raise ValueError(
                'A Stream Socket was expected, got {!r}'.format(sock))
        sockets = [sock]

    server = Server(self, sockets)
    for sock in sockets:
        sock.listen(backlog)
        sock.setblocking(False)
        self._start_serving(protocol_factory, sock, ssl, server, backlog)
    if self._debug:
        logger.info("%r is serving", server)
    return server


def start_server(client_connected_cb, host=None, port=None, *,
                 loop=None, limit=_DEFAULT_LIMIT, **kwds):
    """Start a socket server, call back for each client connected.

    The first parameter, `client_connected_cb`, takes two parameters:
    client_reader, client_writer.  client_reader is a StreamReader
    object, while client_writer is a StreamWriter object.  This
    parameter can either be a plain callback function or a coroutine;
    if it is a coroutine, it will be automatically converted into a
    Task.

    The rest of the arguments are all the usual arguments to
    loop.create_server() except protocol_factory; most common are
    positional host and port, with various optional keyword arguments
    following.  The return value is the same as loop.create_server().

    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).

    The return value is the same as loop.create_server(), i.e. a
    Server object which can be used to stop the service.
    """
    if loop is None:
        loop = events.get_event_loop()

    def factory():
        reader = StreamReader(limit=limit, loop=loop)
        protocol = StreamReaderProtocol(reader, client_connected_cb,
                                        loop=loop)
        return protocol

    return create_server(loop, factory, host, port, **kwds)
