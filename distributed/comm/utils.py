
import logging
import socket

from .. import protocol
from ..utils import get_ip, get_ipv6


logger = logging.getLogger(__name__)


def to_frames(msg):
    """
    """
    try:
        return list(protocol.dumps(msg))
    except Exception as e:
        logger.info("Unserializable Message: %s", msg)
        logger.exception(e)
        raise


def from_frames(frames, deserialize=True):
    """
    """
    return protocol.loads(frames, deserialize=deserialize)


def parse_host_port(address, default_port=None):
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address
    if address.startswith('tcp:'):
        address = address[4:]

    def _fail():
        raise ValueError("invalid address %r" % (address,))

    def _default():
        if default_port is None:
            raise ValueError("missing port number in address %r" % (address,))
        return default_port

    if address.startswith('['):
        host, sep, tail = address[1:].partition(']')
        if not sep:
            _fail()
        if not tail:
            port = _default()
        else:
            if not tail.startswith(':'):
                _fail()
            port = tail[1:]
    else:
        host, sep, port = address.partition(':')
        if not sep:
            port = _default()
        elif ':' in host:
            _fail()

    return host, int(port)


def unparse_host_port(host, port=None):
    """
    """
    if ':' in host and not host.startswith('['):
        host = '[%s]' % host
    if port:
        return '%s:%s' % (host, port)
    else:
        return host


def get_tcp_server_address(tcp_server):
    """
    Get the bound address of a started Tornado TCPServer.
    """
    sockets = list(tcp_server._sockets.values())
    if not sockets:
        raise RuntimeError("TCP Server %r not started yet?" % (tcp_server,))

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

    return sock.getsockname()


def ensure_concrete_host(host):
    """
    """
    if host in ('0.0.0.0', ''):
        return get_ip()
    elif host == '::':
        return get_ipv6()
    else:
        return host