import logging
import math
import socket

import dask
from dask.sizeof import sizeof
from dask.utils import parse_bytes

from .. import protocol
from ..utils import get_ip, get_ipv6, nbytes, offload

logger = logging.getLogger(__name__)


# Offload (de)serializing large frames to improve event loop responsiveness.
OFFLOAD_THRESHOLD = dask.config.get("distributed.comm.offload")
if isinstance(OFFLOAD_THRESHOLD, str):
    OFFLOAD_THRESHOLD = parse_bytes(OFFLOAD_THRESHOLD)


async def to_frames(
    msg, serializers=None, on_error="message", context=None, allow_offload=True
):
    """
    Serialize a message into a list of Distributed protocol frames.
    """

    def _to_frames():
        try:
            return list(
                protocol.dumps(
                    msg, serializers=serializers, on_error=on_error, context=context
                )
            )
        except Exception as e:
            logger.info("Unserializable Message: %s", msg)
            logger.exception(e)
            raise

    if OFFLOAD_THRESHOLD and allow_offload:
        try:
            msg_size = sizeof(msg)
        except RecursionError:
            msg_size = math.inf
    else:
        msg_size = 0

    if allow_offload and OFFLOAD_THRESHOLD and msg_size > OFFLOAD_THRESHOLD:
        return await offload(_to_frames)
    else:
        return _to_frames()


async def from_frames(frames, deserialize=True, deserializers=None, allow_offload=True):
    """
    Unserialize a list of Distributed protocol frames.
    """
    size = False

    def _from_frames():
        try:
            return protocol.loads(
                frames, deserialize=deserialize, deserializers=deserializers
            )
        except EOFError:
            if size > 1000:
                datastr = "[too large to display]"
            else:
                datastr = frames
            # Aid diagnosing
            logger.error("truncated data stream (%d bytes): %s", size, datastr)
            raise

    if allow_offload and deserialize and OFFLOAD_THRESHOLD:
        size = sum(map(nbytes, frames))
    if allow_offload and deserialize and OFFLOAD_THRESHOLD and size > OFFLOAD_THRESHOLD:
        res = await offload(_from_frames)
    else:
        res = _from_frames()

    return res


def get_tcp_server_addresses(tcp_server):
    """
    Get all bound addresses of a started Tornado TCPServer.
    """
    sockets = list(tcp_server._sockets.values())
    if not sockets:
        raise RuntimeError("TCP Server %r not started yet?" % (tcp_server,))

    def _look_for_family(fam):
        socks = []
        for sock in sockets:
            if sock.family == fam:
                socks.append(sock)
        return socks

    # If listening on both IPv4 and IPv6, prefer IPv4 as defective IPv6
    # is common (e.g. Travis-CI).
    socks = _look_for_family(socket.AF_INET)
    if not socks:
        socks = _look_for_family(socket.AF_INET6)
    if not socks:
        raise RuntimeError("No Internet socket found on TCPServer??")

    return [sock.getsockname() for sock in socks]


def get_tcp_server_address(tcp_server):
    """
    Get the first bound address of a started Tornado TCPServer.
    """
    return get_tcp_server_addresses(tcp_server)[0]


def ensure_concrete_host(host):
    """
    Ensure the given host string (or IP) denotes a concrete host, not a
    wildcard listening address.
    """
    if host in ("0.0.0.0", ""):
        return get_ip()
    elif host == "::":
        return get_ipv6()
    else:
        return host
