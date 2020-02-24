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
# We use at most 4 threads to allow for parallel processing of large messages.

FRAME_OFFLOAD_THRESHOLD = dask.config.get("distributed.comm.offload")
if isinstance(FRAME_OFFLOAD_THRESHOLD, str):
    FRAME_OFFLOAD_THRESHOLD = parse_bytes(FRAME_OFFLOAD_THRESHOLD)


async def to_frames(msg, serializers=None, on_error="message", context=None):
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

    try:
        msg_size = sizeof(msg)
    except RecursionError:
        msg_size = math.inf

    if FRAME_OFFLOAD_THRESHOLD and msg_size > FRAME_OFFLOAD_THRESHOLD:
        return await offload(_to_frames)
    else:
        return _to_frames()


async def from_frames(frames, deserialize=True, deserializers=None):
    """
    Unserialize a list of Distributed protocol frames.
    """
    size = sum(map(nbytes, frames))

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

    if deserialize and FRAME_OFFLOAD_THRESHOLD and size > FRAME_OFFLOAD_THRESHOLD:
        res = await offload(_from_frames)
    else:
        res = _from_frames()

    return res


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
    Ensure the given host string (or IP) denotes a concrete host, not a
    wildcard listening address.
    """
    if host in ("0.0.0.0", ""):
        return get_ip()
    elif host == "::":
        return get_ipv6()
    else:
        return host


def _srub_ucx_config():
    """Function to scrub dask config options for valid UCX config options"""

    # configuration of UCX can happen in two ways:
    # 1) high level on/off flags which correspond to UCX configuration
    # 2) explicity defined UCX configuration flags

    # import does not initialize ucp -- this will occur outside this function
    from ucp import get_config

    options = {}

    # if any of the high level flags are set, as long as they are not Null/None,
    # we assume we should configure basic TLS settings for UCX
    if any(
        [
            dask.config.get("ucx.nvlink"),
            dask.config.get("ucx.infiniband"),
            dask.config.get("ucx.tcp-over-ucx"),
            dask.config.get("ucx.net-devices"),
        ]
    ):
        tls = "tcp,sockcm,cuda_copy"
        tls_priority = "sockcm"

        if dask.config.get("ucx.infiniband"):
            tls = "rc," + tls
        if dask.config.get("ucx.nvlink"):
            tls = tls + ",cuda_ipc"

        options = {"TLS": tls, "SOCKADDR_TLS_PRIORITY": tls_priority}

        net_devices = dask.config.get("ucx.net-devices")
        if net_devices is not None and net_devices != "":
            options["NET_DEVICES"] = net_devices

    # ANY UCX options defined in config will overwrite high level dask.ucx flags
    valid_ucx_keys = list(get_config().keys())
    for k, v in dask.config.get("ucx").items():
        if k in valid_ucx_keys:
            options[k] = v
        else:
            logger.debug(
                "Key: %s with value: %s not a valid UCX configuration option" % (k, v)
            )

    return options
