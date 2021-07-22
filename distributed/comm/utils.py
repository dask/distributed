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

def get_array_types():
    """
    Find the correct library and object type for declaring new host and
    device arrays.
    """
    # Find the function, `host_array()`, to use when allocating new host arrays
    try:
        import numpy

        host_array = lambda n: numpy.empty((n,), dtype="u1")
    except ImportError:
        host_array = lambda n: bytearray(n)

    # Find the function, `cuda_array()`, to use when allocating new CUDA arrays
    try:
        import rmm

        if hasattr(rmm, "DeviceBuffer"):
            device_array = lambda n: rmm.DeviceBuffer(size=n)
        else:  # pre-0.11.0
            import numba.cuda
            import weakref

            def rmm_device_array(n):
                a = rmm.device_array(n, dtype="u1")
                weakref.finalize(a, numba.cuda.current_context)
                return a

            device_array = rmm_device_array
    except ImportError:
        try:
            import numba.cuda
            import weakref

            def numba_device_array(n):
                a = numba.cuda.device_array((n,), dtype="u1")
                weakref.finalize(a, numba.cuda.current_context)
                return a

            device_array = numba_device_array
        except ImportError:
            def device_array(n):
                raise RuntimeError(
                    "In order to send/recv CUDA arrays, Numba or RMM is required"
                )

    return host_array, device_array

def init_rmm_pool():
    """
    Initialize an RMM pool based on Dask configuration parameters.
    """
    try:
        import rmm

        pool_size_str = dask.config.get("rmm.pool-size")
        if pool_size_str is not None:
            pool_size = parse_bytes(pool_size_str)
            rmm.reinitialize(
                pool_allocator=True, managed_memory=False, initial_pool_size=pool_size
            )
    except ImportError:
        raise RuntimeError(
            "RMM import failed.  RMM library is required to create an RMM pool."
        )
