"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communcations` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import asyncio
import itertools
import logging
import struct

from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .registry import Backend, backends
from .utils import ensure_concrete_host, to_frames, from_frames
from ..utils import ensure_ip, get_ip, get_ipv6, nbytes

import ucp

import os

os.environ.setdefault("UCX_RNDV_SCHEME", "put_zcopy")
os.environ.setdefault("UCX_MEMTYPE_CACHE", "n")
os.environ.setdefault("UCX_TLS", "rc,cuda_copy")

logger = logging.getLogger(__name__)
MAX_MSG_LOG = 23
PORT = 13337

# set in ~/.dask/config.yaml
# or DASK_DISTRIBUTED__COMM__UCXADDRESS
_PORT_COUNTER = itertools.count(PORT)

_INITIALIZED = False


def _ucp_init():
    global _INITIALIZED

    if not _INITIALIZED:
        ucp.init()
        _INITIALIZED = True


# ----------------------------------------------------------------------------
# Addressing
# TODO: Parts of these should probably be moved to `comm/addressing.py`
# ----------------------------------------------------------------------------


def _parse_address(addr: str, strict=False) -> tuple:
    """
    >>> _parse_address("ucx://10.33.225.160")
    """
    if not addr.startswith("ucx://"):
        raise ValueError("Invalid url scheme {}".format(addr))

    proto, address = addr.split("://", 1)
    return proto, address


def _parse_host_port(address: str, default_port=None) -> tuple:
    """
    Parse an endpoint address given in the form "host:port".

    >>> _parse_host_port("10.33.225.160:13337")
    ("10.33.225.160", 13337)
    """
    if address.startswith("ucx://"):
        _, address = _parse_address(address)

    # if default port is None we select the next port availabe
    # ucx-py does not currently support random port assignment
    import random

    default_port = default_port or random.randint(1024, 65000)
    return parse_host_port(address, default_port=default_port)


def _unparse_host_port(host, port=None):
    return unparse_host_port(host, port)


def get_endpoint_address(endpoint):
    # TODO: ucx-py: 18
    pass


# ----------------------------------------------------------------------------
# Comm Interface
# ----------------------------------------------------------------------------


class UCX(Comm):
    """Comm object using UCP.

    Parameters
    ----------
    ep : ucp.Endpoint
        The UCP endpoint.
    address : str
        The address, prefixed with `ucx://` to use.
    deserialize : bool, default True
        Whether to deserialize data in :meth:`distributed.protocol.loads`

    Notes
    -----
    The read-write cycle uses the following pattern:

    Each msg is serialized into a number of "data" frames. We prepend these
    real frames with two additional frames

        1. is_gpus: Boolean indicator for whether the frame should be
           received into GPU memory. Packed in '?' format. Unpack with
           ``<n_frames>?`` format.
        2. frame_size : Unsigned int describing the size of frame (in bytes)
           to receive. Packed in 'Q' format, so a length-0 frame is equivalent
           to an unsized frame. Unpacked with ``<n_frames>Q``.

    The expected read cycle is

    1. Read the frame describing number of frames
    2. Read the frame describing whether each data frame is gpu-bound
    3. Read the frame describing whether each data frame is sized
    4. Read all the data frames.
    """

    def __init__(
        self, ep: ucp.Endpoint, address: str, listener_instance, deserialize=True
    ):
        logger.debug("UCX.__init__ %s %s", address, listener_instance)
        self._ep = ep
        assert address.startswith("ucx")
        self.address = address
        self.listener_instance = listener_instance
        default_port = next(_PORT_COUNTER)
        self._host, self._port = _parse_host_port(address, default_port)
        self._local_addr = None
        self.deserialize = deserialize
        self.comm_flag = None

        # finalizer?

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        # XXX: This isn't quite for the server (from UCXListener).
        # We need the port? Or the tag?
        return self.address

    async def write(self, msg: dict, serializers=None, on_error: str = "message"):
        # msg can also be a list of dicts when sending batched messages
        frames = await to_frames(
            msg, serializers=serializers, on_error=on_error
        )  # TODO: context=
        gpu_frames = b"".join(
            [
                struct.pack("?", hasattr(frame, "__cuda_array_interface__"))
                for frame in frames
            ]
        )
        size_frames = b"".join([struct.pack("Q", nbytes(frame)) for frame in frames])

        nframes = struct.pack("Q", len(frames))

        meta = b"".join([nframes, gpu_frames, size_frames])

        await self.ep.send_obj(meta)

        for frame in frames:
            await self.ep.send_obj(frame)
        return sum(map(nbytes, frames))

    async def read(self, deserializers=None):
        resp = await self.ep.recv_future()
        obj = ucp.get_obj_from_msg(resp)
        nframes, = struct.unpack("Q", obj[:8])  # first eight bytes for number of frames

        gpu_frame_msg = obj[8 : 8 + nframes]  # next nframes bytes for if they're GPU frames
        is_gpus = struct.unpack("{}?".format(nframes), gpu_frame_msg)

        sized_frame_msg = obj[8 + nframes :]  # then the rest for frame sizes
        sizes = struct.unpack("{}Q".format(nframes), sized_frame_msg)

        frames = []

        for i, (is_gpus, size) in enumerate(zip(is_gpus, sizes)):
            if size > 0:
                resp = await self.ep.recv_obj(size, cuda=is_gpus)
            else:
                resp = await self.ep.recv_future()
            frame = ucp.get_obj_from_msg(resp)
            frames.append(frame)

        msg = await from_frames(
            frames, deserialize=self.deserialize, deserializers=deserializers
        )

        return msg

    def abort(self):
        # breakpoint()
        if self._ep:
            ucp.destroy_ep(self._ep)
            logger.debug("Destroyed UCX endpoint")
            self._ep = None
        # if self.listener_instance:
        # ucp.stop_listener(self.listener_instance)
        # self.listener_instance = None

    @property
    def ep(self):
        if self._ep:
            return self._ep
        else:
            raise CommClosedError("UCX Endpoint is closed")

    async def close(self):
        # TODO: Handle in-flight messages?
        # sleep is currently used to help flush buffer
        self.abort()

    def closed(self):
        return self._ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    client = ...  # TODO: add a client here?

    async def connect(self, address: str, deserialize=True, **connection_args) -> UCX:
        logger.debug("UCXConnector.connect: %s", address)
        _ucp_init()
        ip, port = _parse_host_port(address)
        ep = ucp.get_endpoint(ip.encode(), port)
        return self.comm_class(
            ep, self.prefix + address, listener_instance=None, deserialize=deserialize
        )


class UCXListener(Listener):
    # MAX_LISTENERS 256 in ucx-py
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(
        self, address: str, comm_handler: None, deserialize=False, **connection_args
    ):
        logger.debug("UCXListener.__init__")
        if not address.startswith("ucx"):
            address = "ucx://" + address
        self.address = address
        self.ip, self.port = _parse_host_port(address, default_port=0)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self._ep = None  # type: ucp.Endpoint
        self.listener_instance = None  # type: ucp.ListenerFuture
        self._task = None

        # XXX: The init may be required to take args like
        # {'require_encryption': None, 'ssl_context': None}
        self.connection_args = connection_args
        self._task = None

    def start(self):
        async def serve_forever(client_ep, listener_instance):
            ucx = UCX(
                client_ep, self.address, listener_instance, deserialize=self.deserialize
            )
            self.listener_instance = listener_instance
            if self.comm_handler:
                await self.comm_handler(ucx)

        _ucp_init()
        # XXX: the port handling is probably incorrect.
        # need to figure out if `server_port=None` is
        # server_port=13337, or server_port="next free port"
        server = ucp.start_listener(
            serve_forever, listener_port=self.port, is_coroutine=True
        )

        try:
            loop = asyncio.get_running_loop()
        except (RuntimeError, AttributeError):
            loop = asyncio.get_event_loop()

        t = loop.create_task(server.coroutine)
        self._task = t

    def stop(self):
        # What all should this do?
        if self._task:
            self._task.cancel()

        if self._ep:
            ucp.destroy_ep(self._ep)
        # if self.listener_instance:
        #   ucp.stop_listener(self.listener_instance)

    def get_host_port(self):
        # TODO: TCP raises if this hasn't started yet.
        return self.ip, self.port

    @property
    def listen_address(self):
        return self.prefix + _unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)  # TODO: ensure_concrete_host
        return self.prefix + unparse_host_port(host, port)

    @property
    def bound_address(self):
        # TODO: Does this become part of the base API? Kinda hazy, since
        # we exclude in for inproc.
        return self.get_host_port()


class UCXBackend(Backend):
    # I / O

    def get_connector(self):
        return UCXConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return UCXListener(loc, handle_comm, deserialize, **connection_args)

    # Address handling
    # This duplicates BaseTCPBackend

    def get_address_host(self, loc):
        return _parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return _parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return _unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends["ucx"] = UCXBackend()
