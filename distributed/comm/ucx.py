"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communications` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import functools
import logging
import os
import struct
import warnings
import weakref
from typing import TYPE_CHECKING

import dask
from dask.utils import parse_bytes

from distributed.comm.addressing import parse_host_port, unparse_host_port
from distributed.comm.core import Comm, CommClosedError, Connector, Listener
from distributed.comm.registry import Backend, backends
from distributed.comm.utils import (
    ensure_concrete_host,
    from_frames,
    host_array,
    to_frames,
)
from distributed.diagnostics.nvml import has_cuda_context
from distributed.utils import ensure_ip, get_ip, get_ipv6, log_errors, nbytes

logger = logging.getLogger(__name__)

# In order to avoid double init when forking/spawning new processes (multiprocess),
# we make sure only to import and initialize UCX once at first use. This is also
# required to ensure Dask configuration gets propagated to UCX, which needs
# variables to be set before being imported.
if TYPE_CHECKING:
    try:
        import ucp
    except ImportError:
        pass
else:
    ucp = None

device_array = None
pre_existing_cuda_context = False
cuda_context_created = False


_warning_suffix = (
    "This is often the result of a CUDA-enabled library calling a CUDA runtime function before "
    "Dask-CUDA can spawn worker processes. Please make sure any such function calls don't happen "
    "at import time or in the global scope of a program."
)


def _warn_existing_cuda_context(ctx, pid):
    warnings.warn(
        f"A CUDA context for device {ctx} already exists on process ID {pid}. {_warning_suffix}"
    )


def _warn_cuda_context_wrong_device(ctx_expected, ctx_actual, pid):
    warnings.warn(
        f"Worker with process ID {pid} should have a CUDA context assigned to device "
        f"{ctx_expected}, but instead the CUDA context is on device {ctx_actual}. "
        f"{_warning_suffix}"
    )


def synchronize_stream(stream=0):
    import numba.cuda

    ctx = numba.cuda.current_context()
    cu_stream = numba.cuda.driver.drvapi.cu_stream(stream)
    stream = numba.cuda.driver.Stream(ctx, cu_stream, None)
    stream.synchronize()


def init_once():
    global ucp, device_array
    global ucx_create_endpoint, ucx_create_listener
    global pre_existing_cuda_context, cuda_context_created

    if ucp is not None:
        return

    # remove/process dask.ucx flags for valid ucx options
    ucx_config = _scrub_ucx_config()

    # We ensure the CUDA context is created before initializing UCX. This can't
    # be safely handled externally because communications in Dask start before
    # preload scripts run.
    if dask.config.get("distributed.comm.ucx.create-cuda-context") is True or (
        "TLS" in ucx_config and "cuda_copy" in ucx_config["TLS"]
    ):
        try:
            import numba.cuda
        except ImportError:
            raise ImportError(
                "CUDA support with UCX requires Numba for context management"
            )

        cuda_visible_device = int(
            os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")[0]
        )
        pre_existing_cuda_context = has_cuda_context()
        if pre_existing_cuda_context is not False:
            _warn_existing_cuda_context(pre_existing_cuda_context, os.getpid())

        numba.cuda.current_context()

        cuda_context_created = has_cuda_context()
        if (
            cuda_context_created is not False
            and cuda_context_created != cuda_visible_device
        ):
            _warn_cuda_context_wrong_device(
                cuda_visible_device, cuda_context_created, os.getpid()
            )

    import ucp as _ucp

    ucp = _ucp

    ucp.init(options=ucx_config, env_takes_precedence=True)

    # Find the function, `cuda_array()`, to use when allocating new CUDA arrays
    try:
        import rmm

        device_array = lambda n: rmm.DeviceBuffer(size=n)
    except ImportError:
        try:
            import numba.cuda

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

    pool_size_str = dask.config.get("distributed.rmm.pool-size")
    if pool_size_str is not None:
        pool_size = parse_bytes(pool_size_str)
        rmm.reinitialize(
            pool_allocator=True, managed_memory=False, initial_pool_size=pool_size
        )


def _close_comm(ref):
    """Callback to close Dask Comm when UCX Endpoint closes or errors

    Parameters
    ----------
        ref: weak reference to a Dask UCX comm
    """
    comm = ref()
    if comm is not None:
        comm._closed = True


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

    1. Read the frame describing if connection is closing and number of frames
    2. Read the frame describing whether each data frame is gpu-bound
    3. Read the frame describing whether each data frame is sized
    4. Read all the data frames.
    """

    def __init__(self, ep, local_addr: str, peer_addr: str, deserialize: bool = True):
        super().__init__(deserialize=deserialize)
        self._ep = ep
        if local_addr:
            assert local_addr.startswith("ucx")
        assert peer_addr.startswith("ucx")
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.comm_flag = None

        # When the UCX endpoint closes or errors the registered callback
        # is called.
        if hasattr(self._ep, "set_close_callback"):
            ref = weakref.ref(self)
            self._ep.set_close_callback(functools.partial(_close_comm, ref))
            self._closed = False
            self._has_close_callback = True
        else:
            self._has_close_callback = False

        logger.debug("UCX.__init__ %s", self)

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return self._peer_addr

    @log_errors
    async def write(
        self,
        msg: dict,
        serializers=("cuda", "dask", "pickle", "error"),
        on_error: str = "message",
    ):
        if self.closed():
            raise CommClosedError("Endpoint is closed -- unable to send message")
        try:
            if serializers is None:
                serializers = ("cuda", "dask", "pickle", "error")
            # msg can also be a list of dicts when sending batched messages
            frames = await to_frames(
                msg,
                serializers=serializers,
                on_error=on_error,
                allow_offload=self.allow_offload,
            )
            nframes = len(frames)
            cuda_frames = tuple(hasattr(f, "__cuda_array_interface__") for f in frames)
            sizes = tuple(nbytes(f) for f in frames)
            cuda_send_frames, send_frames = zip(
                *(
                    (is_cuda, each_frame)
                    for is_cuda, each_frame in zip(cuda_frames, frames)
                    if nbytes(each_frame) > 0
                )
            )

            # Send meta data

            # Send close flag and number of frames (_Bool, int64)
            await self.ep.send(struct.pack("?Q", False, nframes))
            # Send which frames are CUDA (bool) and
            # how large each frame is (uint64)
            await self.ep.send(
                struct.pack(nframes * "?" + nframes * "Q", *cuda_frames, *sizes)
            )

            # Send frames

            # It is necessary to first synchronize the default stream before start
            # sending We synchronize the default stream because UCX is not
            # stream-ordered and syncing the default stream will wait for other
            # non-blocking CUDA streams. Note this is only sufficient if the memory
            # being sent is not currently in use on non-blocking CUDA streams.
            if any(cuda_send_frames):
                synchronize_stream(0)

            for each_frame in send_frames:
                await self.ep.send(each_frame)
            return sum(sizes)
        except (ucp.exceptions.UCXBaseException):
            self.abort()
            raise CommClosedError("While writing, the connection was closed")

    @log_errors
    async def read(self, deserializers=("cuda", "dask", "pickle", "error")):
        if deserializers is None:
            deserializers = ("cuda", "dask", "pickle", "error")

        try:
            # Recv meta data

            # Recv close flag and number of frames (_Bool, int64)
            msg = host_array(struct.calcsize("?Q"))
            await self.ep.recv(msg)
            (shutdown, nframes) = struct.unpack("?Q", msg)

            if shutdown:  # The writer is closing the connection
                raise CommClosedError("Connection closed by writer")

            # Recv which frames are CUDA (bool) and
            # how large each frame is (uint64)
            header_fmt = nframes * "?" + nframes * "Q"
            header = host_array(struct.calcsize(header_fmt))
            await self.ep.recv(header)
            header = struct.unpack(header_fmt, header)
            cuda_frames, sizes = header[:nframes], header[nframes:]
        except (
            ucp.exceptions.UCXCloseError,
            ucp.exceptions.UCXCanceled,
        ) + (getattr(ucp.exceptions, "UCXConnectionReset", ()),):
            self.abort()
            raise CommClosedError("Connection closed by writer")
        else:
            # Recv frames
            frames = [
                device_array(each_size) if is_cuda else host_array(each_size)
                for is_cuda, each_size in zip(cuda_frames, sizes)
            ]
            cuda_recv_frames, recv_frames = zip(
                *(
                    (is_cuda, each_frame)
                    for is_cuda, each_frame in zip(cuda_frames, frames)
                    if nbytes(each_frame) > 0
                )
            )

            # It is necessary to first populate `frames` with CUDA arrays and synchronize
            # the default stream before starting receiving to ensure buffers have been allocated
            if any(cuda_recv_frames):
                synchronize_stream(0)

            for each_frame in recv_frames:
                await self.ep.recv(each_frame)
            msg = await from_frames(
                frames,
                deserialize=self.deserialize,
                deserializers=deserializers,
                allow_offload=self.allow_offload,
            )
            return msg

    async def close(self):
        self._closed = True
        if self._ep is not None:
            try:
                await self.ep.send(struct.pack("?Q", True, 0))
            except (
                ucp.exceptions.UCXError,
                ucp.exceptions.UCXCloseError,
                ucp.exceptions.UCXCanceled,
            ) + (getattr(ucp.exceptions, "UCXConnectionReset", ()),):
                # If the other end is in the process of closing,
                # UCX will sometimes raise a `Input/output` error,
                # which we can ignore.
                pass
            self.abort()
            self._ep = None

    def abort(self):
        self._closed = True
        if self._ep is not None:
            self._ep.abort()
            self._ep = None

    @property
    def ep(self):
        if self._ep is not None:
            return self._ep
        else:
            raise CommClosedError("UCX Endpoint is closed")

    def closed(self):
        if self._has_close_callback is True:
            # The self._closed flag is separate from the endpoint's lifetime, even when
            # the endpoint has closed or errored, there may be messages on its buffer
            # still to be received, even though sending is not possible anymore.
            return self._closed
        else:
            return self._ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    async def connect(self, address: str, deserialize=True, **connection_args) -> UCX:
        logger.debug("UCXConnector.connect: %s", address)
        ip, port = parse_host_port(address)
        init_once()
        try:
            ep = await ucp.create_endpoint(ip, port)
        except (ucp.exceptions.UCXCloseError, ucp.exceptions.UCXCanceled,) + (
            getattr(ucp.exceptions, "UCXConnectionReset", ()),
            getattr(ucp.exceptions, "UCXNotConnected", ()),
            getattr(ucp.exceptions, "UCXUnreachable", ()),
        ):  # type: ignore
            raise CommClosedError("Connection closed before handshake completed")
        return self.comm_class(
            ep,
            local_addr="",
            peer_addr=self.prefix + address,
            deserialize=deserialize,
        )


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(
        self,
        address: str,
        comm_handler: None,
        deserialize=False,
        allow_offload=True,
        **connection_args,
    ):
        if not address.startswith("ucx"):
            address = "ucx://" + address
        self.ip, self._input_port = parse_host_port(address, default_port=0)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.allow_offload = allow_offload
        self._ep = None  # type: ucp.Endpoint
        self.ucp_server = None
        self.connection_args = connection_args

    @property
    def port(self):
        return self.ucp_server.port

    @property
    def address(self):
        return "ucx://" + self.ip + ":" + str(self.port)

    async def start(self):
        async def serve_forever(client_ep):
            ucx = UCX(
                client_ep,
                local_addr=self.address,
                peer_addr=self.address,
                deserialize=self.deserialize,
            )
            ucx.allow_offload = self.allow_offload
            try:
                await self.on_connection(ucx)
            except CommClosedError:
                logger.debug("Connection closed before handshake completed")
                return
            if self.comm_handler:
                await self.comm_handler(ucx)

        init_once()
        self.ucp_server = ucp.create_listener(serve_forever, port=self._input_port)

    def stop(self):
        self.ucp_server = None

    def get_host_port(self):
        # TODO: TCP raises if this hasn't started yet.
        return self.ip, self.port

    @property
    def listen_address(self):
        return self.prefix + unparse_host_port(*self.get_host_port())

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


backends["ucx"] = UCXBackend()


def _scrub_ucx_config():
    """Function to scrub dask config options for valid UCX config options"""

    # configuration of UCX can happen in two ways:
    # 1) high level on/off flags which correspond to UCX configuration
    # 2) explicitly defined UCX configuration flags

    # import does not initialize ucp -- this will occur outside this function
    from ucp import get_config

    options = {}

    # if any of the high level flags are set, as long as they are not Null/None,
    # we assume we should configure basic TLS settings for UCX, otherwise we
    # leave UCX to its default configuration
    if any(
        [
            dask.config.get("distributed.comm.ucx.tcp"),
            dask.config.get("distributed.comm.ucx.nvlink"),
            dask.config.get("distributed.comm.ucx.infiniband"),
        ]
    ):
        if dask.config.get("distributed.comm.ucx.rdmacm"):
            tls = "tcp"
            tls_priority = "rdmacm"
        else:
            tls = "tcp"
            tls_priority = "tcp"

        # CUDA COPY can optionally be used with ucx -- we rely on the user
        # to define when messages will include CUDA objects.  Note:
        # defining only the Infiniband flag will not enable cuda_copy
        if any(
            [
                dask.config.get("distributed.comm.ucx.nvlink"),
                dask.config.get("distributed.comm.ucx.cuda-copy"),
            ]
        ):
            tls = tls + ",cuda_copy"

        if dask.config.get("distributed.comm.ucx.infiniband"):
            tls = "rc," + tls
        if dask.config.get("distributed.comm.ucx.nvlink"):
            tls = tls + ",cuda_ipc"

        options = {"TLS": tls, "SOCKADDR_TLS_PRIORITY": tls_priority}

    # ANY UCX options defined in config will overwrite high level dask.ucx flags
    valid_ucx_vars = list(get_config().keys())
    for k, v in options.items():
        if k not in valid_ucx_vars:
            logger.debug(
                f"Key: {k} with value: {v} not a valid UCX configuration option"
            )

    return options
