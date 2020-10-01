import asyncio  # for sleep
import logging
import threading

from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError

from ..protocol import nested_deserialize
from ..utils import get_ip, get_ipv6, ensure_ip

from tornado.ioloop import IOLoop


# MPI communication layer.
#
# Host addresses are of the form "mpi://host ip/rank", e.g. "mpi:/10.0.0.10/4",
# where rank is the MPI rank.  The host IPs are not required for MPI, but their
# use is consistent with the other communication layers and can help with
# debugging.
#
# The default project logging is overridden in this file such that each MPI
# rank logs to a separate file, e.g. "rank2.log" for rank 2, which is useful for
# debugging the setup and teardown of the communication layer.  But it is slow.


# MPI communicator and rank.
from mpi4py import MPI

_mpi_comm = MPI.COMM_WORLD
_mpi_rank = _mpi_comm.Get_rank()


# logger = logging.getLogger(__name__)

logging.basicConfig(
    filename=f"rank{_mpi_rank}.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mpi")


# To identify the completion of asynchronous MPI sends and receives, need to
# keep polling them.  This is the (minimum) time between polls, in seconds.
_polling_time = 0.005


class AsyncMPIRecv:

    def __init__(self, source, tag):
        # source of None means receive from any rank.
        self._source = source if source is not None else MPI.ANY_SOURCE
        self._tag = tag
        self._cancel = False
        logger.debug(f"AsyncMPIRecv.__init__ source={self._source} tag={tag}")

    def __await__(self):
        while True:
            if self._cancel:
                return None, None
            if _mpi_comm.iprobe(source=self._source, tag=self._tag):
                status = MPI.Status()
                msg = _mpi_comm.recv(source=self._source, tag=self._tag, status=status)
                return msg, status.Get_source()
            yield from asyncio.sleep(_polling_time).__await__()

    def cancel(self):
        self._cancel = True


class AsyncMPISend:

    def __init__(self, msg, target, tag):
        self._request = _mpi_comm.isend(msg, dest=target, tag=tag)
        self._lock = threading.Lock()
        logger.debug(f"AsyncMPISend.__init__ target={target} tag={tag} {str(msg)[:99]}")

    def __await__(self):
        status = MPI.Status()
        while True:
            with self._lock:
                if self._request is None:
                    return False
                finished, _ = self._request.test(status)
            if finished:
                return True
            yield from asyncio.sleep(_polling_time).__await__()

    def cancel(self):
        with self._lock:
            if self._request:
                self._request.Cancel()
                self._request = None


# Unique MPI tags for handshakes.
_new_connection_tag = 1
_new_response_tag = 2


# Store for all MPIComm objects used by this rank, and a mechanism to create
# unique MPI tags for new Comm objects.  Don't really need to store the Comms,
# but useful for debugging.
class CommStore:

    def __init__(self):
        # Tags only need to be unique from the sender's point of view, so can
        # reuse the same tags on different ranks.  But useful for them to be
        # different for debugging.
        self._next_available_tag = _mpi_rank * 100 + 3
        self._comms = []

    def get_next_tag(self):
        ret = self._next_available_tag
        self._next_available_tag += 1
        return ret

    def register_comm(self, comm):
        self._comms.append(comm)
        self.write()

    def remove_comm(self, comm):
        self._comms.remove(comm)
        self.write()

    def write(self):
        logger.debug("CommStore start")
        for c in self._comms:
            logger.debug(
                f"cache {c._local_addr} {c._peer_addr} {c._send_tag} {c._recv_tag}"
            )
        logger.debug("CommStore end")


_comm_store = CommStore()


class MPIComm(Comm):

    # Communication object, one sender and one receiver only.
    def __init__(self, local_addr, peer_addr, send_tag, recv_tag, deserialize=True):
        Comm.__init__(self)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self._send_tag = send_tag
        self._recv_tag = recv_tag
        self.deserialize = deserialize
        self._closed = False
        self._async_recv = None
        _comm_store.register_comm(self)

        logger.debug(
            f"MPIComm.__init__ local={local_addr} peer={peer_addr} tags={send_tag} {recv_tag}"
        )

    async def close(self):
        logger.debug("MPIComm.close")
        self.abort()

    def abort(self):
        logger.debug(f"MPIComm.abort {self._async_recv}")
        if not self._closed:
            if self._async_recv:
                self._async_recv.cancel()
            self._closed = True
            _comm_store.remove_comm(self)

    def closed(self):
        return self._closed

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    async def read(self, deserializers="ignored"):
        _, source_rank = parse_host_port(self._peer_addr)
        tag = self._recv_tag
        logger.debug(
            f"MPIComm.read (listening) from source_rank={source_rank} tag={tag}"
        )

        self._async_recv = AsyncMPIRecv(source_rank, tag)
        msg, _ = await self._async_recv

        if msg is None:
            raise CommClosedError

        if self.deserialize:
            msg = nested_deserialize(msg)

        self._async_recv = None
        logger.debug(
            f"MPIComm.read completed from source_rank={source_rank} tag={tag} {str(msg)[:99]}"
        )

        return msg

    async def write(self, msg, serializers=None, on_error=None):
        _, target_rank = parse_host_port(self._peer_addr)
        tag = self._send_tag
        logger.debug(
            f"MPIComm.write to target_rank={target_rank} tag={tag} {str(msg)[:99]}"
        )

        if False:
            # Direct send causes it to hang sometimes...
            _mpi_comm.send(msg, dest=target_rank, tag=tag)
        else:
            async_send = AsyncMPISend(msg, target_rank, tag)
            ok = await async_send
            if not ok:
                raise CommClosedError

        logger.debug("MPIComm.write completed")

        return 0  # Return number of bytes?


class MPIConnector(Connector):

    def __init__(self):
        logger.debug("MPIConnector.__init__")

    async def connect(self, address, deserialize=True, **connection_args):
        logger.debug(f"MPIConnector.connect to remote address={address}")

        _, target_rank = parse_host_port(address)

        send_tag = _comm_store.get_next_tag()
        msg = f"{send_tag}:{get_ip()}"

        if True:  # Is either OK?
            _mpi_comm.send(msg, dest=target_rank, tag=_new_connection_tag)
            logger.debug("MPIConnector send called")
        else:
            async_send = AsyncMPISend(msg, target_rank, _new_connection_tag)
            logger.debug("MPIConnector isend called")
            ok = await async_send
            if not ok:
                raise CommClosedError
            logger.debug("MPIConnector isend returned")

        # Receive tag in other direction.
        if True:
            async_recv = AsyncMPIRecv(target_rank, _new_response_tag)
            logger.debug("MPIConnector irecv called")
            msg, _ = await async_recv
        else:
            logger.debug("MPIConnector recv called")
            msg = _mpi_comm.recv(source=target_rank, tag=_new_response_tag)

        # Should check format of received message.
        recv_tag, recv_ip = msg.split(":")
        recv_tag = int(recv_tag)
        logger.debug(f"MPIConnector recv returned {msg} {recv_tag}")

        # Create and return new comm
        local_address = f"mpi://{get_ip()}:{_mpi_rank}"
        peer_address = f"mpi://{recv_ip}:{target_rank}"
        comm = MPIComm(local_address, peer_address, send_tag, recv_tag, deserialize)
        return comm


class MPIListener(Listener):
    prefix = "mpi://"

    def __init__(
        self, address, comm_handler, deserialize=True, default_port=0, **connection_args
    ):
        logger.debug(f"MPIListener.__init__ address={address}")

        self.ip, _ = parse_host_port(address, default_port)
        self._comm_handler = comm_handler
        self.deserialize = deserialize
        self.server_args = self._get_server_args(**connection_args)

        self._async_recv = None

    def _get_server_args(self, **connection_args):
        return {}

    async def _listen(self):
        logger.debug("MPIListener._listen")

        # Receiving new connection request via MPI.  Only gets called once per
        # rank, so need to repeat whenever a new connection request is received.
        status = MPI.Status()

        while True:
            logger.debug("MPIListener waiting for contact")
            self._async_recv = AsyncMPIRecv(None, _new_connection_tag)
            msg, other_rank = await self._async_recv
            self._async_recv = None
            logger.debug(
                f"MPIListener async recv returned from rank={other_rank} {str(msg)[:99]}"
            )

            if msg is None:
                # Empty message can only be caused by cancelling the receive.
                return

            # Should check format of received message.
            recv_tag, recv_ip = msg.split(":")
            recv_tag = int(recv_tag)

            # Send response message.
            send_tag = _comm_store.get_next_tag()
            msg = f"{send_tag}:{self.ip}"
            _mpi_comm.send(msg, dest=other_rank, tag=_new_response_tag)

            comm = MPIComm(
                local_addr=f"mpi://{self.ip}:{_mpi_rank}",
                peer_addr=f"mpi://{recv_ip}:{other_rank}",
                send_tag=send_tag,
                recv_tag=recv_tag,
                deserialize=self.deserialize,
            )

            try:
                await self.on_connection(comm)
            except CommClosedError:
                logger.debug("Connection closed before handshake completed")
                return

            IOLoop.current().add_callback(self._comm_handler, comm)

    async def start(self):
        logger.debug("MPIListener.start")
        asyncio.ensure_future(self._listen())

    def stop(self):
        logger.debug("MPIListener.stop")
        if self._async_recv:
            self._async_recv.cancel()
            self._async_recv = None

    @property
    def listen_address(self):
        return f"{self.prefix}{self.ip}:{_mpi_rank}"

    @property
    def contact_address(self):
        return f"{self.prefix}{self.ip}:{_mpi_rank}"


class MPIBackend(Backend):
    def get_connector(self):
        logger.debug("MPIBackend.get_connector")
        return MPIConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        logger.debug(f"MPIBackend.get_listener loc={loc}")
        return MPIListener(loc, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        return parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(host, port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends["mpi"] = MPIBackend()
