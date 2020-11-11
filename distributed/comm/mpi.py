import asyncio
from collections import namedtuple
from enum import Enum
import logging
import struct
import threading

from .registry import Backend, backends
from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .utils import from_frames, to_frames

from ..protocol import nested_deserialize
from ..utils import get_ip, get_ipv6, ensure_ip, nbytes

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
    #level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("mpi")


# Log contents of messages sent/received.  Helps with debugging, but very slow
# (conversion of arbitrary objects to/from string).
log_msg = False


# Various MPI message types to experiment with; not expected to be in
# production code.
class MessageType(Enum):
    PythonObjects = 1  # Pickled/unpickled using mpi4py
    Frames = 2  # Byte string using to_frames and from_frames


_message_type = MessageType.PythonObjects
# _message_type = MessageType.Frames


# To identify the completion of asynchronous MPI sends and receives, need to
# keep polling them.  This is the (minimum) time between polls, in seconds.
_polling_time = 0.001


#class AsyncMPIRecv:
#    def __init__(self, source, tag):
#        # source of None means receive from any rank.
#        self._source = source if source is not None else MPI.ANY_SOURCE
#        self._tag = tag
#        self._cancel = False
#        logger.debug(f"AsyncMPIRecv.__init__ source={self._source} tag={tag}")

#    def __await__(self):
#        while True:
#            if self._cancel:
#                return None, None
#            status = MPI.Status()
#            if _mpi_comm.iprobe(source=self._source, tag=self._tag, status=status):
#                source = status.Get_source()
#                tag = status.Get_tag()
#                msg = _mpi_comm.recv(source=source, tag=tag, status=status)
#                return msg, status.Get_source()
#            yield from asyncio.sleep(_polling_time).__await__()

#    def cancel(self):
#        self._cancel = True


class AsyncMPISend:
    def __init__(self, msg, target, tag):
        self._request = _mpi_comm.isend(msg, dest=target, tag=tag)
        self._lock = threading.Lock()
        if log_msg:
            logger.debug(f"AsyncMPISend.__init__ target={target} tag={tag} {str(msg)[:99]}")
        else:
            logger.debug(f"AsyncMPISend.__init__ target={target} tag={tag}")

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


Item = namedtuple("Item", ["event", "message_queue"])


class MessageReceiver:
    def __init__(self):
        logger.debug("MessageReceiver.__init__")
        self._running = False
        self._request_stop = False
        self._lock = threading.Lock()
        self._items = {}
        self._task = None

    def get_message(self, tag, source_rank):
        with self._lock:
            item = self._items.get((tag, source_rank))
            if item is None or len(item.message_queue) == 0:
                # Close down whoever is receiving these messages.
                return None, None
            source, msg = item.message_queue.pop(0)
            if len(item.message_queue) == 0:
                item.event.clear()
            logger.debug(f"removed from: {self.items_as_string()}")
        return source, msg

    def register(self, tag, source_rank):
        logger.debug(f"MessageReceiver.register tag={tag} source_rank={source_rank}")
        with self._lock:
            logger.debug(f"items before {self.items_as_string()}")
            # check it is not already there...
            event = asyncio.Event()
            key = (tag, source_rank)
            if key in self._items:
                logger.debug(f"ERROR: tag,source_rank ({tag},{source_rank} already in items!!!")
            self._items[key] = Item(event, [])
            logger.debug(f"items after  {self.items_as_string()}")
            if not self._running:
                self._task = asyncio.create_task(self.run())
        return event
        
    async def run(self):
        logger.debug("MessageReceiver.run")
        self._running = True
        status = MPI.Status()
        while True:
            if self._request_stop:
                logger.debug("MessageReceiver is stopping...")
                break
            if _mpi_comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                source = status.Get_source()
                tag = status.Get_tag()
                #logger.debug(f"MessageReceiver iprobe tag={tag} source={source}")
                with self._lock:
                    key = (tag, source)
                    item = self._items.get(key)
                    if item is None:
                        # Check if source corresponds to MPI.ANY_SOURCE.
                        key = (tag, None)
                        item = self._items.get(key)
                        
                    if item is None:
                        logger.debug(f"ERROR: MessageReceiver has unexpected message tag={tag} source={source}")
                        # Should I read and dump message????
                        msg = _mpi_comm.recv(source=source, tag=tag, status=status)
                        if log_msg:
                            logger.debug(f"ERROR msg={msg}")
                    else:
                        msg = _mpi_comm.recv(source=source, tag=tag, status=status)
                        if log_msg:
                            logger.debug(f"MessageReceiver has msg for tag {tag} from source {source} msg={str(msg)[:50]}")
                        else:
                            logger.debug(f"MessageReceiver has msg for tag {tag} from source {source}")
                        item.message_queue.append((source, msg))
                        if not item.event.is_set():
                            item.event.set()
                        logger.debug(f"added to: {self.items_as_string()}")
            await asyncio.sleep(_polling_time)

        self._running = False
        logger.debug("MessageReceiver exiting async run()")
        self._task = None

    def stop(self):
        logger.debug("MessageReceiver.stop")
        self._request_stop = True

    def unregister(self, tag, source_rank, send_empty_message=False):
        logger.debug(f"MessageReceiver.unregister tag={tag} source_rank={source_rank}")
        with self._lock:
            logger.debug(f"items before {self.items_as_string()}")
            item = self._items.pop((tag, source_rank))
            if send_empty_message:
                logger.debug(f"MessageReceiver sending empty message to tag={tag} source_rank={source_rank}")
                item.event.set()
            else:
                item.event.clear()
            # check rank and source_rank are the same?
            logger.debug(f"items after  {self.items_as_string()}")
            if self._running and len(self._items) == 0:
                self._request_stop = True

    def items_as_string(self):
        return " ".join(f"{k}:{len(v.message_queue)}" for k,v in self._items.items())


# Unique MPI tags for handshakes.
_new_connection_tag = 1
_new_response_tag = 2


# Mechanism to create unique MPI tags for new Comm and Controller objects.
class TagGenerator:
    def __init__(self):
        # Tags only need to be unique from the sender's point of view, so can
        # reuse the same tags on different ranks.  But useful for them to be
        # different for debugging.
        self._next_available_tag = _mpi_rank * 100 + 2
        self._lock = threading.Lock()

    def get_next_tag(self):
        with self._lock:
            tag = self._next_available_tag
            self._next_available_tag += 1
        logger.debug(f"TagGenerator issuing tag {tag}")
        return tag


_tag_generator = TagGenerator()


class MPIComm(Comm):
    # Communication object, one sender and one receiver only.
    def __init__(self, local_addr, peer_addr, tag, message_receiver,
                 deserialize=True):
        Comm.__init__(self)
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self._tag = tag
        self._message_receiver = message_receiver
        self.deserialize = deserialize
        self._closed = False        

        self._local_rank = parse_host_port(self._local_addr)[1]
        self._peer_rank = parse_host_port(self._peer_addr)[1]

        self._want_event = True
        #self._want_event = False

        if self._want_event:
            self._event = self._message_receiver.register(self._tag, self._peer_rank)
        else:
            self._async_recv = None

        logger.debug(
            f"MPIComm.__init__ peer={self._peer_rank} tag={self._tag}"
        )

    async def close(self):
        logger.debug(f"MPIComm.close peer={self._peer_rank} tag={self._tag}")
        if self._want_event:
            if not self._closed:
                self._message_receiver.unregister(self._tag, self._peer_rank, True)
                self._closed = True
        else:
            self.abort()

    def abort(self):
        logger.debug(f"MPIComm.abort peer={self._peer_rank} tag={self._tag}")
        if self._want_event:
            pass
        else:
            if not self._closed:
                if self._async_recv:
                    self._async_recv.cancel()
                    self._closed = True

    def closed(self):
        return self._closed

    @property
    def local_address(self):
        return self._local_addr

    @property
    def peer_address(self):
        return self._peer_addr

    async def read(self, deserializers="ignored"):
        logger.debug(
            f"MPIComm.read (listening) from rank={self._peer_rank} tag={self._tag}"
        )

        if self._want_event:
            await self._event.wait()
            other_rank, msg = self._message_receiver.get_message(self._tag, self._peer_rank)
        else:
            self._async_recv = AsyncMPIRecv(self._peer_rank, self._tag)
            msg, _ = await self._async_recv

        if msg is None:
            logger.debug(f"MPIComm read has empty message peer={self._peer_rank} tag={self._tag}")
            raise CommClosedError

        if _message_type == MessageType.Frames:
            n_frames = struct.unpack("Q", msg[:8])[0]
            lengths = struct.unpack("Q" * n_frames, msg[8 : 8 + 8 * n_frames])

            offset = 8 + 8 * n_frames
            frames = []
            for length in lengths:
                frames.append(msg[offset : offset + length])
                offset += length

            # Check length of message correct?

            msg = await from_frames(
                frames,
                deserialize=self.deserialize,
                deserializers=deserializers,
                allow_offload=self.allow_offload,
            )
        else:
            if self.deserialize:
                msg = nested_deserialize(msg)

        if self._want_event:
            pass
        else:
            self._async_recv = None

        if log_msg:
            logger.debug(f"MPIComm.read completed from rank={self._peer_rank} tag={self._tag} {str(msg)[:99]}")
        else:
            logger.debug(f"MPIComm.read completed from rank={self._peer_rank} tag={self._tag}")

        return msg

    async def write(self, msg, serializers=None, on_error=None):
        if log_msg:
            logger.debug(f"MPIComm.write to rank={self._peer_rank} tag={self._tag} {str(msg)[:99]}")
        else:
            logger.debug(f"MPIComm.write to rank={self._peer_rank} tag={self._tag}")

        if _message_type == MessageType.Frames:
            frames = await to_frames(
                msg,
                allow_offload=self.allow_offload,
                serializers=serializers,
                on_error=on_error,
                context={
                    "sender": self.local_info,
                    "recipient": self.remote_info,
                    **self.handshake_options,
                },
            )

            lengths = [nbytes(frame) for frame in frames]
            length_bytes = [struct.pack("Q", len(frames))] + [
                struct.pack("Q", x) for x in lengths
            ]
            msg = b"".join(length_bytes + frames)  # Send all in one go!

        if False:
            # Direct send causes it to hang sometimes...
            _mpi_comm.send(msg, dest=self._peer_rank, tag=self._tag)
        else:
            async_send = AsyncMPISend(msg, self._peer_rank, self._tag)
            ok = await async_send
            if not ok:
                raise CommClosedError

        logger.debug(f"MPIComm.write completed  peer={self._peer_rank} tag={self._tag}")

        if _message_type == MessageType.Frames:
            return sum(lengths)
        else:
            return 0  # Return number of bytes sent?


class MPIConnector(Connector):
    def __init__(self, message_receiver):
        logger.debug("MPIConnector.__init__")
        self._message_receiver = message_receiver

    async def connect(self, address, deserialize=True, **connection_args):
        logger.debug(f"MPIConnector.connect to remote address={address}")

        target_ip, target_rank = parse_host_port(address)

        # Tag to be used for MPIComm.
        new_tag = _tag_generator.get_next_tag()
        msg = f"{new_tag}:{get_ip()}"

        if True:  # Is either OK?
            _mpi_comm.send(msg, dest=target_rank, tag=_new_connection_tag)
            if log_msg:
                logger.debug(f"MPIConnector send called {msg}")
            else:
                logger.debug(f"MPIConnector send called")
        else:
            async_send = AsyncMPISend(msg, target_rank, _new_connection_tag)
            if log_msg:
                logger.debug("MPIConnector isend called {msg}")
            else:
                logger.debug("MPIConnector isend called")
            ok = await async_send
            if not ok:
                raise CommClosedError
            logger.debug("MPIConnector isend returned")

        # Create and return new comm
        local_address = f"mpi://{get_ip()}:{_mpi_rank}"
        peer_address = f"mpi://{target_ip}:{target_rank}"
        comm = MPIComm(local_address, peer_address, new_tag,
                       self._message_receiver, deserialize)
        return comm


class MPIListener(Listener):
    prefix = "mpi://"

    def __init__(
        self, address, comm_handler, message_receiver, deserialize=True,
        default_port=0, **connection_args
    ):
        logger.debug(f"MPIListener.__init__ address={address}")

        self.ip = get_ip()
        self._comm_handler = comm_handler
        self._message_receiver = message_receiver
        self.deserialize = deserialize
        self.server_args = self._get_server_args(**connection_args)

        self._listen_future = None
        
        self._want_event = True
        #self._want_event = False

        if self._want_event:
            self._event = None
        else:
            self._async_recv = None

    def _get_server_args(self, **connection_args):
        return {}

    async def _listen(self):
        logger.debug("MPIListener._listen")

        # Receiving new connection request via MPI.  Only gets called once per
        # rank, so need to repeat whenever a new connection request is received.
        status = MPI.Status()

        if self._want_event:
            self._event = self._message_receiver.register(_new_connection_tag, None)

        while True:
            logger.debug("MPIListener waiting for contact")

            if self._want_event:
                await self._event.wait()
                other_rank, msg = self._message_receiver.get_message(_new_connection_tag, None)
            else:
                self._async_recv = AsyncMPIRecv(None, _new_connection_tag)
                msg, other_rank = await self._async_recv
                self._async_recv = None

            if msg is None:
                # Empty message can only be caused by cancelling the receive.
                return

            if log_msg:
                logger.debug(f"MPIListener async recv returned from rank={other_rank} {msg}")
            else:
                logger.debug(f"MPIListener async recv returned from rank={other_rank}")

            # Should check format of received message.
            tag, recv_ip = msg.split(":")
            tag = int(tag)

            comm = MPIComm(
                local_addr=f"mpi://{self.ip}:{_mpi_rank}",
                peer_addr=f"mpi://{recv_ip}:{other_rank}",
                tag=tag,
                message_receiver=self._message_receiver,
                deserialize=self.deserialize,
            )

            try:
                await self.on_connection(comm)
            except CommClosedError:
                logger.debug("Connection closed before handshake completed")
                return

            IOLoop.current().add_callback(self._comm_handler, comm)

        # may have already called unregister if called stop()????????
        if self._want_event:
            if self._event:
                self._message_receiver.unregister(_new_connection_tag, None)
                self._event = None
        self._listen_future = None

    async def start(self):
        logger.debug("MPIListener.start")
        self._listen_future = asyncio.ensure_future(self._listen())

    def stop(self):
        logger.debug("MPIListener.stop")
        if self._listen_future:
            self._listen_future.cancel()
            
        if self._want_event:
            if self._event:
                self._message_receiver.unregister(_new_connection_tag, None)
                self._event = None
        else:
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
    def __init__(self):
        logger.debug("MPIBackend.__init__")
        self._message_receiver = None

    def _get_or_create_message_receiver(self):
        if self._message_receiver is None:
            self._message_receiver = MessageReceiver()
        return self._message_receiver
        
    def get_connector(self):
        logger.debug("MPIBackend.get_connector")
        return MPIConnector(self._get_or_create_message_receiver())

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        logger.debug(f"MPIBackend.get_listener loc={loc}")
        return MPIListener(loc, handle_comm, self._get_or_create_message_receiver(),
                           deserialize, **connection_args)

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
