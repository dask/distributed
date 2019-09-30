from abc import ABC, abstractmethod, abstractproperty
from datetime import timedelta
import logging
import weakref

import dask
from tornado import gen

from ..metrics import time
from ..utils import parse_timedelta, ignoring
from . import registry
from .addressing import parse_address


logger = logging.getLogger(__name__)


class CommClosedError(IOError):
    pass


class FatalCommClosedError(CommClosedError):
    pass


class Comm(ABC):
    """
    A message-oriented communication object, representing an established
    communication channel.  There should be only one reader and one
    writer at a time: to manage current communications, even with a
    single peer, you must create distinct ``Comm`` objects.

    Messages are arbitrary Python objects.  Concrete implementations
    of this class can implement different serialization mechanisms
    depending on the underlying transport's characteristics.
    """

    _instances = weakref.WeakSet()

    def __init__(self):
        self._instances.add(self)
        self.name = None

    # XXX add set_close_callback()?

    @abstractmethod
    def read(self, deserializers=None):
        """
        Read and return a message (a Python object).

        This method is a coroutine.

        Parameters
        ----------
        deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
            An optional dict appropriate for distributed.protocol.deserialize.
            See :ref:`serialization` for more.
        """

    @abstractmethod
    def write(self, msg, on_error=None):
        """
        Write a message (a Python object).

        This method is a coroutine.

        Parameters
        ----------
        msg :
        on_error : Optional[str]
            The behavior when serialization fails. See
            ``distributed.protocol.core.dumps`` for valid values.
        """

    @abstractmethod
    def close(self):
        """
        Close the communication cleanly.  This will attempt to flush
        outgoing buffers before actually closing the underlying transport.

        This method is a coroutine.
        """

    @abstractmethod
    def abort(self):
        """
        Close the communication immediately and abruptly.
        Useful in destructors or generators' ``finally`` blocks.
        """

    @abstractmethod
    def closed(self):
        """
        Return whether the stream is closed.
        """

    @abstractproperty
    def local_address(self):
        """
        The local address.  For logging and debugging purposes only.
        """

    @abstractproperty
    def peer_address(self):
        """
        The peer's address.  For logging and debugging purposes only.
        """

    @property
    def extra_info(self):
        """
        Return backend-specific information about the communication,
        as a dict.  Typically, this is information which is initialized
        when the communication is established and doesn't vary afterwards.
        """
        return {}

    def __repr__(self):
        clsname = self.__class__.__name__
        if self.closed():
            return "<closed %s>" % (clsname,)
        else:
            return "<%s %s local=%s remote=%s>" % (
                clsname,
                self.name or "",
                self.local_address,
                self.peer_address,
            )


class Listener(ABC):
    @abstractmethod
    def start(self):
        """
        Start listening for incoming connections.
        """

    @abstractmethod
    def stop(self):
        """
        Stop listening.  This does not shutdown already established
        communications, but prevents accepting new ones.
        """

    @abstractproperty
    def listen_address(self):
        """
        The listening address as a URI string.
        """

    @abstractproperty
    def contact_address(self):
        """
        An address this listener can be contacted on.  This can be
        different from `listen_address` if the latter is some wildcard
        address such as 'tcp://0.0.0.0:123'.
        """

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()


class Connector(ABC):
    @abstractmethod
    def connect(self, address, deserialize=True):
        """
        Connect to the given address and return a Comm object.
        This function is a coroutine.   It may raise EnvironmentError
        if the other endpoint is unreachable or unavailable.  It
        may raise ValueError if the address is malformed.
        """


async def connect(addr, timeout=None, deserialize=True, connection_args=None):
    """
    Connect to the given address (a URI such as ``tcp://127.0.0.1:1234``)
    and yield a ``Comm`` object.  If the connection attempt fails, it is
    retried until the *timeout* is expired.
    """
    if timeout is None:
        timeout = dask.config.get("distributed.comm.timeouts.connect")
    timeout = parse_timedelta(timeout, default="seconds")

    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    connector = backend.get_connector()
    comm = None

    start = time()
    deadline = start + timeout
    error = None

    def _raise(error):
        error = error or "connect() didn't finish in time"
        msg = "Timed out trying to connect to %r after %s s: %s" % (
            addr,
            timeout,
            error,
        )
        raise IOError(msg)

    # This starts a thread
    while True:
        try:
            while deadline - time() > 0:
                future = connector.connect(
                    loc, deserialize=deserialize, **(connection_args or {})
                )
                with ignoring(gen.TimeoutError):
                    comm = await gen.with_timeout(
                        timedelta(seconds=min(deadline - time(), 1)),
                        future,
                        quiet_exceptions=EnvironmentError,
                    )
                    break
            if not comm:
                _raise(error)
        except FatalCommClosedError:
            raise
        except EnvironmentError as e:
            error = str(e)
            if time() < deadline:
                await gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                _raise(error)
        else:
            break

    return comm


def listen(addr, handle_comm, deserialize=True, connection_args=None):
    """
    Create a listener object with the given parameters.  When its ``start()``
    method is called, the listener will listen on the given address
    (a URI such as ``tcp://0.0.0.0``) and call *handle_comm* with a
    ``Comm`` object for each incoming connection.

    *handle_comm* can be a regular function or a coroutine.
    """
    try:
        scheme, loc = parse_address(addr, strict=True)
    except ValueError:
        if connection_args and connection_args.get("ssl_context"):
            addr = "tls://" + addr
        else:
            addr = "tcp://" + addr
        scheme, loc = parse_address(addr, strict=True)

    backend = registry.get_backend(scheme)

    return backend.get_listener(
        loc, handle_comm, deserialize, **(connection_args or {})
    )
