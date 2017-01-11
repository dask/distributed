# XXX rename this to distributed.comm.core?

from __future__ import print_function, division, absolute_import

from abc import ABCMeta, abstractmethod

from six import with_metaclass

from tornado import gen


# Connector instances

connectors = {
    #'tcp': ...,
    #'zmq': ...,
    }


# Listener classes

listeners = {
    #'tcp': ...,
    # 'zmq': ...,
    }


DEFAULT_SCHEME = 'tcp'


class CommClosedError(IOError):
    pass


class Comm(with_metaclass(ABCMeta)):
    """
    A communication object, representing an established communication
    channel.
    """

    @abstractmethod
    def read(self, deserialize=None):
        """
        Read and return a message.  If *deserialize* is not None, it
        overrides this communication's default setting.

        This method is a coroutine.
        """

    @abstractmethod
    def write(self, msg):
        """
        Write a message.

        This method is a coroutine.
        """

    @abstractmethod
    def close(self):
        """
        Close the communication cleanly.

        This method is a coroutine.
        """

    @abstractmethod
    def closed(self):
        """
        Return whether the stream is closed.
        """


def parse_address(addr):
    if not isinstance(addr, str):
        raise TypeError("expected str, got %r" % addr.__class__.__name__)
    scheme, sep, loc = addr.rpartition('://')
    if not sep:
        scheme = DEFAULT_SCHEME
    return scheme, loc


@gen.coroutine
def connect(addr, deserialize=True):
    """
    Connect to the given address (a URI such as 'tcp://127.0.0.1:1234')
    and yield a Comm object.
    """
    # XXX should timeout be handled here or in each transport?
    scheme, loc = parse_address(addr)
    connector = connectors.get(scheme)
    if connector is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    comm = yield connector.connect(loc, deserialize=deserialize)
    raise gen.Return(comm)


def listen(addr, handle_comm, deserialize=True):
    """
    Listen on the given address (a URI such as 'tcp://192.168.1.254')
    and call *handle_comm* with a Comm object on each incoming connection.
    """
    scheme, loc = parse_address(addr)
    listener_class = listeners.get(scheme)
    if listener_class is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    return listener_class(loc, handle_comm, deserialize)
