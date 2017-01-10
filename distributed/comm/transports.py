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


class Comm(with_metaclass(ABCMeta)):
    """
    A communication object, representing an established communication
    channel.
    """

    def __init__(self, stream, deserialize=True):
        self.stream = stream
        self.deserialize = deserialize
        stream.set_nodelay(True)
        set_tcp_timeout(stream)

    @abstractmethod
    def read(self):
        raise NotImplementedError

    @abstractmethod
    def write(self, msg):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError


def parse_address(addr):
    if not isinstance(addr, str):
        raise TypeError("expected str, got %r" % addr.__class__.__name__)
    scheme, sep, loc = addr.partition('://')
    if not sep:
        scheme = DEFAULT_SCHEME
    return scheme, loc


@gen.coroutine
def connect(addr, deserialize=True):
    """
    Connect to the given address (a URI such as 'tcp://localhost:1234')
    and yield a comm object.
    """
    scheme, loc = parse_address(addr)
    connector = connectors.get(scheme)
    if connector is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    comm = yield connector.connect(loc, deserialize=deserialize)
    raise gen.Return(comm)


def listen(addr, handle_comm, deserialize=True):
    scheme, loc = parse_address(addr)
    listener_class = listeners.get(scheme)
    if listener_class is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    return listener_class(loc, handle_comm, deserialize)
