from __future__ import print_function, division, absolute_import

from collections import namedtuple
import itertools
import os
import sys
import threading
import weakref

from tornado import gen, locks
from tornado.ioloop import IOLoop
from tornado.queues import Queue

from ..compatibility import finalize
from ..utils import get_ip
from .core import (connectors, listeners, Comm, Listener, CommClosedError,
                   )


ConnectionRequest = namedtuple('ConnectionRequest',
                               ('c2s_q', 's2c_q', 'c_loop', 'c_addr',
                                'conn_event', 'close_request'))

_Close = object()


class Manager(object):

    def __init__(self):
        self.listeners = weakref.WeakValueDictionary()
        self.addr_suffixes = itertools.count(1)
        self.ip = get_ip()
        self.lock = threading.Lock()

    def add_listener(self, addr, listener):
        with self.lock:
            if addr in self.listeners:
                raise RuntimeError("already listening on %r" % (addr,))
            self.listeners[addr] = listener

    def remove_listener(self, addr):
        with self.lock:
            del self.listeners[addr]

    def get_listener_for(self, addr):
        with self.lock:
            self.validate_address(addr)
            return self.listeners.get(addr)

    def new_address(self):
        return "%s/%d/%s" % (self.ip, os.getpid(), next(self.addr_suffixes))

    def validate_address(self, addr):
        """
        Validate the address' IP and pid.
        """
        ip, pid, suffix = addr.split('/')
        if ip != self.ip or int(pid) != os.getpid():
            raise ValueError("inproc address %r does not match host (%r) or pid (%r)"
                             % (addr, self.ip, os.getpid()))


global_manager = Manager()

def new_address():
    """
    Generate a new address.
    """
    return 'inproc://' + global_manager.new_address()


class InProc(Comm):
    """
    An established communication based on a pair of in-process queues.
    """

    def __init__(self, peer_addr, read_q, write_q, write_loop,
                 close_request, deserialize=True):
        self._peer_addr = peer_addr
        self.deserialize = deserialize
        self._read_q = read_q
        self._write_q = write_q
        self._write_loop = write_loop
        self._closed = False
        # A "close request" event shared between both comms
        self._close_request = close_request

        self._finalizer = finalize(self, self._get_finalizer())
        self._finalizer.atexit = False

    def _get_finalizer(self):
        def finalize(write_q=self._write_q, write_loop=self._write_loop,
                     close_request=self._close_request):
            if not close_request.is_set():
                logger.warn("Closing dangling queue in %s" % (r,))
                close_request.set()
                write_loop.add_callback(write_q.put_nowait, _Close)

        return finalize

    def __repr__(self):
        return "<InProc %r>" % (self._peer_addr,)

    @property
    def peer_address(self):
        return self._peer_addr

    @gen.coroutine
    def read(self, deserialize=None):
        if self._closed:
            raise CommClosedError

        msg = yield self._read_q.get()
        if msg is _Close:
            assert self._close_request.is_set()
            self._closed = True
            self._finalizer.detach()
            raise CommClosedError

        # XXX does deserialize matter?
        raise gen.Return(msg)

    @gen.coroutine
    def write(self, msg):
        if self._close_request.is_set():
            self._closed = True
            self._finalizer.detach()
            raise CommClosedError

        self._write_loop.add_callback(self._write_q.put_nowait, msg)

        raise gen.Return(1)

    @gen.coroutine
    def close(self):
        self.abort()

    def abort(self):
        if not self._closed:
            self._close_request.set()
            self._write_loop.add_callback(self._write_q.put_nowait, _Close)
            self._write_q = self._read_q = None
            self._closed = True
            self._finalizer.detach()

    def closed(self):
        return self._closed


class InProcListener(Listener):

    def __init__(self, address, comm_handler, deserialize=True):
        self.manager = global_manager
        self.address = address or self.manager.new_address()
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self.listen_q = Queue()

    @gen.coroutine
    def _listen(self):
        while True:
            conn_req = yield self.listen_q.get()
            if conn_req is None:
                break
            comm = InProc(peer_addr='inproc://' + conn_req.c_addr,
                          read_q=conn_req.c2s_q,
                          write_q=conn_req.s2c_q,
                          write_loop=conn_req.c_loop,
                          close_request=conn_req.close_request,
                          deserialize=self.deserialize)
            # Notify connector
            conn_req.c_loop.add_callback(conn_req.conn_event.set)
            self.comm_handler(comm)

    def connect_threadsafe(self, conn_req):
        self.loop.add_callback(self.listen_q.put_nowait, conn_req)

    def start(self):
        self.loop = IOLoop.current()
        self.loop.add_callback(self._listen)
        self.manager.add_listener(self.address, self)

    def stop(self):
        self.listen_q.put_nowait(None)
        self.manager.remove_listener(self.address)

    @property
    def listen_address(self):
        return 'inproc://' + self.address

    @property
    def contact_address(self):
        return 'inproc://' + self.address


class InProcConnector(object):

    def __init__(self, manager):
        self.manager = manager

    @gen.coroutine
    def connect(self, address, deserialize=True):
        listener = self.manager.get_listener_for(address)
        if listener is None:
            raise IOError("no endpoint for inproc address %r")

        conn_req = ConnectionRequest(c2s_q=Queue(),
                                     s2c_q=Queue(),
                                     c_loop=IOLoop.current(),
                                     c_addr=global_manager.new_address(),
                                     conn_event=locks.Event(),
                                     close_request=threading.Event(),
                                     )
        listener.connect_threadsafe(conn_req)
        # Wait for connection acknowledgement
        # (do not pretend we're connected if the other comm never gets
        #  created, for example if the listener was stopped in the meantime)
        yield conn_req.conn_event.wait()

        comm = InProc(peer_addr='inproc://' + address,
                      read_q=conn_req.s2c_q,
                      write_q=conn_req.c2s_q,
                      write_loop=listener.loop,
                      close_request=conn_req.close_request,
                      deserialize=deserialize)
        raise gen.Return(comm)


connectors['inproc'] = InProcConnector(global_manager)
listeners['inproc'] = InProcListener
