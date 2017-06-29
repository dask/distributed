from __future__ import print_function, division, absolute_import

import logging

from tornado import gen
from tornado.ioloop import IOLoop

from .core import Comm, CommClosedError


logger = logging.getLogger(__name__)


class BatchedComm(Comm):
    """
    This takes an interval and ensures that we send no more than one
    message every interval seconds.  We send lists of messages.

    The comm can be passed later on (through start()) to allow for
    buffering early sends.
    """

    def __init__(self, interval, comm=None):
        self.interval = interval
        self.comm = comm
        self.loop = IOLoop.current(instance=False)

        self._please_stop = False
        self._buffer = []
        self.message_count = 0
        self.batch_count = 0
        self.byte_count = 0
        self._last_write = None
        self._next_deadline = None
        self._timeout = None

    def __str__(self):
        return '<BatchedComm: %d messages pending>' % len(self._buffer)

    __repr__ = __str__

    def _check_comm(self):
        if self.comm is None:
            raise ValueError("please call BatchedComm.start() first")

    def _schedule_iteration(self):
        logger.debug("Scheduling iteration at %s (%.3f s. from now)",
                     self._next_deadline,
                     self._next_deadline - self.loop.time())
        assert self._timeout is None
        self._timeout = self.loop.add_timeout(self._next_deadline, self._iterate)

    def _cancel_iteration(self):
        timeout, self._timeout = self._timeout, None
        if timeout is not None:
            logger.debug("Cancelling iteration")
            self.loop.remove_timeout(timeout)

    @gen.coroutine
    def _iterate(self):
        self._timeout = None

        if self._please_stop:
            return
        assert self._next_deadline is not None
        if self.loop.time() < self._next_deadline:
            # Spurious wakeup?  Send interval not expired yet
            logger.info("Iterate: Got spurious wakeup")
            self._schedule_iteration()
            return
        payload, self._buffer = self._buffer, []
        self._next_deadline = None
        if not payload:
            # Nothing to send
            logger.debug("Iterate: Nothing to send")
            return
        self.batch_count += 1
        logger.debug("Iterate: Sending %d messages at once to %r",
                     len(payload), self.peer_address)
        try:
            nbytes = yield self.comm.write(payload)
            self.byte_count += nbytes
        except CommClosedError as e:
            raise
            if not self._please_stop:
                logger.info("BatchedComm Closed: %s", e)
        except Exception:
            raise
            if not self._please_stop:
                logger.exception("Error in batched write")
        else:
            self._last_write = self.loop.time()

    def start(self, comm):
        """
        """
        assert self.comm is None, "cannot be called twice"
        assert self._timeout is None
        self.comm = comm
        if self._buffer:
            self._next_deadline = self.loop.time() + self.interval
            self._schedule_iteration()

    def write(self, msg):
        """
        Schedule a message for sending to the other side.

        This completes quickly and synchronously.
        """
        if self.comm is not None and self.comm.closed():
            raise CommClosedError

        self.message_count += 1
        self._buffer.append(msg)
        logger.debug("Queue one message for sending")
        if self.comm is not None and self._timeout is None:
            if self._last_write is None:
                self._next_deadline = self.loop.time() + self.interval
            else:
                self._next_deadline = self._last_write + self.interval
            self._schedule_iteration()
        return gen.moment

    @gen.coroutine
    def close(self):
        """
        Flush existing messages and then close comm.
        """
        self._cancel_iteration()
        self._buffer, payload = [], self._buffer
        self._please_stop = True
        if self.comm is not None and not self.comm.closed():
            try:
                if payload:
                    yield self.comm.write(payload)
            except CommClosedError:
                pass
            yield self.comm.close()

    def abort(self):
        self._cancel_iteration()
        self._buffer = []
        self._please_stop = True
        if self.comm is not None and not self.comm.closed():
            self.comm.abort()

    def closed(self):
        self._check_comm()
        return self.comm.closed()

    @property
    def peer_address(self):
        self._check_comm()
        return self.comm.peer_address

    @gen.coroutine
    def read(self):
        self._check_comm()
        msg = yield self.comm.read()
        raise gen.Return(msg)

    @property
    def extra_info(self):
        self._check_comm()
        return self.comm.extra_info
