from __future__ import print_function, division, absolute_import

import atexit
from datetime import timedelta
import logging
import sys
from time import sleep
import threading
import weakref

from .compatibility import finalize, Queue as PyQueue
from .utils import mp_context

from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop


logger = logging.getLogger(__name__)


def _call_and_set_future(loop, future, func, *args, **kwargs):
    try:
        res = func(*args, **kwargs)
    except:
        # Tornado futures are not thread-safe, need to
        # set_result() / set_exc_info() from the loop's thread
        loop.add_callback(future.set_exc_info, sys.exc_info())
    else:
        loop.add_callback(future.set_result, res)


class _ProcessState(object):
    is_alive = False
    pid = None
    exitcode = None


class AsyncProcess(object):
    """
    A coroutine-compatible multiprocessing.Process-alike.
    All normally blocking methods are wrapped in Tornado coroutines.
    """

    def __init__(self, loop=None, target=None, name=None, args=(), kwargs={}):
        if not callable(target):
            raise TypeError("`target` needs to be callable, not %r"
                            % (type(target),))
        self._state = _ProcessState()
        self._loop = loop or IOLoop.current(instance=False)

        self._process = mp_context.Process(target=self._run, name=name,
                                           args=(target, args, kwargs))
        _dangling.add(self._process)
        self._name = self._process.name
        self._watch_q = PyQueue()
        self._exit_future = Future()
        self._exit_callback = None
        self._closed = False

        self._start_threads()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self._name)

    def _check_closed(self):
        if self._closed:
            raise ValueError("invalid operation on closed AsyncProcess")

    def _start_threads(self):
        self._watch_message_thread = threading.Thread(
            target=self._watch_message_queue,
            name="AsyncProcess %s watch" % self.name,
            args=(weakref.ref(self), self._process, self._loop,
                  self._state, self._watch_q, self._exit_future,))
        self._watch_message_thread.daemon = True
        self._watch_message_thread.start()

        def stop_thread(q):
            q.put_nowait({'op': 'stop'})
            # We don't join the thread here as a finalizer can be called
            # asynchronously from anywhere

        self._finalizer = finalize(self, stop_thread, q=self._watch_q)
        self._finalizer.atexit = False

    def _on_exit(self, exitcode):
        # Called from the event loop when the child process exited
        self._process = None
        if self._exit_callback is not None:
            self._exit_callback(self)
        self._exit_future.set_result(exitcode)

    @classmethod
    def _run(cls, target, args, kwargs):
        # Child process entry point
        threading.current_thread().name = "MainThread"
        target(*args, **kwargs)

    @classmethod
    def _watch_message_queue(cls, selfref, process, loop, state, q, exit_future):
        # As multiprocessing.Process is not thread-safe, we run all
        # blocking operations from this single loop and ship results
        # back to the caller when needed.
        r = repr(selfref())
        name = selfref().name

        def _start():
            process.start()

            thread = threading.Thread(
                target=AsyncProcess._watch_process,
                name="AsyncProcess %s watch" % name,
                args=(selfref, process, state, q))
            thread.daemon = True
            thread.start()

            state.is_alive = True
            state.pid = process.pid
            logger.debug("[%s] created process with pid %r" % (r, state.pid))

        while True:
            msg = q.get()
            logger.debug("[%s] got message %r" % (r, msg))
            op = msg['op']
            if op == 'start':
                _call_and_set_future(loop, msg['future'], _start)
            elif op == 'terminate':
                _call_and_set_future(loop, msg['future'], process.terminate)
            elif op == 'stop':
                break
            else:
                assert 0, msg

    @classmethod
    def _watch_process(cls, selfref, process, state, q):
        r = repr(selfref())
        process.join()
        exitcode = process.exitcode
        if exitcode is not None:
            logger.debug("[%s] process %r exited with code %r",
                         r, state.pid, exitcode)
            state.is_alive = False
            state.exitcode = exitcode
            # Make sure the process is removed from the global list
            # (see _children in multiprocessing/process.py)
            process.join(timeout=0)
            # Then notify the Process object
            self = selfref()  # only keep self alive when required
            try:
                if self is not None:
                    self._loop.add_callback(self._on_exit, exitcode)
            finally:
                self = None  # lose reference
                q.put({'op': 'stop'})

    def start(self):
        """
        Start the child process.

        This method is a coroutine.
        """
        self._check_closed()
        fut = Future()
        self._watch_q.put_nowait({'op': 'start', 'future': fut})
        return fut

    def terminate(self):
        """
        Terminate the child process.

        This method is a coroutine.
        """
        self._check_closed()
        fut = Future()
        self._watch_q.put_nowait({'op': 'terminate', 'future': fut})
        return fut

    @gen.coroutine
    def join(self, timeout=None):
        """
        Wait for the child process to exit.

        This method is a coroutine.
        """
        self._check_closed()
        assert self._state.pid is not None, 'can only join a started process'
        if self._state.exitcode is not None:
            return
        if timeout is None:
            yield self._exit_future
        else:
            try:
                yield gen.with_timeout(timedelta(seconds=timeout), self._exit_future)
            except gen.TimeoutError:
                pass

    def close(self):
        """
        Stop helper thread and release resources.  This method returns
        immediately and does not ensure the child process has exited.
        """
        if not self._closed:
            self._finalizer()
            self._process = None
            self._closed = True

    def set_exit_callback(self, func):
        """
        Set a function to be called by the event loop when the process exits.
        The function is called with the AsyncProcess as sole argument.

        The function may be a coroutine function.
        """
        # XXX should this be a property instead?
        assert callable(func), "exit callback should be callable"
        assert self._state.pid is None, "cannot set exit callback when process already started"
        self._exit_callback = func

    def is_alive(self):
        return self._state.is_alive

    @property
    def pid(self):
        return self._state.pid

    @property
    def exitcode(self):
        return self._state.exitcode

    @property
    def name(self):
        return self._name

    @property
    def daemon(self):
        return self._process.daemon

    @daemon.setter
    def daemon(self, value):
        self._process.daemon = value


_dangling = weakref.WeakSet()


@atexit.register
def _cleanup_dangling():
    for proc in list(_dangling):
        if proc.daemon and proc.is_alive():
            try:
                logger.warning("reaping stray process %s" % (proc,))
                proc.terminate()
            except OSError:
                pass
