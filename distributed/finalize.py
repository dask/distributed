from __future__ import print_function, division, absolute_import

import atexit
import collections
import contextlib
from functools import partial
import gc
import itertools
import os
import threading
import sys
import traceback
from weakref import ref


class CleanupThread(object):
    """
    Delegate cleanup functions to a dedicated thread, without deadlocks
    or race conditions.

    The underlying thread is started lazily.
    """
    # This implementations relie on atomicity of deque appends and pops.

    def __init__(self):
        self._please_shutdown = False
        self._tasks = collections.deque()
        self._reinit()

    def _reinit(self):
        self._pid = os.getpid()
        self._thread = None
        self._thread_start_lock = threading.Lock()
        self._thread_start_event = threading.Event()
        self._wakeup = threading.Event()

    def start(self):
        if self._pid != os.getpid():
            self._reinit()
        if self._thread is None:
            threading.Thread(target=self.run, daemon=True).start()
            self._thread_start_event.wait()
            # May not be the one we just started...
            assert self._thread is not None

    def run(self):
        try:
            self._run()
        except BaseException:
            if not shutting_down():
                # XXX LOG
                print("!" * 80)
                print("Exception in CleanupThread.run:", str(e))
                traceback.print_exc(file=sys.stdout)
                print("!" * 80)

    def _run(self):
        # Guard against race condition when calling start() from several
        # threads.
        if not self._thread_start_lock.acquire():
            return
        self._thread = threading.current_thread()
        self._pid = os.getpid()
        self._thread_start_event.set()

        tasks = collections.deque()

        while True:
            self._wakeup.wait(timeout=10)
            if self._please_shutdown or shutting_down():
                break
            self._wakeup.clear()
            n = len(self._tasks)
            for i in range(n):
                tasks.append(self._tasks.popleft())

            while tasks:
                func = tasks.popleft()
                try:
                    func()
                except BaseException:
                    # XXX LOG
                    print("!" * 80)
                    print("Exception in CleanupThread task:")
                    traceback.print_exc(file=sys.stdout)
                    print("!" * 80)
                finally:
                    # Lose any reference to the callback
                    func = None

    def stop(self):
        self._please_shutdown = True
        if self._thread is not None:
            self._wakeup.set()
            # The thread is daemonic, ensure it terminates
            self._thread.join()

    def enqueue_task(self, func):
        if shutting_down():
            return lambda: True

        self.start()
        self._tasks.append(func)
        self._wakeup.set()


cleanup_thread = CleanupThread()
atexit.register(cleanup_thread.stop)


_shutting_down = False

def _at_shutdown():
    global _shutting_down
    _shutting_down = True

def shutting_down(globals=globals):
    """
    Whether the interpreter is currently shutting down.
    For use in finalizers, __del__ methods, and similar; it is advised
    to early bind this function rather than look it up when calling it,
    since at shutdown module globals may be cleared.
    """
    # At shutdown, the attribute may have been cleared or set to None.
    v = globals().get('_shutting_down')
    return v is True or v is None


# Backported from Python 3.4: weakref.finalize()

class finalize:
    """Class for finalization of weakrefable objects

    finalize(obj, func, *args, **kwargs) returns a callable finalizer
    object which will be called when obj is garbage collected. The
    first time the finalizer is called it evaluates func(*arg, **kwargs)
    and returns the result. After this the finalizer is dead, and
    calling it just returns None.

    When the program exits any remaining finalizers for which the
    atexit attribute is true will be run in reverse order of creation.
    By default atexit is true.
    """

    # Finalizer objects don't have any state of their own.  They are
    # just used as keys to lookup _Info objects in the registry.  This
    # ensures that they cannot be part of a ref-cycle.

    __slots__ = ()
    _registry = {}
    _shutdown = False
    _index_iter = itertools.count()
    _dirty = False
    _registered_with_atexit = False

    class _Info:
        __slots__ = ("weakref", "func", "args", "kwargs", "atexit", "index")

    def __init__(self, obj, func, *args, **kwargs):
        if not self._registered_with_atexit:
            # We may register the exit function more than once because
            # of a thread race, but that is harmless
            import atexit
            atexit.register(self._exitfunc)
            finalize._registered_with_atexit = True
            atexit.register(_at_shutdown)
        info = self._Info()
        info.weakref = ref(obj, self)
        info.func = func
        info.args = args
        info.kwargs = kwargs or None
        info.atexit = True
        info.index = next(self._index_iter)
        self._registry[self] = info
        finalize._dirty = True

    def __call__(self, _=None):
        """If alive then mark as dead and return func(*args, **kwargs);
        otherwise return None"""
        info = self._registry.pop(self, None)
        if info and not self._shutdown:
            return info.func(*info.args, **(info.kwargs or {}))

    def detach(self):
        """If alive then mark as dead and return (obj, func, args, kwargs);
        otherwise return None"""
        info = self._registry.get(self)
        obj = info and info.weakref()
        if obj is not None and self._registry.pop(self, None):
            return (obj, info.func, info.args, info.kwargs or {})

    def peek(self):
        """If alive then return (obj, func, args, kwargs);
        otherwise return None"""
        info = self._registry.get(self)
        obj = info and info.weakref()
        if obj is not None:
            return (obj, info.func, info.args, info.kwargs or {})

    @property
    def alive(self):
        """Whether finalizer is alive"""
        return self in self._registry

    @property
    def atexit(self):
        """Whether finalizer should be called at exit"""
        info = self._registry.get(self)
        return bool(info) and info.atexit

    @atexit.setter
    def atexit(self, value):
        info = self._registry.get(self)
        if info:
            info.atexit = bool(value)

    def __repr__(self):
        info = self._registry.get(self)
        obj = info and info.weakref()
        if obj is None:
            return '<%s object at %#x; dead>' % (type(self).__name__, id(self))
        else:
            return '<%s object at %#x; for %r at %#x>' % \
                (type(self).__name__, id(self), type(obj).__name__, id(obj))

    @classmethod
    def _select_for_exit(cls):
        # Return live finalizers marked for exit, oldest first
        L = [(f,i) for (f,i) in cls._registry.items() if i.atexit]
        L.sort(key=lambda item:item[1].index)
        return [f for (f,i) in L]

    @classmethod
    def _exitfunc(cls):
        # At shutdown invoke finalizers for which atexit is true.
        # This is called once all other non-daemonic threads have been
        # joined.
        reenable_gc = False
        try:
            if cls._registry:
                import gc
                if gc.isenabled():
                    reenable_gc = True
                    gc.disable()
                pending = None
                while True:
                    if pending is None or finalize._dirty:
                        pending = cls._select_for_exit()
                        finalize._dirty = False
                    if not pending:
                        break
                    f = pending.pop()
                    try:
                        # gc is disabled, so (assuming no daemonic
                        # threads) the following is the only line in
                        # this function which might trigger creation
                        # of a new finalizer
                        f()
                    except Exception:
                        sys.excepthook(*sys.exc_info())
                    assert f not in cls._registry
        finally:
            # prevent any more finalizers from executing during shutdown
            finalize._shutdown = True
            if reenable_gc:
                gc.enable()


# dummy invocation to force _at_shutdown() to be registered
finalize(lambda: None, lambda: None)
assert finalize._registered_with_atexit


class AsyncFinalize(finalize):
    """An asynchronous variant of the finalize() object.

    The finalizer callback will be executed in the background cleanup
    thread, to avoid deadlocks or oddities depending on where __del__
    is called from.
    """

    def __init__(self, obj, func, *args, **kwargs):
        assert callable(func)
        func = partial(self._call_in_thread, func)
        finalize.__init__(self, obj, func, *args, **kwargs)

    @staticmethod
    def _call_in_thread(func, *args, **kwargs):
        cleanup_thread.enqueue_task(partial(func, *args, **kwargs))
