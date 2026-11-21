from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from contextlib import suppress

from dask.utils import parse_timedelta

from distributed.lock import Lock
from distributed.utils import Deadline, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """An extension for the scheduler to manage Conditions

    This adds the following routes to the scheduler

    *  condition_register
    *  condition_wait
    *  condition_notify
    *  condition_notify_all

    A waiter always calls ``condition_register`` *before* releasing its lock
    and only then calls ``condition_wait``. This guarantees that the
    ``asyncio.Event`` backing a waiter exists before the lock is given up, so
    a ``notify``/``notify_all`` that runs in the gap between "release the
    lock" and "start waiting" can never be missed (no lost wakeups).
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # name -> {waiter_id: asyncio.Event}
        self.waiters = defaultdict(dict)

        self.scheduler.handlers.update(
            {
                "condition_register": self.register,
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )
        self.scheduler.extensions["conditions"] = self

    @log_errors
    def register(self, name=None, waiter_id=None):
        """Create the waiter's event ahead of time, before the lock is released."""
        self.waiters[name][waiter_id] = asyncio.Event()

    @log_errors
    async def wait(self, name=None, waiter_id=None, timeout=None):
        """Block until the given waiter is notified, or ``timeout`` elapses."""
        event = self.waiters[name].get(waiter_id)
        if event is None:
            # Defensive only: register() is always called first.
            event = self.waiters[name][waiter_id] = asyncio.Event()

        future = event.wait()
        if timeout is not None:
            future = wait_for(future, timeout)

        try:
            await future
            return True
        except TimeoutError:
            return False
        finally:
            with suppress(KeyError):
                del self.waiters[name][waiter_id]
            if not self.waiters[name]:
                with suppress(KeyError):
                    del self.waiters[name]

    @log_errors
    def notify(self, name=None, n=1):
        """Wake up to ``n`` of the waiters currently registered for ``name``."""
        woken = 0
        for event in list(self.waiters.get(name, {}).values()):
            if woken >= n:
                break
            if not event.is_set():
                event.set()
                woken += 1

    @log_errors
    def notify_all(self, name=None):
        """Wake every waiter currently registered for ``name``."""
        for event in self.waiters.get(name, {}).values():
            event.set()


class Condition:
    """Distributed Condition variable, equivalent to ``asyncio.Condition``/``threading.Condition``

    A Condition is always associated with a :class:`~distributed.Lock`. It
    must be acquired before ``wait``/``notify``/``notify_all`` are called,
    exactly like the standard library equivalents.

    Parameters
    ----------
    name: string (optional)
        Name of the condition.  Choosing the same name allows two
        disconnected processes to coordinate.  If not given, a random
        name will be generated.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given,
        the default global client will be used.
    lock: Lock (optional)
        Lock to associate with this condition.  If not given, a new one is
        created.

    Examples
    --------
    >>> condition = Condition('a')  # doctest: +SKIP
    >>> with condition:  # doctest: +SKIP
    ...     condition.wait(timeout=1)

    >>> # in another process
    >>> condition = Condition('a')  # doctest: +SKIP
    >>> with condition:  # doctest: +SKIP
    ...     condition.notify()
    """

    def __init__(self, name=None, client=None, lock=None):
        self._client = client
        self.name = name or f"condition-{uuid.uuid4().hex}"

        if lock is None:
            lock = Lock(f"{self.name}-lock")
        elif not isinstance(lock, Lock):
            raise TypeError(f"lock must be a Lock, not {type(lock)}")
        self._lock = lock

    @property
    def client(self):
        if not self._client:
            try:
                self._client = get_client()
            except ValueError:
                pass
        return self._client

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(
                f"{type(self)} object not properly initialized. This can happen"
                " if the object is being deserialized outside of the context of"
                " a Client or Worker."
            )

    def __repr__(self):
        return f"<Condition: {self.name}>"

    def acquire(self, timeout=None):
        """Acquire the underlying lock. See :meth:`Lock.acquire`."""
        self._verify_running()
        return self._lock.acquire(timeout=timeout)

    def release(self):
        """Release the underlying lock."""
        self._verify_running()
        return self._lock.release()

    def locked(self):
        """Return True if the underlying lock is currently held."""
        self._verify_running()
        return self._lock.locked()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.release()

    async def _wait(self, timeout=None):
        if not self._lock._leases:
            raise RuntimeError("cannot wait on un-acquired lock")

        waiter_id = uuid.uuid4().hex

        # Register the waiter's event *before* releasing the lock so that a
        # concurrent notify() can't run in the gap and be missed.
        await self.client.scheduler.condition_register(
            name=self.name, waiter_id=waiter_id
        )
        await self.release()

        try:
            return await self.client.scheduler.condition_wait(
                name=self.name, waiter_id=waiter_id, timeout=timeout
            )
        finally:
            await self.acquire()

    def wait(self, timeout=None):
        """Wait until notified.

        Must be called while holding the lock (i.e. inside ``with``/``async
        with``). Releases the lock, blocks until notified by ``notify``/
        ``notify_all`` (or until ``timeout`` elapses), then re-acquires the
        lock before returning.

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Seconds to wait for a notification.  Instead of a number of
            seconds, a timedelta string such as ``"200ms"`` may be given.

        Returns
        -------
        bool
            True if woken by a notification; False if ``timeout`` elapsed.
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._wait, timeout=timeout)

    async def _wait_for(self, predicate, timeout=None):
        deadline = Deadline.after(timeout)
        result = predicate()
        while not result:
            if deadline.expired:
                return predicate()
            # Call the raw async primitive directly rather than the dual-mode
            # ``wait()``: this coroutine is already running inside the
            # client's own event loop (via ``client.sync`` below), so
            # ``self.wait(...)`` would itself return an unawaited coroutine
            # here instead of a bool.
            woken = await self._wait(timeout=deadline.remaining)
            if not woken:
                return predicate()
            result = predicate()
        return result

    def wait_for(self, predicate, timeout=None):
        """Wait until ``predicate()`` returns a truthy value.

        Parameters
        ----------
        predicate : callable
            Called with no arguments; ``wait_for`` returns once this
            returns something truthy.
        timeout : number or string or timedelta, optional
            Overall time budget, across all internal ``wait`` calls.

        Returns
        -------
        The truthy value returned by ``predicate``, or its last (falsy)
        return value if ``timeout`` elapsed first.
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._wait_for, predicate=predicate, timeout=timeout)

    def notify(self, n=1):
        """Wake up to ``n`` waiters (default: 1)."""
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify, name=self.name, n=n
        )

    def notify_all(self):
        """Wake up all waiters."""
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all, name=self.name
        )

    def __getstate__(self):
        return (self.name, self._lock)

    def __setstate__(self, state):
        name, lock = state
        self.__init__(name=name, lock=lock)
