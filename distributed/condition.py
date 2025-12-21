from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict

from dask.utils import parse_timedelta

from distributed.lock import Lock
from distributed.utils import SyncMethodMixin, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension for managing Condition variable notifications

    Coordinates wait/notify between distributed clients.
    The lock itself is managed by LockExtension.
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # {condition_name: {waiter_id: asyncio.Event}}
        self._waiters = defaultdict(dict)

        self.scheduler.handlers.update(
            {
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )

    @log_errors
    async def wait(self, name=None, id=None, timeout=None):
        """Register waiter and block until notified

        Caller must have released the lock before calling this.
        """
        event = asyncio.Event()
        self._waiters[name][id] = event

        future = event.wait()
        if timeout is not None:
            future = wait_for(future, timeout)

        try:
            await future
            result = True
        except TimeoutError:
            result = False
        finally:
            self._waiters[name].pop(id, None)
            if not self._waiters[name]:
                del self._waiters[name]

        return result

    @log_errors
    def notify(self, name=None, n=1):
        """Wake up n waiters"""
        waiters = self._waiters.get(name, {})
        count = 0
        for event in list(waiters.values())[:n]:
            event.set()
            count += 1
        return count

    @log_errors
    def notify_all(self, name=None):
        """Wake up all waiters"""
        waiters = self._waiters.get(name, {})
        for event in waiters.values():
            event.set()
        return len(waiters)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Combines a Lock with wait/notify coordination across the cluster.

    Parameters
    ----------
    name : str, optional
        Name of the condition. Conditions with the same name share state.
    client : Client, optional
        Client for scheduler communication.

    Examples
    --------
    Producer-consumer pattern:

    >>> condition = Condition('data-ready')
    >>> # Consumer
    >>> async with condition:
    ...     while not data_available():
    ...         await condition.wait()
    ...     process_data()

    >>> # Producer
    >>> async with condition:
    ...     produce_data()
    ...     condition.notify_all()
    """

    def __init__(self, name=None, client=None):
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self.id = uuid.uuid4().hex
        self._lock = Lock(name=f"{self.name}-lock")
        self._client = client

    @property
    def client(self):
        if not self._client:
            try:
                self._client = get_client()
            except ValueError:
                pass
        return self._client

    @property
    def loop(self):
        return self._lock.loop

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(
                f"{type(self)} object not properly initialized. This can happen"
                " if the object is being deserialized outside of the context of"
                " a Client or Worker."
            )

    async def acquire(self):
        """Acquire the underlying lock"""
        return await self._lock.acquire()

    async def release(self):
        """Release the underlying lock"""
        await self._lock.release()

    async def wait(self, timeout=None):
        """Wait until notified

        Must be called while lock is held. Atomically releases lock,
        waits for notify(), then reacquires lock before returning.

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Maximum time to wait for notification.

        Returns
        -------
        bool
            True if notified, False if timeout occurred

        Raises
        ------
        RuntimeError
            If called without holding the lock
        """
        if not self._lock.locked():
            raise RuntimeError("wait() called without holding the lock")

        self._verify_running()
        timeout = parse_timedelta(timeout)

        # Atomically: release lock, wait for notify, reacquire lock
        await self._lock.release()
        try:
            result = await self.client.scheduler.condition_wait(
                name=self.name, id=self.id, timeout=timeout
            )
        finally:
            await self._lock.acquire()

        return result

    def notify(self, n=1):
        """Wake up one or more waiters

        Must be called while holding the lock.

        Parameters
        ----------
        n : int, optional
            Number of waiters to wake. Default is 1.

        Returns
        -------
        int
            Number of waiters actually notified
        """
        if not self._lock.locked():
            raise RuntimeError("notify() called without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify, name=self.name, n=n
        )

    def notify_all(self):
        """Wake up all waiters

        Must be called while holding the lock.

        Returns
        -------
        int
            Number of waiters notified
        """
        if not self._lock.locked():
            raise RuntimeError("notify_all() called without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all, name=self.name
        )

    def locked(self):
        """Return True if the lock is currently held"""
        return self._lock.locked()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()

    def __enter__(self):
        return self.sync(self.__aenter__)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sync(self.__aexit__, exc_type, exc_val, exc_tb)

    def __repr__(self):
        return f"<Condition: {self.name}>"

    def __reduce__(self):
        return (Condition, (self.name,))
