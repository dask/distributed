from __future__ import annotations

import asyncio
import logging
import uuid

from dask.utils import parse_timedelta

from distributed.utils import SyncMethodMixin, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension for managing distributed Conditions"""

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self._locks = {}  # {name: asyncio.Lock}
        self._lock_holders = {}  # {name: client_id}
        self._waiters = {}  # {name: {waiter_id: asyncio.Event}}

        self.scheduler.handlers.update(
            {
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_acquire": self.acquire,
                "condition_release": self.release,
                "condition_notify_all": self.notify_all,
            }
        )

    def _get_lock(self, name):
        if name not in self._locks:
            self._locks[name] = asyncio.Lock()
        return self._locks[name]

    @log_errors
    async def acquire(self, name=None, id=None):
        """Acquire the underlying lock"""
        lock = self._get_lock(name)
        await lock.acquire()
        self._lock_holders[name] = id
        return True

    @log_errors
    async def release(self, name=None, id=None):
        """Release the underlying lock"""
        if self._lock_holders.get(name) != id:
            return False

        lock = self._locks[name]
        lock.release()
        del self._lock_holders[name]

        # Cleanup if no waiters
        if name not in self._waiters or not self._waiters[name]:
            del self._locks[name]

        return True

    @log_errors
    async def wait(self, name=None, id=None, timeout=None):
        """Wait on condition"""
        # Verify lock is held by this client
        if self._lock_holders.get(name) != id:
            raise RuntimeError("wait() called without holding the lock")

        lock = self._locks[name]

        # Create event for this waiter
        if name not in self._waiters:
            self._waiters[name] = {}
        event = asyncio.Event()
        self._waiters[name][id] = event

        # Release lock
        lock.release()
        del self._lock_holders[name]

        # Wait on event
        future = event.wait()
        if timeout is not None:
            future = wait_for(future, timeout)

        try:
            await future
            result = True
        except TimeoutError:
            result = False
        finally:
            # Cleanup waiter
            self._waiters[name].pop(id, None)
            if not self._waiters[name]:
                del self._waiters[name]

        # Reacquire lock
        await lock.acquire()
        self._lock_holders[name] = id

        return result

    @log_errors
    def notify(self, name=None, n=1):
        """Notify n waiters"""
        if self._lock_holders.get(name) is None:
            raise RuntimeError("notify() called without holding the lock")

        waiters = self._waiters.get(name, {})
        count = 0
        for event in list(waiters.values())[:n]:
            event.set()
            count += 1
        return count

    @log_errors
    def notify_all(self, name=None):
        """Notify all waiters"""
        if self._lock_holders.get(name) is None:
            raise RuntimeError("notify_all() called without holding the lock")

        waiters = self._waiters.get(name, {})
        for event in waiters.values():
            event.set()
        return len(waiters)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Mimics asyncio.Condition API. Allows coordination between
    distributed workers using wait/notify pattern.

    Parameters
    ----------
    name : str, optional
        Name of the condition. Same name = shared state.
    client : Client, optional
        Client for scheduler communication.

    Examples
    --------
    >>> from distributed import Condition
    >>> condition = Condition('my-condition')
    >>> async with condition:
    ...     await condition.wait()  # Wait for notification

    >>> # In another worker/client
    >>> condition = Condition('my-condition')
    >>> async with condition:
    ...     condition.notify()  # Wake one waiter
    """

    def __init__(self, name=None, client=None):
        self._client = client
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self.id = uuid.uuid4().hex
        self._locked = False

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
        return self.client.loop if self.client else None

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(
                f"{type(self)} object not properly initialized. This can happen"
                " if the object is being deserialized outside of the context of"
                " a Client or Worker."
            )

    async def acquire(self):
        """Acquire underlying lock"""
        self._verify_running()
        result = await self.client.scheduler.condition_acquire(
            name=self.name, id=self.id
        )
        self._locked = result
        return result

    async def release(self):
        """Release underlying lock"""
        if not self._locked:
            raise RuntimeError("Cannot release un-acquired lock")
        self._verify_running()
        await self.client.scheduler.condition_release(name=self.name, id=self.id)
        self._locked = False

    async def wait(self, timeout=None):
        """Wait until notified

        Must be called while lock is held. Releases lock and waits
        for notify(), then reacquires lock before returning.

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Seconds to wait on the condition in the scheduler.

        Returns
        -------
        bool
            True if notified, False if timeout occurred
        """
        if not self._locked:
            raise RuntimeError("wait() called without holding the lock")

        self._verify_running()
        timeout = parse_timedelta(timeout)
        result = await self.client.scheduler.condition_wait(
            name=self.name, id=self.id, timeout=timeout
        )
        return result

    def notify(self, n=1):
        """Wake up one or more waiters

        Parameters
        ----------
        n : int, optional
            Number of waiters to wake. Default is 1.

        Returns
        -------
        int
            Number of waiters notified
        """
        if not self._locked:
            raise RuntimeError("Cannot notify without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify, name=self.name, n=n
        )

    def notify_all(self):
        """Wake up all waiters

        Returns
        -------
        int
            Number of waiters notified
        """
        if not self._locked:
            raise RuntimeError("Cannot notify without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all, name=self.name
        )

    def locked(self):
        """Return True if lock is held"""
        return self._locked

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
