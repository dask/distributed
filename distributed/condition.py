from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict

from dask.utils import parse_timedelta

from distributed.semaphore import Semaphore
from distributed.utils import SyncMethodMixin, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension for managing Condition variable notifications

    This extension only handles wait/notify coordination.
    The underlying lock is a Semaphore managed by SemaphoreExtension.
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
        """Wait to be notified

        Caller must already hold the lock (Semaphore lease).
        This only manages the wait/notify Events.
        """
        # Create event for this waiter
        event = asyncio.Event()
        self._waiters[name][id] = event

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

        return result

    @log_errors
    def notify(self, name=None, n=1):
        """Notify n waiters"""
        waiters = self._waiters.get(name, {})
        count = 0
        for event in list(waiters.values())[:n]:
            event.set()
            count += 1
        return count

    @log_errors
    def notify_all(self, name=None):
        """Notify all waiters"""
        waiters = self._waiters.get(name, {})
        for event in waiters.values():
            event.set()
        return len(waiters)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Combines a Semaphore (lock) with wait/notify coordination.

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
    ...     await condition.wait()

    >>> # In another worker/client
    >>> condition = Condition('my-condition')
    >>> async with condition:
    ...     condition.notify()
    """

    def __init__(self, name=None, client=None):
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self.id = uuid.uuid4().hex
        # Use Semaphore(max_leases=1) as the underlying lock
        self._lock = Semaphore(max_leases=1, name=f"{self.name}-lock")
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
        """Acquire underlying lock"""
        result = await self._lock.acquire()
        return result

    async def release(self):
        """Release underlying lock"""
        await self._lock.release()

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
        if not self._lock.locked():
            raise RuntimeError("wait() called without holding the lock")

        self._verify_running()
        timeout = parse_timedelta(timeout)

        # Release lock
        await self._lock.release()

        # Wait for notification
        try:
            result = await self.client.scheduler.condition_wait(
                name=self.name, id=self.id, timeout=timeout
            )
        finally:
            # Reacquire lock
            await self._lock.acquire()

        return result

    def notify(self, n=1):
        """Wake up one or more waiters"""
        if not self._lock.locked():
            raise RuntimeError("Cannot notify without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify, name=self.name, n=n
        )

    def notify_all(self):
        """Wake up all waiters"""
        if not self._lock.locked():
            raise RuntimeError("Cannot notify without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all, name=self.name
        )

    def locked(self):
        """Return True if lock is held"""
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
