from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from contextlib import suppress

from distributed.lock import Lock
from distributed.utils import log_errors
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension for managing distributed Conditions

    Tracks waiters for each condition variable and implements notify logic.
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # name -> {client_id -> asyncio.Event}
        self.waiters = defaultdict(dict)

        self.scheduler.handlers.update(
            {
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )

    @log_errors
    async def wait(self, name=None, client=None):
        """Register a waiter and block until notified"""
        # Create event for this specific waiter
        event = asyncio.Event()
        self.waiters[name][client] = event

        try:
            # Block until notified
            await event.wait()
        finally:
            # Cleanup after waking or cancellation
            with suppress(KeyError):
                del self.waiters[name][client]
                if not self.waiters[name]:
                    del self.waiters[name]

    async def notify(self, name=None, n=1):
        """Wake up n waiters"""
        if name not in self.waiters:
            return

        # Wake up to n waiters
        notified = 0
        for client_id in list(self.waiters[name].keys()):
            if notified >= n:
                break
            event = self.waiters[name].get(client_id)
            if event and not event.is_set():
                event.set()
                notified += 1

    async def notify_all(self, name=None):
        """Wake up all waiters"""
        if name not in self.waiters:
            return

        for event in self.waiters[name].values():
            if not event.is_set():
                event.set()


class Condition:
    """Distributed Condition Variable

    A distributed version of asyncio.Condition. Allows one or more clients
    to wait until notified by another client.

    Like asyncio.Condition, this must be used with a lock. The lock is
    released before waiting and reacquired afterwards.

    Parameters
    ----------
    name : str, optional
        Name of the condition. If not provided, a random name is generated.
    client : Client, optional
        Client instance. If not provided, uses the default client.
    lock : Lock, optional
        Lock to use with this condition. If not provided, creates a new Lock.

    Examples
    --------
    >>> from distributed import Client, Condition
    >>> client = Client()  # doctest: +SKIP
    >>> condition = Condition()  # doctest: +SKIP

    >>> async with condition:  # doctest: +SKIP
    ...     # Wait for some condition
    ...     await condition.wait()

    >>> async with condition:  # doctest: +SKIP
    ...     # Modify shared state
    ...     condition.notify()  # Wake one waiter
    """

    def __init__(self, name=None, client=None, lock=None):
        self._client = client
        self.name = name or "condition-" + uuid.uuid4().hex

        if lock is None:
            lock = Lock(client=client)
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
                f"{type(self)} object not properly initialized. Ensure it's created within a Client context."
            )

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    def __repr__(self):
        return f"<Condition: {self.name}>"

    async def acquire(self, timeout=None):
        """Acquire the underlying lock"""
        self._verify_running()
        return await self._lock.acquire(timeout=timeout)

    async def release(self):
        """Release the underlying lock"""
        self._verify_running()
        return await self._lock.release()

    def locked(self):
        """Return True if lock is held"""
        return self._lock.locked()

    async def wait(self, timeout=None):
        """Wait until notified.

        This method releases the underlying lock, waits until notified,
        then reacquires the lock before returning.

        Must be called with the lock held.

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds. If None, wait indefinitely.

        Returns
        -------
        bool
            True if woken by notify, False if timeout occurred

        Raises
        ------
        RuntimeError
            If called without holding the lock
        """
        self._verify_running()

        # Release lock before waiting (will error if not held)
        await self.release()

        # Track if we need to reacquire (for proper cleanup on cancellation)
        reacquired = False

        try:
            # Wait for notification from scheduler
            if timeout is not None:
                try:
                    await asyncio.wait_for(
                        self.client.scheduler.condition_wait(
                            name=self.name, client=self.client.id
                        ),
                        timeout=timeout,
                    )
                    return True
                except asyncio.TimeoutError:
                    return False
            else:
                await self.client.scheduler.condition_wait(
                    name=self.name, client=self.client.id
                )
                return True
        finally:
            # CRITICAL: Always reacquire lock, even on cancellation
            # This maintains the invariant that wait() returns with lock held
            try:
                await self.acquire()
                reacquired = True
            except asyncio.CancelledError:
                # If reacquisition is cancelled, we're in a bad state
                # Try again without allowing cancellation
                if not reacquired:
                    with suppress(Exception):
                        await asyncio.shield(self.acquire())
                raise

    async def wait_for(self, predicate, timeout=None):
        """Wait until a predicate becomes true.

        Parameters
        ----------
        predicate : callable
            Function that returns True when the condition is met
        timeout : float, optional
            Maximum time to wait in seconds

        Returns
        -------
        bool
            The predicate result (should be True unless timeout)
        """
        result = predicate()
        while not result:
            if timeout is not None:
                # Consume timeout across multiple waits
                import time

                start = time.time()
                if not await self.wait(timeout=timeout):
                    return predicate()
                timeout -= time.time() - start
                if timeout <= 0:
                    return predicate()
            else:
                await self.wait()
            result = predicate()
        return result

    async def notify(self, n=1):
        """Wake up n waiters (default: 1)

        Parameters
        ----------
        n : int
            Number of waiters to wake up
        """
        self._verify_running()
        await self.client.scheduler.condition_notify(name=self.name, n=n)

    async def notify_all(self):
        """Wake up all waiters"""
        self._verify_running()
        await self.client.scheduler.condition_notify_all(name=self.name)
