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
    """Scheduler extension for managing distributed Conditions.

    Waiters are keyed by (name, waiter_id) so multiple waiters from the
    same client can coexist without overwriting each other's events.
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # name -> {waiter_id -> asyncio.Event}
        self.waiters = defaultdict(dict)
        self._closed = False

        self.scheduler.handlers.update(
            {
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )
        self.scheduler.extensions["conditions"] = self

    async def close(self):
        """Cancel all pending waiters so scheduler can shut down cleanly."""
        self._closed = True
        for name in list(self.waiters):
            for event in self.waiters[name].values():
                event.set()
            del self.waiters[name]

    @log_errors
    async def wait(self, name=None, waiter_id=None, **kwargs):
        """Register a waiter and block until notified or closed."""
        if self._closed:
            return

        event = asyncio.Event()
        self.waiters[name][waiter_id] = event

        try:
            await event.wait()
        finally:
            with suppress(KeyError):
                del self.waiters[name][waiter_id]
                if not self.waiters[name]:
                    del self.waiters[name]

    async def notify(self, name=None, n=1, **kwargs):
        """Wake up to n waiters."""
        if name not in self.waiters:
            return

        notified = 0
        for wid in list(self.waiters[name]):
            if notified >= n:
                break
            event = self.waiters[name].get(wid)
            if event and not event.is_set():
                event.set()
                notified += 1

    async def notify_all(self, name=None, **kwargs):
        """Wake all waiters."""
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
    ...     await condition.wait()

    >>> async with condition:  # doctest: +SKIP
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
        self._verify_running()
        return await self._lock.acquire(timeout=timeout)

    async def release(self):
        self._verify_running()
        return await self._lock.release()

    def locked(self):
        return self._lock.locked()

    async def wait(self, timeout=None):
        """Wait until notified.

        Releases the underlying lock, waits until notified, then reacquires
        the lock before returning. Must be called with the lock held.

        Returns True if woken by notify, False on timeout.
        """
        self._verify_running()
        await self.release()

        # Each wait() call gets a unique ID so the scheduler can track
        # multiple waiters from the same client independently.
        waiter_id = uuid.uuid4().hex

        try:
            coro = self.client.scheduler.condition_wait(
                name=self.name, waiter_id=waiter_id
            )
            if timeout is not None:
                try:
                    await asyncio.wait_for(coro, timeout=timeout)
                    return True
                except asyncio.TimeoutError:
                    return False
            else:
                await coro
                return True
        finally:
            # Always reacquire lock — mirrors asyncio.Condition semantics
            try:
                await self.acquire()
            except asyncio.CancelledError:
                with suppress(Exception):
                    await asyncio.shield(self.acquire())
                raise

    async def wait_for(self, predicate, timeout=None):
        """Wait until predicate() returns True.

        Returns the predicate result (True unless timeout).
        """
        result = predicate()
        if result:
            return result

        if timeout is not None:
            import time

            deadline = time.monotonic() + timeout
            while not result:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return predicate()
                if not await self.wait(timeout=remaining):
                    return predicate()
                result = predicate()
        else:
            while not result:
                await self.wait()
                result = predicate()

        return result

    async def notify(self, n=1):
        """Wake up n waiters (default: 1)."""
        self._verify_running()
        await self.client.scheduler.condition_notify(name=self.name, n=n)

    async def notify_all(self):
        """Wake up all waiters."""
        self._verify_running()
        await self.client.scheduler.condition_notify_all(name=self.name)
