from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict, deque

from dask.utils import parse_timedelta

from distributed.utils import SyncMethodMixin, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension managing Condition lock and notifications"""

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # {condition_name: client_id} - who holds each lock
        self._lock_holders = {}
        # {condition_name: deque of (client_id, future)} - waiting to acquire
        self._acquire_waiters = defaultdict(deque)
        # {condition_name: {waiter_id: (client_id, Event)}} - waiting for notify
        self._notify_waiters = defaultdict(dict)

        self.scheduler.handlers.update(
            {
                "condition_acquire": self.acquire,
                "condition_release": self.release,
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )

    @log_errors
    async def acquire(self, name=None, client_id=None):
        """Acquire lock - blocks until available"""
        if name not in self._lock_holders:
            # Lock is free
            self._lock_holders[name] = client_id
            return True

        if self._lock_holders[name] == client_id:
            # Already hold it (shouldn't happen in normal use)
            return True

        # Lock is held by someone else - wait our turn
        future = asyncio.Future()
        self._acquire_waiters[name].append((client_id, future))
        await future
        return True

    @log_errors
    async def release(self, name=None, client_id=None):
        """Release lock"""
        if name not in self._lock_holders:
            raise RuntimeError("Released too often")

        if self._lock_holders[name] != client_id:
            raise RuntimeError("Cannot release lock held by another client")

        del self._lock_holders[name]

        # Wake next waiter if any
        waiters = self._acquire_waiters.get(name, deque())
        while waiters:
            next_client_id, future = waiters.popleft()
            if not future.done():
                self._lock_holders[name] = next_client_id
                future.set_result(True)
                break

    @log_errors
    async def wait(self, name=None, waiter_id=None, client_id=None, timeout=None):
        """Release lock, wait for notify, reacquire lock"""
        # Verify caller holds lock
        if self._lock_holders.get(name) != client_id:
            raise RuntimeError("wait() called without holding the lock")

        # Release lock (waking next acquire waiter if any)
        await self.release(name=name, client_id=client_id)

        # Register as notify waiter
        event = asyncio.Event()
        self._notify_waiters[name][waiter_id] = (client_id, event)

        # Wait for notification
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
            self._notify_waiters[name].pop(waiter_id, None)
            if not self._notify_waiters[name]:
                del self._notify_waiters[name]

            # Reacquire lock - blocks until available
            await self.acquire(name=name, client_id=client_id)

        return result

    @log_errors
    def notify(self, name=None, client_id=None, n=1):
        """Wake up n waiters"""
        # Verify caller holds lock
        if self._lock_holders.get(name) != client_id:
            raise RuntimeError("notify() called without holding the lock")

        waiters = self._notify_waiters.get(name, {})
        count = 0
        for _, (_, event) in list(waiters.items())[:n]:
            event.set()
            count += 1
        return count

    @log_errors
    def notify_all(self, name=None, client_id=None):
        """Wake up all waiters"""
        # Verify caller holds lock
        if self._lock_holders.get(name) != client_id:
            raise RuntimeError("notify_all() called without holding the lock")

        waiters = self._notify_waiters.get(name, {})
        for _, event in waiters.values():
            event.set()
        return len(waiters)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable"""

    def __init__(self, name=None, client=None):
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self._waiter_id = uuid.uuid4().hex
        self._client_id = uuid.uuid4().hex
        self._client = client
        self._is_locked = False  # Track local state

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
        return self.client.loop

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(f"{type(self)} object not properly initialized")

    async def acquire(self):
        """Acquire lock"""
        self._verify_running()
        await self.client.scheduler.condition_acquire(
            name=self.name, client_id=self._client_id
        )
        self._is_locked = True

    async def release(self):
        """Release lock"""
        self._verify_running()
        await self.client.scheduler.condition_release(
            name=self.name, client_id=self._client_id
        )
        self._is_locked = False

    async def wait(self, timeout=None):
        """Wait for notification - atomically releases and reacquires lock"""
        if not self._is_locked:
            raise RuntimeError("wait() called without holding the lock")

        self._verify_running()
        timeout = parse_timedelta(timeout)

        # This handles release, wait, reacquire atomically on scheduler
        result = await self.client.scheduler.condition_wait(
            name=self.name,
            waiter_id=self._waiter_id,
            client_id=self._client_id,
            timeout=timeout,
        )
        # Lock is reacquired by the time this returns
        return result

    def notify(self, n=1):
        """Wake up n waiters"""
        if not self._is_locked:
            raise RuntimeError("notify() called without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify,
            name=self.name,
            client_id=self._client_id,
            n=n,
        )

    def notify_all(self):
        """Wake up all waiters"""
        if not self._is_locked:
            raise RuntimeError("notify_all() called without holding the lock")
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all,
            name=self.name,
            client_id=self._client_id,
        )

    def locked(self):
        """Return True if lock is held by this instance"""
        return self._is_locked

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
