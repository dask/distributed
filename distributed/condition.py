from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict

from distributed.utils import SyncMethodMixin, log_errors
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension for managing distributed Conditions"""

    def __init__(self, scheduler):
        self.scheduler = scheduler
        # {condition_name: asyncio.Condition}
        self._conditions = {}
        # {condition_name: set of waiter_ids}
        self._waiters = defaultdict(set)

        self.scheduler.handlers.update(
            {
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_acquire": self.acquire,
                "condition_release": self.release,
            }
        )

    def _get_condition(self, name):
        if name not in self._conditions:
            self._conditions[name] = asyncio.Condition()
        return self._conditions[name]

    @log_errors
    async def acquire(self, name=None, id=None):
        """Acquire the underlying lock"""
        condition = self._get_condition(name)
        await condition.acquire()
        return True

    @log_errors
    async def release(self, name=None, id=None):
        """Release the underlying lock"""
        if name not in self._conditions:
            return False
        condition = self._conditions[name]
        condition.release()
        return True

    @log_errors
    async def wait(self, name=None, id=None, timeout=None):
        """Wait on condition"""
        condition = self._get_condition(name)
        self._waiters[name].add(id)

        try:
            if timeout:
                await asyncio.wait_for(condition.wait(), timeout=timeout)
            else:
                await condition.wait()
            return True
        except asyncio.TimeoutError:
            return False
        except asyncio.CancelledError:
            raise
        finally:
            self._waiters[name].discard(id)
            if not self._waiters[name]:
                del self._waiters[name]

    @log_errors
    def notify(self, name=None, n=1):
        """Notify n waiters"""
        if name not in self._conditions:
            return 0
        condition = self._conditions[name]
        condition.notify(n=n)
        return min(n, len(self._waiters.get(name, [])))

    @log_errors
    def notify_all(self, name=None):
        """Notify all waiters"""
        if name not in self._conditions:
            return 0
        condition = self._conditions[name]
        count = len(self._waiters.get(name, []))
        condition.notify_all()
        return count


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Mimics asyncio.Condition API. Allows coordination between
    distributed workers using wait/notify pattern.

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

    def __init__(self, name=None, scheduler_rpc=None, loop=None):
        self._scheduler = scheduler_rpc
        self._loop = loop
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self.id = uuid.uuid4().hex
        self._locked = False

    def _get_scheduler_rpc(self):
        if self._scheduler:
            return self._scheduler
        try:
            client = get_client()
            return client.scheduler
        except ValueError:
            from distributed.worker import get_worker

            worker = get_worker()
            return worker.scheduler

    async def acquire(self):
        """Acquire underlying lock"""
        scheduler = self._get_scheduler_rpc()
        result = await scheduler.condition_acquire(name=self.name, id=self.id)
        self._locked = result
        return result

    async def release(self):
        """Release underlying lock"""
        if not self._locked:
            raise RuntimeError("Cannot release un-acquired lock")
        scheduler = self._get_scheduler_rpc()
        await scheduler.condition_release(name=self.name, id=self.id)
        self._locked = False

    async def wait(self, timeout=None):
        """Wait until notified

        Must be called while lock is held. Releases lock and waits
        for notify(), then reacquires lock before returning.
        """
        if not self._locked:
            raise RuntimeError("Cannot wait on un-acquired condition")

        scheduler = self._get_scheduler_rpc()
        result = await scheduler.condition_wait(
            name=self.name, id=self.id, timeout=timeout
        )
        return result

    async def notify(self, n=1):
        """Wake up one or more waiters"""
        if not self._locked:
            raise RuntimeError("Cannot notify on un-acquired condition")
        scheduler = self._get_scheduler_rpc()
        return await scheduler.condition_notify(name=self.name, n=n)

    async def notify_all(self):
        """Wake up all waiters"""
        if not self._locked:
            raise RuntimeError("Cannot notify on un-acquired condition")
        scheduler = self._get_scheduler_rpc()
        return await scheduler.condition_notify_all(name=self.name)

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
