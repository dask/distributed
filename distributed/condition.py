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
    """Scheduler extension managing Condition lock and notifications

    State managed:
    - _locks: Which client holds which condition's lock
    - _acquire_queue: Clients waiting to acquire lock (FIFO)
    - _waiters: Clients in wait() (released lock, awaiting notify)
    - _client_conditions: Reverse index for cleanup on disconnect
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler

        # {condition_name: client_id} - who holds each lock
        self._locks = {}

        # {condition_name: deque[(client_id, future)]} - waiting to acquire
        self._acquire_queue = defaultdict(deque)

        # {condition_name: {waiter_id: (client_id, event, reacquire_future)}}
        # - clients in wait(), will need to reacquire after notify
        self._waiters = defaultdict(dict)

        # {client_id: set(condition_names)} - for cleanup on disconnect
        self._client_conditions = defaultdict(set)

        self.scheduler.handlers.update(
            {
                "condition_acquire": self.acquire,
                "condition_release": self.release,
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )

        # Register cleanup on client disconnect
        self.scheduler.extensions["conditions"] = self

    def _track_client(self, name, client_id):
        """Track that a client is using this condition"""
        self._client_conditions[client_id].add(name)

    def _untrack_client(self, name, client_id):
        """Stop tracking client for this condition"""
        if client_id in self._client_conditions:
            self._client_conditions[client_id].discard(name)
            if not self._client_conditions[client_id]:
                del self._client_conditions[client_id]

    @log_errors
    async def acquire(self, name=None, client_id=None):
        """Acquire lock - blocks until available"""
        self._track_client(name, client_id)

        if name not in self._locks:
            # Lock is free
            self._locks[name] = client_id
            return True

        if self._locks[name] == client_id:
            # Re-entrant acquire (from same client)
            return True

        # Lock is held - queue up and wait
        future = asyncio.Future()
        self._acquire_queue[name].append((client_id, future))

        try:
            await future
            return True
        except asyncio.CancelledError:
            # Remove from queue if cancelled
            queue = self._acquire_queue.get(name, deque())
            try:
                queue.remove((client_id, future))
            except ValueError:
                pass  # Already removed
            raise

    def _wake_next_acquirer(self, name):
        """Wake the next client waiting to acquire this lock"""
        queue = self._acquire_queue.get(name, deque())

        while queue:
            client_id, future = queue.popleft()
            if not future.done():
                self._locks[name] = client_id
                future.set_result(True)
                return True

        # No waiters left
        if name in self._acquire_queue:
            del self._acquire_queue[name]
        return False

    @log_errors
    async def release(self, name=None, client_id=None):
        """Release lock"""
        if name not in self._locks:
            raise RuntimeError("Released too often")

        if self._locks[name] != client_id:
            raise RuntimeError("Cannot release lock held by another client")

        del self._locks[name]

        # Wake next waiter trying to acquire
        if not self._wake_next_acquirer(name):
            # No acquire waiters - cleanup if no notify waiters either
            if name not in self._waiters:
                self._untrack_client(name, client_id)

    @log_errors
    async def wait(self, name=None, waiter_id=None, client_id=None, timeout=None):
        """Release lock, wait for notify, reacquire lock

        Critical: Register for notify BEFORE releasing lock to prevent lost wakeup
        """
        # Verify caller holds lock
        if self._locks.get(name) != client_id:
            raise RuntimeError("wait() called without holding the lock")

        # 1. Register for notification FIRST (prevents lost wakeup)
        notify_event = asyncio.Event()
        reacquire_future = asyncio.Future()
        self._waiters[name][waiter_id] = (client_id, notify_event, reacquire_future)

        # 2. Release lock (allows notifier to proceed)
        await self.release(name=name, client_id=client_id)

        # 3. Wait for notification
        wait_future = notify_event.wait()
        if timeout is not None:
            wait_future = wait_for(wait_future, timeout)

        notified = False
        try:
            await wait_future
            notified = True
        except TimeoutError:
            notified = False
        except asyncio.CancelledError:
            # Cancelled - cleanup and don't reacquire
            self._waiters[name].pop(waiter_id, None)
            if not self._waiters[name]:
                del self._waiters[name]
            raise
        finally:
            # Cleanup waiter registration
            self._waiters[name].pop(waiter_id, None)
            if not self._waiters[name]:
                del self._waiters[name]

        # 4. Reacquire lock before returning
        # This might block if other clients are waiting
        await self.acquire(name=name, client_id=client_id)

        return notified

    @log_errors
    def notify(self, name=None, client_id=None, n=1):
        """Wake up n waiters"""
        # Verify caller holds lock
        if self._locks.get(name) != client_id:
            raise RuntimeError("notify() called without holding the lock")

        waiters = self._waiters.get(name, {})
        count = 0

        for waiter_id in list(waiters.keys())[:n]:
            _, event, _ = waiters[waiter_id]
            event.set()
            count += 1

        return count

    @log_errors
    def notify_all(self, name=None, client_id=None):
        """Wake up all waiters"""
        # Verify caller holds lock
        if self._locks.get(name) != client_id:
            raise RuntimeError("notify_all() called without holding the lock")

        waiters = self._waiters.get(name, {})

        for _, event, _ in waiters.values():
            event.set()

        return len(waiters)

    async def remove_client(self, client):
        """Cleanup when client disconnects"""
        conditions = self._client_conditions.pop(client, set())

        for name in conditions:
            # Release any locks held by this client
            if self._locks.get(name) == client:
                try:
                    await self.release(name=name, client_id=client)
                except Exception as e:
                    logger.warning(f"Error releasing lock for {name}: {e}")

            # Cancel acquire waiters from this client
            queue = self._acquire_queue.get(name, deque())
            to_remove = []
            for i, (cid, future) in enumerate(queue):
                if cid == client and not future.done():
                    future.cancel()
                    to_remove.append(i)
            for i in reversed(to_remove):
                try:
                    del queue[i]
                except IndexError:
                    pass

            # Cancel notify waiters from this client
            waiters = self._waiters.get(name, {})
            to_remove = []
            for waiter_id, (cid, event, reacq) in waiters.items():
                if cid == client:
                    event.set()  # Wake them up so they can cleanup
                    if not reacq.done():
                        reacq.cancel()
                    to_remove.append(waiter_id)
            for wid in to_remove:
                waiters.pop(wid, None)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Provides wait/notify synchronization across distributed clients.
    Multiple Condition instances with the same name share state.

    Examples
    --------
    >>> condition = Condition('data-ready')
    >>>
    >>> # Consumer
    >>> async with condition:
    ...     while not data_available():
    ...         await condition.wait()
    ...     process_data()
    >>>
    >>> # Producer
    >>> async with condition:
    ...     produce_data()
    ...     condition.notify_all()
    """

    def __init__(self, name=None, client=None):
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self._waiter_id = uuid.uuid4().hex
        self._client = client
        self._is_locked = False

    @property
    def client(self):
        if not self._client:
            try:
                self._client = get_client()
            except ValueError:
                pass
        return self._client

    @property
    def _client_id(self):
        """Use actual Dask client ID - all Conditions in same client share identity"""
        if self.client:
            return self.client.id
        raise RuntimeError(f"{type(self).__name__} requires a connected client")

    @property
    def loop(self):
        return self.client.loop

    def _verify_running(self):
        if not self.client:
            raise RuntimeError(
                f"{type(self)} object not properly initialized. "
                "This can happen if the object is being deserialized "
                "outside of the context of a Client or Worker."
            )

    async def acquire(self):
        """Acquire the lock"""
        self._verify_running()
        await self.client.scheduler.condition_acquire(
            name=self.name, client_id=self._client_id
        )
        self._is_locked = True

    async def release(self):
        """Release the lock"""
        self._verify_running()
        await self.client.scheduler.condition_release(
            name=self.name, client_id=self._client_id
        )
        self._is_locked = False

    async def wait(self, timeout=None):
        """Wait for notification

        Must be called while holding the lock. Atomically releases lock,
        waits for notify(), then reacquires lock before returning.

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds

        Returns
        -------
        bool
            True if notified, False if timeout
        """
        if not self._is_locked:
            raise RuntimeError("wait() called without holding the lock")

        self._verify_running()
        timeout = parse_timedelta(timeout)

        # Scheduler handles atomic release/wait/reacquire
        result = await self.client.scheduler.condition_wait(
            name=self.name,
            waiter_id=self._waiter_id,
            client_id=self._client_id,
            timeout=timeout,
        )

        # Lock is reacquired when this returns
        # _is_locked stays True
        return result

    def notify(self, n=1):
        """Wake up n waiters (default 1)"""
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
        """Return True if this instance holds the lock"""
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
