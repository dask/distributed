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

    A Condition provides wait/notify synchronization across distributed clients.
    The lock is NOT re-entrant - attempting to acquire while holding will block.

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

        # {condition_name: deque[(client_id, future)]} - FIFO queue waiting to acquire
        self._acquire_queue = defaultdict(deque)

        # {condition_name: {waiter_id: (client_id, event)}} - clients in wait()
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
                "condition_locked": self.locked,
            }
        )

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
        """Acquire lock - blocks until available

        NOT re-entrant: if same client tries to acquire while holding,
        it will block like any other client.
        """
        self._track_client(name, client_id)

        if name not in self._locks:
            # Lock is free
            self._locks[name] = client_id
            return True

        # Lock is held (even if by same client) - must wait
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
                pass  # Already removed or processed
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

        # No waiters left, clean up queue
        if name in self._acquire_queue:
            del self._acquire_queue[name]
        return False

    @log_errors
    async def release(self, name=None, client_id=None):
        """Release lock

        Raises RuntimeError if:
        - Lock is not held
        - Lock is held by a different client
        """
        if name not in self._locks:
            raise RuntimeError("release() called without holding the lock")

        if self._locks[name] != client_id:
            raise RuntimeError("release() called without holding the lock")

        del self._locks[name]

        # Wake next waiter trying to acquire
        if not self._wake_next_acquirer(name):
            # No acquire waiters - cleanup if no notify waiters either
            if name not in self._waiters or not self._waiters[name]:
                self._untrack_client(name, client_id)

    @log_errors
    async def wait(self, name=None, waiter_id=None, client_id=None, timeout=None):
        """Release lock, wait for notify, reacquire lock

        This is atomic from the caller's perspective. The lock is physically
        released to allow notifiers to proceed, but the waiter will reacquire
        before returning.

        Returns:
        - True if notified
        - False if timeout occurred
        """
        # Verify caller holds lock
        if self._locks.get(name) != client_id:
            raise RuntimeError("wait() called without holding the lock")

        # 1. Register for notification FIRST (prevents lost wakeup race)
        notify_event = asyncio.Event()
        self._waiters[name][waiter_id] = (client_id, notify_event)

        # 2. Release lock (allows notifier to proceed)
        del self._locks[name]
        self._wake_next_acquirer(name)

        # 3. Wait for notification
        notified = False
        try:
            if timeout is not None:
                await wait_for(notify_event.wait(), timeout)
            else:
                await notify_event.wait()
            notified = True
        except (TimeoutError, asyncio.TimeoutError):
            notified = False
        except asyncio.CancelledError:
            # On cancellation, still cleanup and don't reacquire
            self._waiters[name].pop(waiter_id, None)
            if not self._waiters[name]:
                del self._waiters[name]
            raise
        finally:
            # Always cleanup waiter registration (except CancelledError which raises above)
            if waiter_id in self._waiters.get(name, {}):
                self._waiters[name].pop(waiter_id, None)
                if not self._waiters[name]:
                    del self._waiters[name]

        # 4. Reacquire lock before returning (will block if others waiting)
        await self.acquire(name=name, client_id=client_id)

        return notified

    @log_errors
    def notify(self, name=None, client_id=None, n=1):
        """Wake up n waiters (default 1)

        Must be called while holding the lock.
        Returns number of waiters actually notified.
        """
        if self._locks.get(name) != client_id:
            raise RuntimeError("notify() called without holding the lock")

        waiters = self._waiters.get(name, {})
        count = 0

        # Wake first n waiters
        for waiter_id in list(waiters.keys())[:n]:
            _, event = waiters[waiter_id]
            if not event.is_set():
                event.set()
                count += 1

        return count

    @log_errors
    def notify_all(self, name=None, client_id=None):
        """Wake up all waiters

        Must be called while holding the lock.
        Returns number of waiters actually notified.
        """
        if self._locks.get(name) != client_id:
            raise RuntimeError("notify_all() called without holding the lock")

        waiters = self._waiters.get(name, {})
        count = 0

        for _, event in waiters.values():
            if not event.is_set():
                event.set()
                count += 1

        return count

    def locked(self, name=None, client_id=None):
        """Check if this client holds the lock"""
        return self._locks.get(name) == client_id

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
            # Remove in reverse to maintain indices
            for i in reversed(to_remove):
                try:
                    del queue[i]
                except IndexError:
                    pass

            # Wake and cleanup notify waiters from this client
            waiters = self._waiters.get(name, {})
            for waiter_id, (cid, event) in list(waiters.items()):
                if cid == client:
                    event.set()  # Wake them so they can cleanup
                    waiters.pop(waiter_id, None)


class Condition(SyncMethodMixin):
    """Distributed Condition Variable

    Provides wait/notify synchronization across distributed clients.
    Multiple Condition instances with the same name share state.

    The lock is NOT re-entrant. Attempting to acquire while holding
    will block indefinitely.

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

    Notes
    -----
    Like threading.Condition, wait() atomically releases the lock and
    waits for notification, then reacquires before returning. This means
    the condition can change between being notified and reacquiring the lock,
    so always use wait() in a while loop checking the actual condition.
    """

    def __init__(self, name=None, client=None):
        self.name = name or f"condition-{uuid.uuid4().hex}"
        self._waiter_id = None  # Created fresh for each wait() call
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
    def _client_id(self):
        """Use actual Dask client ID"""
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
        """Acquire the lock

        Blocks until the lock is available. NOT re-entrant.
        """
        self._verify_running()
        await self.client.scheduler.condition_acquire(
            name=self.name, client_id=self._client_id
        )

    async def release(self):
        """Release the lock

        Raises RuntimeError if not holding the lock.
        """
        self._verify_running()
        await self.client.scheduler.condition_release(
            name=self.name, client_id=self._client_id
        )

    async def wait(self, timeout=None):
        """Wait for notification

        Must be called while holding the lock. Atomically releases lock,
        waits for notify(), then reacquires lock before returning.

        Because the lock is released and reacquired, the condition may have
        changed by the time this returns. Always use in a while loop:

        >>> async with condition:
        ...     while not predicate():
        ...         await condition.wait()

        Parameters
        ----------
        timeout : float, optional
            Maximum time to wait in seconds. If None, wait indefinitely.

        Returns
        -------
        bool
            True if notified, False if timeout occurred
        """
        self._verify_running()
        timeout = parse_timedelta(timeout)

        # Create fresh waiter_id for this wait() call
        waiter_id = uuid.uuid4().hex

        result = await self.client.scheduler.condition_wait(
            name=self.name,
            waiter_id=waiter_id,
            client_id=self._client_id,
            timeout=timeout,
        )

        return result

    def notify(self, n=1):
        """Wake up n waiters (default 1)

        Must be called while holding the lock.

        Parameters
        ----------
        n : int
            Number of waiters to wake up

        Returns
        -------
        int
            Number of waiters actually notified
        """
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify,
            name=self.name,
            client_id=self._client_id,
            n=n,
        )

    def notify_all(self):
        """Wake up all waiters

        Must be called while holding the lock.

        Returns
        -------
        int
            Number of waiters actually notified
        """
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_notify_all,
            name=self.name,
            client_id=self._client_id,
        )

    def locked(self):
        """Check if this client holds the lock

        Returns
        -------
        bool
            True if this client currently holds the lock
        """
        self._verify_running()
        return self.client.sync(
            self.client.scheduler.condition_locked,
            name=self.name,
            client_id=self._client_id,
        )

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
        return False

    def __enter__(self):
        return self.sync(self.__aenter__)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sync(self.__aexit__, exc_type, exc_val, exc_tb)

    def __repr__(self):
        return f"<Condition: {self.name}>"

    def __reduce__(self):
        return (Condition, (self.name,))
