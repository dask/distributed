import asyncio
from collections import defaultdict
import logging
import uuid

from .client import Client
from .utils import log_errors, TimeoutError
from .worker import get_worker
from .utils import parse_timedelta

logger = logging.getLogger(__name__)


class MultiLockExtension:
    """An extension for the scheduler to manage MultiLocks

    This adds the following routes to the scheduler

    *  multi_lock_acquire
    *  multi_lock_release
    """


    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.locks = defaultdict(list)  # lock -> users
        self.requests = {}  # user -> locks
        self.requests_left = {}  # user -> locks still needed
        self.events = {}

        self.scheduler.handlers.update(
            {"multi_lock_acquire": self.acquire, "multi_lock_release": self.release}
        )

        self.scheduler.extensions["multi_locks"] = self

    def _request_locks(self, locks, id, num_locks):
        assert id not in self.requests
        self.requests[id] = set(locks)
        assert len(locks) >= num_locks and num_locks > 0
        self.requests_left[id] = num_locks

        locks = sorted(locks, key=lambda x: len(self.locks[x]))
        for i, lock in enumerate(locks):
            self.locks[lock].append(id)
            if len(self.locks[lock]) == 1:  # The lock was free
                self.requests_left[id] -= 1
                if self.requests_left[id] == 0:  # Got all locks needed
                    # Since we got all locks need, we can remove the rest of the requests
                    self.requests[id] -= set(locks[i + 1 :])
                    return True
        return False

    def _refain_locks(self, locks, id):  # TODO: add test
        for lock in locks:
            if self.locks[lock][0] == id:
                self.locks[lock].pop(0)
                if self.locks[lock]:
                    self.requests_left[self.locks[lock][0]] -= 1
                    # TODO: maybe wake up `self.locks[lock][0]`
            else:
                self.locks[lock].remove(id)
            assert id not in self.locks[lock]
        del self.requests[id]
        del self.requests_left[id]

    async def acquire(
        self, comm=None, locks=None, id=None, timeout=None, num_locks=None
    ):

        with log_errors():
            if not self._request_locks(locks, id, num_locks):
                assert id not in self.events
                event = asyncio.Event()
                self.events[id] = event
                future = event.wait()
                if timeout is not None:
                    future = asyncio.wait_for(future, timeout)
                try:
                    await future
                except TimeoutError:
                    self._refain_locks(locks, id)
                    return False
                finally:
                    del self.events[id]
            # At this point `id` acquired all `locks`
            assert self.requests_left[id] == 0
            return True

    def release(self, comm=None, id=None):
        with log_errors():
            waiters_ready = set()
            for lock in self.requests[id]:
                assert self.locks[lock][0] == id
                self.locks[lock].pop(0)
                assert id not in self.locks[lock]
                if len(self.locks[lock]) > 0:
                    new_first = self.locks[lock][0]
                    self.requests_left[new_first] -= 1
                    if self.requests_left[new_first] <= 0:
                        self.requests_left[new_first] = 0
                        waiters_ready.add(new_first)
            del self.requests[id]
            del self.requests_left[id]

            for waiter in waiters_ready:
                self.scheduler.loop.add_callback(self.events[waiter].set)


class MultiLock:
    """Distributed Centralized Lock

    Parameters
    ----------
    lock_names: List[str]
        Names of the locks to acquire. Choosing the same name allows two
        disconnected processes to coordinate a lock.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> lock = MultiLock(['x', 'y'])  # doctest: +SKIP
    >>> lock.acquire(timeout=1)  # doctest: +SKIP
    >>> # do things with protected resource 'x' and 'y'
    >>> lock.release()  # doctest: +SKIP
    """

    def __init__(self, lock_names=[], client=None):
        try:
            self.client = client or Client.current()
        except ValueError:
            # Initialise new client
            self.client = get_worker().client

        self.lock_names = lock_names
        self.id = uuid.uuid4().hex
        self._locked = False

    def acquire(self, blocking=True, timeout=None, num_locks=None):
        """Acquire the lock

        Parameters
        ----------
        blocking : bool, optional
            If false, don't wait on the lock in the scheduler at all.
        timeout : string or number or timedelta, optional
            Seconds to wait on the lock in the scheduler.  This does not
            include local coroutine time, network transfer time, etc..
            It is forbidden to specify a timeout when blocking is false.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        num_locks : int, optional
            Number of locks needed. If None, all locks are needed

        Examples
        --------
        >>> lock = MultiLock(['x', 'y'])  # doctest: +SKIP
        >>> lock.acquire(timeout="1s")  # doctest: +SKIP

        Returns
        -------
        True or False whether or not it successfully acquired the lock
        """
        timeout = parse_timedelta(timeout)

        if not blocking:
            if timeout is not None:
                raise ValueError("can't specify a timeout for a non-blocking call")
            timeout = 0

        result = self.client.sync(
            self.client.scheduler.multi_lock_acquire,
            locks=self.lock_names,
            id=self.id,
            timeout=timeout,
            num_locks=num_locks or len(self.lock_names),
        )
        self._locked = True
        return result

    def release(self):
        """ Release the lock if already acquired """
        if not self.locked():
            raise ValueError("Lock is not yet acquired")
        ret = self.client.sync(self.client.scheduler.multi_lock_release, id=self.id)
        self._locked = False
        return ret

    def locked(self):
        return self._locked

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.release()

    def __reduce__(self):
        return (type(self), (self.lock_names,))
