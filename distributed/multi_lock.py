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
    """An extension for the scheduler to manage Locks

    This adds the following routes to the scheduler

    *  multi_lock_acquire
    *  multi_lock_release
    """

    # self.locks: Dict[Hashable, List[Hashable]] = {}
    # self.waiters: Dict[Hashable, List[Hashable]] = {}

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.locks = defaultdict(list)
        self.waiters = defaultdict(set)
        self.events = {}

        self.scheduler.handlers.update(
            {"multi_lock_acquire": self.acquire, "multi_lock_release": self.release}
        )

        self.scheduler.extensions["multi_locks"] = self

    def _request_locks(self, locks, id):
        assert id not in self.waiters
        self.waiters[id] = set()
        all_locks_acquired = True
        for lock in locks:
            self.locks[lock].append(id)
            if len(self.locks[lock]) > 1:
                self.waiters[id].add(lock)
                all_locks_acquired = False
        return all_locks_acquired

    def _refain_locks(self, locks, id):
        for lock in locks:
            self.locks[lock].remove(id)
            assert id not in self.locks[lock]
        del self.waiters[id]

    async def acquire(self, comm=None, locks=None, id=None, timeout=None):
        print(
            f"acquire() - locks: {locks}, id: {id}, self.locks: {dict(self.locks)}, self.waiters: {dict(self.waiters)}"
        )
        with log_errors():
            if not self._request_locks(locks, id):
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
            return True

    def release(self, comm=None, locks=None, id=None):
        print(
            f"release() - locks: {locks}, id: {id}, self.locks: {dict(self.locks)}, self.waiters: {dict(self.waiters)}"
        )
        with log_errors():
            waiters_ready = set()
            for lock in locks:
                assert self.locks[lock][0] == id
                self.locks[lock].pop(0)
                assert id not in self.locks[lock]
                if len(self.locks[lock]) > 0:
                    self.waiters[self.locks[lock][0]].remove(lock)
                    if len(self.waiters[self.locks[lock][0]]) == 0:
                        waiters_ready.add(self.locks[lock][0])
            del self.waiters[id]

            for waiter in waiters_ready:
                self.scheduler.loop.add_callback(self.events[waiter].set)


class MultiLock:
    """Distributed Centralized Lock

    Parameters
    ----------
    lock_names: List[str]
        Names of the locks to acquire.  Choosing the same name allows two
        disconnected processes to coordinate a lock.
    client: Client (optional)
        Client to use for communication with the scheduler.  If not given, the
        default global client will be used.

    Examples
    --------
    >>> lock = Lock(['x', 'y'])  # doctest: +SKIP
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
        self.id = uuid.uuid4().hex[0:6]
        self._locked = False

    def acquire(self, blocking=True, timeout=None):
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

        Examples
        --------
        >>> lock = Lock(['x', 'y'])  # doctest: +SKIP
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
        )
        self._locked = True
        return result

    def release(self):
        """ Release the lock if already acquired """
        if not self.locked():
            raise ValueError("Lock is not yet acquired")
        ret = self.client.sync(
            self.client.scheduler.multi_lock_release, locks=self.lock_names, id=self.id
        )
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
