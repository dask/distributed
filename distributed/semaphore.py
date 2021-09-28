import asyncio
import logging
import uuid
import warnings
from asyncio import TimeoutError
from collections import defaultdict, deque

from tornado.ioloop import IOLoop, PeriodicCallback

import dask
from dask.utils import parse_timedelta

from distributed.utils_comm import retry_operation

from .metrics import time
from .utils import log_errors, sync, thread_state
from .worker import get_client, get_worker

logger = logging.getLogger(__name__)


class _Watch:
    def __init__(self, duration=None):
        self.duration = duration
        self.started_at = None

    def start(self):
        self.started_at = time()

    def elapsed(self):
        return time() - self.started_at

    def leftover(self):
        if self.duration is None:
            return None
        else:
            return max(0, self.duration - self.elapsed())


class SemaphoreExtension:
    """An extension for the scheduler to manage Semaphores

    This adds the following routes to the scheduler

    * semaphore_acquire
    * semaphore_release
    * semaphore_close
    * semaphore_refresh_leases
    * semaphore_register
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler

        # {semaphore_name: asyncio.Event}
        self.events = defaultdict(asyncio.Event)
        # {semaphore_name: max_leases}
        self.max_leases = dict()
        # {semaphore_name: {lease_id: lease_last_seen_timestamp}}
        self.leases = defaultdict(dict)

        self.scheduler.handlers.update(
            {
                "semaphore_register": self.create,
                "semaphore_acquire": self.acquire,
                "semaphore_release": self.release,
                "semaphore_close": self.close,
                "semaphore_refresh_leases": self.refresh_leases,
                "semaphore_value": self.get_value,
            }
        )

        self.scheduler.extensions["semaphores"] = self

        # {metric_name: {semaphore_name: metric}}
        self.metrics = {
            "acquire_total": defaultdict(int),  # counter
            "release_total": defaultdict(int),  # counter
            "average_pending_lease_time": defaultdict(float),  # gauge
            "pending": defaultdict(int),  # gauge
        }

        validation_callback_time = parse_timedelta(
            dask.config.get("distributed.scheduler.locks.lease-validation-interval"),
            default="s",
        )
        self._pc_lease_timeout = PeriodicCallback(
            self._check_lease_timeout, validation_callback_time * 1000
        )
        self._pc_lease_timeout.start()
        self.lease_timeout = parse_timedelta(
            dask.config.get("distributed.scheduler.locks.lease-timeout"), default="s"
        )

    async def get_value(self, comm=None, name=None):
        return len(self.leases[name])

    # `comm` here is required by the handler interface
    def create(self, comm=None, name=None, max_leases=None):
        # We use `self.max_leases` as the point of truth to find out if a semaphore with a specific
        # `name` has been created.
        if name not in self.max_leases:
            assert isinstance(max_leases, int), max_leases
            self.max_leases[name] = max_leases
        else:
            if max_leases != self.max_leases[name]:
                raise ValueError(
                    "Inconsistent max leases: %s, expected: %s"
                    % (max_leases, self.max_leases[name])
                )

    def refresh_leases(self, comm=None, name=None, lease_ids=None):
        with log_errors():
            now = time()
            logger.debug(
                "Refresh leases for %s with ids %s at %s", name, lease_ids, now
            )
            for id_ in lease_ids:
                if id_ not in self.leases[name]:
                    logger.critical(
                        f"Refreshing an unknown lease ID {id_} for {name}. This might be due to leases "
                        f"timing out and may cause overbooking of the semaphore!"
                        f"This is often caused by long-running GIL-holding in the task which acquired the lease."
                    )
                self.leases[name][id_] = now

    def _get_lease(self, name, lease_id):
        result = True

        if (
            # This allows request idempotency
            lease_id in self.leases[name]
            or len(self.leases[name]) < self.max_leases[name]
        ):
            now = time()
            logger.debug("Acquire lease %s for %s at %s", lease_id, name, now)
            self.leases[name][lease_id] = now
            self.metrics["acquire_total"][name] += 1
        else:
            result = False
        return result

    def _semaphore_exists(self, name):
        if name not in self.max_leases:
            return False
        return True

    async def acquire(self, comm=None, name=None, timeout=None, lease_id=None):
        with log_errors():
            if not self._semaphore_exists(name):
                raise RuntimeError(f"Semaphore `{name}` not known or already closed.")

            if isinstance(name, list):
                name = tuple(name)
            w = _Watch(timeout)
            w.start()

            self.metrics["pending"][name] += 1
            while True:
                logger.debug(
                    "Trying to acquire %s for %s with %s seconds left.",
                    lease_id,
                    name,
                    w.leftover(),
                )
                # Reset the event and try to get a release. The event will be set if the state
                # is changed and helps to identify when it is worth to retry an acquire
                self.events[name].clear()

                result = self._get_lease(name, lease_id)

                # If acquiring fails, we wait for the event to be set, i.e. something has
                # been released and we can try to acquire again (continue loop)
                if not result:
                    future = asyncio.wait_for(
                        self.events[name].wait(), timeout=w.leftover()
                    )
                    try:
                        await future
                        continue
                    except TimeoutError:
                        result = False
                logger.debug(
                    "Acquisition of lease %s for %s is %s after waiting for %ss.",
                    lease_id,
                    name,
                    result,
                    w.elapsed(),
                )
                # We're about to return, so the lease is no longer "pending"
                self.metrics["average_pending_lease_time"][name] = (
                    self.metrics["average_pending_lease_time"][name] + w.elapsed()
                ) / 2
                self.metrics["pending"][name] -= 1

                return result

    def release(self, comm=None, name=None, lease_id=None):
        with log_errors():
            if not self._semaphore_exists(name):
                logger.warning(
                    f"Tried to release semaphore `{name}` but it is not known or already closed."
                )
                return
            if isinstance(name, list):
                name = tuple(name)
            if name in self.leases and lease_id in self.leases[name]:
                self._release_value(name, lease_id)
            else:
                logger.warning(
                    f"Tried to release semaphore but it was already released: "
                    f"name={name}, lease_id={lease_id}. This can happen if the semaphore timed out before."
                )

    def _release_value(self, name, lease_id):
        logger.debug("Releasing %s for %s", lease_id, name)
        # Everything needs to be atomic here.
        del self.leases[name][lease_id]
        self.events[name].set()
        self.metrics["release_total"][name] += 1

    def _check_lease_timeout(self):
        now = time()
        semaphore_names = list(self.leases.keys())
        for name in semaphore_names:
            ids = list(self.leases[name])
            logger.debug(
                "Validating leases for %s at time %s. Currently known %s",
                name,
                now,
                self.leases[name],
            )
            for _id in ids:
                time_since_refresh = now - self.leases[name][_id]
                if time_since_refresh > self.lease_timeout:
                    logger.debug(
                        "Lease %s for %s timed out after %ss.",
                        _id,
                        name,
                        time_since_refresh,
                    )
                    self._release_value(name=name, lease_id=_id)

    def close(self, comm=None, name=None):
        """Hard close the semaphore without warning clients which still hold a lease."""
        with log_errors():
            if not self._semaphore_exists(name):
                return

            del self.max_leases[name]
            if name in self.events:
                del self.events[name]
            if name in self.leases:
                if self.leases[name]:
                    warnings.warn(
                        f"Closing semaphore {name} but there remain unreleased leases {sorted(self.leases[name])}",
                        RuntimeWarning,
                    )
                del self.leases[name]
            if name in self.metrics["pending"]:
                if self.metrics["pending"][name]:
                    warnings.warn(
                        f"Closing semaphore {name} but there remain pending leases",
                        RuntimeWarning,
                    )
            # Clean-up state of semaphore metrics
            for _, metric_dict in self.metrics.items():
                if name in metric_dict:
                    del metric_dict[name]


class Semaphore:
    """Semaphore

    This `semaphore <https://en.wikipedia.org/wiki/Semaphore_(programming)>`_
    will track leases on the scheduler which can be acquired and
    released by an instance of this class. If the maximum amount of leases are
    already acquired, it is not possible to acquire more and the caller waits
    until another lease has been released.

    The lifetime or leases are controlled using a timeout. This timeout is
    refreshed in regular intervals by the ``Client`` of this instance and
    provides protection from deadlocks or resource starvation in case of worker
    failure.
    The timeout can be controlled using the configuration option
    ``distributed.scheduler.locks.lease-timeout`` and the interval in which the
    scheduler verifies the timeout is set using the option
    ``distributed.scheduler.locks.lease-validation-interval``.

    A noticeable difference to the Semaphore of the python standard library is
    that this implementation does not allow to release more often than it was
    acquired. If this happens, a warning is emitted but the internal state is
    not modified.

    .. warning::

        This implementation is still in an experimental state and subtle
        changes in behavior may occur without any change in the major version
        of this library.

    .. warning::

        This implementation is susceptible to lease overbooking in case of
        lease timeouts. It is advised to monitor log information and adjust
        above configuration options to suitable values for the user application.

    Parameters
    ----------
    max_leases: int (optional)
        The maximum amount of leases that may be granted at the same time. This
        effectively sets an upper limit to the amount of parallel access to a specific resource.
        Defaults to 1.
    name: string (optional)
        Name of the semaphore to acquire.  Choosing the same name allows two
        disconnected processes to coordinate.  If not given, a random
        name will be generated.
    register: bool
        If True, register the semaphore with the scheduler. This needs to be
        done before any leases can be acquired. If not done during
        initialization, this can also be done by calling the register method of
        this class.
        When registering, this needs to be awaited.
    scheduler_rpc: ConnectionPool
        The ConnectionPool to connect to the scheduler. If None is provided, it
        uses the worker or client pool. This paramter is mostly used for
        testing.
    loop: IOLoop
        The event loop this instance is using. If None is provided, reuse the
        loop of the active worker or client.

    Examples
    --------
    >>> from distributed import Semaphore
    ... sem = Semaphore(max_leases=2, name='my_database')
    ...
    ... def access_resource(s, sem):
    ...     # This automatically acquires a lease from the semaphore (if available) which will be
    ...     # released when leaving the context manager.
    ...     with sem:
    ...         pass
    ...
    ... futures = client.map(access_resource, range(10), sem=sem)
    ... client.gather(futures)
    ... # Once done, close the semaphore to clean up the state on scheduler side.
    ... sem.close()

    Notes
    -----
    If a client attempts to release the semaphore but doesn't have a lease acquired, this will raise an exception.


    When a semaphore is closed, if, for that closed semaphore, a client attempts to:

    - Acquire a lease: an exception will be raised.
    - Release: a warning will be logged.
    - Close: nothing will happen.


    dask executes functions by default assuming they are pure, when using semaphore acquire/releases inside
    such a function, it must be noted that there *are* in fact side-effects, thus, the function can no longer be
    considered pure. If this is not taken into account, this may lead to unexpected behavior.

    """

    def __init__(
        self,
        max_leases=1,
        name=None,
        register=True,
        scheduler_rpc=None,
        loop=None,
    ):

        try:
            worker = get_worker()
            self.scheduler = scheduler_rpc or worker.scheduler
            self.loop = loop or worker.loop

        except ValueError:
            client = get_client()
            self.scheduler = scheduler_rpc or client.scheduler
            self.loop = loop or client.io_loop

        self.name = name or "semaphore-" + uuid.uuid4().hex
        self.max_leases = max_leases
        self.id = uuid.uuid4().hex
        self._leases = deque()

        self.refresh_leases = True

        self._registered = None
        if register:
            self._registered = self.register()

        # this should give ample time to refresh without introducing another
        # config parameter since this *must* be smaller than the timeout anyhow
        refresh_leases_interval = (
            parse_timedelta(
                dask.config.get("distributed.scheduler.locks.lease-timeout"),
                default="s",
            )
            / 5
        )
        pc = PeriodicCallback(
            self._refresh_leases, callback_time=refresh_leases_interval * 1000
        )
        self.refresh_callback = pc

        # Need to start the callback using IOLoop.add_callback to ensure that the
        # PC uses the correct event loop.
        self.loop.add_callback(pc.start)

    @property
    def asynchronous(self):
        return self.loop is IOLoop.current()

    async def _register(self):
        await retry_operation(
            self.scheduler.semaphore_register,
            name=self.name,
            max_leases=self.max_leases,
            operation=f"semaphore register id={self.id} name={self.name}",
        )

    def register(self, **kwargs):
        return self.sync(self._register)

    def __await__(self):
        async def create_semaphore():
            if self._registered:
                await self._registered
            return self

        return create_semaphore().__await__()

    def sync(self, func, *args, asynchronous=None, callback_timeout=None, **kwargs):
        callback_timeout = parse_timedelta(callback_timeout)
        if (
            asynchronous
            or self.asynchronous
            or getattr(thread_state, "asynchronous", False)
        ):
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = asyncio.wait_for(future, callback_timeout)
            return future
        else:
            return sync(
                self.loop, func, *args, callback_timeout=callback_timeout, **kwargs
            )

    async def _refresh_leases(self):
        if self.refresh_leases and self._leases:
            logger.debug(
                "%s refreshing leases for %s with IDs %s",
                self.id,
                self.name,
                self._leases,
            )
            await retry_operation(
                self.scheduler.semaphore_refresh_leases,
                lease_ids=list(self._leases),
                name=self.name,
                operation="semaphore refresh leases: id=%s, lease_ids=%s, name=%s"
                % (self.id, list(self._leases), self.name),
            )

    async def _acquire(self, timeout=None):
        lease_id = uuid.uuid4().hex
        logger.debug(
            "%s requests lease for %s with ID %s", self.id, self.name, lease_id
        )

        # Using a unique lease id generated here allows us to retry since the
        # server handle is idempotent
        result = await retry_operation(
            self.scheduler.semaphore_acquire,
            name=self.name,
            timeout=timeout,
            lease_id=lease_id,
            operation="semaphore acquire: id=%s, lease_id=%s, name=%s"
            % (self.id, lease_id, self.name),
        )
        if result:
            self._leases.append(lease_id)
        return result

    def acquire(self, timeout=None):
        """
        Acquire a semaphore.

        If the internal counter is greater than zero, decrement it by one and return True immediately.
        If it is zero, wait until a release() is called and return True.

        Parameters
        ----------
        timeout : number or string or timedelta, optional
            Seconds to wait on acquiring the semaphore.  This does not
            include local coroutine time, network transfer time, etc..
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        """
        timeout = parse_timedelta(timeout)
        return self.sync(self._acquire, timeout=timeout)

    async def _release(self, lease_id):
        try:
            await retry_operation(
                self.scheduler.semaphore_release,
                name=self.name,
                lease_id=lease_id,
                operation="semaphore release: id=%s, lease_id=%s, name=%s"
                % (self.id, lease_id, self.name),
            )
            return True
        except Exception:  # Release fails for whatever reason
            logger.error(
                "Release failed for id=%s, lease_id=%s, name=%s. Cluster network might be unstable?"
                % (self.id, lease_id, self.name),
                exc_info=True,
            )
            return False

    def release(self):
        """
        Release the semaphore.

        Returns
        -------
        bool
            This value indicates whether a lease was released immediately or not. Note that a user should  *not* retry
            this operation. Under certain circumstances (e.g. scheduler overload) the lease may not be released
            immediately, but it will always be automatically released after a specific interval configured using
            "distributed.scheduler.locks.lease-validation-interval" and "distributed.scheduler.locks.lease-timeout".
        """
        if not self._leases:
            raise RuntimeError("Released too often")

        # popleft to release the oldest lease first
        lease_id = self._leases.popleft()
        logger.debug("%s releases %s for %s", self.id, lease_id, self.name)
        return self.sync(self._release, lease_id=lease_id)

    def get_value(self):
        """
        Return the number of currently registered leases.
        """
        return self.sync(self.scheduler.semaphore_value, name=self.name)

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

    def __getstate__(self):
        # Do not serialize the address since workers may have different
        # addresses for the scheduler (e.g. if a proxy is between them)
        return (self.name, self.max_leases)

    def __setstate__(self, state):
        name, max_leases = state
        self.__init__(
            name=name,
            max_leases=max_leases,
            register=False,
        )

    def close(self):
        return self.sync(self.scheduler.semaphore_close, name=self.name)

    def __del__(self):
        self.refresh_callback.stop()
