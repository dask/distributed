from __future__ import annotations

import asyncio
import atexit
import copy
import errno
import inspect
import json
import logging
import os
import re
import sys
import threading
import traceback
import uuid
import warnings
import weakref
from collections import defaultdict
from collections.abc import Collection, Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import DoneAndNotDoneFutures
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from functools import partial
from numbers import Number
from queue import Queue as pyQueue
from typing import ClassVar, Literal

from tlz import first, groupby, keymap, merge, partition_all, valmap

import dask
from dask.base import collections_to_dsk, normalize_token, tokenize
from dask.core import flatten
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable
from dask.utils import (
    _deprecated,
    apply,
    ensure_dict,
    format_bytes,
    funcname,
    parse_timedelta,
    stringify,
    typename,
)
from dask.widgets import get_template

try:
    from dask.delayed import single_key
except ImportError:
    single_key = first
from tornado import gen
from tornado.ioloop import PeriodicCallback

from . import preloading
from . import versions as version_module  # type: ignore
from .batched import BatchedSend
from .cfexecutor import ClientExecutor
from .core import (
    CommClosedError,
    ConnectionPool,
    PooledRPCCall,
    Status,
    clean_exception,
    connect,
    rpc,
)
from .diagnostics.plugin import NannyPlugin, UploadFile, WorkerPlugin, _get_plugin_name
from .metrics import time
from .objects import HasWhat, SchedulerInfo, WhoHas
from .protocol import to_serialize
from .protocol.pickle import dumps, loads
from .publish import Datasets
from .pubsub import PubSubClientExtension
from .security import Security
from .sizeof import sizeof
from .threadpoolexecutor import rejoin
from .utils import (
    All,
    Any,
    CancelledError,
    LoopRunner,
    NoOpAwaitable,
    SyncMethodMixin,
    TimeoutError,
    format_dashboard_link,
    has_keyword,
    import_term,
    log_errors,
    no_default,
    sync,
    thread_state,
)
from .utils_comm import (
    WrappedKey,
    gather_from_workers,
    pack_data,
    retry_operation,
    scatter_to_workers,
    unpack_remotedata,
)
from .worker import get_client, get_worker, secede

logger = logging.getLogger(__name__)

_global_clients: weakref.WeakValueDictionary[
    int, Client
] = weakref.WeakValueDictionary()
_global_client_index = [0]

_current_client = ContextVar("_current_client", default=None)

DEFAULT_EXTENSIONS = [PubSubClientExtension]
# Placeholder used in the get_dataset function(s)
NO_DEFAULT_PLACEHOLDER = "_no_default_"


def _get_global_client() -> Client | None:
    L = sorted(list(_global_clients), reverse=True)
    for k in L:
        c = _global_clients[k]
        if c.status != "closed":
            return c
        else:
            del _global_clients[k]
    return None


def _set_global_client(c: Client | None) -> None:
    if c is not None:
        _global_clients[_global_client_index[0]] = c
        _global_client_index[0] += 1


def _del_global_client(c: Client) -> None:
    for k in list(_global_clients):
        try:
            if _global_clients[k] is c:
                del _global_clients[k]
        except KeyError:  # pragma: no cover
            pass


class Future(WrappedKey):
    """A remotely running computation

    A Future is a local proxy to a result running on a remote worker.  A user
    manages future objects in the local Python process to determine what
    happens in the larger cluster.

    Parameters
    ----------
    key: str, or tuple
        Key of remote data to which this future refers
    client: Client
        Client that should own this future.  Defaults to _get_global_client()
    inform: bool
        Do we inform the scheduler that we need an update on this future
    state: FutureState
        The state of the future

    Examples
    --------
    Futures typically emerge from Client computations

    >>> my_future = client.submit(add, 1, 2)  # doctest: +SKIP

    We can track the progress and results of a future

    >>> my_future  # doctest: +SKIP
    <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>

    We can get the result or the exception and traceback from the future

    >>> my_future.result()  # doctest: +SKIP

    See Also
    --------
    Client:  Creates futures
    """

    _cb_executor = None
    _cb_executor_pid = None

    def __init__(self, key, client=None, inform=True, state=None):
        self.key = key
        self._cleared = False
        tkey = stringify(key)
        self.client = client or Client.current()
        self.client._inc_ref(tkey)
        self._generation = self.client.generation

        if tkey in self.client.futures:
            self._state = self.client.futures[tkey]
        else:
            self._state = self.client.futures[tkey] = FutureState()

        if inform:
            self.client._send_to_scheduler(
                {
                    "op": "client-desires-keys",
                    "keys": [stringify(key)],
                    "client": self.client.id,
                }
            )

        if state is not None:
            try:
                handler = self.client._state_handlers[state]
            except KeyError:
                pass
            else:
                handler(key=key)

    @property
    def executor(self):
        """Returns the executor, which is the client.

        Returns
        -------
        Client
            The executor
        """
        return self.client

    @property
    def status(self):
        """Returns the status

        Returns
        -------
        str
            The status
        """
        return self._state.status

    def done(self):
        """Returns whether or not the computation completed.

        Returns
        -------
        bool
            True if the computation is complete, otherwise False
        """
        return self._state.done()

    def result(self, timeout=None):
        """Wait until computation completes, gather result to local process.

        Parameters
        ----------
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``

        Raises
        ------
        dask.distributed.TimeoutError
            If *timeout* seconds are elapsed before returning, a
            ``dask.distributed.TimeoutError`` is raised.

        Returns
        -------
        result
            The result of the computation. Or a coroutine if the client is asynchronous.
        """
        if self.client.asynchronous:
            return self.client.sync(self._result, callback_timeout=timeout)

        # shorten error traceback
        result = self.client.sync(self._result, callback_timeout=timeout, raiseit=False)
        if self.status == "error":
            typ, exc, tb = result
            raise exc.with_traceback(tb)
        elif self.status == "cancelled":
            raise result
        else:
            return result

    async def _result(self, raiseit=True):
        await self._state.wait()
        if self.status == "error":
            exc = clean_exception(self._state.exception, self._state.traceback)
            if raiseit:
                typ, exc, tb = exc
                raise exc.with_traceback(tb)
            else:
                return exc
        elif self.status == "cancelled":
            exception = CancelledError(self.key)
            if raiseit:
                raise exception
            else:
                return exception
        else:
            result = await self.client._gather([self])
            return result[0]

    async def _exception(self):
        await self._state.wait()
        if self.status == "error":
            return self._state.exception
        else:
            return None

    def exception(self, timeout=None, **kwargs):
        """Return the exception of a failed task

        Parameters
        ----------
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
        **kwargs : dict
            Optional keyword arguments for the function

        Returns
        -------
        Exception
            The exception that was raised
            If *timeout* seconds are elapsed before returning, a
            ``dask.distributed.TimeoutError`` is raised.

        See Also
        --------
        Future.traceback
        """
        return self.client.sync(self._exception, callback_timeout=timeout, **kwargs)

    def add_done_callback(self, fn):
        """Call callback on future when callback has finished

        The callback ``fn`` should take the future as its only argument.  This
        will be called regardless of if the future completes successfully,
        errs, or is cancelled

        The callback is executed in a separate thread.

        Parameters
        ----------
        fn : callable
            The method or function to be called
        """
        cls = Future
        if cls._cb_executor is None or cls._cb_executor_pid != os.getpid():
            try:
                cls._cb_executor = ThreadPoolExecutor(
                    1, thread_name_prefix="Dask-Callback-Thread"
                )
            except TypeError:
                cls._cb_executor = ThreadPoolExecutor(1)
            cls._cb_executor_pid = os.getpid()

        def execute_callback(fut):
            try:
                fn(fut)
            except BaseException:
                logger.exception("Error in callback %s of %s:", fn, fut)

        self.client.loop.add_callback(
            done_callback, self, partial(cls._cb_executor.submit, execute_callback)
        )

    def cancel(self, **kwargs):
        """Cancel the request to run this future

        See Also
        --------
        Client.cancel
        """
        return self.client.cancel([self], **kwargs)

    def retry(self, **kwargs):
        """Retry this future if it has failed

        See Also
        --------
        Client.retry
        """
        return self.client.retry([self], **kwargs)

    def cancelled(self):
        """Returns True if the future has been cancelled

        Returns
        -------
        bool
            True if the future was 'cancelled', otherwise False
        """
        return self._state.status == "cancelled"

    async def _traceback(self):
        await self._state.wait()
        if self.status == "error":
            return self._state.traceback
        else:
            return None

    def traceback(self, timeout=None, **kwargs):
        """Return the traceback of a failed task

        This returns a traceback object.  You can inspect this object using the
        ``traceback`` module.  Alternatively if you call ``future.result()``
        this traceback will accompany the raised exception.

        Parameters
        ----------
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
            If *timeout* seconds are elapsed before returning, a
            ``dask.distributed.TimeoutError`` is raised.

        Examples
        --------
        >>> import traceback  # doctest: +SKIP
        >>> tb = future.traceback()  # doctest: +SKIP
        >>> traceback.format_tb(tb)  # doctest: +SKIP
        [...]

        Returns
        -------
        traceback
            The traceback object. Or a coroutine if the client is asynchronous.

        See Also
        --------
        Future.exception
        """
        return self.client.sync(self._traceback, callback_timeout=timeout, **kwargs)

    @property
    def type(self):
        """Returns the type"""
        return self._state.type

    def release(self):
        """
        Notes
        -----
        This method can be called from different threads
        (see e.g. Client.get() or Future.__del__())
        """
        if not self._cleared and self.client.generation == self._generation:
            self._cleared = True
            try:
                self.client.loop.add_callback(self.client._dec_ref, stringify(self.key))
            except TypeError:  # pragma: no cover
                pass  # Shutting down, add_callback may be None

    def __getstate__(self):
        return self.key, self.client.scheduler.address

    def __setstate__(self, state):
        key, address = state
        try:
            c = Client.current(allow_global=False)
        except ValueError:
            c = get_client(address)
        self.__init__(key, c)
        c._send_to_scheduler(
            {
                "op": "update-graph",
                "tasks": {},
                "keys": [stringify(self.key)],
                "client": c.id,
            }
        )

    def __del__(self):
        try:
            self.release()
        except AttributeError:
            # Ocassionally we see this error when shutting down the client
            # https://github.com/dask/distributed/issues/4305
            if not sys.is_finalizing():
                raise
        except RuntimeError:  # closed event loop
            pass

    def __repr__(self):
        if self.type:
            return (
                f"<Future: {self.status}, type: {typename(self.type)}, key: {self.key}>"
            )
        else:
            return f"<Future: {self.status}, key: {self.key}>"

    def _repr_html_(self):
        return get_template("future.html.j2").render(
            key=str(self.key),
            type=typename(self.type),
            status=self.status,
        )

    def __await__(self):
        return self.result().__await__()


class FutureState:
    """A Future's internal state.

    This is shared between all Futures with the same key and client.
    """

    __slots__ = ("_event", "status", "type", "exception", "traceback")

    def __init__(self):
        self._event = None
        self.status = "pending"
        self.type = None

    def _get_event(self):
        # Can't create Event eagerly in constructor as it can fetch
        # its IOLoop from the wrong thread
        # (https://github.com/tornadoweb/tornado/issues/2189)
        event = self._event
        if event is None:
            event = self._event = asyncio.Event()
        return event

    def cancel(self):
        """Cancels the operation"""
        self.status = "cancelled"
        self.exception = CancelledError()
        self._get_event().set()

    def finish(self, type=None):
        """Sets the status to 'finished' and sets the event

        Parameters
        ----------
        type : any
            The type
        """
        self.status = "finished"
        self._get_event().set()
        if type is not None:
            self.type = type

    def lose(self):
        """Sets the status to 'lost' and clears the event"""
        self.status = "lost"
        self._get_event().clear()

    def retry(self):
        """Sets the status to 'pending' and clears the event"""
        self.status = "pending"
        self._get_event().clear()

    def set_error(self, exception, traceback):
        """Sets the error data

        Sets the status to 'error'. Sets the exception, the traceback,
        and the event

        Parameters
        ----------
        exception: Exception
            The exception
        traceback: Exception
            The traceback
        """
        _, exception, traceback = clean_exception(exception, traceback)

        self.status = "error"
        self.exception = exception
        self.traceback = traceback
        self._get_event().set()

    def done(self):
        """Returns 'True' if the event is not None and the event is set"""
        return self._event is not None and self._event.is_set()

    def reset(self):
        """Sets the status to 'pending' and clears the event"""
        self.status = "pending"
        if self._event is not None:
            self._event.clear()

    async def wait(self, timeout=None):
        """Wait for the awaitable to complete with a timeout.

        Parameters
        ----------
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
        """
        await asyncio.wait_for(self._get_event().wait(), timeout)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.status}>"


async def done_callback(future, callback):
    """Coroutine that waits on the future, then calls the callback

    Parameters
    ----------
    future : asyncio.Future
        The future
    callback : callable
        The callback
    """
    while future.status == "pending":
        await future._state.wait()
    callback(future)


@partial(normalize_token.register, Future)
def normalize_future(f):
    """Returns the key and the type as a list

    Parameters
    ----------
    list
        The key and the type
    """
    return [f.key, type(f)]


class AllExit(Exception):
    """Custom exception class to exit All(...) early."""


def _handle_print(event):
    _, msg = event
    if isinstance(msg, dict) and "args" in msg and "kwargs" in msg:
        print(*msg["args"], **msg["kwargs"])
    else:
        print(msg)


def _handle_warn(event):
    _, msg = event
    if isinstance(msg, dict) and "args" in msg and "kwargs" in msg:
        warnings.warn(*msg["args"], **msg["kwargs"])
    else:
        warnings.warn(msg)


def _maybe_call_security_loader(address):
    security_loader_term = dask.config.get("distributed.client.security-loader")
    if security_loader_term:
        try:
            security_loader = import_term(security_loader_term)
        except Exception as exc:
            raise ImportError(
                f"Failed to import `{security_loader_term}` configured at "
                f"`distributed.client.security-loader` - is this module "
                f"installed?"
            ) from exc
        return security_loader({"address": address})
    return None


class Client(SyncMethodMixin):
    """Connect to and submit computation to a Dask cluster

    The Client connects users to a Dask cluster.  It provides an asynchronous
    user interface around functions and futures.  This class resembles
    executors in ``concurrent.futures`` but also allows ``Future`` objects
    within ``submit/map`` calls.  When a Client is instantiated it takes over
    all ``dask.compute`` and ``dask.persist`` calls by default.

    It is also common to create a Client without specifying the scheduler
    address , like ``Client()``.  In this case the Client creates a
    :class:`LocalCluster` in the background and connects to that.  Any extra
    keywords are passed from Client to LocalCluster in this case.  See the
    LocalCluster documentation for more information.

    Parameters
    ----------
    address: string, or Cluster
        This can be the address of a ``Scheduler`` server like a string
        ``'127.0.0.1:8786'`` or a cluster object like ``LocalCluster()``
    loop
        The event loop
    timeout: int
        Timeout duration for initial connection to the scheduler
    set_as_default: bool (True)
        Use this Client as the global dask scheduler
    scheduler_file: string (optional)
        Path to a file with scheduler information if available
    security: Security or bool, optional
        Optional security information. If creating a local cluster can also
        pass in ``True``, in which case temporary self-signed credentials will
        be created automatically.
    asynchronous: bool (False by default)
        Set to True if using this client within async/await functions or within
        Tornado gen.coroutines.  Otherwise this should remain False for normal
        use.
    name: string (optional)
        Gives the client a name that will be included in logs generated on
        the scheduler for matters relating to this client
    heartbeat_interval: int (optional)
        Time in milliseconds between heartbeats to scheduler
    serializers
        Iterable of approaches to use when serializing the object.
        See :ref:`serialization` for more.
    deserializers
        Iterable of approaches to use when deserializing the object.
        See :ref:`serialization` for more.
    extensions : list
        The extensions
    direct_to_workers: bool (optional)
        Whether or not to connect directly to the workers, or to ask
        the scheduler to serve as intermediary.
    connection_limit : int
        The number of open comms to maintain at once in the connection pool

    **kwargs:
        If you do not pass a scheduler address, Client will create a
        ``LocalCluster`` object, passing any extra keyword arguments.

    Examples
    --------
    Provide cluster's scheduler node address on initialization:

    >>> client = Client('127.0.0.1:8786')  # doctest: +SKIP

    Use ``submit`` method to send individual computations to the cluster

    >>> a = client.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = client.submit(add, 10, 20)  # doctest: +SKIP

    Continue using submit or map on results to build up larger computations

    >>> c = client.submit(add, a, b)  # doctest: +SKIP

    Gather results with the ``gather`` method.

    >>> client.gather(c)  # doctest: +SKIP
    33

    You can also call Client with no arguments in order to create your own
    local cluster.

    >>> client = Client()  # makes your own local "cluster" # doctest: +SKIP

    Extra keywords will be passed directly to LocalCluster

    >>> client = Client(n_workers=2, threads_per_worker=4)  # doctest: +SKIP

    See Also
    --------
    distributed.scheduler.Scheduler: Internal scheduler
    distributed.LocalCluster:
    """

    _instances: ClassVar[weakref.WeakSet[Client]] = weakref.WeakSet()

    _default_event_handlers = {"print": _handle_print, "warn": _handle_warn}

    preloads: list[preloading.Preload]

    def __init__(
        self,
        address=None,
        loop=None,
        timeout=no_default,
        set_as_default=True,
        scheduler_file=None,
        security=None,
        asynchronous=False,
        name=None,
        heartbeat_interval=None,
        serializers=None,
        deserializers=None,
        extensions=DEFAULT_EXTENSIONS,
        direct_to_workers=None,
        connection_limit=512,
        **kwargs,
    ):
        if timeout == no_default:
            timeout = dask.config.get("distributed.comm.timeouts.connect")
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")
        self._timeout = timeout

        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self._handle_report_task = None
        if name is None:
            name = dask.config.get("client-name", None)
        self.id = (
            type(self).__name__
            + ("-" + name + "-" if name else "-")
            + str(uuid.uuid1(clock_seq=os.getpid()))
        )
        self.generation = 0
        self.status = "newly-created"
        self._pending_msg_buffer = []
        self.extensions = {}
        self.scheduler_file = scheduler_file
        self._startup_kwargs = kwargs
        self.cluster = None
        self.scheduler = None
        self._scheduler_identity = {}
        # A reentrant-lock on the refcounts for futures associated with this
        # client. Should be held by individual operations modifying refcounts,
        # or any bulk operation that needs to ensure the set of futures doesn't
        # change during operation.
        self._refcount_lock = threading.RLock()
        self.datasets = Datasets(self)
        self._serializers = serializers
        if deserializers is None:
            deserializers = serializers
        self._deserializers = deserializers
        self.direct_to_workers = direct_to_workers

        # Communication
        self.scheduler_comm = None

        if address is None:
            address = dask.config.get("scheduler-address", None)
            if address:
                logger.info("Config value `scheduler-address` found: %s", address)

        if address is not None and kwargs:
            raise ValueError(f"Unexpected keyword arguments: {sorted(kwargs)}")

        if isinstance(address, (rpc, PooledRPCCall)):
            self.scheduler = address
        elif isinstance(getattr(address, "scheduler_address", None), str):
            # It's a LocalCluster or LocalCluster-compatible object
            self.cluster = address
            status = getattr(self.cluster, "status")
            if status and status in [Status.closed, Status.closing]:
                raise RuntimeError(
                    f"Trying to connect to an already closed or closing Cluster {self.cluster}."
                )
            with suppress(AttributeError):
                loop = address.loop
            if security is None:
                security = getattr(self.cluster, "security", None)
        elif address is not None and not isinstance(address, str):
            raise TypeError(
                "Scheduler address must be a string or a Cluster instance, got {}".format(
                    type(address)
                )
            )

        # If connecting to an address and no explicit security is configured, attempt
        # to load security credentials with a security loader (if configured).
        if security is None and isinstance(address, str):
            security = _maybe_call_security_loader(address)

        if security is None:
            security = Security()
        elif isinstance(security, dict):
            security = Security(**security)
        elif security is True:
            security = Security.temporary()
            self._startup_kwargs["security"] = security
        elif not isinstance(security, Security):  # pragma: no cover
            raise TypeError("security must be a Security object")

        self.security = security

        if name == "worker":
            self.connection_args = self.security.get_connection_args("worker")
        else:
            self.connection_args = self.security.get_connection_args("client")

        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.io_loop = self.loop = self._loop_runner.loop
        self._connecting_to_scheduler = False

        self._gather_keys = None
        self._gather_future = None

        if heartbeat_interval is None:
            heartbeat_interval = dask.config.get("distributed.client.heartbeat")
        heartbeat_interval = parse_timedelta(heartbeat_interval, default="ms")

        scheduler_info_interval = parse_timedelta(
            dask.config.get("distributed.client.scheduler-info-interval", default="ms")
        )

        self._periodic_callbacks = dict()
        self._periodic_callbacks["scheduler-info"] = PeriodicCallback(
            self._update_scheduler_info, scheduler_info_interval * 1000
        )
        self._periodic_callbacks["heartbeat"] = PeriodicCallback(
            self._heartbeat, heartbeat_interval * 1000
        )

        self._start_arg = address
        self._set_as_default = set_as_default
        if set_as_default:
            self._set_config = dask.config.set(
                scheduler="dask.distributed", shuffle="tasks"
            )
        self._event_handlers = {}

        self._stream_handlers = {
            "key-in-memory": self._handle_key_in_memory,
            "lost-data": self._handle_lost_data,
            "cancelled-key": self._handle_cancelled_key,
            "task-retried": self._handle_retried_key,
            "task-erred": self._handle_task_erred,
            "restart": self._handle_restart,
            "error": self._handle_error,
            "event": self._handle_event,
        }

        self._state_handlers = {
            "memory": self._handle_key_in_memory,
            "lost": self._handle_lost_data,
            "erred": self._handle_task_erred,
        }

        self.rpc = ConnectionPool(
            limit=connection_limit,
            serializers=serializers,
            deserializers=deserializers,
            deserialize=True,
            connection_args=self.connection_args,
            timeout=timeout,
            server=self,
        )

        for ext in extensions:
            ext(self)

        preload = dask.config.get("distributed.client.preload")
        preload_argv = dask.config.get("distributed.client.preload-argv")
        self.preloads = preloading.process_preloads(self, preload, preload_argv)

        self.start(timeout=timeout)
        Client._instances.add(self)

        from distributed.recreate_tasks import ReplayTaskClient

        ReplayTaskClient(self)

    @contextmanager
    def as_current(self):
        """Thread-local, Task-local context manager that causes the Client.current
        class method to return self. Any Future objects deserialized inside this
        context manager will be automatically attached to this Client.
        """
        tok = _current_client.set(self)
        try:
            yield
        finally:
            _current_client.reset(tok)

    @classmethod
    def current(cls, allow_global=True):
        """When running within the context of `as_client`, return the context-local
        current client. Otherwise, return the latest initialised Client.
        If no Client instances exist, raise ValueError.
        If allow_global is set to False, raise ValueError if running outside of
        the `as_client` context manager.

        Parameters
        ----------
        allow_global : bool
            If True returns the default client

        Returns
        -------
        Client
            The current client

        Raises
        ------
        ValueError
            If there is no client set, a ValueError is raised
        """
        out = _current_client.get()
        if out:
            return out
        if allow_global:
            return default_client()
        raise ValueError("Not running inside the `as_current` context manager")

    @property
    def dashboard_link(self):
        """Link to the scheduler's dashboard.

        Returns
        -------
        str
            Dashboard URL.

        Examples
        --------
        Opening the dashboard in your default web browser:

        >>> import webbrowser
        >>> from distributed import Client
        >>> client = Client()
        >>> webbrowser.open(client.dashboard_link)

        """
        try:
            return self.cluster.dashboard_link
        except AttributeError:
            scheduler, info = self._get_scheduler_info()
            if scheduler is None:
                return None
            else:
                protocol, rest = scheduler.address.split("://")

            port = info["services"]["dashboard"]
            if protocol == "inproc":
                host = "localhost"
            else:
                host = rest.split(":")[0]

            return format_dashboard_link(host, port)

    def _get_scheduler_info(self):
        from .scheduler import Scheduler

        if (
            self.cluster
            and hasattr(self.cluster, "scheduler")
            and isinstance(self.cluster.scheduler, Scheduler)
        ):
            info = self.cluster.scheduler.identity()
            scheduler = self.cluster.scheduler
        elif (
            self._loop_runner.is_started() and self.scheduler and not self.asynchronous
        ):
            info = sync(self.loop, self.scheduler.identity)
            scheduler = self.scheduler
        else:
            info = self._scheduler_identity
            scheduler = self.scheduler

        return scheduler, SchedulerInfo(info)

    def __repr__(self):
        # Note: avoid doing I/O here...
        info = self._scheduler_identity
        addr = info.get("address")
        if addr:
            workers = info.get("workers", {})
            nworkers = len(workers)
            nthreads = sum(w["nthreads"] for w in workers.values())
            text = "<%s: %r processes=%d threads=%d" % (
                self.__class__.__name__,
                addr,
                nworkers,
                nthreads,
            )
            memory = [w["memory_limit"] for w in workers.values()]
            if all(memory):
                text += ", memory=" + format_bytes(sum(memory))
            text += ">"
            return text

        elif self.scheduler is not None:
            return "<{}: scheduler={!r}>".format(
                self.__class__.__name__,
                self.scheduler.address,
            )
        else:
            return f"<{self.__class__.__name__}: No scheduler connected>"

    def _repr_html_(self):
        scheduler, info = self._get_scheduler_info()

        return get_template("client.html.j2").render(
            id=self.id,
            scheduler=scheduler,
            info=info,
            cluster=self.cluster,
            scheduler_file=self.scheduler_file,
            dashboard_link=self.dashboard_link,
        )

    def start(self, **kwargs):
        """Start scheduler running in separate thread"""
        if self.status != "newly-created":
            return

        self._loop_runner.start()
        if self._set_as_default:
            _set_global_client(self)

        if self.asynchronous:
            self._started = asyncio.ensure_future(self._start(**kwargs))
        else:
            sync(self.loop, self._start, **kwargs)

    def __await__(self):
        if hasattr(self, "_started"):
            return self._started.__await__()
        else:

            async def _():
                return self

            return _().__await__()

    def _send_to_scheduler_safe(self, msg):
        if self.status in ("running", "closing"):
            try:
                self.scheduler_comm.send(msg)
            except (CommClosedError, AttributeError):
                if self.status == "running":
                    raise
        elif self.status in ("connecting", "newly-created"):
            self._pending_msg_buffer.append(msg)

    def _send_to_scheduler(self, msg):
        if self.status in ("running", "closing", "connecting", "newly-created"):
            self.loop.add_callback(self._send_to_scheduler_safe, msg)
        else:
            raise Exception(
                "Tried sending message after closing.  Status: %s\n"
                "Message: %s" % (self.status, msg)
            )

    async def _start(self, timeout=no_default, **kwargs):
        self.status = "connecting"

        await self.rpc.start()

        if timeout == no_default:
            timeout = self._timeout
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")

        address = self._start_arg
        if self.cluster is not None:
            # Ensure the cluster is started (no-op if already running)
            try:
                await self.cluster
            except Exception:
                logger.info(
                    "Tried to start cluster and received an error. Proceeding.",
                    exc_info=True,
                )
            address = self.cluster.scheduler_address
        elif self.scheduler_file is not None:
            while not os.path.exists(self.scheduler_file):
                await asyncio.sleep(0.01)
            for i in range(10):
                try:
                    with open(self.scheduler_file) as f:
                        cfg = json.load(f)
                    address = cfg["address"]
                    break
                except (ValueError, KeyError):  # JSON file not yet flushed
                    await asyncio.sleep(0.01)
        elif self._start_arg is None:
            from .deploy import LocalCluster

            try:
                self.cluster = await LocalCluster(
                    loop=self.loop,
                    asynchronous=self._asynchronous,
                    **self._startup_kwargs,
                )
            except OSError as e:
                if e.errno != errno.EADDRINUSE:
                    raise
                # The default port was taken, use a random one
                self.cluster = await LocalCluster(
                    scheduler_port=0,
                    loop=self.loop,
                    asynchronous=True,
                    **self._startup_kwargs,
                )

            address = self.cluster.scheduler_address

        self._gather_semaphore = asyncio.Semaphore(5)

        if self.scheduler is None:
            self.scheduler = self.rpc(address)
        self.scheduler_comm = None

        try:
            await self._ensure_connected(timeout=timeout)
        except (OSError, ImportError):
            await self._close()
            raise

        for pc in self._periodic_callbacks.values():
            pc.start()

        for topic, handler in Client._default_event_handlers.items():
            self.subscribe_topic(topic, handler)

        for preload in self.preloads:
            await preload.start()

        self._handle_report_task = asyncio.create_task(self._handle_report())

        return self

    async def _reconnect(self):
        with log_errors():
            assert self.scheduler_comm.comm.closed()

            self.status = "connecting"
            self.scheduler_comm = None

            for st in self.futures.values():
                st.cancel()
            self.futures.clear()

            timeout = self._timeout
            deadline = time() + timeout
            while timeout > 0 and self.status == "connecting":
                try:
                    await self._ensure_connected(timeout=timeout)
                    break
                except OSError:
                    # Wait a bit before retrying
                    await asyncio.sleep(0.1)
                    timeout = deadline - time()
                except ImportError:
                    await self._close()
                    break
            else:
                logger.error(
                    "Failed to reconnect to scheduler after %.2f "
                    "seconds, closing client",
                    self._timeout,
                )
                await self._close()

    async def _ensure_connected(self, timeout=None):
        if (
            self.scheduler_comm
            and not self.scheduler_comm.closed()
            or self._connecting_to_scheduler
            or self.scheduler is None
        ):
            return

        self._connecting_to_scheduler = True

        try:
            comm = await connect(
                self.scheduler.address, timeout=timeout, **self.connection_args
            )
            comm.name = "Client->Scheduler"
            if timeout is not None:
                await asyncio.wait_for(self._update_scheduler_info(), timeout)
            else:
                await self._update_scheduler_info()
            await comm.write(
                {
                    "op": "register-client",
                    "client": self.id,
                    "reply": False,
                    "versions": version_module.get_versions(),
                }
            )
        except Exception:
            if self.status == "closed":
                return
            else:
                raise
        finally:
            self._connecting_to_scheduler = False
        if timeout is not None:
            msg = await asyncio.wait_for(comm.read(), timeout)
        else:
            msg = await comm.read()
        assert len(msg) == 1
        assert msg[0]["op"] == "stream-start"

        if msg[0].get("error"):
            raise ImportError(msg[0]["error"])
        if msg[0].get("warning"):
            warnings.warn(version_module.VersionMismatchWarning(msg[0]["warning"]))

        bcomm = BatchedSend(interval="10ms", loop=self.loop)
        bcomm.start(comm)
        self.scheduler_comm = bcomm
        if self._set_as_default:
            _set_global_client(self)
        self.status = "running"

        for msg in self._pending_msg_buffer:
            self._send_to_scheduler(msg)
        del self._pending_msg_buffer[:]

        logger.debug("Started scheduling coroutines. Synchronized")

    async def _update_scheduler_info(self):
        if self.status not in ("running", "connecting"):
            return
        try:
            self._scheduler_identity = SchedulerInfo(await self.scheduler.identity())
        except OSError:
            logger.debug("Not able to query scheduler for identity")

    async def _wait_for_workers(self, n_workers=0, timeout=None):
        info = await self.scheduler.identity()
        self._scheduler_identity = SchedulerInfo(info)
        if timeout:
            deadline = time() + parse_timedelta(timeout)
        else:
            deadline = None
        while n_workers and len(info["workers"]) < n_workers:
            if deadline and time() > deadline:
                raise TimeoutError(
                    "Only %d/%d workers arrived after %s"
                    % (len(info["workers"]), n_workers, timeout)
                )
            await asyncio.sleep(0.1)
            info = await self.scheduler.identity()
            self._scheduler_identity = SchedulerInfo(info)

    def wait_for_workers(self, n_workers=0, timeout=None):
        """Blocking call to wait for n workers before continuing

        Parameters
        ----------
        n_workers : int
            The number of workers
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
        """
        return self.sync(self._wait_for_workers, n_workers, timeout=timeout)

    def _heartbeat(self):
        if self.scheduler_comm:
            self.scheduler_comm.send({"op": "heartbeat-client"})

    def __enter__(self):
        if not self._loop_runner.is_started():
            self.start()
        return self

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, typ, value, traceback):
        await self._close()

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        # If the loop never got assigned, we failed early in the constructor,
        # nothing to do
        if hasattr(self, "loop"):
            self.close()

    def _inc_ref(self, key):
        with self._refcount_lock:
            self.refcount[key] += 1

    def _dec_ref(self, key):
        with self._refcount_lock:
            self.refcount[key] -= 1
            if self.refcount[key] == 0:
                del self.refcount[key]
                self._release_key(key)

    def _release_key(self, key):
        """Release key from distributed memory"""
        logger.debug("Release key %s", key)
        st = self.futures.pop(key, None)
        if st is not None:
            st.cancel()
        if self.status != "closed":
            self._send_to_scheduler(
                {"op": "client-releases-keys", "keys": [key], "client": self.id}
            )

    async def _handle_report(self):
        """Listen to scheduler"""
        with log_errors():
            try:
                while True:
                    if self.scheduler_comm is None:
                        break
                    try:
                        msgs = await self.scheduler_comm.comm.read()
                    except CommClosedError:
                        if self.status == "running":
                            logger.info("Client report stream closed to scheduler")
                            logger.info("Reconnecting...")
                            self.status = "connecting"
                            await self._reconnect()
                            continue
                        else:
                            break
                    if not isinstance(msgs, (list, tuple)):
                        msgs = (msgs,)

                    breakout = False
                    for msg in msgs:
                        logger.debug("Client receives message %s", msg)

                        if "status" in msg and "error" in msg["status"]:
                            typ, exc, tb = clean_exception(**msg)
                            raise exc.with_traceback(tb)

                        op = msg.pop("op")

                        if op == "close" or op == "stream-closed":
                            breakout = True
                            break

                        try:
                            handler = self._stream_handlers[op]
                            result = handler(**msg)
                            if inspect.isawaitable(result):
                                await result
                        except Exception as e:
                            logger.exception(e)
                    if breakout:
                        break
            except CancelledError:
                pass

    def _handle_key_in_memory(self, key=None, type=None, workers=None):
        state = self.futures.get(key)
        if state is not None:
            if type and not state.type:  # Type exists and not yet set
                try:
                    type = loads(type)
                except Exception:
                    type = None
                # Here, `type` may be a str if actual type failed
                # serializing in Worker
            else:
                type = None
            state.finish(type)

    def _handle_lost_data(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.lose()

    def _handle_cancelled_key(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.cancel()

    def _handle_retried_key(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.retry()

    def _handle_task_erred(self, key=None, exception=None, traceback=None):
        state = self.futures.get(key)
        if state is not None:
            state.set_error(exception, traceback)

    def _handle_restart(self):
        logger.info("Receive restart signal from scheduler")
        for state in self.futures.values():
            state.cancel()
        self.futures.clear()
        with suppress(AttributeError):
            self._restart_event.set()

    def _handle_error(self, exception=None):
        logger.warning("Scheduler exception:")
        logger.exception(exception)

    async def _close(self, fast=False):
        """Send close signal and wait until scheduler completes"""
        if self.status == "closed":
            return

        self.status = "closing"

        for preload in self.preloads:
            await preload.teardown()

        with suppress(AttributeError):
            for pc in self._periodic_callbacks.values():
                pc.stop()

        with log_errors():
            _del_global_client(self)
            self._scheduler_identity = {}
            with suppress(AttributeError):
                # clear the dask.config set keys
                with self._set_config:
                    pass
            if self.get == dask.config.get("get", None):
                del dask.config.config["get"]

            if (
                self.scheduler_comm
                and self.scheduler_comm.comm
                and not self.scheduler_comm.comm.closed()
            ):
                self._send_to_scheduler({"op": "close-client"})
                self._send_to_scheduler({"op": "close-stream"})

            current_task = asyncio.current_task()
            handle_report_task = self._handle_report_task
            # Give the scheduler 'stream-closed' message 100ms to come through
            # This makes the shutdown slightly smoother and quieter
            if (
                handle_report_task is not None
                and handle_report_task is not current_task
            ):
                with suppress(asyncio.CancelledError, TimeoutError):
                    await asyncio.wait_for(asyncio.shield(handle_report_task), 0.1)

            if (
                self.scheduler_comm
                and self.scheduler_comm.comm
                and not self.scheduler_comm.comm.closed()
            ):
                await self.scheduler_comm.close()

            for key in list(self.futures):
                self._release_key(key=key)

            if self._start_arg is None:
                with suppress(AttributeError):
                    await self.cluster.close()

            await self.rpc.close()

            self.status = "closed"

            if _get_global_client() is self:
                _set_global_client(None)

            if (
                handle_report_task is not None
                and handle_report_task is not current_task
            ):
                with suppress(TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(handle_report_task, 0 if fast else 2)

            with suppress(AttributeError):
                await self.scheduler.close_rpc()

            self.scheduler = None

        self.status = "closed"

    def close(self, timeout=no_default):
        """Close this client

        Clients will also close automatically when your Python session ends

        If you started a client without arguments like ``Client()`` then this
        will also close the local cluster that was started at the same time.


        Parameters
        ----------
        timeout : number
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``

        See Also
        --------
        Client.restart
        """
        if timeout == no_default:
            timeout = self._timeout * 2
        # XXX handling of self.status here is not thread-safe
        if self.status in ["closed", "newly-created"]:
            if self.asynchronous:
                return NoOpAwaitable()
            return
        self.status = "closing"

        with suppress(AttributeError):
            for pc in self._periodic_callbacks.values():
                pc.stop()

        if self.asynchronous:
            coro = self._close()
            if timeout:
                coro = asyncio.wait_for(coro, timeout)
            return coro

        if self._start_arg is None:
            with suppress(AttributeError):
                f = self.cluster.close()
                if asyncio.iscoroutine(f):

                    async def _():
                        await f

                    self.sync(_)

        sync(self.loop, self._close, fast=True, callback_timeout=timeout)

        assert self.status == "closed"

        if not sys.is_finalizing():
            self._loop_runner.stop()

    async def _shutdown(self):
        logger.info("Shutting down scheduler from Client")
        if self.cluster:
            await self.cluster.close()
        else:
            with suppress(CommClosedError):
                self.status = "closing"
                await self.scheduler.terminate(close_workers=True)

    def shutdown(self):
        """Shut down the connected scheduler and workers

        Note, this may disrupt other clients that may be using the same
        scheduler and workers.

        See Also
        --------
        Client.close : close only this client
        """
        return self.sync(self._shutdown)

    def get_executor(self, **kwargs):
        """
        Return a concurrent.futures Executor for submitting tasks on this
        Client

        Parameters
        ----------
        **kwargs
            Any submit()- or map()- compatible arguments, such as
            `workers` or `resources`.

        Returns
        -------
        ClientExecutor
            An Executor object that's fully compatible with the
            concurrent.futures API.
        """
        return ClientExecutor(self, **kwargs)

    def submit(
        self,
        func,
        *args,
        key=None,
        workers=None,
        resources=None,
        retries=None,
        priority=0,
        fifo_timeout="100 ms",
        allow_other_workers=False,
        actor=False,
        actors=False,
        pure=None,
        **kwargs,
    ):

        """Submit a function application to the scheduler

        Parameters
        ----------
        func : callable
            Callable to be scheduled as ``func(*args **kwargs)``. If ``func`` returns a
            coroutine, it will be run on the main event loop of a worker. Otherwise
            ``func`` will be run in a worker's task executor pool (see
            ``Worker.executors`` for more information.)
        *args : tuple
            Optional positional arguments
        key : str
            Unique identifier for the task.  Defaults to function-name and hash
        workers : string or iterable of strings
            A set of worker addresses or hostnames on which computations may be
            performed. Leave empty to default to all workers (common case)
        resources : dict (defaults to {})
            Defines the ``resources`` each instance of this mapped task
            requires on the worker; e.g. ``{'GPU': 2}``.
            See :doc:`worker resources <resources>` for details on defining
            resources.
        retries : int (default to 0)
            Number of allowed automatic retries if the task fails
        priority : Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout : str timedelta (default '100ms')
            Allowed amount of time between calls to consider the same priority
        allow_other_workers : bool (defaults to False)
            Used with ``workers``. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        actor : bool (default False)
            Whether this task should exist on the worker as a stateful actor.
            See :doc:`actors` for additional details.
        actors : bool (default False)
            Alias for `actor`
        pure : bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
            See :ref:`pure functions` for more details.
        **kwargs

        Examples
        --------
        >>> c = client.submit(add, a, b)  # doctest: +SKIP

        Returns
        -------
        Future
            If running in asynchronous mode, returns the future. Otherwise
            returns the concrete value

        Raises
        ------
        TypeError
            If 'func' is not callable, a TypeError is raised
        ValueError
            If 'allow_other_workers'is True and 'workers' is None, a
            ValueError is raised

        See Also
        --------
        Client.map : Submit on many arguments at once
        """
        if not callable(func):
            raise TypeError("First input to submit must be a callable function")

        actor = actor or actors
        if pure is None:
            pure = not actor

        if allow_other_workers not in (True, False, None):
            raise TypeError("allow_other_workers= must be True or False")

        if key is None:
            if pure:
                key = funcname(func) + "-" + tokenize(func, kwargs, *args)
            else:
                key = funcname(func) + "-" + str(uuid.uuid4())

        skey = stringify(key)

        with self._refcount_lock:
            if skey in self.futures:
                return Future(key, self, inform=False)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        if isinstance(workers, (str, Number)):
            workers = [workers]

        if kwargs:
            dsk = {skey: (apply, func, list(args), kwargs)}
        else:
            dsk = {skey: (func,) + tuple(args)}

        futures = self._graph_to_futures(
            dsk,
            [skey],
            workers=workers,
            allow_other_workers=allow_other_workers,
            priority={skey: 0},
            user_priority=priority,
            resources=resources,
            retries=retries,
            fifo_timeout=fifo_timeout,
            actors=actor,
        )

        logger.debug("Submit %s(...), %s", funcname(func), key)

        return futures[skey]

    def map(
        self,
        func,
        *iterables,
        key=None,
        workers=None,
        retries=None,
        resources=None,
        priority=0,
        allow_other_workers=False,
        fifo_timeout="100 ms",
        actor=False,
        actors=False,
        pure=None,
        batch_size=None,
        **kwargs,
    ):
        """Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

        Parameters
        ----------
        func : callable
            Callable to be scheduled for execution. If ``func`` returns a coroutine, it
            will be run on the main event loop of a worker. Otherwise ``func`` will be
            run in a worker's task executor pool (see ``Worker.executors`` for more
            information.)
        iterables : Iterables
            List-like objects to map over.  They should have the same length.
        key : str, list
            Prefix for task names if string.  Explicit names if list.
        workers : string or iterable of strings
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)
        retries : int (default to 0)
            Number of allowed automatic retries if a task fails
        resources : dict (defaults to {})
            Defines the `resources` each instance of this mapped task requires
            on the worker; e.g. ``{'GPU': 2}``.
            See :doc:`worker resources <resources>` for details on defining
            resources.
        priority : Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        allow_other_workers : bool (defaults to False)
            Used with `workers`. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        fifo_timeout : str timedelta (default '100ms')
            Allowed amount of time between calls to consider the same priority
        actor : bool (default False)
            Whether these tasks should exist on the worker as stateful actors.
            See :doc:`actors` for additional details.
        actors : bool (default False)
            Alias for `actor`
        pure : bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
            See :ref:`pure functions` for more details.
        batch_size : int, optional
            Submit tasks to the scheduler in batches of (at most)
            ``batch_size``.
            Larger batch sizes can be useful for very large ``iterables``,
            as the cluster can start processing tasks while later ones are
            submitted asynchronously.
        **kwargs : dict
            Extra keyword arguments to send to the function.
            Large values will be included explicitly in the task graph.

        Examples
        --------
        >>> L = client.map(func, sequence)  # doctest: +SKIP

        Returns
        -------
        List, iterator, or Queue of futures, depending on the type of the
        inputs.

        See Also
        --------
        Client.submit : Submit a single function
        """
        if not callable(func):
            raise TypeError("First input to map must be a callable function")

        if all(isinstance(it, pyQueue) for it in iterables) or all(
            isinstance(i, Iterator) for i in iterables
        ):
            raise TypeError(
                "Dask no longer supports mapping over Iterators or Queues."
                "Consider using a normal for loop and Client.submit"
            )
        total_length = sum(len(x) for x in iterables)

        if batch_size and batch_size > 1 and total_length > batch_size:
            batches = list(
                zip(*(partition_all(batch_size, iterable) for iterable in iterables))
            )
            if isinstance(key, list):
                keys = [list(element) for element in partition_all(batch_size, key)]
            else:
                keys = [key for _ in range(len(batches))]
            return sum(
                (
                    self.map(
                        func,
                        *batch,
                        key=key,
                        workers=workers,
                        retries=retries,
                        priority=priority,
                        allow_other_workers=allow_other_workers,
                        fifo_timeout=fifo_timeout,
                        resources=resources,
                        actor=actor,
                        actors=actors,
                        pure=pure,
                        **kwargs,
                    )
                    for key, batch in zip(keys, batches)
                ),
                [],
            )

        key = key or funcname(func)
        actor = actor or actors
        if pure is None:
            pure = not actor

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        iterables = list(zip(*zip(*iterables)))
        if isinstance(key, list):
            keys = key
        else:
            if pure:
                keys = [
                    key + "-" + tokenize(func, kwargs, *args)
                    for args in zip(*iterables)
                ]
            else:
                uid = str(uuid.uuid4())
                keys = (
                    [
                        key + "-" + uid + "-" + str(i)
                        for i in range(min(map(len, iterables)))
                    ]
                    if iterables
                    else []
                )

        if not kwargs:
            dsk = {key: (func,) + args for key, args in zip(keys, zip(*iterables))}
        else:
            kwargs2 = {}
            dsk = {}
            for k, v in kwargs.items():
                if sizeof(v) > 1e5:
                    vv = dask.delayed(v)
                    kwargs2[k] = vv._key
                    dsk.update(vv.dask)
                else:
                    kwargs2[k] = v
            dsk.update(
                {
                    key: (apply, func, (tuple, list(args)), kwargs2)
                    for key, args in zip(keys, zip(*iterables))
                }
            )

        if isinstance(workers, (str, Number)):
            workers = [workers]
        if workers is not None and not isinstance(workers, (list, set)):
            raise TypeError("Workers must be a list or set of workers or None")

        internal_priority = dict(zip(keys, range(len(keys))))

        futures = self._graph_to_futures(
            dsk,
            keys,
            workers=workers,
            allow_other_workers=allow_other_workers,
            priority=internal_priority,
            resources=resources,
            retries=retries,
            user_priority=priority,
            fifo_timeout=fifo_timeout,
            actors=actor,
        )
        logger.debug("map(%s, ...)", funcname(func))

        return [futures[stringify(k)] for k in keys]

    async def _gather(self, futures, errors="raise", direct=None, local_worker=None):
        unpacked, future_set = unpack_remotedata(futures, byte_keys=True)
        mismatched_futures = [f for f in future_set if f.client is not self]
        if mismatched_futures:
            raise ValueError(
                "Cannot gather Futures created by another client. "
                f"These are the {len(mismatched_futures)} (out of {len(futures)}) mismatched Futures and their client IDs "
                f"(this client is {self.id}): "
                f"{ {f: f.client.id for f in mismatched_futures} }"
            )
        keys = [stringify(future.key) for future in future_set]
        bad_data = dict()
        data = {}

        if direct is None:
            direct = self.direct_to_workers
        if direct is None:
            try:
                w = get_worker()
            except Exception:
                direct = False
            else:
                if w.scheduler.address == self.scheduler.address:
                    direct = True

        async def wait(k):
            """Want to stop the All(...) early if we find an error"""
            st = self.futures[k]
            await st.wait()
            if st.status != "finished" and errors == "raise":
                raise AllExit()

        while True:
            logger.debug("Waiting on futures to clear before gather")

            with suppress(AllExit):
                await All(
                    [wait(key) for key in keys if key in self.futures],
                    quiet_exceptions=AllExit,
                )

            failed = ("error", "cancelled")

            exceptions = set()
            bad_keys = set()
            for key in keys:
                if key not in self.futures or self.futures[key].status in failed:
                    exceptions.add(key)
                    if errors == "raise":
                        try:
                            st = self.futures[key]
                            exception = st.exception
                            traceback = st.traceback
                        except (KeyError, AttributeError):
                            exc = CancelledError(key)
                        else:
                            raise exception.with_traceback(traceback)
                        raise exc
                    if errors == "skip":
                        bad_keys.add(key)
                        bad_data[key] = None
                    else:  # pragma: no cover
                        raise ValueError("Bad value, `errors=%s`" % errors)

            keys = [k for k in keys if k not in bad_keys and k not in data]

            if local_worker:  # look inside local worker
                data.update(
                    {k: local_worker.data[k] for k in keys if k in local_worker.data}
                )
                keys = [k for k in keys if k not in data]

            # We now do an actual remote communication with workers or scheduler
            if self._gather_future:  # attach onto another pending gather request
                self._gather_keys |= set(keys)
                response = await self._gather_future
            else:  # no one waiting, go ahead
                self._gather_keys = set(keys)
                future = asyncio.ensure_future(
                    self._gather_remote(direct, local_worker)
                )
                if self._gather_keys is None:
                    self._gather_future = None
                else:
                    self._gather_future = future
                response = await future

            if response["status"] == "error":
                log = logger.warning if errors == "raise" else logger.debug
                log(
                    "Couldn't gather %s keys, rescheduling %s",
                    len(response["keys"]),
                    response["keys"],
                )
                for key in response["keys"]:
                    self._send_to_scheduler({"op": "report-key", "key": key})
                for key in response["keys"]:
                    try:
                        self.futures[key].reset()
                    except KeyError:  # TODO: verify that this is safe
                        pass
            else:  # pragma: no cover
                break

        if bad_data and errors == "skip" and isinstance(unpacked, list):
            unpacked = [f for f in unpacked if f not in bad_data]

        data.update(response["data"])
        result = pack_data(unpacked, merge(data, bad_data))
        return result

    async def _gather_remote(self, direct, local_worker):
        """Perform gather with workers or scheduler

        This method exists to limit and batch many concurrent gathers into a
        few.  In controls access using a Tornado semaphore, and picks up keys
        from other requests made recently.
        """
        async with self._gather_semaphore:
            keys = list(self._gather_keys)
            self._gather_keys = None  # clear state, these keys are being sent off
            self._gather_future = None

            if direct or local_worker:  # gather directly from workers
                who_has = await retry_operation(self.scheduler.who_has, keys=keys)
                data2, missing_keys, missing_workers = await gather_from_workers(
                    who_has, rpc=self.rpc, close=False
                )
                response = {"status": "OK", "data": data2}
                if missing_keys:
                    keys2 = [key for key in keys if key not in data2]
                    response = await retry_operation(self.scheduler.gather, keys=keys2)
                    if response["status"] == "OK":
                        response["data"].update(data2)

            else:  # ask scheduler to gather data for us
                response = await retry_operation(self.scheduler.gather, keys=keys)

        return response

    def gather(self, futures, errors="raise", direct=None, asynchronous=None):
        """Gather futures from distributed memory

        Accepts a future, nested container of futures, iterator, or queue.
        The return type will match the input type.

        Parameters
        ----------
        futures : Collection of futures
            This can be a possibly nested collection of Future objects.
            Collections can be lists, sets, or dictionaries
        errors : string
            Either 'raise' or 'skip' if we should raise if a future has erred
            or skip its inclusion in the output collection
        direct : boolean
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.
        asynchronous: bool
            If True the client is in asynchronous mode

        Returns
        -------
        results: a collection of the same type as the input, but now with
        gathered results rather than futures

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> x = c.submit(add, 1, 2)  # doctest: +SKIP
        >>> c.gather(x)  # doctest: +SKIP
        3
        >>> c.gather([x, [x], x])  # support lists and dicts # doctest: +SKIP
        [3, [3], 3]

        See Also
        --------
        Client.scatter : Send data out to cluster
        """
        if isinstance(futures, pyQueue):
            raise TypeError(
                "Dask no longer supports gathering over Iterators and Queues. "
                "Consider using a normal for loop and Client.submit/gather"
            )

        elif isinstance(futures, Iterator):
            return (self.gather(f, errors=errors, direct=direct) for f in futures)
        else:
            if hasattr(thread_state, "execution_state"):  # within worker task
                local_worker = thread_state.execution_state["worker"]
            else:
                local_worker = None
            return self.sync(
                self._gather,
                futures,
                errors=errors,
                direct=direct,
                local_worker=local_worker,
                asynchronous=asynchronous,
            )

    async def _scatter(
        self,
        data,
        workers=None,
        broadcast=False,
        direct=None,
        local_worker=None,
        timeout=no_default,
        hash=True,
    ):
        if timeout == no_default:
            timeout = self._timeout
        if isinstance(workers, (str, Number)):
            workers = [workers]
        if isinstance(data, dict) and not all(
            isinstance(k, (bytes, str)) for k in data
        ):
            d = await self._scatter(keymap(stringify, data), workers, broadcast)
            return {k: d[stringify(k)] for k in data}

        if isinstance(data, type(range(0))):
            data = list(data)
        input_type = type(data)
        names = False
        unpack = False
        if isinstance(data, Iterator):
            data = list(data)
        if isinstance(data, (set, frozenset)):
            data = list(data)
        if not isinstance(data, (dict, list, tuple, set, frozenset)):
            unpack = True
            data = [data]
        if isinstance(data, (list, tuple)):
            if hash:
                names = [type(x).__name__ + "-" + tokenize(x) for x in data]
            else:
                names = [type(x).__name__ + "-" + uuid.uuid4().hex for x in data]
            data = dict(zip(names, data))

        assert isinstance(data, dict)

        types = valmap(type, data)

        if direct is None:
            direct = self.direct_to_workers
        if direct is None:
            try:
                w = get_worker()
            except Exception:
                direct = False
            else:
                if w.scheduler.address == self.scheduler.address:
                    direct = True

        if local_worker:  # running within task
            local_worker.update_data(data=data, report=False)

            await self.scheduler.update_data(
                who_has={key: [local_worker.address] for key in data},
                nbytes=valmap(sizeof, data),
                client=self.id,
            )

        else:
            data2 = valmap(to_serialize, data)
            if direct:
                nthreads = None
                start = time()
                while not nthreads:
                    if nthreads is not None:
                        await asyncio.sleep(0.1)
                    if time() > start + timeout:
                        raise TimeoutError("No valid workers found")
                    # Exclude paused and closing_gracefully workers
                    nthreads = await self.scheduler.ncores_running(workers=workers)
                if not nthreads:  # pragma: no cover
                    raise ValueError("No valid workers found")

                _, who_has, nbytes = await scatter_to_workers(
                    nthreads, data2, report=False, rpc=self.rpc
                )

                await self.scheduler.update_data(
                    who_has=who_has, nbytes=nbytes, client=self.id
                )
            else:
                await self.scheduler.scatter(
                    data=data2,
                    workers=workers,
                    client=self.id,
                    broadcast=broadcast,
                    timeout=timeout,
                )

        out = {k: Future(k, self, inform=False) for k in data}
        for key, typ in types.items():
            self.futures[key].finish(type=typ)

        if direct and broadcast:
            n = None if broadcast is True else broadcast
            await self._replicate(list(out.values()), workers=workers, n=n)

        if issubclass(input_type, (list, tuple, set, frozenset)):
            out = input_type(out[k] for k in names)

        if unpack:
            assert len(out) == 1
            out = list(out.values())[0]
        return out

    def scatter(
        self,
        data,
        workers=None,
        broadcast=False,
        direct=None,
        hash=True,
        timeout=no_default,
        asynchronous=None,
    ):
        """Scatter data into distributed memory

        This moves data from the local client process into the workers of the
        distributed scheduler.  Note that it is often better to submit jobs to
        your workers to have them load the data rather than loading data
        locally and then scattering it out to them.

        Parameters
        ----------
        data : list, dict, or object
            Data to scatter out to workers.  Output type matches input type.
        workers : list of tuples (optional)
            Optionally constrain locations of data.
            Specify workers as hostname/port pairs, e.g.
            ``('127.0.0.1', 8787)``.
        broadcast : bool (defaults to False)
            Whether to send each data element to all workers.
            By default we round-robin based on number of cores.
        direct : bool (defaults to automatically check)
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.
        hash : bool (optional)
            Whether or not to hash data to determine key.
            If False then this uses a random key
        timeout : number, optional
            Time in seconds after which to raise a
            ``dask.distributed.TimeoutError``
        asynchronous: bool
            If True the client is in asynchronous mode

        Returns
        -------
        List, dict, iterator, or queue of futures matching the type of input.

        Examples
        --------
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.scatter(1) # doctest: +SKIP
        <Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>

        >>> c.scatter([1, 2, 3])  # doctest: +SKIP
        [<Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,
         <Future: status: finished, key: 58e78e1b34eb49a68c65b54815d1b158>,
         <Future: status: finished, key: d3395e15f605bc35ab1bac6341a285e2>]

        >>> c.scatter({'x': 1, 'y': 2, 'z': 3})  # doctest: +SKIP
        {'x': <Future: status: finished, key: x>,
         'y': <Future: status: finished, key: y>,
         'z': <Future: status: finished, key: z>}

        Constrain location of data to subset of workers

        >>> c.scatter([1, 2, 3], workers=[('hostname', 8788)])   # doctest: +SKIP

        Broadcast data to all workers

        >>> [future] = c.scatter([element], broadcast=True)  # doctest: +SKIP

        Send scattered data to parallelized function using client futures
        interface

        >>> data = c.scatter(data, broadcast=True)  # doctest: +SKIP
        >>> res = [c.submit(func, data, i) for i in range(100)]

        See Also
        --------
        Client.gather : Gather data back to local process
        """
        if timeout == no_default:
            timeout = self._timeout
        if isinstance(data, pyQueue) or isinstance(data, Iterator):
            raise TypeError(
                "Dask no longer supports mapping over Iterators or Queues."
                "Consider using a normal for loop and Client.submit"
            )

        if hasattr(thread_state, "execution_state"):  # within worker task
            local_worker = thread_state.execution_state["worker"]
        else:
            local_worker = None
        return self.sync(
            self._scatter,
            data,
            workers=workers,
            broadcast=broadcast,
            direct=direct,
            local_worker=local_worker,
            timeout=timeout,
            asynchronous=asynchronous,
            hash=hash,
        )

    async def _cancel(self, futures, force=False):
        keys = list({stringify(f.key) for f in futures_of(futures)})
        await self.scheduler.cancel(keys=keys, client=self.id, force=force)
        for k in keys:
            st = self.futures.pop(k, None)
            if st is not None:
                st.cancel()

    def cancel(self, futures, asynchronous=None, force=False):
        """
        Cancel running futures

        This stops future tasks from being scheduled if they have not yet run
        and deletes them if they have already run.  After calling, this result
        and all dependent results will no longer be accessible

        Parameters
        ----------
        futures : List[Future]
            The list of Futures
        asynchronous: bool
            If True the client is in asynchronous mode
        force : boolean (False)
            Cancel this future even if other clients desire it
        """
        return self.sync(self._cancel, futures, asynchronous=asynchronous, force=force)

    async def _retry(self, futures):
        keys = list({stringify(f.key) for f in futures_of(futures)})
        response = await self.scheduler.retry(keys=keys, client=self.id)
        for key in response:
            st = self.futures[key]
            st.retry()

    def retry(self, futures, asynchronous=None):
        """
        Retry failed futures

        Parameters
        ----------
        futures : list of Futures
            The list of Futures
        asynchronous: bool
            If True the client is in asynchronous mode
        """
        return self.sync(self._retry, futures, asynchronous=asynchronous)

    async def _publish_dataset(self, *args, name=None, override=False, **kwargs):
        with log_errors():
            coroutines = []

            def add_coro(name, data):
                keys = [stringify(f.key) for f in futures_of(data)]
                coroutines.append(
                    self.scheduler.publish_put(
                        keys=keys,
                        name=name,
                        data=to_serialize(data),
                        override=override,
                        client=self.id,
                    )
                )

            if name:
                if len(args) == 0:
                    raise ValueError(
                        "If name is provided, expecting call signature like"
                        " publish_dataset(df, name='ds')"
                    )
                # in case this is a singleton, collapse it
                elif len(args) == 1:
                    args = args[0]
                add_coro(name, args)

            for name, data in kwargs.items():
                add_coro(name, data)

            await asyncio.gather(*coroutines)

    def publish_dataset(self, *args, **kwargs):
        """
        Publish named datasets to scheduler

        This stores a named reference to a dask collection or list of futures
        on the scheduler.  These references are available to other Clients
        which can download the collection or futures with ``get_dataset``.

        Datasets are not immediately computed.  You may wish to call
        ``Client.persist`` prior to publishing a dataset.

        Parameters
        ----------
        args : list of objects to publish as name
        kwargs : dict
            named collections to publish on the scheduler

        Examples
        --------
        Publishing client:

        >>> df = dd.read_csv('s3://...')  # doctest: +SKIP
        >>> df = c.persist(df) # doctest: +SKIP
        >>> c.publish_dataset(my_dataset=df)  # doctest: +SKIP

        Alternative invocation
        >>> c.publish_dataset(df, name='my_dataset')

        Receiving client:

        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> df2 = c.get_dataset('my_dataset')  # doctest: +SKIP

        Returns
        -------
        None

        See Also
        --------
        Client.list_datasets
        Client.get_dataset
        Client.unpublish_dataset
        Client.persist
        """
        return self.sync(self._publish_dataset, *args, **kwargs)

    def unpublish_dataset(self, name, **kwargs):
        """
        Remove named datasets from scheduler

        Parameters
        ----------
        name : str
            The name of the dataset to unpublish

        Examples
        --------
        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> c.unpublish_dataset('my_dataset')  # doctest: +SKIP
        >>> c.list_datasets()  # doctest: +SKIP
        []

        See Also
        --------
        Client.publish_dataset
        """
        return self.sync(self.scheduler.publish_delete, name=name, **kwargs)

    def list_datasets(self, **kwargs):
        """
        List named datasets available on the scheduler

        See Also
        --------
        Client.publish_dataset
        Client.get_dataset
        """
        return self.sync(self.scheduler.publish_list, **kwargs)

    async def _get_dataset(self, name, default=NO_DEFAULT_PLACEHOLDER):
        with self.as_current():
            out = await self.scheduler.publish_get(name=name, client=self.id)

        if out is None:
            if default is NO_DEFAULT_PLACEHOLDER:
                raise KeyError(f"Dataset '{name}' not found")
            else:
                return default
        return out["data"]

    def get_dataset(self, name, default=NO_DEFAULT_PLACEHOLDER, **kwargs):
        """
        Get named dataset from the scheduler if present.
        Return the default or raise a KeyError if not present.

        Parameters
        ----------
        name : str
            name of the dataset to retrieve
        default : str
            optional, not set by default
            If set, do not raise a KeyError if the name is not present but
            return this default
        kwargs : dict
            additional keyword arguments to _get_dataset

        Returns
        -------
        The dataset from the scheduler, if present

        See Also
        --------
        Client.publish_dataset
        Client.list_datasets
        """
        return self.sync(self._get_dataset, name, default=default, **kwargs)

    async def _run_on_scheduler(self, function, *args, wait=True, **kwargs):
        response = await self.scheduler.run_function(
            function=dumps(function, protocol=4),
            args=dumps(args, protocol=4),
            kwargs=dumps(kwargs, protocol=4),
            wait=wait,
        )
        if response["status"] == "error":
            typ, exc, tb = clean_exception(**response)
            raise exc.with_traceback(tb)
        else:
            return response["result"]

    def run_on_scheduler(self, function, *args, **kwargs):
        """Run a function on the scheduler process

        This is typically used for live debugging.  The function should take a
        keyword argument ``dask_scheduler=``, which will be given the scheduler
        object itself.

        Parameters
        ----------
        function : callable
            The function to run on the scheduler process
        *args : tuple
            Optional arguments for the function
        **kwargs : dict
            Optional keyword arguments for the function

        Examples
        --------
        >>> def get_number_of_tasks(dask_scheduler=None):
        ...     return len(dask_scheduler.tasks)

        >>> client.run_on_scheduler(get_number_of_tasks)  # doctest: +SKIP
        100

        Run asynchronous functions in the background:

        >>> async def print_state(dask_scheduler):  # doctest: +SKIP
        ...    while True:
        ...        print(dask_scheduler.status)
        ...        await asyncio.sleep(1)

        >>> c.run(print_state, wait=False)  # doctest: +SKIP

        See Also
        --------
        Client.run : Run a function on all workers
        Client.start_ipython_scheduler : Start an IPython session on scheduler
        """
        return self.sync(self._run_on_scheduler, function, *args, **kwargs)

    async def _run(
        self,
        function,
        *args,
        nanny: bool = False,
        workers: list[str] | None = None,
        wait: bool = True,
        on_error: Literal["raise", "return", "ignore"] = "raise",
        **kwargs,
    ):
        responses = await self.scheduler.broadcast(
            msg=dict(
                op="run",
                function=dumps(function, protocol=4),
                args=dumps(args, protocol=4),
                wait=wait,
                kwargs=dumps(kwargs, protocol=4),
            ),
            workers=workers,
            nanny=nanny,
            on_error="return_pickle",
        )
        results = {}
        for key, resp in responses.items():
            if isinstance(resp, bytes):
                # Pickled RPC exception
                exc = loads(resp)
                assert isinstance(exc, Exception)
            elif resp["status"] == "error":
                # Exception raised by the remote function
                _, exc, tb = clean_exception(**resp)
                exc = exc.with_traceback(tb)
            else:
                assert resp["status"] == "OK"
                results[key] = resp["result"]
                continue

            if on_error == "raise":
                raise exc
            elif on_error == "return":
                results[key] = exc
            elif on_error != "ignore":
                raise ValueError(
                    "on_error must be 'raise', 'return', or 'ignore'; "
                    f"got {on_error!r}"
                )

        if wait:
            return results

    def run(
        self,
        function,
        *args,
        workers: list[str] | None = None,
        wait: bool = True,
        nanny: bool = False,
        on_error: Literal["raise", "return", "ignore"] = "raise",
        **kwargs,
    ):
        """
        Run a function on all workers outside of task scheduling system

        This calls a function on all currently known workers immediately,
        blocks until those results come back, and returns the results
        asynchronously as a dictionary keyed by worker address.  This method
        is generally used for side effects such as collecting diagnostic
        information or installing libraries.

        If your function takes an input argument named ``dask_worker`` then
        that variable will be populated with the worker itself.

        Parameters
        ----------
        function : callable
            The function to run
        *args : tuple
            Optional arguments for the remote function
        **kwargs : dict
            Optional keyword arguments for the remote function
        workers : list
            Workers on which to run the function. Defaults to all known
            workers.
        wait : boolean (optional)
            If the function is asynchronous whether or not to wait until that
            function finishes.
        nanny : bool, defualt False
            Whether to run ``function`` on the nanny. By default, the function
            is run on the worker process.  If specified, the addresses in
            ``workers`` should still be the worker addresses, not the nanny addresses.
        on_error: "raise" | "return" | "ignore"
            If the function raises an error on a worker:

            raise
                (default) Re-raise the exception on the client.
                The output from other workers will be lost.
            return
                Return the Exception object instead of the function output for
                the worker
            ignore
                Ignore the exception and remove the worker from the result dict

        Examples
        --------
        >>> c.run(os.getpid)  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321,
         '192.168.0.102:9000': 5555}

        Restrict computation to particular workers with the ``workers=``
        keyword argument.

        >>> c.run(os.getpid, workers=['192.168.0.100:9000',
        ...                           '192.168.0.101:9000'])  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321}

        >>> def get_status(dask_worker):
        ...     return dask_worker.status

        >>> c.run(get_hostname)  # doctest: +SKIP
        {'192.168.0.100:9000': 'running',
         '192.168.0.101:9000': 'running}

        Run asynchronous functions in the background:

        >>> async def print_state(dask_worker):  # doctest: +SKIP
        ...    while True:
        ...        print(dask_worker.status)
        ...        await asyncio.sleep(1)

        >>> c.run(print_state, wait=False)  # doctest: +SKIP
        """
        return self.sync(
            self._run,
            function,
            *args,
            workers=workers,
            wait=wait,
            nanny=nanny,
            on_error=on_error,
            **kwargs,
        )

    @_deprecated(use_instead="Client.run which detects async functions automatically")
    def run_coroutine(self, function, *args, **kwargs):
        """
        Spawn a coroutine on all workers.

        This spawns a coroutine on all currently known workers and then waits
        for the coroutine on each worker.  The coroutines' results are returned
        as a dictionary keyed by worker address.

        Parameters
        ----------
        function : a coroutine function
            (typically a function wrapped in gen.coroutine or
             a Python 3.5+ async function)
        *args : tuple
            Optional arguments for the remote function
        **kwargs : dict
            Optional keyword arguments for the remote function
        """
        return self.run(function, *args, **kwargs)

    @staticmethod
    def _get_computation_code(stacklevel=None) -> str:
        """Walk up the stack to the user code and extract the code surrounding
        the compute/submit/persist call. All modules encountered which are
        ignored through the option
        `distributed.diagnostics.computations.ignore-modules` will be ignored.
        This can be used to exclude commonly used libraries which wrap
        dask/distributed compute calls.

        ``stacklevel`` may be used to explicitly indicate from which frame on
        the stack to get the source code.
        """

        ignore_modules = dask.config.get(
            "distributed.diagnostics.computations.ignore-modules"
        )
        if not isinstance(ignore_modules, list):
            raise TypeError(
                "Ignored modules must be a list. Instead got "
                f"({type(ignore_modules)}, {ignore_modules})"
            )
        if stacklevel is None:
            pattern: re.Pattern | None
            if ignore_modules:
                pattern = re.compile("|".join([f"(?:{mod})" for mod in ignore_modules]))
            else:
                pattern = None
        else:
            # stacklevel 0 or less - shows dask internals which likely isn't helpful
            stacklevel = stacklevel if stacklevel > 0 else 1
        for i, (fr, _) in enumerate(traceback.walk_stack(None), 1):
            if stacklevel is not None:
                if i != stacklevel:
                    continue
            elif pattern is not None and (
                pattern.match(fr.f_globals.get("__name__", ""))
                or fr.f_code.co_name in ("<listcomp>", "<dictcomp>")
            ):
                continue
            try:
                return inspect.getsource(fr)
            except OSError:
                # Try to fine the source if we are in %%time or %%timeit magic.
                if (
                    fr.f_code.co_filename in {"<timed exec>", "<magic-timeit>"}
                    and "IPython" in sys.modules
                ):
                    from IPython import get_ipython

                    ip = get_ipython()
                    if ip is not None:
                        # The current cell
                        return ip.history_manager._i00
                break
        return "<Code not available>"

    def _graph_to_futures(
        self,
        dsk,
        keys,
        workers=None,
        allow_other_workers=None,
        priority=None,
        user_priority=0,
        resources=None,
        retries=None,
        fifo_timeout=0,
        actors=None,
    ):
        with self._refcount_lock:
            if actors is not None and actors is not True and actors is not False:
                actors = list(self._expand_key(actors))

            # Make sure `dsk` is a high level graph
            if not isinstance(dsk, HighLevelGraph):
                dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())

            annotations = {}
            if user_priority:
                annotations["priority"] = user_priority
            if workers:
                if not isinstance(workers, (list, tuple, set)):
                    workers = [workers]
                annotations["workers"] = workers
            if retries:
                annotations["retries"] = retries
            if allow_other_workers not in (True, False, None):
                raise TypeError("allow_other_workers= must be True, False, or None")
            if allow_other_workers:
                annotations["allow_other_workers"] = allow_other_workers
            if resources:
                annotations["resources"] = resources

            # Merge global and local annotations
            annotations = merge(dask.config.get("annotations", {}), annotations)

            # Pack the high level graph before sending it to the scheduler
            keyset = set(keys)
            dsk = dsk.__dask_distributed_pack__(self, keyset, annotations)

            # Create futures before sending graph (helps avoid contention)
            futures = {key: Future(key, self, inform=False) for key in keyset}

            self._send_to_scheduler(
                {
                    "op": "update-graph-hlg",
                    "hlg": dsk,
                    "keys": list(map(stringify, keys)),
                    "priority": priority,
                    "submitting_task": getattr(thread_state, "key", None),
                    "fifo_timeout": fifo_timeout,
                    "actors": actors,
                    "code": self._get_computation_code(),
                }
            )
            return futures

    def get(
        self,
        dsk,
        keys,
        workers=None,
        allow_other_workers=None,
        resources=None,
        sync=True,
        asynchronous=None,
        direct=None,
        retries=None,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        **kwargs,
    ):
        """Compute dask graph

        Parameters
        ----------
        dsk : dict
        keys : object, or nested lists of objects
        workers : string or iterable of strings
            A set of worker addresses or hostnames on which computations may be
            performed. Leave empty to default to all workers (common case)
        allow_other_workers : bool (defaults to False)
            Used with ``workers``. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        resources : dict (defaults to {})
            Defines the ``resources`` each instance of this mapped task
            requires on the worker; e.g. ``{'GPU': 2}``.
            See :doc:`worker resources <resources>` for details on defining
            resources.
        sync : bool (optional)
            Returns Futures if False or concrete values if True (default).
        asynchronous: bool
            If True the client is in asynchronous mode
        direct : bool
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.
        retries : int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority : Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout : timedelta str (defaults to '60s')
            Allowed amount of time between calls to consider the same priority
        actors : bool or dict (default None)
            Whether these tasks should exist on the worker as stateful actors.
            Specified on a global (True/False) or per-task (``{'x': True,
            'y': False}``) basis. See :doc:`actors` for additional details.


        Returns
        -------
        results
            If 'sync' is True, returns the results. Otherwise, returns the
            known data packed
            If 'sync' is False, returns the known data. Otherwise, returns
            the results

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3

        See Also
        --------
        Client.compute : Compute asynchronous collections
        """
        futures = self._graph_to_futures(
            dsk,
            keys=set(flatten([keys])),
            workers=workers,
            allow_other_workers=allow_other_workers,
            resources=resources,
            fifo_timeout=fifo_timeout,
            retries=retries,
            user_priority=priority,
            actors=actors,
        )
        packed = pack_data(keys, futures)
        if sync:
            if getattr(thread_state, "key", False):
                try:
                    secede()
                    should_rejoin = True
                except Exception:
                    should_rejoin = False
            try:
                results = self.gather(packed, asynchronous=asynchronous, direct=direct)
            finally:
                for f in futures.values():
                    f.release()
                if getattr(thread_state, "key", False) and should_rejoin:
                    rejoin()
            return results
        return packed

    def _optimize_insert_futures(self, dsk, keys):
        """Replace known keys in dask graph with Futures

        When given a Dask graph that might have overlapping keys with our known
        results we replace the values of that graph with futures.  This can be
        used as an optimization to avoid recomputation.

        This returns the same graph if unchanged but a new graph if any changes
        were necessary.
        """
        with self._refcount_lock:
            changed = False
            for key in list(dsk):
                if stringify(key) in self.futures:
                    if not changed:
                        changed = True
                        dsk = ensure_dict(dsk)
                    dsk[key] = Future(key, self, inform=False)

        if changed:
            dsk, _ = dask.optimization.cull(dsk, keys)

        return dsk

    def normalize_collection(self, collection):
        """
        Replace collection's tasks by already existing futures if they exist

        This normalizes the tasks within a collections task graph against the
        known futures within the scheduler.  It returns a copy of the
        collection with a task graph that includes the overlapping futures.

        Parameters
        ----------
        collection : dask object
            Collection like dask.array or dataframe or dask.value objects

        Returns
        -------
        collection : dask object
            Collection with its tasks replaced with any existing futures.

        Examples
        --------
        >>> len(x.__dask_graph__())  # x is a dask collection with 100 tasks  # doctest: +SKIP
        100
        >>> set(client.futures).intersection(x.__dask_graph__())  # some overlap exists  # doctest: +SKIP
        10

        >>> x = client.normalize_collection(x)  # doctest: +SKIP
        >>> len(x.__dask_graph__())  # smaller computational graph  # doctest: +SKIP
        20

        See Also
        --------
        Client.persist : trigger computation of collection's tasks
        """
        dsk_orig = collection.__dask_graph__()
        dsk = self._optimize_insert_futures(dsk_orig, collection.__dask_keys__())

        if dsk is dsk_orig:
            return collection
        else:
            return redict_collection(collection, dsk)

    def compute(
        self,
        collections,
        sync=False,
        optimize_graph=True,
        workers=None,
        allow_other_workers=False,
        resources=None,
        retries=0,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        traverse=True,
        **kwargs,
    ):
        """Compute dask collections on cluster

        Parameters
        ----------
        collections : iterable of dask objects or single dask object
            Collections like dask.array or dataframe or dask.value objects
        sync : bool (optional)
            Returns Futures if False (default) or concrete values if True
        optimize_graph : bool
            Whether or not to optimize the underlying graphs
        workers : string or iterable of strings
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)
        allow_other_workers : bool (defaults to False)
            Used with `workers`. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        retries : int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority : Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout : timedelta str (defaults to '60s')
            Allowed amount of time between calls to consider the same priority
        traverse : bool (defaults to True)
            By default dask traverses builtin python collections looking for
            dask objects passed to ``compute``. For large collections this can
            be expensive. If none of the arguments contain any dask objects,
            set ``traverse=False`` to avoid doing this traversal.
        resources : dict (defaults to {})
            Defines the `resources` each instance of this mapped task requires
            on the worker; e.g. ``{'GPU': 2}``.
            See :doc:`worker resources <resources>` for details on defining
            resources.
        actors : bool or dict (default None)
            Whether these tasks should exist on the worker as stateful actors.
            Specified on a global (True/False) or per-task (``{'x': True,
            'y': False}``) basis. See :doc:`actors` for additional details.
        **kwargs
            Options to pass to the graph optimize calls

        Returns
        -------
        List of Futures if input is a sequence, or a single future otherwise

        Examples
        --------
        >>> from dask import delayed
        >>> from operator import add
        >>> x = delayed(add)(1, 2)
        >>> y = delayed(add)(x, x)
        >>> xx, yy = client.compute([x, y])  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6

        Also support single arguments

        >>> xx = client.compute(x)  # doctest: +SKIP

        See Also
        --------
        Client.get : Normal synchronous dask.get function
        """
        if isinstance(collections, (list, tuple, set, frozenset)):
            singleton = False
        else:
            collections = [collections]
            singleton = True

        if traverse:
            collections = tuple(
                dask.delayed(a)
                if isinstance(a, (list, set, tuple, dict, Iterator))
                else a
                for a in collections
            )

        variables = [a for a in collections if dask.is_dask_collection(a)]

        dsk = self.collections_to_dsk(variables, optimize_graph, **kwargs)
        names = ["finalize-%s" % tokenize(v) for v in variables]
        dsk2 = {}
        for i, (name, v) in enumerate(zip(names, variables)):
            func, extra_args = v.__dask_postcompute__()
            keys = v.__dask_keys__()
            if func is single_key and len(keys) == 1 and not extra_args:
                names[i] = keys[0]
            else:
                dsk2[name] = (func, keys) + extra_args

        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())

        # Let's append the finalize graph to dsk
        finalize_name = tokenize(names)
        layers = {finalize_name: dsk2}
        layers.update(dsk.layers)
        dependencies = {finalize_name: set(dsk.layers.keys())}
        dependencies.update(dsk.dependencies)
        dsk = HighLevelGraph(layers, dependencies)

        futures_dict = self._graph_to_futures(
            dsk,
            names,
            workers=workers,
            allow_other_workers=allow_other_workers,
            resources=resources,
            retries=retries,
            user_priority=priority,
            fifo_timeout=fifo_timeout,
            actors=actors,
        )

        i = 0
        futures = []
        for arg in collections:
            if dask.is_dask_collection(arg):
                futures.append(futures_dict[names[i]])
                i += 1
            else:
                futures.append(arg)

        if sync:
            result = self.gather(futures)
        else:
            result = futures

        if singleton:
            return first(result)
        else:
            return result

    def persist(
        self,
        collections,
        optimize_graph=True,
        workers=None,
        allow_other_workers=None,
        resources=None,
        retries=None,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        **kwargs,
    ):
        """Persist dask collections on cluster

        Starts computation of the collection on the cluster in the background.
        Provides a new dask collection that is semantically identical to the
        previous one, but now based off of futures currently in execution.

        Parameters
        ----------
        collections : sequence or single dask object
            Collections like dask.array or dataframe or dask.value objects
        optimize_graph : bool
            Whether or not to optimize the underlying graphs
        workers : string or iterable of strings
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)
        allow_other_workers : bool (defaults to False)
            Used with `workers`. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        retries : int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority : Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout : timedelta str (defaults to '60s')
            Allowed amount of time between calls to consider the same priority
        resources : dict (defaults to {})
            Defines the `resources` each instance of this mapped task requires
            on the worker; e.g. ``{'GPU': 2}``.
            See :doc:`worker resources <resources>` for details on defining
            resources.
        actors : bool or dict (default None)
            Whether these tasks should exist on the worker as stateful actors.
            Specified on a global (True/False) or per-task (``{'x': True,
            'y': False}``) basis. See :doc:`actors` for additional details.
        **kwargs
            Options to pass to the graph optimize calls

        Returns
        -------
        List of collections, or single collection, depending on type of input.

        Examples
        --------
        >>> xx = client.persist(x)  # doctest: +SKIP
        >>> xx, yy = client.persist([x, y])  # doctest: +SKIP

        See Also
        --------
        Client.compute
        """
        if isinstance(collections, (tuple, list, set, frozenset)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        assert all(map(dask.is_dask_collection, collections))

        dsk = self.collections_to_dsk(collections, optimize_graph, **kwargs)

        names = {k for c in collections for k in flatten(c.__dask_keys__())}

        futures = self._graph_to_futures(
            dsk,
            names,
            workers=workers,
            allow_other_workers=allow_other_workers,
            resources=resources,
            retries=retries,
            user_priority=priority,
            fifo_timeout=fifo_timeout,
            actors=actors,
        )

        postpersists = [c.__dask_postpersist__() for c in collections]
        result = [
            func({k: futures[k] for k in flatten(c.__dask_keys__())}, *args)
            for (func, args), c in zip(postpersists, collections)
        ]

        if singleton:
            return first(result)
        else:
            return result

    async def _restart(self, timeout=no_default):
        if timeout == no_default:
            timeout = self._timeout * 2
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")

        self._send_to_scheduler({"op": "restart", "timeout": timeout})
        self._restart_event = asyncio.Event()
        try:
            await asyncio.wait_for(self._restart_event.wait(), timeout)
        except TimeoutError:
            logger.error("Restart timed out after %.2f seconds", timeout)

        self.generation += 1
        with self._refcount_lock:
            self.refcount.clear()

        return self

    def restart(self, **kwargs):
        """Restart the distributed network

        This kills all active work, deletes all data on the network, and
        restarts the worker processes.
        """
        return self.sync(self._restart, **kwargs)

    async def _upload_large_file(self, local_filename, remote_filename=None):
        if remote_filename is None:
            remote_filename = os.path.split(local_filename)[1]

        with open(local_filename, "rb") as f:
            data = f.read()

        [future] = await self._scatter([data])
        key = future.key
        await self._replicate(future)

        def dump_to_file(dask_worker=None):
            if not os.path.isabs(remote_filename):
                fn = os.path.join(dask_worker.local_directory, remote_filename)
            else:
                fn = remote_filename
            with open(fn, "wb") as f:
                f.write(dask_worker.data[key])

            return len(dask_worker.data[key])

        response = await self._run(dump_to_file)

        assert all(len(data) == v for v in response.values())

    def upload_file(self, filename, **kwargs):
        """Upload local package to workers

        This sends a local file up to all worker nodes.  This file is placed
        into a temporary directory on Python's system path so any .py,  .egg
        or .zip  files will be importable.

        Parameters
        ----------
        filename : string
            Filename of .py, .egg or .zip file to send to workers
        **kwargs : dict
            Optional keyword arguments for the function

        Examples
        --------
        >>> client.upload_file('mylibrary.egg')  # doctest: +SKIP
        >>> from mylibrary import myfunc  # doctest: +SKIP
        >>> L = client.map(myfunc, seq)  # doctest: +SKIP
        """
        return self.register_worker_plugin(
            UploadFile(filename),
            name=filename + str(uuid.uuid4()),
        )

    async def _rebalance(self, futures=None, workers=None):
        if futures is not None:
            await _wait(futures)
            keys = list({stringify(f.key) for f in self.futures_of(futures)})
        else:
            keys = None
        result = await self.scheduler.rebalance(keys=keys, workers=workers)
        if result["status"] == "partial-fail":
            raise KeyError(f"Could not rebalance keys: {result['keys']}")
        assert result["status"] == "OK", result

    def rebalance(self, futures=None, workers=None, **kwargs):
        """Rebalance data within network

        Move data between workers to roughly balance memory burden.  This
        either affects a subset of the keys/workers or the entire network,
        depending on keyword arguments.

        For details on the algorithm and configuration options, refer to the matching
        scheduler-side method :meth:`~distributed.scheduler.Scheduler.rebalance`.

        .. warning::
           This operation is generally not well tested against normal operation of the
           scheduler. It is not recommended to use it while waiting on computations.

        Parameters
        ----------
        futures : list, optional
            A list of futures to balance, defaults all data
        workers : list, optional
            A list of workers on which to balance, defaults to all workers
        **kwargs : dict
            Optional keyword arguments for the function
        """
        return self.sync(self._rebalance, futures, workers, **kwargs)

    async def _replicate(self, futures, n=None, workers=None, branching_factor=2):
        futures = self.futures_of(futures)
        await _wait(futures)
        keys = {stringify(f.key) for f in futures}
        await self.scheduler.replicate(
            keys=list(keys), n=n, workers=workers, branching_factor=branching_factor
        )

    def replicate(self, futures, n=None, workers=None, branching_factor=2, **kwargs):
        """Set replication of futures within network

        Copy data onto many workers.  This helps to broadcast frequently
        accessed data and it helps to improve resilience.

        This performs a tree copy of the data throughout the network
        individually on each piece of data.  This operation blocks until
        complete.  It does not guarantee replication of data to future workers.

        Parameters
        ----------
        futures : list of futures
            Futures we wish to replicate
        n : int, optional
            Number of processes on the cluster on which to replicate the data.
            Defaults to all.
        workers : list of worker addresses
            Workers on which we want to restrict the replication.
            Defaults to all.
        branching_factor : int, optional
            The number of workers that can copy data in each generation
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> x = c.submit(func, *args)  # doctest: +SKIP
        >>> c.replicate([x])  # send to all workers  # doctest: +SKIP
        >>> c.replicate([x], n=3)  # send to three workers  # doctest: +SKIP
        >>> c.replicate([x], workers=['alice', 'bob'])  # send to specific  # doctest: +SKIP
        >>> c.replicate([x], n=1, workers=['alice', 'bob'])  # send to one of specific workers  # doctest: +SKIP
        >>> c.replicate([x], n=1)  # reduce replications # doctest: +SKIP

        See Also
        --------
        Client.rebalance
        """
        return self.sync(
            self._replicate,
            futures,
            n=n,
            workers=workers,
            branching_factor=branching_factor,
            **kwargs,
        )

    def nthreads(self, workers=None, **kwargs):
        """The number of threads/cores available on each worker node

        Parameters
        ----------
        workers : list (optional)
            A list of workers that we care about specifically.
            Leave empty to receive information about all workers.
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> c.threads()  # doctest: +SKIP
        {'192.168.1.141:46784': 8,
         '192.167.1.142:47548': 8,
         '192.167.1.143:47329': 8,
         '192.167.1.144:37297': 8}

        See Also
        --------
        Client.who_has
        Client.has_what
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]
        return self.sync(self.scheduler.ncores, workers=workers, **kwargs)

    ncores = nthreads

    def who_has(self, futures=None, **kwargs):
        """The workers storing each future's data

        Parameters
        ----------
        futures : list (optional)
            A list of futures, defaults to all data
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.who_has()  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784'],
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': ['192.168.1.141:46784']}

        >>> c.who_has([x, y])  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784']}

        See Also
        --------
        Client.has_what
        Client.nthreads
        """
        if futures is not None:
            futures = self.futures_of(futures)
            keys = list(map(stringify, {f.key for f in futures}))
        else:
            keys = None

        async def _():
            return WhoHas(await self.scheduler.who_has(keys=keys, **kwargs))

        return self.sync(_)

    def has_what(self, workers=None, **kwargs):
        """Which keys are held by which workers

        This returns the keys of the data that are held in each worker's
        memory.

        Parameters
        ----------
        workers : list (optional)
            A list of worker addresses, defaults to all
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.has_what()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.who_has
        Client.nthreads
        Client.processing
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]

        async def _():
            return HasWhat(await self.scheduler.has_what(workers=workers, **kwargs))

        return self.sync(_)

    def processing(self, workers=None):
        """The tasks currently running on each worker

        Parameters
        ----------
        workers : list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.processing()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.who_has
        Client.has_what
        Client.nthreads
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]
        return self.sync(self.scheduler.processing, workers=workers)

    def nbytes(self, keys=None, summary=True, **kwargs):
        """The bytes taken up by each key on the cluster

        This is as measured by ``sys.getsizeof`` which may not accurately
        reflect the true cost.

        Parameters
        ----------
        keys : list (optional)
            A list of keys, defaults to all keys
        summary : boolean, (optional)
            Summarize keys into key types
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.nbytes(summary=False)  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': 28,
         'inc-1e297fc27658d7b67b3a758f16bcf47a': 28,
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': 28}

        >>> c.nbytes(summary=True)  # doctest: +SKIP
        {'inc': 84}

        See Also
        --------
        Client.who_has
        """
        return self.sync(self.scheduler.nbytes, keys=keys, summary=summary, **kwargs)

    def call_stack(self, futures=None, keys=None):
        """The actively running call stack of all relevant keys

        You can specify data of interest either by providing futures or
        collections in the ``futures=`` keyword or a list of explicit keys in
        the ``keys=`` keyword.  If neither are provided then all call stacks
        will be returned.

        Parameters
        ----------
        futures : list (optional)
            List of futures, defaults to all data
        keys : list (optional)
            List of key names, defaults to all data

        Examples
        --------
        >>> df = dd.read_parquet(...).persist()  # doctest: +SKIP
        >>> client.call_stack(df)  # call on collections

        >>> client.call_stack()  # Or call with no arguments for all activity  # doctest: +SKIP
        """
        keys = keys or []
        if futures is not None:
            futures = self.futures_of(futures)
            keys += list(map(stringify, {f.key for f in futures}))
        return self.sync(self.scheduler.call_stack, keys=keys or None)

    def profile(
        self,
        key=None,
        start=None,
        stop=None,
        workers=None,
        merge_workers=True,
        plot=False,
        filename=None,
        server=False,
        scheduler=False,
    ):
        """Collect statistical profiling information about recent work

        Parameters
        ----------
        key : str
            Key prefix to select, this is typically a function name like 'inc'
            Leave as None to collect all data
        start : time
        stop : time
        workers : list
            List of workers to restrict profile information
        server : bool
            If true, return the profile of the worker's administrative thread
            rather than the worker threads.
            This is useful when profiling Dask itself, rather than user code.
        scheduler : bool
            If true, return the profile information from the scheduler's
            administrative thread rather than the workers.
            This is useful when profiling Dask's scheduling itself.
        plot : boolean or string
            Whether or not to return a plot object
        filename : str
            Filename to save the plot

        Examples
        --------
        >>> client.profile()  # call on collections
        >>> client.profile(filename='dask-profile.html')  # save to html file
        """
        return self.sync(
            self._profile,
            key=key,
            workers=workers,
            merge_workers=merge_workers,
            start=start,
            stop=stop,
            plot=plot,
            filename=filename,
            server=server,
            scheduler=scheduler,
        )

    async def _profile(
        self,
        key=None,
        start=None,
        stop=None,
        workers=None,
        merge_workers=True,
        plot=False,
        filename=None,
        server=False,
        scheduler=False,
    ):
        if isinstance(workers, (str, Number)):
            workers = [workers]

        state = await self.scheduler.profile(
            key=key,
            workers=workers,
            merge_workers=merge_workers,
            start=start,
            stop=stop,
            server=server,
            scheduler=scheduler,
        )

        if filename:
            plot = True

        if plot:
            from . import profile

            data = profile.plot_data(state)
            figure, source = profile.plot_figure(data, sizing_mode="stretch_both")

            if plot == "save" and not filename:
                filename = "dask-profile.html"

            if filename:
                from bokeh.plotting import output_file, save

                output_file(filename=filename, title="Dask Profile")
                save(figure, filename=filename)
            return (state, figure)

        else:
            return state

    def scheduler_info(self, **kwargs):
        """Basic information about the workers in the cluster

        Parameters
        ----------
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        >>> c.scheduler_info()  # doctest: +SKIP
        {'id': '2de2b6da-69ee-11e6-ab6a-e82aea155996',
         'services': {},
         'type': 'Scheduler',
         'workers': {'127.0.0.1:40575': {'active': 0,
                                         'last-seen': 1472038237.4845693,
                                         'name': '127.0.0.1:40575',
                                         'services': {},
                                         'stored': 0,
                                         'time-delay': 0.0061032772064208984}}}
        """
        if not self.asynchronous:
            self.sync(self._update_scheduler_info)
        return self._scheduler_identity

    async def _dump_cluster_state(
        self,
        filename: str,
        exclude: Collection[str],
        format: Literal["msgpack", "yaml"],
    ) -> None:

        scheduler_info = self.scheduler.dump_state(exclude=exclude)

        workers_info = self.scheduler.broadcast(
            msg={"op": "dump_state", "exclude": exclude},
            on_error="return_pickle",
        )
        versions_info = self._get_versions()
        scheduler_info, workers_info, versions_info = await asyncio.gather(
            scheduler_info, workers_info, versions_info
        )
        # Unpickle RPC errors and convert them to string
        workers_info = {
            k: repr(loads(v)) if isinstance(v, bytes) else v
            for k, v in workers_info.items()
        }

        state = {
            "scheduler": scheduler_info,
            "workers": workers_info,
            "versions": versions_info,
        }

        def tuple_to_list(node):
            if isinstance(node, (list, tuple)):
                return [tuple_to_list(el) for el in node]
            elif isinstance(node, dict):
                return {k: tuple_to_list(v) for k, v in node.items()}
            else:
                return node

        # lists are converted to tuples by the RPC
        state = tuple_to_list(state)

        filename = str(filename)
        if format == "msgpack":
            import gzip

            import msgpack

            suffix = ".msgpack.gz"
            if not filename.endswith(suffix):
                filename += suffix

            with gzip.open(filename, "wb") as fdg:
                msgpack.pack(state, fdg)
        elif format == "yaml":
            import yaml

            suffix = ".yaml"
            if not filename.endswith(suffix):
                filename += suffix

            with open(filename, "w") as fd:
                yaml.dump(state, fd)
        else:
            raise ValueError(
                f"Unsupported format {format}. Possible values are `msgpack` or `yaml`"
            )

    def dump_cluster_state(
        self,
        filename: str = "dask-cluster-dump",
        exclude: Collection[str] = ("run_spec",),
        format: Literal["msgpack", "yaml"] = "msgpack",
    ):
        """Extract a dump of the entire cluster state and persist to disk.
        This is intended for debugging purposes only.

        Warning: Memory usage on client side can be large.

        Results will be stored in a dict::

            {
                "scheduler_info": {...},
                "worker_info": {
                    worker_addr: {...},  # worker attributes
                    ...
                }
            }

        Parameters
        ----------
        filename:
            The output filename. The appropriate file suffix (`.msgpack.gz` or
            `.yaml`) will be appended automatically.
        exclude:
            A collection of attribute names which are supposed to be excluded
            from the dump, e.g. to exclude code, tracebacks, logs, etc.

            Defaults to exclude `run_spec` which is the serialized user code. This
            is typically not required for debugging. To allow serialization of
            this, pass an empty tuple.
        format:
            Either msgpack or yaml. If msgpack is used (default), the output
            will be stored in a gzipped file as msgpack.

            To read::

                import gzip, msgpack
                with gzip.open("filename") as fd:
                    state = msgpack.unpack(fd)

            or::

                import yaml
                try:
                    from yaml import CLoader as Loader
                except ImportError:
                    from yaml import Loader
                with open("filename") as fd:
                    state = yaml.load(fd, Loader=Loader)

        """
        return self.sync(
            self._dump_cluster_state,
            filename=filename,
            format=format,
            exclude=exclude,
        )

    def write_scheduler_file(self, scheduler_file):
        """Write the scheduler information to a json file.

        This facilitates easy sharing of scheduler information using a file
        system. The scheduler file can be used to instantiate a second Client
        using the same scheduler.

        Parameters
        ----------
        scheduler_file : str
            Path to a write the scheduler file.

        Examples
        --------
        >>> client = Client()  # doctest: +SKIP
        >>> client.write_scheduler_file('scheduler.json')  # doctest: +SKIP
        # connect to previous client's scheduler
        >>> client2 = Client(scheduler_file='scheduler.json')  # doctest: +SKIP
        """
        if self.scheduler_file:
            raise ValueError("Scheduler file already set")
        else:
            self.scheduler_file = scheduler_file

        with open(self.scheduler_file, "w") as f:
            json.dump(self.scheduler_info(), f, indent=2)

    def get_metadata(self, keys, default=no_default):
        """Get arbitrary metadata from scheduler

        See set_metadata for the full docstring with examples

        Parameters
        ----------
        keys : key or list
            Key to access.  If a list then gets within a nested collection
        default : optional
            If the key does not exist then return this value instead.
            If not provided then this raises a KeyError if the key is not
            present

        See Also
        --------
        Client.set_metadata
        """
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        return self.sync(self.scheduler.get_metadata, keys=keys, default=default)

    def get_scheduler_logs(self, n=None):
        """Get logs from scheduler

        Parameters
        ----------
        n : int
            Number of logs to retrive.  Maxes out at 10000 by default,
            configurable via the ``distributed.admin.log-length``
            configuration value.

        Returns
        -------
        Logs in reversed order (newest first)
        """
        return self.sync(self.scheduler.logs, n=n)

    def get_worker_logs(self, n=None, workers=None, nanny=False):
        """Get logs from workers

        Parameters
        ----------
        n : int
            Number of logs to retrive.  Maxes out at 10000 by default,
            configurable via the ``distributed.admin.log-length``
            configuration value.
        workers : iterable
            List of worker addresses to retrieve.  Gets all workers by default.
        nanny : bool, default False
            Whether to get the logs from the workers (False) or the nannies
            (True). If specified, the addresses in `workers` should still be
            the worker addresses, not the nanny addresses.

        Returns
        -------
        Dictionary mapping worker address to logs.
        Logs are returned in reversed order (newest first)
        """
        return self.sync(self.scheduler.worker_logs, n=n, workers=workers, nanny=nanny)

    def log_event(self, topic, msg):
        """Log an event under a given topic

        Parameters
        ----------
        topic : str, list
            Name of the topic under which to log an event. To log the same
            event under multiple topics, pass a list of topic names.
        msg
            Event message to log. Note this must be msgpack serializable.

        Examples
        --------
        >>> from time import time
        >>> client.log_event("current-time", time())
        """
        return self.sync(self.scheduler.log_event, topic=topic, msg=msg)

    def get_events(self, topic: str = None):
        """Retrieve structured topic logs

        Parameters
        ----------
        topic : str, optional
            Name of topic log to retrieve events for. If no ``topic`` is
            provided, then logs for all topics will be returned.
        """
        return self.sync(self.scheduler.events, topic=topic)

    async def _handle_event(self, topic, event):
        if topic not in self._event_handlers:
            self.unsubscribe_topic(topic)
            return
        handler = self._event_handlers[topic]
        ret = handler(event)
        if inspect.isawaitable(ret):
            await ret

    def subscribe_topic(self, topic, handler):
        """Subscribe to a topic and execute a handler for every received event

        Parameters
        ----------
        topic: str
            The topic name
        handler: callable or coroutine function
            A handler called for every received event. The handler must accept a
            single argument `event` which is a tuple `(timestamp, msg)` where
            timestamp refers to the clock on the scheduler.

        Example
        -------

        >>> import logging
        >>> logger = logging.getLogger("myLogger")  # Log config not shown
        >>> client.subscribe_topic("topic-name", lambda: logger.info)

        See Also
        --------
        dask.distributed.Client.unsubscribe_topic
        dask.distributed.Client.get_events
        dask.distributed.Client.log_event
        """
        if topic in self._event_handlers:
            logger.info("Handler for %s already set. Overwriting.", topic)
        self._event_handlers[topic] = handler
        msg = {"op": "subscribe-topic", "topic": topic, "client": self.id}
        self._send_to_scheduler(msg)

    def unsubscribe_topic(self, topic):
        """Unsubscribe from a topic and remove event handler

        See Also
        --------
        dask.distributed.Client.subscribe_topic
        dask.distributed.Client.get_events
        dask.distributed.Client.log_event
        """
        if topic in self._event_handlers:
            msg = {"op": "unsubscribe-topic", "topic": topic, "client": self.id}
            self._send_to_scheduler(msg)
        else:
            raise ValueError(f"No event handler known for topic {topic}.")

    def retire_workers(
        self, workers: list[str] | None = None, close_workers: bool = True, **kwargs
    ):
        """Retire certain workers on the scheduler

        See :meth:`distributed.Scheduler.retire_workers` for the full docstring.

        Parameters
        ----------
        workers
        close_workers
        **kwargs : dict
            Optional keyword arguments for the remote function

        Examples
        --------
        You can get information about active workers using the following:

        >>> workers = client.scheduler_info()['workers']

        From that list you may want to select some workers to close

        >>> client.retire_workers(workers=['tcp://address:port', ...])

        See Also
        --------
        dask.distributed.Scheduler.retire_workers
        """
        return self.sync(
            self.scheduler.retire_workers,
            workers=workers,
            close_workers=close_workers,
            **kwargs,
        )

    def set_metadata(self, key, value):
        """Set arbitrary metadata in the scheduler

        This allows you to store small amounts of data on the central scheduler
        process for administrative purposes.  Data should be msgpack
        serializable (ints, strings, lists, dicts)

        If the key corresponds to a task then that key will be cleaned up when
        the task is forgotten by the scheduler.

        If the key is a list then it will be assumed that you want to index
        into a nested dictionary structure using those keys.  For example if
        you call the following::

            >>> client.set_metadata(['a', 'b', 'c'], 123)

        Then this is the same as setting

            >>> scheduler.task_metadata['a']['b']['c'] = 123

        The lower level dictionaries will be created on demand.

        Examples
        --------
        >>> client.set_metadata('x', 123)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        123

        >>> client.set_metadata(['x', 'y'], 123)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        {'y': 123}

        >>> client.set_metadata(['x', 'w', 'z'], 456)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        {'y': 123, 'w': {'z': 456}}

        >>> client.get_metadata(['x', 'w'])  # doctest: +SKIP
        {'z': 456}

        See Also
        --------
        get_metadata
        """
        if not isinstance(key, list):
            key = (key,)
        return self.sync(self.scheduler.set_metadata, keys=key, value=value)

    def get_versions(self, check=False, packages=[]):
        """Return version info for the scheduler, all workers and myself

        Parameters
        ----------
        check : boolean, default False
            raise ValueError if all required & optional packages
            do not match
        packages : List[str]
            Extra package names to check

        Examples
        --------
        >>> c.get_versions()  # doctest: +SKIP

        >>> c.get_versions(packages=['sklearn', 'geopandas'])  # doctest: +SKIP
        """
        return self.sync(self._get_versions, check=check, packages=packages)

    async def _get_versions(self, check=False, packages=[]):
        client = version_module.get_versions(packages=packages)
        scheduler = await self.scheduler.versions(packages=packages)
        workers = await self.scheduler.broadcast(
            msg={"op": "versions", "packages": packages},
            on_error="ignore",
        )
        result = {"scheduler": scheduler, "workers": workers, "client": client}

        if check:
            msg = version_module.error_message(scheduler, workers, client)
            if msg["warning"]:
                warnings.warn(msg["warning"])
            if msg["error"]:
                raise ValueError(msg["error"])

        return result

    def futures_of(self, futures):
        """Wrapper method of futures_of

        Parameters
        ----------
        futures : tuple
            The futures
        """
        return futures_of(futures, client=self)

    def start_ipython(self, *args, **kwargs):
        """Deprecated - Method moved to start_ipython_workers

        Parameters
        ----------
        *args : tuple
            Optional arguments for the function
        **kwargs : dict
            Optional keyword arguments for the function
        """
        raise Exception("Method moved to start_ipython_workers")

    async def _start_ipython_workers(self, workers):
        if workers is None:
            workers = await self.scheduler.ncores()

        responses = await self.scheduler.broadcast(
            msg=dict(op="start_ipython"), workers=workers
        )
        return workers, responses

    def start_ipython_workers(
        self, workers=None, magic_names=False, qtconsole=False, qtconsole_args=None
    ):
        """Start IPython kernels on workers

        Parameters
        ----------
        workers : list (optional)
            A list of worker addresses, defaults to all
        magic_names : str or list(str) (optional)
            If defined, register IPython magics with these names for
            executing code on the workers.  If string has asterix then expand
            asterix into 0, 1, ..., n for n workers
        qtconsole : bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).
        qtconsole_args : list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> info = c.start_ipython_workers() # doctest: +SKIP
        >>> %remote info['192.168.1.101:5752'] worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', magic_names='w') # doctest: +SKIP
        >>> %w worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', qtconsole=True) # doctest: +SKIP

        Add asterix * in magic names to add one magic per worker

        >>> c.start_ipython_workers(magic_names='w_*') # doctest: +SKIP
        >>> %w_0 worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}
        >>> %w_1 worker.data  # doctest: +SKIP
        {'z': 5}

        Returns
        -------
        iter_connection_info: list
            List of connection_info dicts containing info necessary
            to connect Jupyter clients to the workers.

        See Also
        --------
        Client.start_ipython_scheduler : start ipython on the scheduler
        """
        if isinstance(workers, (str, Number)):
            workers = [workers]

        (workers, info_dict) = sync(self.loop, self._start_ipython_workers, workers)

        if magic_names and isinstance(magic_names, str):
            if "*" in magic_names:
                magic_names = [
                    magic_names.replace("*", str(i)) for i in range(len(workers))
                ]
            else:
                magic_names = [magic_names]

        if "IPython" in sys.modules:
            from ._ipython_utils import register_remote_magic

            register_remote_magic()
        if magic_names:
            from ._ipython_utils import register_worker_magic

            for worker, magic_name in zip(workers, magic_names):
                connection_info = info_dict[worker]
                register_worker_magic(connection_info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole

            for worker, connection_info in info_dict.items():
                name = "dask-" + worker.replace(":", "-").replace("/", "-")
                connect_qtconsole(connection_info, name=name, extra_args=qtconsole_args)
        return info_dict

    def start_ipython_scheduler(
        self, magic_name="scheduler_if_ipython", qtconsole=False, qtconsole_args=None
    ):
        """Start IPython kernel on the scheduler

        Parameters
        ----------
        magic_name : str or None (optional)
            If defined, register IPython magic with this name for
            executing code on the scheduler.
            If not defined, register %scheduler magic if IPython is running.
        qtconsole : bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).
        qtconsole_args : list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> c.start_ipython_scheduler() # doctest: +SKIP
        >>> %scheduler scheduler.processing  # doctest: +SKIP
        {'127.0.0.1:3595': {'inc-1', 'inc-2'},
         '127.0.0.1:53589': {'inc-2', 'add-5'}}

        >>> c.start_ipython_scheduler(qtconsole=True) # doctest: +SKIP

        Returns
        -------
        connection_info: dict
            connection_info dict containing info necessary
            to connect Jupyter clients to the scheduler.

        See Also
        --------
        Client.start_ipython_workers : Start IPython on the workers
        """
        info = sync(self.loop, self.scheduler.start_ipython)
        if magic_name == "scheduler_if_ipython":
            # default to %scheduler if in IPython, no magic otherwise
            in_ipython = False
            if "IPython" in sys.modules:
                from IPython import get_ipython

                in_ipython = bool(get_ipython())
            if in_ipython:
                magic_name = "scheduler"
            else:
                magic_name = None
        if magic_name:
            from ._ipython_utils import register_worker_magic

            register_worker_magic(info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole

            connect_qtconsole(info, name="dask-scheduler", extra_args=qtconsole_args)
        return info

    @classmethod
    def _expand_key(cls, k):
        """
        Expand a user-provided task key specification, e.g. in a resources
        or retries dictionary.
        """
        if not isinstance(k, tuple):
            k = (k,)
        for kk in k:
            if dask.is_dask_collection(kk):
                for kkk in kk.__dask_keys__():
                    yield stringify(kkk)
            else:
                yield stringify(kk)

    @staticmethod
    def collections_to_dsk(collections, *args, **kwargs):
        """Convert many collections into a single dask graph, after optimization"""
        return collections_to_dsk(collections, *args, **kwargs)

    def get_task_stream(
        self,
        start=None,
        stop=None,
        count=None,
        plot=False,
        filename="task-stream.html",
        bokeh_resources=None,
    ):
        """Get task stream data from scheduler

        This collects the data present in the diagnostic "Task Stream" plot on
        the dashboard.  It includes the start, stop, transfer, and
        deserialization time of every task for a particular duration.

        Note that the task stream diagnostic does not run by default.  You may
        wish to call this function once before you start work to ensure that
        things start recording, and then again after you have completed.

        Parameters
        ----------
        start : Number or string
            When you want to start recording
            If a number it should be the result of calling time()
            If a string then it should be a time difference before now,
            like '60s' or '500 ms'
        stop : Number or string
            When you want to stop recording
        count : int
            The number of desired records, ignored if both start and stop are
            specified
        plot : boolean, str
            If true then also return a Bokeh figure
            If plot == 'save' then save the figure to a file
        filename : str (optional)
            The filename to save to if you set ``plot='save'``
        bokeh_resources : bokeh.resources.Resources (optional)
            Specifies if the resource component is INLINE or CDN

        Examples
        --------
        >>> client.get_task_stream()  # prime plugin if not already connected
        >>> x.compute()  # do some work
        >>> client.get_task_stream()
        [{'task': ...,
          'type': ...,
          'thread': ...,
          ...}]

        Pass the ``plot=True`` or ``plot='save'`` keywords to get back a Bokeh
        figure

        >>> data, figure = client.get_task_stream(plot='save', filename='myfile.html')

        Alternatively consider the context manager

        >>> from dask.distributed import get_task_stream
        >>> with get_task_stream() as ts:
        ...     x.compute()
        >>> ts.data
        [...]

        Returns
        -------
        L: List[Dict]

        See Also
        --------
        get_task_stream : a context manager version of this method
        """
        return self.sync(
            self._get_task_stream,
            start=start,
            stop=stop,
            count=count,
            plot=plot,
            filename=filename,
            bokeh_resources=bokeh_resources,
        )

    async def _get_task_stream(
        self,
        start=None,
        stop=None,
        count=None,
        plot=False,
        filename="task-stream.html",
        bokeh_resources=None,
    ):
        msgs = await self.scheduler.get_task_stream(start=start, stop=stop, count=count)
        if plot:
            from .diagnostics.task_stream import rectangles

            rects = rectangles(msgs)
            from .dashboard.components.scheduler import task_stream_figure

            source, figure = task_stream_figure(sizing_mode="stretch_both")
            source.data.update(rects)
            if plot == "save":
                from bokeh.plotting import output_file, save

                output_file(filename=filename, title="Dask Task Stream")
                save(figure, filename=filename, resources=bokeh_resources)
            return (msgs, figure)
        else:
            return msgs

    async def _register_scheduler_plugin(self, plugin, name, **kwargs):
        if isinstance(plugin, type):
            plugin = plugin(**kwargs)

        return await self.scheduler.register_scheduler_plugin(
            plugin=dumps(plugin, protocol=4),
            name=name,
        )

    def register_scheduler_plugin(self, plugin, name=None, **kwargs):
        """Register a scheduler plugin.

        See https://distributed.readthedocs.io/en/latest/plugins.html#scheduler-plugins

        Parameters
        ----------
        plugin : SchedulerPlugin
            SchedulerPlugin instance to pass to the scheduler.
        name : str
            Name for the plugin; if None, a name is taken from the
            plugin instance or automatically generated if not present.
        **kwargs : Any
            deprecated; Arguments passed to the Plugin class (if Plugin is an
            instance kwargs are unused).

        """
        if isinstance(plugin, type):
            warnings.warn(
                "Adding plugins by class is deprecated and will be disabled in a "
                "future release. Please add plugins by instance instead.",
                category=FutureWarning,
            )
            # note: plugin is constructed in async def _register_scheduler_plugin
        elif kwargs:
            raise ValueError("kwargs provided but plugin is already an instance")

        if name is None:
            name = _get_plugin_name(plugin)

        return self.sync(
            self._register_scheduler_plugin,
            plugin=plugin,
            name=name,
            **kwargs,
        )

    def register_worker_callbacks(self, setup=None):
        """
        Registers a setup callback function for all current and future workers.

        This registers a new setup function for workers in this cluster. The
        function will run immediately on all currently connected workers. It
        will also be run upon connection by any workers that are added in the
        future. Multiple setup functions can be registered - these will be
        called in the order they were added.

        If the function takes an input argument named ``dask_worker`` then
        that variable will be populated with the worker itself.

        Parameters
        ----------
        setup : callable(dask_worker: Worker) -> None
            Function to register and run on all workers
        """
        return self.register_worker_plugin(_WorkerSetupPlugin(setup))

    async def _register_worker_plugin(self, plugin=None, name=None, nanny=None):
        if nanny or nanny is None and isinstance(plugin, NannyPlugin):
            method = self.scheduler.register_nanny_plugin
        else:
            method = self.scheduler.register_worker_plugin

        responses = await method(plugin=dumps(plugin, protocol=4), name=name)
        for response in responses.values():
            if response["status"] == "error":
                _, exc, tb = clean_exception(
                    response["exception"], response["traceback"]
                )
                raise exc.with_traceback(tb)
        return responses

    def register_worker_plugin(self, plugin=None, name=None, nanny=None, **kwargs):
        """
        Registers a lifecycle worker plugin for all current and future workers.

        This registers a new object to handle setup, task state transitions and
        teardown for workers in this cluster. The plugin will instantiate
        itself on all currently connected workers. It will also be run on any
        worker that connects in the future.

        The plugin may include methods ``setup``, ``teardown``, ``transition``,
        and ``release_key``.  See the
        ``dask.distributed.WorkerPlugin`` class or the examples below for the
        interface and docstrings.  It must be serializable with the pickle or
        cloudpickle modules.

        If the plugin has a ``name`` attribute, or if the ``name=`` keyword is
        used then that will control idempotency.  If a plugin with that name has
        already been registered, then it will be removed and replaced by the new one.

        For alternatives to plugins, you may also wish to look into preload
        scripts.

        Parameters
        ----------
        plugin : WorkerPlugin or NannyPlugin
            WorkerPlugin or NannyPlugin instance to register.
        name : str, optional
            A name for the plugin.
            Registering a plugin with the same name will have no effect.
            If plugin has no name attribute a random name is used.
        nanny : bool, optional
            Whether to register the plugin with workers or nannies.
        **kwargs : optional
            Deprecated; If you pass a class as the plugin, instead of a class
            instance, then the class will be instantiated with any extra
            keyword arguments.

        Examples
        --------
        >>> class MyPlugin(WorkerPlugin):
        ...     def __init__(self, *args, **kwargs):
        ...         pass  # the constructor is up to you
        ...     def setup(self, worker: dask.distributed.Worker):
        ...         pass
        ...     def teardown(self, worker: dask.distributed.Worker):
        ...         pass
        ...     def transition(self, key: str, start: str, finish: str,
        ...                    **kwargs):
        ...         pass
        ...     def release_key(self, key: str, state: str, cause: str | None, reason: None, report: bool):
        ...         pass

        >>> plugin = MyPlugin(1, 2, 3)
        >>> client.register_worker_plugin(plugin)

        You can get access to the plugin with the ``get_worker`` function

        >>> client.register_worker_plugin(other_plugin, name='my-plugin')
        >>> def f():
        ...    worker = get_worker()
        ...    plugin = worker.plugins['my-plugin']
        ...    return plugin.my_state

        >>> future = client.run(f)

        See Also
        --------
        distributed.WorkerPlugin
        unregister_worker_plugin
        """
        if isinstance(plugin, type):
            warnings.warn(
                "Adding plugins by class is deprecated and will be disabled in a "
                "future release. Please add plugins by instance instead.",
                category=FutureWarning,
            )
            plugin = plugin(**kwargs)
        elif kwargs:
            raise ValueError("kwargs provided but plugin is already an instance")

        if name is None:
            name = _get_plugin_name(plugin)

        assert name

        return self.sync(
            self._register_worker_plugin, plugin=plugin, name=name, nanny=nanny
        )

    async def _unregister_worker_plugin(self, name, nanny=None):
        if nanny:
            responses = await self.scheduler.unregister_nanny_plugin(name=name)
        else:
            responses = await self.scheduler.unregister_worker_plugin(name=name)

        for response in responses.values():
            if response["status"] == "error":
                exc = response["exception"]
                tb = response["traceback"]
                raise exc.with_traceback(tb)
        return responses

    def unregister_worker_plugin(self, name, nanny=None):
        """Unregisters a lifecycle worker plugin

        This unregisters an existing worker plugin. As part of the unregistration process
        the plugin's ``teardown`` method will be called.

        Parameters
        ----------
        name : str
            Name of the plugin to unregister. See the :meth:`Client.register_worker_plugin`
            docstring for more information.

        Examples
        --------
        >>> class MyPlugin(WorkerPlugin):
        ...     def __init__(self, *args, **kwargs):
        ...         pass  # the constructor is up to you
        ...     def setup(self, worker: dask.distributed.Worker):
        ...         pass
        ...     def teardown(self, worker: dask.distributed.Worker):
        ...         pass
        ...     def transition(self, key: str, start: str, finish: str, **kwargs):
        ...         pass
        ...     def release_key(self, key: str, state: str, cause: str | None, reason: None, report: bool):
        ...         pass

        >>> plugin = MyPlugin(1, 2, 3)
        >>> client.register_worker_plugin(plugin, name='foo')
        >>> client.unregister_worker_plugin(name='foo')

        See Also
        --------
        register_worker_plugin
        """
        return self.sync(self._unregister_worker_plugin, name=name, nanny=nanny)

    @property
    def amm(self):
        """Convenience accessors for the :doc:`active_memory_manager`"""
        from .active_memory_manager import AMMClientProxy

        return AMMClientProxy(self)


class _WorkerSetupPlugin(WorkerPlugin):
    """This is used to support older setup functions as callbacks"""

    def __init__(self, setup):
        self._setup = setup

    def setup(self, worker):
        if has_keyword(self._setup, "dask_worker"):
            return self._setup(dask_worker=worker)
        else:
            return self._setup()


class Executor(Client):
    """Deprecated: see Client"""

    def __init__(self, *args, **kwargs):
        warnings.warn("Executor has been renamed to Client")
        super().__init__(*args, **kwargs)


def CompatibleExecutor(*args, **kwargs):
    raise Exception("This has been moved to the Client.get_executor() method")


ALL_COMPLETED = "ALL_COMPLETED"
FIRST_COMPLETED = "FIRST_COMPLETED"


async def _wait(fs, timeout=None, return_when=ALL_COMPLETED):
    if timeout is not None and not isinstance(timeout, Number):
        raise TypeError(
            "timeout= keyword received a non-numeric value.\n"
            "Beware that wait expects a list of values\n"
            "  Bad:  wait(x, y, z)\n"
            "  Good: wait([x, y, z])"
        )
    fs = futures_of(fs)
    if return_when == ALL_COMPLETED:
        wait_for = All
    elif return_when == FIRST_COMPLETED:
        wait_for = Any
    else:
        raise NotImplementedError(
            "Only return_when='ALL_COMPLETED' and 'FIRST_COMPLETED' are supported"
        )

    future = wait_for({f._state.wait() for f in fs})
    if timeout is not None:
        future = asyncio.wait_for(future, timeout)
    await future

    done, not_done = (
        {fu for fu in fs if fu.status != "pending"},
        {fu for fu in fs if fu.status == "pending"},
    )
    cancelled = [f.key for f in done if f.status == "cancelled"]
    if cancelled:
        raise CancelledError(cancelled)

    return DoneAndNotDoneFutures(done, not_done)


def wait(fs, timeout=None, return_when=ALL_COMPLETED):
    """Wait until all/any futures are finished

    Parameters
    ----------
    fs : List[Future]
    timeout : number, optional
        Time in seconds after which to raise a
        ``dask.distributed.TimeoutError``
    return_when : str, optional
        One of `ALL_COMPLETED` or `FIRST_COMPLETED`

    Returns
    -------
    Named tuple of completed, not completed
    """
    client = default_client()
    result = client.sync(_wait, fs, timeout=timeout, return_when=return_when)
    return result


async def _as_completed(fs, queue):
    fs = futures_of(fs)
    groups = groupby(lambda f: f.key, fs)
    firsts = [v[0] for v in groups.values()]
    wait_iterator = gen.WaitIterator(
        *map(asyncio.ensure_future, [f._state.wait() for f in firsts])
    )

    while not wait_iterator.done():
        await wait_iterator.next()
        # TODO: handle case of restarted futures
        future = firsts[wait_iterator.current_index]
        for f in groups[future.key]:
            queue.put_nowait(f)


async def _first_completed(futures):
    """Return a single completed future

    See Also:
        _as_completed
    """
    q = asyncio.Queue()
    await _as_completed(futures, q)
    result = await q.get()
    return result


class as_completed:
    """
    Return futures in the order in which they complete

    This returns an iterator that yields the input future objects in the order
    in which they complete.  Calling ``next`` on the iterator will block until
    the next future completes, irrespective of order.

    Additionally, you can also add more futures to this object during
    computation with the ``.add`` method

    Parameters
    ----------
    futures: Collection of futures
        A list of Future objects to be iterated over in the order in which they
        complete
    with_results: bool (False)
        Whether to wait and include results of futures as well;
        in this case `as_completed` yields a tuple of (future, result)
    raise_errors: bool (True)
        Whether we should raise when the result of a future raises an
        exception; only affects behavior when `with_results=True`.

    Examples
    --------
    >>> x, y, z = client.map(inc, [1, 2, 3])  # doctest: +SKIP
    >>> for future in as_completed([x, y, z]):  # doctest: +SKIP
    ...     print(future.result())  # doctest: +SKIP
    3
    2
    4

    Add more futures during computation

    >>> x, y, z = client.map(inc, [1, 2, 3])  # doctest: +SKIP
    >>> ac = as_completed([x, y, z])  # doctest: +SKIP
    >>> for future in ac:  # doctest: +SKIP
    ...     print(future.result())  # doctest: +SKIP
    ...     if random.random() < 0.5:  # doctest: +SKIP
    ...         ac.add(c.submit(double, future))  # doctest: +SKIP
    4
    2
    8
    3
    6
    12
    24

    Optionally wait until the result has been gathered as well

    >>> ac = as_completed([x, y, z], with_results=True)  # doctest: +SKIP
    >>> for future, result in ac:  # doctest: +SKIP
    ...     print(result)  # doctest: +SKIP
    2
    4
    3
    """

    def __init__(self, futures=None, loop=None, with_results=False, raise_errors=True):
        if futures is None:
            futures = []
        self.futures = defaultdict(lambda: 0)
        self.queue = pyQueue()
        self.lock = threading.Lock()
        self.loop = loop or default_client().loop
        self.thread_condition = threading.Condition()
        self.with_results = with_results
        self.raise_errors = raise_errors

        if futures:
            self.update(futures)

    @property
    def condition(self):
        try:
            return self._condition
        except AttributeError:
            self._condition = asyncio.Condition()
            return self._condition

    async def _track_future(self, future):
        try:
            await _wait(future)
        except CancelledError:
            pass
        if self.with_results:
            try:
                result = await future._result(raiseit=False)
            except CancelledError as exc:
                result = exc
        with self.lock:
            if future in self.futures:
                self.futures[future] -= 1
                if not self.futures[future]:
                    del self.futures[future]
                if self.with_results:
                    self.queue.put_nowait((future, result))
                else:
                    self.queue.put_nowait(future)
                async with self.condition:
                    self.condition.notify()
                with self.thread_condition:
                    self.thread_condition.notify()

    def update(self, futures):
        """Add multiple futures to the collection.

        The added futures will emit from the iterator once they finish"""
        from .actor import BaseActorFuture

        with self.lock:
            for f in futures:
                if not isinstance(f, (Future, BaseActorFuture)):
                    raise TypeError("Input must be a future, got %s" % f)
                self.futures[f] += 1
                self.loop.add_callback(self._track_future, f)

    def add(self, future):
        """Add a future to the collection

        This future will emit from the iterator once it finishes
        """
        self.update((future,))

    def is_empty(self):
        """Returns True if there no completed or computing futures"""
        return not self.count()

    def has_ready(self):
        """Returns True if there are completed futures available."""
        return not self.queue.empty()

    def count(self):
        """Return the number of futures yet to be returned

        This includes both the number of futures still computing, as well as
        those that are finished, but have not yet been returned from this
        iterator.
        """
        with self.lock:
            return len(self.futures) + len(self.queue.queue)

    def __repr__(self):
        return "<as_completed: waiting={} done={}>".format(
            len(self.futures), len(self.queue.queue)
        )

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def _get_and_raise(self):
        res = self.queue.get()
        if self.with_results:
            future, result = res
            if self.raise_errors and future.status == "error":
                typ, exc, tb = result
                raise exc.with_traceback(tb)
        return res

    def __next__(self):
        while self.queue.empty():
            if self.is_empty():
                raise StopIteration()
            with self.thread_condition:
                self.thread_condition.wait(timeout=0.100)
        return self._get_and_raise()

    async def __anext__(self):
        if not self.futures and self.queue.empty():
            raise StopAsyncIteration
        while self.queue.empty():
            if not self.futures:
                raise StopAsyncIteration
            async with self.condition:
                await self.condition.wait()

        return self._get_and_raise()

    next = __next__

    def next_batch(self, block=True):
        """Get the next batch of completed futures.

        Parameters
        ----------
        block : bool, optional
            If True then wait until we have some result, otherwise return
            immediately, even with an empty list.  Defaults to True.

        Examples
        --------
        >>> ac = as_completed(futures)  # doctest: +SKIP
        >>> client.gather(ac.next_batch())  # doctest: +SKIP
        [4, 1, 3]

        >>> client.gather(ac.next_batch(block=False))  # doctest: +SKIP
        []

        Returns
        -------
        List of futures or (future, result) tuples
        """
        if block:
            batch = [next(self)]
        else:
            batch = []
        while not self.queue.empty():
            batch.append(self.queue.get())
        return batch

    def batches(self):
        """
        Yield all finished futures at once rather than one-by-one

        This returns an iterator of lists of futures or lists of
        (future, result) tuples rather than individual futures or individual
        (future, result) tuples.  It will yield these as soon as possible
        without waiting.

        Examples
        --------
        >>> for batch in as_completed(futures).batches():  # doctest: +SKIP
        ...     results = client.gather(batch)
        ...     print(results)
        [4, 2]
        [1, 3, 7]
        [5]
        [6]
        """
        while True:
            try:
                yield self.next_batch(block=True)
            except StopIteration:
                return

    def clear(self):
        """Clear out all submitted futures"""
        with self.lock:
            self.futures.clear()
            while not self.queue.empty():
                self.queue.get()


def AsCompleted(*args, **kwargs):
    raise Exception("This has moved to as_completed")


def default_client(c=None):
    """Return a client if one has started

    Parameters
    ----------
    c : Client
        The client to return. If None, the default client is returned.

    Returns
    -------
    c : Client
        The client, if one has started
    """
    c = c or _get_global_client()
    if c:
        return c
    else:
        raise ValueError(
            "No clients found\n"
            "Start a client and point it to the scheduler address\n"
            "  from distributed import Client\n"
            "  client = Client('ip-addr-of-scheduler:8786')\n"
        )


def ensure_default_client(client):
    """Ensures the client passed as argument is set as the default

    Parameters
    ----------
    client : Client
        The client
    """
    dask.config.set(scheduler="dask.distributed")
    _set_global_client(client)


def redict_collection(c, dsk):
    """Change the dictionary in the collection

    Parameters
    ----------
    c : collection
        The collection
    dsk : dict
        The dictionary

    Returns
    -------
    c : Delayed
        If the collection is a 'Delayed' object the collection is returned
    cc : collection
        If the collection is not a 'Delayed' object a copy of the
        collection with xthe new dictionary is returned

    """
    from dask.delayed import Delayed

    if isinstance(c, Delayed):
        return Delayed(c.key, dsk)
    else:
        cc = copy.copy(c)
        cc.dask = dsk
        return cc


def futures_of(o, client=None):
    """Future objects in a collection

    Parameters
    ----------
    o : collection
        A possibly nested collection of Dask objects
    client : Client, optional
        The client

    Examples
    --------
    >>> futures_of(my_dask_dataframe)
    [<Future: finished key: ...>,
     <Future: pending  key: ...>]

    Raises
    ------
    CancelledError
        If one of the futures is cancelled a CancelledError is raised

    Returns
    -------
    futures : List[Future]
        A list of futures held by those collections
    """
    stack = [o]
    seen = set()
    futures = list()
    while stack:
        x = stack.pop()
        if type(x) in (tuple, set, list):
            stack.extend(x)
        elif type(x) is dict:
            stack.extend(x.values())
        elif type(x) is SubgraphCallable:
            stack.extend(x.dsk.values())
        elif isinstance(x, Future):
            if x not in seen:
                seen.add(x)
                futures.append(x)
        elif dask.is_dask_collection(x):
            stack.extend(x.__dask_graph__().values())

    if client is not None:
        bad = {f for f in futures if f.cancelled()}
        if bad:
            raise CancelledError(bad)

    return futures[::-1]


def fire_and_forget(obj):
    """Run tasks at least once, even if we release the futures

    Under normal operation Dask will not run any tasks for which there is not
    an active future (this avoids unnecessary work in many situations).
    However sometimes you want to just fire off a task, not track its future,
    and expect it to finish eventually.  You can use this function on a future
    or collection of futures to ask Dask to complete the task even if no active
    client is tracking it.

    The results will not be kept in memory after the task completes (unless
    there is an active future) so this is only useful for tasks that depend on
    side effects.

    Parameters
    ----------
    obj : Future, list, dict, dask collection
        The futures that you want to run at least once

    Examples
    --------
    >>> fire_and_forget(client.submit(func, *args))  # doctest: +SKIP
    """
    futures = futures_of(obj)
    for future in futures:
        future.client._send_to_scheduler(
            {
                "op": "client-desires-keys",
                "keys": [stringify(future.key)],
                "client": "fire-and-forget",
            }
        )


class get_task_stream:
    """
    Collect task stream within a context block

    This provides diagnostic information about every task that was run during
    the time when this block was active.

    This must be used as a context manager.

    Parameters
    ----------
    plot: boolean, str
        If true then also return a Bokeh figure
        If plot == 'save' then save the figure to a file
    filename: str (optional)
        The filename to save to if you set ``plot='save'``

    Examples
    --------
    >>> with get_task_stream() as ts:
    ...     x.compute()
    >>> ts.data
    [...]

    Get back a Bokeh figure and optionally save to a file

    >>> with get_task_stream(plot='save', filename='task-stream.html') as ts:
    ...    x.compute()
    >>> ts.figure
    <Bokeh Figure>

    To share this file with others you may wish to upload and serve it online.
    A common way to do this is to upload the file as a gist, and then serve it
    on https://raw.githack.com ::

       $ python -m pip install gist
       $ gist task-stream.html
       https://gist.github.com/8a5b3c74b10b413f612bb5e250856ceb

    You can then navigate to that site, click the "Raw" button to the right of
    the ``task-stream.html`` file, and then provide that URL to
    https://raw.githack.com .  This process should provide a sharable link that
    others can use to see your task stream plot.

    See Also
    --------
    Client.get_task_stream: Function version of this context manager
    """

    def __init__(self, client=None, plot=False, filename="task-stream.html"):
        self.data = []
        self._plot = plot
        self._filename = filename
        self.figure = None
        self.client = client or default_client()
        self.client.get_task_stream(start=0, stop=0)  # ensure plugin

    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, typ, value, traceback):
        L = self.client.get_task_stream(
            start=self.start, plot=self._plot, filename=self._filename
        )
        if self._plot:
            L, self.figure = L
        self.data.extend(L)

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        L = await self.client.get_task_stream(
            start=self.start, plot=self._plot, filename=self._filename
        )
        if self._plot:
            L, self.figure = L
        self.data.extend(L)


class performance_report:
    """Gather performance report

    This creates a static HTML file that includes many of the same plots of the
    dashboard for later viewing.

    The resulting file uses JavaScript, and so must be viewed with a web
    browser.  Locally we recommend using ``python -m http.server`` or hosting
    the file live online.

    Parameters
    ----------
    filename: str, optional
        The filename to save the performance report locally

    stacklevel: int, optional
        The code execution frame utilized for populating the Calling Code section
        of the report. Defaults to `1` which is the frame calling ``performance_report``

    mode: str, optional
        Mode parameter to pass to :func:`bokeh.io.output.output_file`. Defaults to ``None``.

    Examples
    --------
    >>> with performance_report(filename="myfile.html", stacklevel=1):
    ...     x.compute()

    $ python -m http.server
    $ open myfile.html
    """

    def __init__(self, filename="dask-report.html", stacklevel=1, mode=None):
        self.filename = filename
        # stacklevel 0 or less - shows dask internals which likely isn't helpful
        self._stacklevel = stacklevel if stacklevel > 0 else 1
        self.mode = mode

    async def __aenter__(self):
        self.start = time()
        self.last_count = await get_client().run_on_scheduler(
            lambda dask_scheduler: dask_scheduler.monitor.count
        )
        await get_client().get_task_stream(start=0, stop=0)  # ensure plugin

    async def __aexit__(self, typ, value, traceback, code=None):
        client = get_client()
        if code is None:
            code = client._get_computation_code(self._stacklevel + 1)
        data = await client.scheduler.performance_report(
            start=self.start, last_count=self.last_count, code=code, mode=self.mode
        )
        with open(self.filename, "w") as f:
            f.write(data)

    def __enter__(self):
        get_client().sync(self.__aenter__)

    def __exit__(self, typ, value, traceback):
        client = get_client()
        code = client._get_computation_code(self._stacklevel + 1)
        client.sync(self.__aexit__, type, value, traceback, code=code)


class get_task_metadata:
    """Collect task metadata within a context block

    This gathers ``TaskState`` metadata and final state from the scheduler
    for tasks which are submitted and finished within the scope of this
    context manager.

    Examples
    --------
    >>> with get_task_metadata() as tasks:
    ...     x.compute()
    >>> tasks.metadata
    {...}
    >>> tasks.state
    {...}
    """

    def __init__(self):
        self.name = f"task-metadata-{uuid.uuid4().hex}"
        self.keys = set()
        self.metadata = None
        self.state = None

    async def __aenter__(self):
        await get_client().scheduler.start_task_metadata(name=self.name)
        return self

    async def __aexit__(self, typ, value, traceback):
        response = await get_client().scheduler.stop_task_metadata(name=self.name)
        self.metadata = response["metadata"]
        self.state = response["state"]

    def __enter__(self):
        return get_client().sync(self.__aenter__)

    def __exit__(self, typ, value, traceback):
        return get_client().sync(self.__aexit__, type, value, traceback)


@contextmanager
def temp_default_client(c):
    """Set the default client for the duration of the context

    .. note::
       This function should be used exclusively for unit testing the default
       client functionality. In all other cases, please use
       ``Client.as_current`` instead.

    .. note::
       Unlike ``Client.as_current``, this context manager is neither
       thread-local nor task-local.

    Parameters
    ----------
    c : Client
        This is what default_client() will return within the with-block.
    """
    old_exec = default_client()
    _set_global_client(c)
    try:
        yield
    finally:
        _set_global_client(old_exec)


def _close_global_client():
    """
    Force close of global client.  This cleans up when a client
    wasn't close explicitly, e.g. interactive sessions.
    """
    c = _get_global_client()
    if c is not None:
        c._should_close_loop = False
        with suppress(TimeoutError, RuntimeError):
            if c.asynchronous:
                c.loop.add_callback(c.close, timeout=3)
            else:
                c.close(timeout=3)


atexit.register(_close_global_client)


def __getattr__(name):
    if name == "ensure_default_get":
        warnings.warn(
            "`ensure_default_get` is deprecated and will be removed in a future release. "
            "Please use `distributed.client.ensure_default_client` instead.",
            category=FutureWarning,
            stacklevel=2,
        )
        return ensure_default_client
    else:
        raise AttributeError(f"module {__name__} has no attribute {name}")
