# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import atexit
import collections
import concurrent.futures
import contextlib
import functools
import getpass
import inspect
import io
import itertools
import logging
import math
import numbers
import os
import pickle
import random
import sys
import sysconfig
import threading
import time
import uuid
import warnings
import weakref
from collections import ChainMap, defaultdict, deque
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Generator,
    Hashable,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
    Set,
)
from concurrent.futures import CancelledError, Future
from contextlib import suppress
from datetime import timedelta
from functools import partial
from numbers import Number
from operator import getitem
from time import sleep
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    ClassVar,
    Generic,
    Literal,
    NamedTuple,
    NoReturn,
    TypeVar,
    Union,
    cast,
)

import dask
import dask.config
from dask.base import DaskMethodsMixin, is_dask_collection, tokenize
from dask.core import istask
from dask.delayed import Delayed, delayed
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable, fuse
from dask.utils import (
    SerializableLock,
    apply,
    format_bytes,
    format_time_ago,
    funcname,
    get_meta_library,
    has_keyword,
    key_split,
    parse_bytes,
    parse_timedelta,
    stringify,
)
from tlz import first, groupby, merge, partition_all, pluck, valmap
from tornado.ioloop import IOLoop

from . import config, versions
from ._async import Condition, Event, Lock, add_callback, asyncio_ensure_future
from ._concurrent_futures_thread import _shutdown_thread_pool
from ._stories import story
from .active_memory_manager import ActiveMemoryManager
from .batched import BatchedSend
from .cfexecutor import ClientExecutor
from .collections import AwaitableQueue
from .comm import (
    Comm,
    CommClosedError,
    connect,
    get_address_host,
    get_local_address_for,
    parse_address,
    parse_addresses,
    resolve_address,
    unparse_address,
)
from .comm.registry import backends, get_backend
from .comm.utils import OFFLOAD_THRESHOLD
from .compatibility import PeriodicCallback
from .core import CommNanny, Status, dumps, loads, rpc
from .diagnostics.plugin import WorkerPlugin
from .diagnostics.progress import AllProgress, MultiProgress, Progress, progress
from .event import Event as SyncEvent
from .exceptions import AllExit, CancelledError as DistributedCancelledError, TimeoutError
from .metrics import context_meter, time_meter
from .objects import HasWhat, SchedulerInfo, WhoHas
from .preloading import validate_preload_argv
from .protocol import HasWhat_Msg, LoseData, ToPickle
from .protocol.serialize import (
    Serialize,
    Serialized,
    extract_serialize,
    nested_deserialize,
    serialize,
    to_serialize,
)
from .publish import Datasets
from .queues import Queue
from .recreate_tasks import ReplayTaskGroup, unpack_remotedata
from .security import Security
from .semaphore import Semaphore
from .sizeof import sizeof
from .threadpoolexecutor import ThreadPoolExecutor
from .utils import (
    CancelledError as _CancelledError,
    LoopRunner,
    NoOpAwaitable,
    TimeoutError as _TimeoutError,
    _maybe_complex,
    get_ip,
    get_ipv6,
    has_arg,
    is_coroutine_function,
    is_valid_key,
    log_errors,
    maybe_await,
    no_default,
    offload,
    parse_ports,
    short_exception,
    sync,
    thread_state,
    warn_on_duration,
)
from .utils_comm import (
    gather_from_workers,
    pack_data,
    retry_operation,
    scatter_to_workers,
)
from .variable import Variable
from .worker import FutureState, get_client, get_worker, secede, wait

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
    T = TypeVar("T")
    NoneType = type(None)

    # circular dependencies
    from .deploy.cluster import Cluster
    from .deploy.spec import SpecCluster
    from .diagnostics.progressbar import TextProgressBar
    from .diagnostics.websocket import WebsocketHandler
    from .nanny import Nanny
    from .scheduler import Scheduler


logger = logging.getLogger(__name__)

DEFAULT_EXTENSIONS: tuple[str, ...] = ()


def _get_global_client() -> Client | None:
    return getattr(thread_state, "global_client", None)


def _set_global_client(c: Client | None) -> None:
    thread_state.global_client = c


def _del_global_client() -> None:
    del thread_state.global_client


class SourceCode(NamedTuple):
    """Source code of a function, broken down by modules"""

    # Function source code proper
    func_source: str
    # Source code of the module where the function is defined
    module_source: str
    # Source code of any other modules the function may be using through closures
    closure_modules_sources: dict[str, str]


@functools.total_ordering
class Future(FutureState[T]):
    """A remotely running computation

    A Future is a local proxy to a result computation running on a cluster.

    A Future is created either by ``Client.submit``, ``Client.map``,
    ``Client.get``, or from distributed data structures. It refers to a task
    that will run on the cluster.

    Parameters
    ----------
    key: str, Key
        The key of the task this future refers to
    client: Client
        The client that created this future. Usually this is the default client
    inform: bool
        Whether or not the scheduler should inform the client when this future
        finishes
    state: FutureState
        The state of the future

    See Also
    --------
    Client
    """

    _client: Client | None
    _state: FutureState[T]
    _key: Hashable
    _inform: bool
    _shared_state: dict[str, Any] | None
    _instance_name: str | None

    # Make Future pickleable according to the conventional protocol
    __getstate__ = None
    __setstate__ = None

    def __init__(
        self,
        key: Hashable,
        client: Client | None = None,
        inform: bool = True,
        state: FutureState[T] | None = None,
    ):
        self._key = key
        self._client = client or _get_global_client()
        self._inform = inform
        self._shared_state = None
        # instance_name is used by repr and str to give some helpful context
        # about what the future is for. e.g. the name of the variable it's
        # assigned to. This isn't serialized.
        self._instance_name = None
        if self._client is None:
            raise ValueError(
                "No global client found and no client specified. "
                "Use Client() as a context manager or "
                "set a global client using Client.as_current()."
            )
        if state is None:
            # Avoid initializing FutureState if we already have one.
            # This is useful for methods like 'gather' that already have this information
            try:
                self._state = self._client.futures[key]
            except KeyError:
                self._state = FutureState(status="pending")
                self._client.futures[key] = self._state
                if inform:
                    self._client._send_to_scheduler(
                        {"op": "client-desires-keys", "keys": [key], "client": self._client.id}
                    )
        else:
            self._state = state
            self._client.futures[key] = self._state

        if self._state.client is None:
            self._state.client = self._client

    @property
    def client(self) -> Client:
        if self._client is None:
            raise ValueError("Future has no associated client")
        return self._client

    @property
    def key(self) -> Hashable:
        return self._key

    @property
    def status(self) -> str:
        """The status of the Future

        Returns
        -------
        status: {'pending', 'processing', 'finished', 'error', 'cancelled'}
        """
        return self._state.status

    @property
    def done(self) -> bool:
        """Return True if the computation is finished or cancelled."""
        return self._state.done()

    @property
    def cancelled(self) -> bool:
        """Return True if the computation was cancelled."""
        return self._state.cancelled()

    def cancel(self, **kwargs: Any) -> None:
        """Cancel the running task

        This attempts to cancel the remotely running task. If the task is
        already finished then this does nothing. If the task is currently
        running then the worker will attempt to stop the task. If the task has
        not yet been started then it will be stopped prior to starting on a
        worker.

        Parameters
        ----------
        **kwargs : dict
            Optional keyword arguments for the remote function

        Returns
        -------
        None

        See Also
        --------
        Client.cancel
        """
        self._client.cancel([self], **kwargs)

    @property
    def exception(self) -> Exception | None:
        """Return the exception of a failed task

        If the task failed, this returns the exception object. Otherwise it
        returns None.

        See Also
        --------
        Future.traceback
        Client.recreate_error_locally
        """
        if self._state.status == "error":
            return self._state.exception
        else:
            return None

    @property
    def traceback(self) -> BaseException | None:
        """Return the traceback of a failed task

        If the task failed, this returns the traceback object. Otherwise it
        returns None.

        See Also
        --------
        Future.exception
        Client.recreate_error_locally
        """
        if self._state.status == "error":
            return self._state.traceback
        else:
            return None

    def add_done_callback(self, fn: Callable[[Future], None]) -> None:
        """Call callback function when future is finished

        The callback ``fn`` should take the future as its argument. This will
        be called regardless of whether the future completes successfully,
        errs, or is cancelled.

        This is particularly useful for asynchronous computations.

        Warning: The callback is executed by the same thread that completes
        the Future. Therefore, it should not perform any blocking operations,
        to avoid stalling the client's event loop.
        """
        self._state.add_done_callback(fn)

    def result(self, timeout: float | str | None = None) -> T:
        """Wait for the computation to complete and return the result.

        Parameters
        ----------
        timeout : number, optional
            Time in seconds after which to raise a
            ``distributed.TimeoutError`` if the future has not completed.
            Can also be a string like "10s".

        Returns
        -------
        result : object

        Raises
        ------
        distributed.TimeoutError
            If *timeout* is reached before the future completes.
        CancelledError
            If the task was cancelled. This definition is consistent with the
            standard library ``concurrent.futures``.
        Exception
            If the remote task raised an exception.
        """
        if self._client.asynchronous:
            raise RuntimeError(
                "Future.result() cannot be used from within asynchronous function"
            )

        event = Event()
        self.add_done_callback(lambda f: event.set())

        if not self.done():
            sync(self._client.loop, self._wait, timeout=timeout)
            # Ensure that the event is set by the time _wait returns
            assert event.is_set()

        if self._state.status == "finished":
            return self._state.result
        elif self._state.status == "cancelled":
            raise self._state.exception
        elif self._state.status == "error":
            typ, exc, tb = self._state.exception_info
            raise exc.with_traceback(tb)
        else:
            raise RuntimeError(f"Future in unknown state: {self._state.status}")

    async def _wait(self, timeout=None):
        if timeout is not None:
            timeout = parse_timedelta(timeout)
        if not self.done():
            future = self._state.event.wait(timeout)
            if self._client._timeout_factor:
                # Give the remote scheduler some time to heartbeat back
                future = asyncio.wait_for(future, timeout * self._client._timeout_factor)
            try:
                await future
            except asyncio.TimeoutError:
                raise TimeoutError from None

    def __await__(self):
        return self._wait().__await__()

    def __repr__(self) -> str:
        st = self._state.status
        if self._instance_name is not None:
            st = f"{st}, name: {self._instance_name}"
        if self.type is not None:
            st = f"{st}, type: {self.type.__name__}"
        return f"<Future: {st}, key: {self.key!s}>"

    def __str__(self) -> str:
        return repr(self)

    def __del__(self):
        # If the client has already been destroyed (during interpreter
        # shutdown, for example), we cannot remove the future any more.
        if self._client is not None and not self._client._closed:
            if self._inform:
                try:
                    self._client._dec_ref(self.key)
                except (RuntimeError, KeyError):  # closed client or missing key
                    pass
            # Remove the Future state from the Client's dict.
            # This is mostly to avoid leaking memory.
            # We use a weakref to self._state in case this Future
            # was cancelled and another one created with the same key.
            st_ref = weakref.ref(self._state)
            self._client._maybe_remove_future_later(self.key, st_ref)

    def __lt__(self, other):
        if not isinstance(other, Future):
            return NotImplemented
        return self.key < other.key

    def __getstate__(self):
        return {"key": self.key, "client": self.client.id, "inform": self._inform}

    def __setstate__(self, state):
        try:
            client = Client._instances[state["client"]]
        except KeyError:
            client = _get_global_client()
            if client is None:
                raise RuntimeError(
                    "Can not deserialize Future: could not find Client %r" % state["client"]
                )
        try:
            future = client.futures[state["key"]]
        except KeyError:
            future = FutureState(status="lost")
            client.futures[state["key"]] = future
        self.__init__(state["key"], client, inform=state["inform"], state=future)

    @property
    def type(self):
        return self._state.type

    def release(self):
        """Release the responsibility of this future to the scheduler

        This notifies the scheduler that the client no longer desires the result
        of this future. Often this allows the scheduler to release the result
        from memory.

        This is often useful if you have many futures but only need the results
        of a few. Releasing the others can significantly reduce memory use.

        Examples
        --------
        >>> from distributed import Client, Future
        >>> client = Client()  # connect to cluster
        >>> x = client.submit(lambda: [1, 2, 3])  # compute large list
        >>> x.result()  # wait until result arrives
        [1, 2, 3]
        >>> x.release()  # tell scheduler that I no longer care about this result

        If the future is needed by other futures it will remain in memory.
        If the future has already been released then this method does nothing.

        See Also
        --------
        Client.release
        """
        if not self._state.released:
            self._client.release([self])
            self._state.released = True


class Client:
    """Connect to and submit computation to a Dask cluster

    The Client connects users to a Dask cluster. It provides an asynchronous
    user interface around functions and futures. This class resembles pools
    in ``concurrent.futures`` but also allows ``Future`` objects within
    ``submit/map`` calls.

    Parameters
    ----------
    address: string, list of strings, or ``Cluster`` object, optional
        This can be the address of a single Scheduler server, a list of
        addresses (if you have a multi-headed cluster), or a ``Cluster``
        object like ``LocalCluster``, ``SSHCluster``, or ``KubeCluster``.
        If set to None (default), it will try to connect to a local cluster
        if one exists, otherwise it will create a ``LocalCluster``.
    loop: tornado.ioloop.IOLoop
        The tornado IOLoop event loop object to use.
        Use None for the default loop.
    timeout: number, string
        Timeout duration for initial connection to the scheduler. Can also be a
        string like "10s".
    set_as_default: bool (True)
        Whether to set this client as the default client.
    security: Security or bool, optional
        Configures communication security. Can be a security object, or True.
        If True, temporary self-signed credentials will be created automatically.
    asynchronous: bool (False by default)
        Set to True if using this client within async/await functions or within
        Tornado gen.coroutines. Otherwise this will be False by default.
    name: string (optional)
        Gives the client a name that will be included in logs generated on
        the scheduler for matters relating to this client
    heartbeat_interval: number or string (optional)
        Time between heartbeats to scheduler in seconds. Can also be a
        string like "10s". Advice: set this significantly shorter than the scheduler's
        configured worker timeout.
    serializers
        A list of custom serializers for data transmission. See the Dask
        documentation on serialization for more information.
        Default is ['dask', 'pickle'].
    deserializers
        A list of custom deserializers for data reception. See the Dask
        documentation on serialization for more information.
        Default is ['dask', 'error', 'pickle'].
    extensions : list of functions
        A list of functions to be run on the client and scheduler at setup.
        This is useful for extending Dask's behavior for customization.
        The functions should take a client or scheduler instance as an argument.
    direct_to_workers: bool (optional)
        Whether or not to connect directly to the workers, or to ask
        the scheduler to serve as intermediary. Set this to True if you
        have direct connectivity to the workers. It is common to disable
        this in secured clusters. Default is False if you pass an address,
        or True if you pass a ``LocalCluster`` or other ``Cluster`` object.
    processes: bool
        Whether to use processes (True) or threads (False) for workers
        in a :class:`~distributed.LocalCluster`. Defaults to True. Ignored if
        *address* is not None or a :class:`~distributed.LocalCluster`.
    silence_logs: logging level
        The logging level threshold below which messages should be silenced.
        Default is ``logging.WARN``. Set this to ``logging.DEBUG`` for more
        verbose reporting or ``logging.ERROR`` for only errors.
    dashboard_link: str
        Location of the scheduler dashboard. If None, determined automatically.
    proxy_address: str
        Address of a proxy server between the client and the scheduler, if applicable
    shutdown_on_close: bool
        Whether to shutdown the scheduler and workers when this client is closed.
        This is useful for temporary ``LocalCluster`` objects.

    Examples
    --------
    Provide cluster's scheduler address on initialization:

    >>> client = Client('127.0.0.1:8786')  # doctest: +SKIP

    Use ``submit`` method to send individual computations to the cluster

    >>> a = client.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = client.submit(add, 10, 20)  # doctest: +SKIP

    Continue using submit or map on results to build up larger computations.

    >>> c = client.submit(add, a, b)  # doctest: +SKIP

    Gather results with the ``gather`` method.

    >>> client.gather([c])  # doctest: +SKIP
    [33]

    See Also
    --------
    distributed.scheduler.Scheduler
    distributed.LocalCluster
    """

    _instances: ClassVar[weakref.WeakValueDictionary[str, Client]] = (
        weakref.WeakValueDictionary()
    )
    _initialized_clients: ClassVar[weakref.WeakSet[Client]] = weakref.WeakSet()
    _loop_runner: ClassVar[LoopRunner] = LoopRunner()
    _global_clients_lock = threading.Lock()
    _default_event_loop: ClassVar[IOLoop | None] = None

    asynchronous: bool
    futures: MutableMapping[Hashable, FutureState]
    id: str
    loop: IOLoop
    security: Security
    serializers: dict[str, tuple[Callable, Callable, bool]]
    _scheduler_identity: SchedulerInfo
    _serializers: dict[str, tuple[Callable, Callable, bool]] | None
    _stream_handlers: dict[str, Callable]
    _extensions: dict[str, Any]
    _startup_kwargs: dict[str, Any]
    cluster: Cluster | None
    _cluster_kwargs: dict[str, Any]
    _scheduler: rpc | None
    _scheduler_conn: Comm | None
    _periodic_callbacks: dict[str, PeriodicCallback]
    _refcount: dict[Hashable, int]
    _who_has: dict[Hashable, set[str]]
    _pending_futures_per_worker: dict[str, set[Hashable]]
    _pending_msg_buffer: list[dict]
    _pending_uploads: set[str]
    _handle_report_task: asyncio.Task | None
    _handle_stream_task: asyncio.Task | None
    _heartbeat_task: asyncio.Task | None
    _gather_semaphore: asyncio.Semaphore
    _exceptions: dict[int, Exception]
    _exceptions_lock: threading.Lock
    _exception_i: int
    _datasets: Datasets
    _serializers_orig: list[str] | None
    _deserializers_orig: list[str] | None
    _timeout_factor: float
    _direct_to_workers: bool
    _dashboard_link: str | None
    _scheduler_address: str | None
    _proxy_address: str | None
    _scheduler_addresses: list[str]
    _name: str | None
    _status: Status
    _startTime: float
    _wait_for_workers_task: asyncio.Task | None
    _connecting_to_scheduler: bool
    _periodic_callback_cache: dict[str, Any]
    _executor: ThreadPoolExecutor | None
    _executor_wait_callback: Callable | None
    _executor_err_callback: Callable | None
    _file_upload_lock: Lock
    _who_has_cv: Condition
    _shutdown_on_close: bool
    _silence_logs: int
    _connection_event: Event
    _start_arg: str | Cluster | None
    _start_kwargs: dict[str, Any]
    _close_future: asyncio.Future | None
    _closed: bool

    def __init__(
        self,
        address: str | Cluster | None = None,
        loop: IOLoop | None = None,
        timeout: float | str = config.timeouts["connect"],
        set_as_default: bool = True,
        security: Security | bool | None = None,
        asynchronous: bool = False,
        name: str | None = None,
        heartbeat_interval: float | str | None = None,
        serializers: list[str] | None = None,
        deserializers: list[str] | None = None,
        extensions: Collection[Callable] = DEFAULT_EXTENSIONS,
        direct_to_workers: bool | None = None,
        **kwargs: Any,
    ):
        if isinstance(address, (list, tuple)):
            address = list(address)

        if kwargs.get("processes"):
            raise TypeError("``processes=`` is no longer a valid keyword argument.")

        self.futures = weakref.WeakValueDictionary()
        self.id = type(self).__name__ + "-" + str(uuid.uuid4())
        self._instances[self.id] = self
        self.datasets = Datasets(self)
        self._serializers_orig = serializers
        self._deserializers_orig = deserializers
        self._serializers = None
        self._scheduler_identity = SchedulerInfo(
            type="Scheduler",
            id=None,
            address=None,
            services={},
            workers={},
            n_workers=-1,
            total_threads=-1,
            total_memory=-1,
            started=-1,
        )
        self._stream_handlers = {
            "key-in-memory": self._handle_key_in_memory,
            "lost-data": self._handle_lost_data,
            "cancelled-key": self._handle_cancelled_key,
            "task-retried": self._handle_retried_key,
            "task-erred": self._handle_task_erred,
            "restart": self._handle_restart,
            "error": self._handle_error,
            "cluster-dump-error": self._handle_cluster_dump_error,
        }
        self._extensions = {}
        self._startup_kwargs = kwargs
        # Communication streams relevant to this client
        self._scheduler = None
        self._scheduler_conn = None  # type: Comm | None
        self.cluster = None
        self._cluster_kwargs = {}
        self._periodic_callbacks = {}
        self._refcount = defaultdict(int)
        self._who_has = defaultdict(set)
        self._pending_futures_per_worker = defaultdict(set)
        self._pending_msg_buffer = []
        self._pending_uploads = set()
        self._handle_report_task = None
        self._handle_stream_task = None
        self._heartbeat_task = None
        self._gather_semaphore = asyncio.Semaphore(
            dask.config.get("distributed.comm.gather-semaphore")
        )
        self._exceptions = {}
        self._exceptions_lock = threading.Lock()
        self._exception_i = 0
        self._timeout_factor = 1.5

        # Connection state
        self._loop_runner.start()
        # TODO: remove self.loop attribute after removing Tornado support
        self.loop = self._loop_runner.loop
        if timeout:
            timeout = parse_timedelta(timeout, "s")
        self._timeout = timeout
        self._direct_to_workers = direct_to_workers
        self._dashboard_link = kwargs.pop("dashboard_link", None)
        self._scheduler_address = None
        self._proxy_address = kwargs.pop("proxy_address", None)
        self._scheduler_addresses = []
        self._name = name
        self._status = Status.created
        self._startTime = time.time()
        self._wait_for_workers_task = None
        self._connecting_to_scheduler = False
        self._set_as_default = set_as_default
        self._periodic_callback_cache = {}

        self.asynchronous = asynchronous
        if asynchronous:
            self._executor = None
            self._executor_wait_callback = None
            self._executor_err_callback = None
        else:
            self._executor = ThreadPoolExecutor(1, thread_name_prefix="Client-Executor")
            # Ensure the executor is shutdown gracefully to avoid hanging threads
            # and associated warnings.
            atexit.register(_shutdown_thread_pool, self._executor)
            self._executor_wait_callback = self.loop.add_callback
            self._executor_err_callback = functools.partial(
                self.loop.add_callback, self._handle_executor_error
            )

        self._file_upload_lock = Lock()
        self._who_has_cv = Condition()

        if heartbeat_interval is None:
            heartbeat_interval = dask.config.get("distributed.client.heartbeat")
        self._heartbeat_interval = parse_timedelta(heartbeat_interval, "s")

        self._shutdown_on_close = kwargs.pop("shutdown_on_close", None)
        self._silence_logs = kwargs.pop("silence_logs", logging.WARN)

        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {list(kwargs)}")

        # Initialization methods
        self._connection_event = Event()
        self._start_arg = address
        self._start_kwargs = {
            "timeout": timeout,
            "security": security,
            "serializers": serializers,
            "deserializers": deserializers,
            "extensions": extensions,
            "direct_to_workers": direct_to_workers,
        }

        self._start()
        if not self.asynchronous:
            try:
                self.sync(self._start, **self._start_kwargs)
            except BaseException:
                # Avoid GeneratorExit errors during interpreter shutdown
                with suppress(AttributeError, RuntimeError):
                    self.close()
                raise
        self._initialized_clients.add(self)

    def _handle_executor_error(self, typ, exc, tb):
        logger.exception("Error in Client executor thread", exc_info=(typ, exc, tb))
        self.close()

    def __repr__(self):
        if self.scheduler:
            addr = self.scheduler.address
            if self._proxy_address:
                addr += f" [proxy: {self._proxy_address}]"
            workers = len(self.scheduler_info()["workers"])
            threads = sum(w["nthreads"] for w in self.scheduler_info()["workers"].values())
            memory = sum(w["memory_limit"] for w in self.scheduler_info()["workers"].values())
            text = (
                f"<Client: {self.status} scheduler={addr!r} processes={workers} "
                f"threads={threads} memory={format_bytes(memory)}>"
            )
        elif self._start_arg:
            text = f"<Client: {self.status} scheduler={self._start_arg!r}>"
        else:
            text = f"<Client: {self.status}>"

        if self._instance_name is not None:
            text = f"{text[:-1]}, name: {self._instance_name}>"

        return text

    def __await__(self):
        """For usage in await statements"""
        yield from self._started.__await__()
        return self

    @property
    def status(self):
        return self._status.name

    @property
    def scheduler(self):
        """The RPC object for the scheduler

        This is deprecated, and will be removed in a future version.
        """
        # TODO: deprecate this?
        return self._scheduler

    @property
    def scheduler_address(self):
        """The address of the scheduler"""
        return self._scheduler_address

    @property
    def security(self):
        return self._security

    @property
    def dashboard_link(self):
        """Link to the scheduler's dashboard"""
        if self._dashboard_link is not None:
            return self._dashboard_link
        if self.cluster is not None and self.cluster.dashboard_link is not None:
            return self.cluster.dashboard_link
        try:
            port = self.scheduler_info()["services"]["dashboard"]
            host = get_address_host(self.scheduler.address)
            # If the dashboard is served on the same host as the scheduler,
            # replace the scheduler port with the dashboard port
            if host in self.scheduler.address:
                addr = self.scheduler.address.replace(
                    str(self.scheduler.port), str(port)
                )
            # Otherwise assume the dashboard is served on the same host the client is on
            else:
                addr = f"{host}:{port}"
            # Use "http" because "tls" is not a valid scheme for web browsers
            if self.security.require_encryption:
                # Assume the dashboard is served over HTTPS
                scheme = "https"
            else:
                scheme = "http"
            return f"{scheme}://{addr}"
        except (KeyError, IndexError, TypeError):
            return ""

    def _repr_html_(self):
        from .widgets import get_template

        return get_template("client.html.j2").render(client=self)

    def _ipython_key_completions_(self):
        """Completion for IPython attribute access"""
        return ["datasets", "futures", "cluster"]

    def _start(self, **kwargs):
        """Start the client"""
        if self._status != Status.created:
            return

        self._status = Status.starting
        if self.asynchronous:
            self._started = asyncio_ensure_future(
                self._start_async(**merge(self._start_kwargs, kwargs))
            )
        else:
            # Ensure that the IO loop is running if starting in sync mode
            self.loop.add_callback(
                self._start_async, **merge(self._start_kwargs, kwargs)
            )

    async def _start_async(
        self,
        timeout: float | None = config.timeouts["connect"],
        security: Security | bool | None = None,
        serializers: list[str] | None = None,
        deserializers: list[str] | None = None,
        extensions: Collection[Callable] = DEFAULT_EXTENSIONS,
        direct_to_workers: bool | None = None,
        **kwargs: Any,
    ):
        # Wait for the loop to start if it hasn't already
        await self._loop_runner.running.wait()

        self._start_kwargs = {
            "timeout": timeout,
            "security": security,
            "serializers": serializers,
            "deserializers": deserializers,
            "extensions": extensions,
            "direct_to_workers": direct_to_workers,
        }

        if security is None:
            security = dask.config.get("distributed.comm.security")
        if security is False or security == "off":
            self._security = Security()
        elif security is True:
            self._security = Security.temporary()
        elif isinstance(security, Security):
            self._security = security
        elif isinstance(security, dict):
            self._security = Security(**security)
        else:
            raise TypeError("security must be Security object or boolean")

        if serializers is None:
            serializers = dask.config.get("distributed.comm.serializers", [])
        if deserializers is None:
            deserializers = dask.config.get("distributed.comm.serializers", [])

        serializers = list(serializers)
        deserializers = list(deserializers)

        if "pickle" not in serializers:
            serializers.append("pickle")
        if "pickle" not in deserializers:
            deserializers.append("pickle")
        if "dask" not in serializers:
            serializers = ["dask"] + serializers
        if "dask" not in deserializers:
            deserializers = ["dask"] + deserializers
        if "error" not in deserializers:
            deserializers = ["error"] + deserializers

        self._serializers = merge(
            *(dask.config.get("distributed.comm.serialization").get(name, {}) for name in serializers)
        )
        self._deserializers = merge(
            *(dask.config.get("distributed.comm.serialization").get(name, {}) for name in deserializers)
        )

        if self._start_arg is None and dask.config.get("scheduler-address", None):
            self._start_arg = dask.config.get("scheduler-address")
            logger.info(
                "Config value `scheduler-address` detected. Connecting to %s",
                self._start_arg,
            )

        if self._start_arg is None and not os.environ.get(
            "DASK_INTERNAL_INHERIT_CONFIG"
        ):
            # If no address given, try connecting to local cluster (`Client()`)
            try:
                addr = Client._running_clients_on_this_process[-1].scheduler.address
                await self._connect(addr, timeout=1)
            except (IndexError, OSError, TimeoutError):
                pass
            # If no address given and no local cluster, start LocalCluster
            if not self.scheduler:
                try:
                    from .deploy import LocalCluster

                    self.cluster = LocalCluster(
                        loop=self.loop,
                        security=self._security,
                        asynchronous=self.asynchronous,
                        silence_logs=self._silence_logs,
                        serializers=serializers,
                        deserializers=deserializers,
                        extensions=extensions,
                        direct_to_workers=direct_to_workers,
                        **self._startup_kwargs,
                    )

                    if self._shutdown_on_close is None:
                        self._shutdown_on_close = True

                    if self.asynchronous:
                        await self.cluster
                    self._cluster_kwargs = self.cluster.kwargs
                    self._start_arg = self.cluster
                except Exception:
                    logger.error("Failed to start local Dask cluster", exc_info=True)
                    await self._close(fast=True)
                    raise

        if isinstance(self._start_arg, str):
            await self._connect(self._start_arg, timeout=timeout)
        elif isinstance(self._start_arg, (list, tuple)):
            await self._connect(self._start_arg, timeout=timeout)
        elif hasattr(self._start_arg, "scheduler_address"):  # Cluster object
            cluster = self._start_arg
            if cluster.status == Status.created:
                if self.asynchronous:
                    await cluster
                else:
                    cluster.sync(cluster._start)
            elif cluster.status == Status.failed:
                raise ValueError(
                    f"Cluster {cluster} has already failed: {cluster.reason}"
                )
            elif cluster.status == Status.closed:
                raise ValueError(f"Cluster {cluster} has already been closed")

            if self.cluster is None:
                self.cluster = cluster
                if self._shutdown_on_close is None:
                    self._shutdown_on_close = False

            await self._connect(self.cluster.scheduler_address, timeout=timeout)
        elif self._start_arg is not None:
            raise TypeError(
                "First argument must be scheduler address string or Cluster instance"
            )
        else:
            assert self.scheduler

        if direct_to_workers is None:
            # We connect directly to workers for LocalCluster by default
            if self.cluster and type(self.cluster).__name__ == "LocalCluster":
                self._direct_to_workers = True
            else:
                self._direct_to_workers = False
        else:
            self._direct_to_workers = direct_to_workers

        if self._set_as_default:
            cls = type(self)
            with cls._global_clients_lock:
                cls._running_clients_on_this_process.append(self)
            _set_global_client(self)

        # Start background tasks
        self._handle_report_task = asyncio_ensure_future(self._handle_report())
        self._handle_stream_task = asyncio_ensure_future(self._handle_stream())
        self._heartbeat_task = asyncio_ensure_future(self._heartbeat())

        # Run extensions
        for ext in extensions:
            await self._run_extension(ext)

        return self

    async def _connect(self, address: str | list[str], timeout: float | None = None):
        if self._connecting_to_scheduler:
            return
        self._connecting_to_scheduler = True

        if self._proxy_address:
            address = self._proxy_address
            self._scheduler_addresses = [address]
            logger.info("Connecting to scheduler at proxy address %s", address)
        elif isinstance(address, str):
            self._scheduler_addresses = [address]
            logger.info("Connecting to scheduler at %s", address)
        elif isinstance(address, (list, tuple)):
            address = list(address)
            random.shuffle(address)
            self._scheduler_addresses = address
            logger.info("Connecting to scheduler at %s", address[0])
        else:
            raise TypeError(address)

        comm = None
        start = time.time()
        deadline = start + timeout if timeout is not None else None
        last_error = None

        if self._proxy_address:
            # If we have a proxy address, we only try that one address
            addresses = [self._proxy_address]
        else:
            addresses = list(self._scheduler_addresses)

        while True:
            if not addresses:
                msg = "Could not connect to %s after trying for %s" % (
                    self._scheduler_addresses,
                    format_time_ago(start),
                )
                if last_error:
                    raise last_error from TimeoutError(msg)
                else:
                    raise TimeoutError(msg)

            addr = addresses.pop(0)
            if self._proxy_address:
                # If we have a proxy address, we must connect via the proxy
                # even if the scheduler address is different
                connect_addr = self._proxy_address
                # We still need to know the real scheduler address for the handshake
                handshake_addr = addr
            else:
                connect_addr = addr
                handshake_addr = addr

            try:
                comm = await connect(
                    connect_addr,
                    timeout=timeout,
                    security=self.security,
                    local_addr=self._start_kwargs.get("local_addr"),
                    serializers=self._serializers_orig,
                    deserializers=self._deserializers_orig,
                    connection_args=self.security.get_connection_args("client"),
                )
                comm.name = "Client->Scheduler"
                if self._proxy_address:
                    comm.remote_address = handshake_addr
                logger.info(
                    "Receive scheduler info: %r",
                    await comm.read(),
                )
                await comm.write(
                    {
                        "op": "register-client",
                        "client": self.id,
                        "reply": False,
                        "pid": os.getpid(),
                        "versions": versions.get_versions(),
                        "name": self._name,
                    }
                )
                break
            except (OSError, TimeoutError) as e:
                last_error = e
                comm = None
                if deadline is not None and time.time() > deadline:
                    msg = "Could not connect to %s after trying for %s" % (
                        self._scheduler_addresses,
                        format_time_ago(start),
                    )
                    raise last_error from TimeoutError(msg)

        assert comm
        self._scheduler_conn = comm
        # Use a proxy rpc object when talking to the scheduler.
        # This allows the comm to be safely replaced on the fly
        # (e.g. on heartbeat reconnects).
        self._scheduler = rpc(comm, serializers=self._serializers_orig)
        self._scheduler_address = comm.peer_address

        await self._update_scheduler_info()
        self._connection_event.set()
        self._status = Status.running
        self._connecting_to_scheduler = False

    async def _reconnect(self):
        self._connection_event.clear()
        if self._scheduler_conn:
            self._scheduler_conn.abort()
            self._scheduler_conn = None
            self._scheduler = None

        addresses = list(self._scheduler_addresses)
        random.shuffle(addresses)
        await self._connect(addresses, timeout=self._timeout)

    async def _send_to_scheduler(self, msg: dict) -> Any:
        """Send message to scheduler

        If the scheduler connection is down, wait for it to come back up, but fail after a timeout.
        """
        if self._closed:
            raise ClientClosedError()

        await self._connection_event.wait()
        assert self._scheduler
        try:
            return await self._scheduler.send_comm_safe(msg)
        except CommClosedError:
            # Rely on heartbeat task to notice and reconnect
            await self._connection_event.wait()
            assert self._scheduler
            return await self._scheduler.send_comm_safe(msg)

    def _send_to_scheduler_safe(self, msg: dict) -> None:
        """Send message to scheduler without expecting a response"""
        if self._closed:
            raise ClientClosedError()

        if not self._connection_event.is_set():
            # Connection not ready, buffer message
            self._pending_msg_buffer.append(msg)
            return

        assert self._scheduler_conn
        try:
            self._scheduler_conn.send(msg)
        except CommClosedError:
            # Connection broken, buffer message and rely on heartbeat task
            # to notice and reconnect
            self._pending_msg_buffer.append(msg)
            self._connection_event.clear()

    @log_errors
    async def _handle_stream(self):
        """Listen to messages from scheduler"""
        while self._status != Status.closed:
            if not self._connection_event.is_set():
                # Connection is down, wait until it's back up
                await self._connection_event.wait()
                continue

            assert self._scheduler_conn
            try:
                msgs = await self._scheduler_conn.read()
            except CommClosedError:
                if self._closed:
                    break
                if self._status != Status.running:  # Shutting down, leave
                    break
                logger.warning(
                    "Client stream closed to scheduler (%s). "
                    "Trying to reconnect",
                    self._scheduler_address,
                    exc_info=True,
                )
                self._connection_event.clear()
                continue
            except Exception:
                if self._status != Status.running:  # Shutting down, leave
                    break
                logger.error("Client stream errored", exc_info=True)
                # Hopefully the scheduler is still alive. Try to reconnect.
                self._connection_event.clear()
                continue

            if not isinstance(msgs, list):
                msgs = [msgs]

            for msg in msgs:
                op = msg.pop("op", None)

                if op == "stream-start":
                    continue

                if not op:
                    logger.error("Received unexpected message from scheduler: %s", msg)
                    continue

                handler = self._stream_handlers.get(op)
                if handler is None:
                    logger.warning("Received unknown operation '%s'", op)
                else:
                    try:
                        await handler(**msg)
                    except Exception:
                        logger.exception("Failed to handle message %r", op)

    @log_errors
    async def _heartbeat(self) -> None:
        """Periodically send heartbeat to scheduler"""
        wait_for = self._heartbeat_interval
        while self._status != Status.closed:
            await asyncio.sleep(wait_for)
            if not self._connection_event.is_set():
                # Connection down, try to reconnect
                if not self._connecting_to_scheduler:
                    logger.info("Attempting to reconnect to scheduler %s", self._scheduler_address)
                    try:
                        await self._reconnect()
                        logger.info("Reconnected to scheduler")
                        # Heartbeat again after an interval
                        wait_for = self._heartbeat_interval
                    except Exception as e:
                        logger.warning(
                            "Failed to reconnect to scheduler: %s", e, exc_info=True
                        )
                        # Wait longer before retrying
                        wait_for = min(max(wait_for * 2, self._heartbeat_interval), 30)
                continue

            assert self._scheduler_conn
            if self._scheduler_conn.closed():
                self._connection_event.clear()
                continue

            # Flush pending message buffer
            if self._pending_msg_buffer:
                try:
                    self._scheduler_conn.send(self._pending_msg_buffer)
                    self._pending_msg_buffer.clear()
                except CommClosedError:
                    self._connection_event.clear()
                    continue

            # Send heartbeat
            logger.debug("Client heartbeat to scheduler: %s", self._scheduler_address)
            resp = await self._scheduler.heartbeat_client(
                client=self.id, metrics=context_meter.digest(), started=self._startTime
            )
            if resp["status"] == "missing":
                logger.warning(
                    "Scheduler is unaware of this client %s. Reregistering.", self.id
                )
                await self._reconnect()

    async def _handle_report(self):
        """Handle reports from the scheduler, assigning them to futures"""
        await self._connection_event.wait()
        while self._status != Status.closed:
            await self._who_has_cv.wait()
            if self._status == Status.closed:
                break
            self._send_to_scheduler_safe(
                {"op": "who-has", "keys": list(self._who_has), "client": self.id}
            )
            # This is a performance optimization. Without this sleep, the client can flood
            # the scheduler with identical who_has requests. See
            # https://github.com/dask/distributed/issues/6659
            await asyncio.sleep(0.01)

    async def _handle_key_in_memory(self, key: Hashable, workers: list[str]) -> None:
        """Handle confirmation that a key is now in memory"""
        state = self.futures.get(key)
        if state is None:
            return

        async with self._who_has_cv:
            if key in self._who_has:
                self._who_has.pop(key)
                self._who_has_cv.notify_all()

        if state.status == "pending":
            state.set_result(None)
        elif state.status == "processing":
            state.set_processing(workers)
        elif state.status == "error":
            return
        if state.status == "finished":
            # Do not overwrite result if we have it already
            return

        if self._inform:
            state.set_result(None)

    async def _handle_lost_data(self, key: Hashable) -> None:
        """Handle notification that a key has been lost"""
        state = self.futures.get(key)
        if state is not None:
            state.set_lost()

        async with self._who_has_cv:
            if key in self._who_has:
                self._who_has.pop(key)
                self._who_has_cv.notify_all()

    async def _handle_cancelled_key(self, key: Hashable) -> None:
        """Handle notification that a key has been cancelled"""
        state = self.futures.get(key)
        if state is None:
            return
        state.set_cancelled()

    async def _handle_retried_key(self, key: Hashable) -> None:
        state = self.futures.get(key)
        if state is None:
            return
        state.retry()

    async def _handle_task_erred(
        self,
        key: Hashable,
        exception: Serialized,
        traceback: Serialized | None = None,
    ) -> None:
        """Handle notification that a task has erred"""
        state = self.futures.get(key)
        if state is None:
            return

        try:
            exception = await offload(exception.deserialize)
        except Exception as e:
            exception = e
            logger.info("Could not deserialize exception: %s", str(e)[:1000])

        if traceback:
            try:
                traceback = await offload(traceback.deserialize)
            except Exception as e:
                traceback = None
                logger.info("Could not deserialize traceback: %s", str(e)[:1000])

        state.set_exception(exception, traceback)

    async def _handle_restart(self) -> None:
        logger.info("Receive restart signal from scheduler")
        for state in self.futures.values():
            state.set_lost()
        if self.cluster:
            self.cluster.scheduler_info = self.scheduler_info()
            self.cluster.workers = {}
        await self._update_scheduler_info()
        if self.cluster:
            # Update this ক্লাস্টার's known workers by asking the scheduler
            # for the full worker list. This is important so that the ক্লাস্টার
            # object's representation is accurate.
            await self.cluster._correct_state()

    async def _handle_error(self, message: str) -> None:
        """Handle unknown errors reported by the scheduler"""
        logger.error("Received error from scheduler: %s", message)

    async def _handle_cluster_dump_error(self, exc: Serialized) -> None:
        """Handle errors during cluster state dump"""
        logger.error("Error during cluster state dump", exc_info=exc.deserialize())

    async def _update_scheduler_info(self, n_workers: int = -1):
        """Get info from scheduler, update local state"""
        if self._closed:
            raise ClientClosedError()
        info = await self._send_to_scheduler({"op": "identity", "n_workers": n_workers})
        self._scheduler_identity.update(info)

        # Notify cluster object about new workers
        if self.cluster is not None:
            self.cluster.scheduler_info = self.scheduler_info()
            for addr, w in self.cluster.scheduler_info["workers"].items():
                if addr not in self.cluster.workers:
                    from .worker import Worker

                    worker = Worker(
                        scheduler_ip=self.scheduler.address,
                        address=addr,
                        loop=self.loop,
                        name=w.get("name"),
                        nthreads=w["nthreads"],
                        memory_limit=w["memory_limit"],
                        security=self.security,
                        silence_logs=self._silence_logs,
                        serializers=self._serializers_orig,
                        deserializers=self._deserializers_orig,
                        **self.cluster.new_worker_spec(w),
                    )
                    self.cluster.workers[addr] = worker

        # Remove pending futures for workers that no longer exist
        workers = set(self.scheduler_info()["workers"])
        for w in list(self._pending_futures_per_worker):
            if w not in workers:
                del self._pending_futures_per_worker[w]

    async def _run_extension(self, ext: Callable) -> None:
        """Run extension locally and on scheduler"""
        if ext is None:
            return
        logger.info("Setting up client extension %s", ext)
        if is_coroutine_function(ext):
            await ext(self)
        else:
            ext(self)

        if hasattr(ext, "setup"):
            ext_setup = ext.setup
            if is_coroutine_function(ext_setup):
                ext_setup = await ext_setup(self)
            else:
                ext_setup = ext_setup(self)
        else:
            ext_setup = Serialize(ext)

        name = funcname(ext)
        self._extensions[name] = ext
        logger.info("Setting up scheduler extension %s", name)
        await self._send_to_scheduler({"op": "add-client-extension", "name": name, "extension": ext_setup})

    def start(self, **kwargs):
        """Start the client if it is has not yet started"""
        if self.asynchronous:
            raise TypeError(
                "client.start() is not safe to use in asynchronous mode. "
                "Please use `await client` or `await client._start()` instead."
            )
        if self._status == Status.created:
            sync(self.loop, self._start, **kwargs)

    @classmethod
    def current(cls, allow_global: bool = True) -> Client:
        """Return the currently active client

        If no clients are active, raise ValueError

        Parameters
        ----------
        allow_global : bool
            If True (the default), then we search for a default client if we
            can't find a context specific client.

        """
        client = _get_global_client()
        if client is not None:
            return client

        if allow_global:
            with cls._global_clients_lock:
                if cls._running_clients_on_this_process:
                    return cls._running_clients_on_this_process[-1]

        raise ValueError("No clients found")

    @classmethod
    def get(
        cls,
        address: str | Cluster | None = None,
        *,
        loop: IOLoop | None = None,
        timeout: float | str = config.timeouts["connect"],
        security: Security | bool | None = None,
        asynchronous: bool = False,
        **kwargs: Any,
    ) -> Client:
        """Get a client with the given arguments. Create one if necessary.

        This allows us to avoid creating clients repeatedly with the same arguments.

        Parameters
        ----------
        address : str, Cluster, or None
            Address or cluster to connect to.
        loop : IOLoop
        timeout : int or str
        security : Security or bool
        asynchronous : bool
        **kwargs :
            Other keyword arguments for the Client constructor.

        Returns
        -------
        Client

        Examples
        --------
        >>> client = Client.get(asynchronous=True)  # Create a new client
        >>> client2 = Client.get(asynchronous=True)  # Get the same client
        >>> assert client is client2
        >>> client.close()  # doctest: +SKIP
        >>> client3 = Client.get(asynchronous=True)  # Create a new client
        >>> assert client is not client3
        >>> client3.close()  # doctest: +SKIP
        """
        if asynchronous:
            raise TypeError(
                "Client.get is not safe to use in asynchronous mode. "
                "Please instantiate Client class directly."
            )

        kwargs.update(
            {
                "address": address,
                "loop": loop,
                "timeout": timeout,
                "security": security,
                "asynchronous": asynchronous,
            }
        )
        kwargs["set_as_default"] = False

        key = tokenize(kwargs)

        with cls._global_clients_lock:
            if key in cls._active_clients:
                client = cls._active_clients[key]
            else:
                try:
                    client = cls(**kwargs)
                except Exception:
                    logger.error(
                        "Failed to start client with arguments %s",
                        kwargs,
                        exc_info=True,
                    )
                    raise
                cls._active_clients[key] = client

            return client

    @classmethod
    async def get_current(cls) -> Client:
        """Get the current client for this Tornado loop.

        Returns the most recently created client.
        Raises ValueError if no clients are found.
        """
        # Deprecated: this method was intended for Tornado users.
        # Since Tornado support is deprecated, this method is deprecated too.
        warnings.warn(
            "`Client.get_current` is deprecated and will be removed in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.current()

    ##################
    # Client methods #
    ##################

    @overload
    def submit(
        self,
        func: Callable[P, T],
        *args: P.args,
        key: Hashable | None = None,
        workers: str | Iterable[str] | None = None,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        allow_other_workers: bool = False,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        pure: bool = True,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: P.kwargs,
    ) -> Future[T]: ...

    @overload
    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        key: Hashable | None = None,
        workers: str | Iterable[str] | None = None,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        allow_other_workers: bool = False,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        pure: bool = True,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: Any,
    ) -> Future[T]: ...

    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        key: Hashable | None = None,
        workers: str | Iterable[str] | None = None,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        allow_other_workers: bool = False,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        pure: bool = True,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: Any,
    ) -> Future[T]:
        """Submit a function application to the scheduler

        Parameters
        ----------
        func : function
        *args :
        key : hashable, optional
            Unique identifier for the task. Defaults to hashing the function
            and arguments.
        workers : string or iterable of strings, optional
            A list of worker addresses to restrict this task to.
            Ex: ``['127.0.0.1:8787', '127.0.0.1:8788']``
        resources : dict, optional
            A dictionary of resource constraints for this task.
            Ex: ``{'GPU': 2, 'memory': 7e9}``
        retries : int, optional
            Number of times to retry the task if it fails.
        priority : int, optional
            Priority of task. Higher numbers mean higher priority. Defaults to 0.
        fifo_timeout : timedelta or string, optional like '1s' or '100ms'
            Maximum time for the task to be runnable. If it doesn't run by this
            deadline then it is cancelled.
        allow_other_workers : bool, optional
            If True, allow this task to run on workers that were not specified
            in the ``workers=`` keyword. Default is False.
        actor : bool, optional
            Whether this task creates an Actor. Default is False.
        actors : bool, string, list, optional
            Addresses of actors that this task can run on.
        pure : bool, optional
            Whether the function is pure. Default is True. If True, the task
            will be executed at most once. If False, the task may be executed
            multiple times if it is needed by multiple results.
        on_error: 'retry', 'raise', 'ignore', optional
            What to do if the task raises an error. Defaults to 'raise'.
            If 'retry', the task will be retried up to ``retries`` times.
            If 'raise', the exception will be raised when the future is awaited.
            If 'ignore', the future will be marked as cancelled.
        **kwargs :

        Returns
        -------
        Future

        Examples
        --------
        >>> c = client.submit(add, a, b)  # doctest: +SKIP

        See Also
        --------
        Client.map
        Future
        """
        if not callable(func):
            raise TypeError("First input to submit must be a callable function")

        key = key or tokenize(func, *args, **kwargs)
        if not isinstance(key, Hashable):
            raise TypeError("key= must be Hashable")

        if key in self.futures:
            return Future(key, self)

        if actor:
            if workers or resources or retries or priority or fifo_timeout:
                raise ValueError(
                    "Cannot specify workers, resources, retries, priority, "
                    "or fifo_timeout for actors"
                )
            if actor is True:
                if key is None:
                    key = "actor-" + str(uuid.uuid4())
                else:
                    key = "actor-" + str(key)
            else:
                raise TypeError("actor= must be boolean or string")
        if actors:
            if workers or resources:
                raise ValueError("Cannot specify workers or resources for actors")
            if actors is True:
                raise TypeError("actors= must be an address or list of addresses")

        if kwargs:
            task = (apply, func, list(args), kwargs)
        else:
            task = (func,) + tuple(args)

        futures = {key: self._get_computation(task, key=key, pure=pure)}

        info = {
            "actors": actors,
            "workers": workers,
            "allow_other_workers": allow_other_workers,
            "resources": resources,
            "retries": retries,
            "priority": priority,
            "fifo_timeout": fifo_timeout,
            "on_error": on_error,
        }
        futures, annotations, dependencies = self._gather_keys(futures=futures, annotations=info)

        logger.debug("Submit %s(...), key: %s", funcname(func), key)
        self._send_update_graph(
            tasks=futures,
            keys=[key],
            dependencies=dependencies,
            annotations=annotations,
        )

        return Future(key, self)

    def map(
        self,
        func: Callable[..., T],
        *iterables: Iterable,
        key: Hashable | None = None,
        workers: str | Iterable[str] | None = None,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        allow_other_workers: bool = False,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        pure: bool = True,
        batch_size: int | None = None,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: Any,
    ) -> list[Future[T]]:
        """Map a function on a sequence of arguments.

        Arguments can be Futures or normal data.

        Parameters
        ----------
        func : function
        iterables : Iterables
        key : hashable, optional
            Prefix for task names. Defaults to the function name.
        workers : string or list, optional
            A list of worker addresses to restrict this task to.
            Ex: ``['127.0.0.1:8787', '127.0.0.1:8788']``
        resources : dict, optional
            A dictionary of resource constraints for this task.
            Ex: ``{'GPU': 2, 'memory': 7e9}``
        retries : int, optional
            Number of times to retry the task if it fails.
        priority : int, optional
            Priority of task. Higher numbers mean higher priority. Defaults to 0.
        fifo_timeout : timedelta or string, optional like '1s' or '100ms'
            Maximum time for the task to be runnable. If it doesn't run by this
            deadline then it is cancelled.
        allow_other_workers : bool, optional
            If True, allow this task to run on workers that were not specified
            in the ``workers=`` keyword. Default is False.
        actor : bool, optional
            Whether these tasks create Actors. Default is False.
        actors : bool, string, list, optional
            Addresses of actors that these tasks can run on.
        pure : bool, optional
            Whether the function is pure. Default is True. If True, the task
            will be executed at most once. If False, the task may be executed
            multiple times if it is needed by multiple results.
        batch_size: int, optional
            If > 1, maybe group tasks into batches of this size. By default,
            do not batch.
            Not recommended unless the function has very low overhead and the
            arguments are either not futures or are futures that are guaranteed
            to be computed on the same worker.
        on_error: 'retry', 'raise', 'ignore', optional
            What to do if the task raises an error. Defaults to 'raise'.
            If 'retry', the task will be retried up to ``retries`` times.
            If 'raise', the exception will be raised when the future is awaited.
            If 'ignore', the future will be marked as cancelled.
        **kwargs :

        Returns
        -------
        List of Futures

        Examples
        --------
        >>> L = client.map(add, range(10), range(10))  # doctest: +SKIP

        See Also
        --------
        Client.submit
        Future
        """
        if not callable(func):
            raise TypeError("First input to map must be a callable function")

        iterables = [list(it) for it in iterables]
        if not iterables:
            return []

        if key is None:
            key = funcname(func)
        if not isinstance(key, (str, bytes)):
            raise TypeError(
                "key= keyword argument must be string or bytes, got %s" % type(key)
            )

        info = {
            "actors": actors,
            "workers": workers,
            "allow_other_workers": allow_other_workers,
            "resources": resources,
            "retries": retries,
            "priority": priority,
            "fifo_timeout": fifo_timeout,
            "on_error": on_error,
        }

        if actor:
            raise ValueError("Cannot use map to create actors")
        if actors:
            if workers or resources:
                raise ValueError("Cannot specify workers or resources for actors")
            if actors is True:
                raise TypeError("actors= must be an address or list of addresses")

        if batch_size is not None and batch_size > 1:
            keys = [
                f"{key}-{uuid.uuid4()}"
                for i in range(math.ceil(len(iterables[0]) / batch_size))
            ]
        else:
            keys = [f"{key}-{uuid.uuid4()}" for i in range(len(iterables[0]))]

        if len(set(keys)) != len(keys):
            raise ValueError(
                "Hash collision on keys. Perhaps the function name is too short"
            )

        if kwargs:
            func = partial(func, **kwargs)

        arglist = list(zip(*iterables))

        if batch_size is not None and batch_size > 1:
            tasks = {}
            for i, batch in enumerate(partition_all(batch_size, arglist)):
                dsk = {
                    (keys[i], j): (apply, func, list(map(lambda x: x[j], batch)))
                    for j in range(len(batch[0]))
                }
                tasks[(keys[i], 0)] = (list, [(keys[i], j) for j in range(len(batch))])
        else:
            tasks = {keys[i]: (func,) + arglist[i] for i in range(len(keys))}

        futures, annotations, dependencies = self._gather_keys(
            futures=tasks, annotations=info, keys=keys, pure=pure
        )

        results = [Future(k, self) for k in keys]

        logger.debug("map(%s, ...)", funcname(func))

        self._send_update_graph(
            tasks=futures,
            keys=keys,
            dependencies=dependencies,
            annotations=annotations,
        )

        if batch_size is not None and batch_size > 1:
            from .actor import BatchActorFuture

            results = [BatchActorFuture(k, i, self) for k in keys for i in range(batch_size)]

        return results

    def get(
        self,
        dsk: Mapping,
        keys: Hashable | Collection[Hashable],
        workers: str | Iterable[str] | None = None,
        resources: dict[str, float] | None = None,
        sync: bool = True,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        allow_other_workers: bool = False,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        pure: bool = True,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Compute dask graph.

        Parameters
        ----------
        dsk : dict
        keys : object or list
        sync : bool (optional)
            Returns Futures if False or concrete values if True (default).
        workers : string or list, optional
            A list of worker addresses to restrict this task to.
            Ex: ``['127.0.0.1:8787', '127.0.0.1:8788']``
        resources : dict, optional
            A dictionary of resource constraints for this task.
            Ex: ``{'GPU': 2, 'memory': 7e9}``
        retries : int, optional
            Number of times to retry the task if it fails.
        priority : int, optional
            Priority of task. Higher numbers mean higher priority. Defaults to 0.
        fifo_timeout : timedelta or string, optional like '1s' or '100ms'
            Maximum time for the task to be runnable. If it doesn't run by this
            deadline then it is cancelled.
        allow_other_workers : bool, optional
            If True, allow this task to run on workers that were not specified
            in the ``workers=`` keyword. Default is False.
        actor : bool, optional
            Whether these tasks create Actors. Default is False.
        actors : bool, string, list, optional
            Addresses of actors that these tasks can run on.
        pure : bool, optional
            Whether the function is pure. Default is True. If True, the task
            will be executed at most once. If False, the task may be executed
            multiple times if it is needed by multiple results.
        on_error: 'retry', 'raise', 'ignore', optional
            What to do if the task raises an error. Defaults to 'raise'.
            If 'retry', the task will be retried up to ``retries`` times.
            If 'raise', the exception will be raised when the future is awaited.
            If 'ignore', the future will be marked as cancelled.
        **kwargs
            Extra keyword arguments to forward to the scheduler.

        Returns
        -------
        results : list or object
            results has the same structure as keys

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> dsk = {'x': 1, 'y': 2, 'z': (add, 'x', 'y')}  # doctest: +SKIP
        >>> future = client.get(dsk, 'z')  # doctest: +SKIP
        >>> future.result()  # doctest: +SKIP
        3

        See Also
        --------
        Client.compute
        Client.persist
        """
        singleton = False
        if not isinstance(keys, (list, set)):
            singleton = True
            keys = [keys]

        if actor:
            raise ValueError("Cannot use get to create actors")
        if actors:
            if workers or resources:
                raise ValueError("Cannot specify workers or resources for actors")
            if actors is True:
                raise TypeError("actors= must be an address or list of addresses")

        info = {
            "actors": actors,
            "workers": workers,
            "allow_other_workers": allow_other_workers,
            "resources": resources,
            "retries": retries,
            "priority": priority,
            "fifo_timeout": fifo_timeout,
            "on_error": on_error,
        }

        futures, annotations, dependencies = self._gather_keys(
            futures=dsk, annotations=info, keys=keys, pure=pure
        )

        logger.debug("Get %d keys from graph", len(keys))

        self._send_update_graph(
            tasks=futures,
            keys=keys,
            dependencies=dependencies,
            annotations=annotations,
            **kwargs,
        )

        results = [Future(key, self) for key in keys]

        if singleton:
            results = results[0]

        if sync:
            if self.asynchronous:
                raise ValueError(
                    "sync=True is not allowed in asynchronous mode. "
                    "Use sync=False and await the result instead."
                )
            else:
                return self.gather(results)
        else:
            return results

    def _gather_keys(
        self,
        futures: Mapping,
        keys: Hashable | Collection[Hashable] | None = None,
        annotations: dict[str, Any] | None = None,
        pure: bool = True,
    ) -> tuple[dict[Hashable, Any], dict[str, Any], set[Hashable]]:
        """Take a graph and traverse through it gathering keys and annotations

        This takes a Dask graph and the desired keys and walks up the graph
        gathering all keys and keys of futures. It also collects annotations
        and applies the given annotation to all keys in the graph that connect
        to the output keys.

        Parameters
        ----------
        futures : dict
            A dask graph
        keys : list
            A list of keys desired by the user
        annotations : dict
            Annotations to apply to the keys that connect to the desired keys

        Returns
        --------
        tasks : dict
            A dictionary mapping keys to tasks
        annotations : dict
            A dictionary mapping annotations like 'priority' or 'resources'
            to dictionaries that map keys to values like ``{'x': 1}``
        dependencies : set
            A set of keys for Futures results upon which these tasks depend
        """
        tasks = {}
        dependencies = set()
        queue = deque(keys or futures)
        keyset = set(keys or futures)
        seen = set()

        if annotations:
            # Copy as we mutate annotations below
            annotations = annotations.copy()
            if fifo_timeout := annotations.get("fifo_timeout"):
                annotations["fifo_timeout"] = parse_timedelta(fifo_timeout)
            if workers := annotations.get("workers"):
                if isinstance(workers, str):
                    workers = {workers}
                else:
                    workers = set(workers)
                annotations["workers"] = workers
        else:
            annotations = {}

        annotations_out: dict[str, dict] = {k: {} for k in annotations}
        annotations_out["__workers_loose"] = {}
        annotations_out["workers"] = {}

        while queue:
            key = queue.popleft()
            if key in seen or key not in futures:
                continue
            seen.add(key)

            if isinstance(key, Future):
                dependencies.add(key.key)
                if key.client is not self:
                    raise ValueError("Inputs contain futures from another client.")
                continue

            if key not in tasks:
                tasks[key] = futures[key]

            # Put key in annotations
            for k, v in annotations.items():
                if v is not None:
                    if k == "workers":
                        annotations_out["__workers_loose"][key] = v
                    elif k == "allow_other_workers":
                        if v is True:
                            annotations_out["__workers_loose"].pop(key, None)
                        else:
                            annotations_out["workers"][key] = annotations_out[
                                "__workers_loose"
                            ].pop(key, None)
                    else:
                        annotations_out[k][key] = v

            # If the task is a tuple, recursively search its contents
            task = futures[key]
            if istask(task):
                queue.extend(task[1:])
            elif isinstance(task, list):
                queue.extend(task)

        if pure:
            annotations_out["pure"] = {key: True for key in tasks}

        del annotations_out["__workers_loose"]
        if not annotations_out["workers"]:
            del annotations_out["workers"]
        for k in list(annotations_out):
            if not annotations_out[k]:
                del annotations_out[k]

        return tasks, annotations_out, dependencies

    def compute(
        self,
        collections: Any,
        sync: bool = False,
        optimize_graph: bool = True,
        workers: str | Iterable[str] | None = None,
        allow_other_workers: bool = False,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        traverse: bool = True,
        **kwargs: Any,
    ):
        """Compute dask collections on cluster

        Parameters
        ----------
        collections : iterable of dask collections or single dask collection
            Collections like dask.array, dask.dataframe, dask.delayed or dask.bag
            or list of dask collections to compute.
        sync : bool (optional)
            Returns Futures if False (default) or concrete values if True.
        optimize_graph : bool (optional)
            Whether to optimize the underlying graphs. Defaults to True.
        workers : string or list, optional
            A list of worker addresses to restrict this task to.
            Ex: ``['127.0.0.1:8787', '127.0.0.1:8788']``
        allow_other_workers : bool, optional
            If True, allow this task to run on workers that were not specified
            in the ``workers=`` keyword. Default is False.
        resources : dict, optional
            A dictionary of resource constraints for this task.
            Ex: ``{'GPU': 2, 'memory': 7e9}``
        retries : int, optional
            Number of times to retry the task if it fails.
        priority : int, optional
            Priority of task. Higher numbers mean higher priority. Defaults to 0.
        fifo_timeout : timedelta or string, optional like '1s' or '100ms'
            Maximum time for the task to be runnable. If it doesn't run by this
            deadline then it is cancelled.
        actor : bool, optional
            Whether these tasks create Actors. Default is False.
        actors : bool, string, list, optional
            Addresses of actors that these tasks can run on.
        traverse : bool, optional
            By default dask traverses builtin python collections looking for dask
            objects stopping when it reaches a user defined object. If traverse is
            False dask will stop traversing when it reaches any object that is not
            a list, dict, set, or tuple. Defaults to True.

        Returns
        -------
            Future, list of Futures, or collection wrapped in compute

        Examples
        --------
        >>> from dask import delayed
        >>> from operator import add
        >>> x = delayed(add)(1, 2)
        >>> y = delayed(add)(x, x)
        >>> futures = client.compute([x, y])  # Starts computation
        >>> results = client.gather(futures)  # Waits for results
        [3, 6]

        Also support single arguments

        >>> future = client.compute(y) # doctest: +SKIP
        >>> future.result()  # doctest: +SKIP
        6

        See Also
        --------
        Client.get : underlying function
        Client.persist : similar function that keeps data on the cluster
        """
        if isinstance(collections, (list, tuple, set)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        info = {
            "actors": actors,
            "workers": workers,
            "allow_other_workers": allow_other_workers,
            "resources": resources,
            "retries": retries,
            "priority": priority,
            "fifo_timeout": fifo_timeout,
        }

        variables = [a for a in collections if isinstance(a, Variable)]
        collections = [a for a in collections if not isinstance(a, Variable)]

        futures_dict = self._graph_to_futures(
            collections,
            info,
            optimize_graph=optimize_graph,
            traverse=traverse,
            **kwargs,
        )
        futures = [futures_dict[v] for v in collections]

        if variables:
            futures.extend([v.current_future() for v in variables])

        if singleton:
            out = futures[0]
        else:
            out = futures

        if sync:
            if self.asynchronous:
                raise ValueError(
                    "sync=True is not allowed in asynchronous mode. "
                    "Use sync=False and await gather(futures) instead."
                )
            else:
                return self.gather(out)
        else:
            return out

    def persist(
        self,
        collections: Any,
        optimize_graph: bool = True,
        workers: str | Iterable[str] | None = None,
        allow_other_workers: bool = False,
        resources: dict[str, float] | None = None,
        retries: int = 0,
        priority: int = 0,
        fifo_timeout: str | timedelta = 0,
        actor: bool = False,
        actors: bool | str | Iterable[str] = False,
        traverse: bool = True,
        **kwargs: Any,
    ):
        """Persist dask collections on cluster

        Starts computation of the collection on the cluster in the background.
        Provides a new collection that is semantically identical to the old collection,
        but now points to data that is remote.

        Parameters
        ----------
        collections : iterable of dask collections or single dask collection
            Collections like dask.array, dask.dataframe, dask.delayed or dask.bag
            or list of dask collections to compute.
        optimize_graph : bool (optional)
            Whether to optimize the underlying graphs. Defaults to True.
        workers : string or list, optional
            A list of worker addresses to restrict this task to.
            Ex: ``['127.0.0.1:8787', '127.0.0.1:8788']``
        allow_other_workers : bool, optional
            If True, allow this task to run on workers that were not specified
            in the ``workers=`` keyword. Default is False.
        resources : dict, optional
            A dictionary of resource constraints for this task.
            Ex: ``{'GPU': 2, 'memory': 7e9}``
        retries : int, optional
            Number of times to retry the task if it fails.
        priority : int, optional
            Priority of task. Higher numbers mean higher priority. Defaults to 0.
        fifo_timeout : timedelta or string, optional like '1s' or '100ms'
            Maximum time for the task to be runnable. If it doesn't run by this
            deadline then it is cancelled.
        actor : bool, optional
            Whether these tasks create Actors. Default is False.
        actors : bool, string, list, optional
            Addresses of actors that these tasks can run on.
        traverse : bool, optional
            By default dask traverses builtin python collections looking for dask
            objects stopping when it reaches a user defined object. If traverse is
            False dask will stop traversing when it reaches any object that is not
            a list, dict, set, or tuple. Defaults to True.

        Returns
        -------
            New dask collections backed by futures

        Examples
        --------
        >>> from dask import delayed
        >>> from operator import add
        >>> x = delayed(add)(1, 2)
        >>> y = delayed(add)(x, x)
        >>> x2, y2 = client.persist([x, y])  # Starts computation
        >>> # x2 and y2 are equivalent to x and y, but point to remote data

        >>> y2.compute()  # Computation is already done, this is fast
        6

        See Also
        --------
        Client.compute
        """
        info = {
            "actors": actors,
            "workers": workers,
            "allow_other_workers": allow_other_workers,
            "resources": resources,
            "retries": retries,
            "priority": priority,
            "fifo_timeout": fifo_timeout,
        }

        if isinstance(collections, (list, tuple, set)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        variables = [a for a in collections if isinstance(a, Variable)]
        collections = [a for a in collections if not isinstance(a, Variable)]

        futures_dict = self._graph_to_futures(
            collections,
            info,
            optimize_graph=optimize_graph,
            traverse=traverse,
            **kwargs,
        )
        rebuilt, state = dask.base.replace_data_in_collection(
            collections, futures_dict, connection_client=self
        )

        if variables:
            rebuilt.extend(variables)

        if singleton:
            return rebuilt[0]
        else:
            return rebuilt

    def _graph_to_futures(
        self,
        collections: Collection,
        extra_annotations: dict[str, Any],
        optimize_graph: bool = True,
        traverse: bool = True,
        **kwargs: Any,
    ) -> dict:
        """Turn Dask collections into dictionary of Futures"""
        dsk = dask.base.collections_to_dsk(collections, optimize_graph, traverse=traverse)
        dsk = HighLevelGraph.from_collections(str(uuid.uuid4()), dsk, dependencies=())
        user_keys = {c.key for c in collections}
        dsk, _ = dask.optimization.cull(dsk, user_keys)
        dsk, _ = dask.optimization.fuse(dsk, user_keys)

        # Prepare graph annotations
        annotations: dict[str, Any] = {
            k: {} for k in extra_annotations if extra_annotations[k] is not None
        }
        annotations["__workers_loose"] = {}
        annotations["workers"] = {}
        annotations.update(kwargs.get("annotations", {}))

        for key, ann in dsk.annotations.items():
            ann = ann.copy()
            workers = ann.pop("workers", None)
            allow_other_workers = ann.pop("allow_other_workers", False)

            if workers:
                if isinstance(workers, str):
                    workers = {workers}
                else:
                    workers = set(workers)
                if allow_other_workers:
                    annotations["__workers_loose"][key] = workers
                else:
                    annotations["workers"][key] = workers

            for k, v in extra_annotations.items():
                # Explicit keywords override graph annotations
                if v is not None:
                    ann[k] = v

            for k, v in ann.items():
                # Set annotation if key doesn't exist or value is None
                annotations.setdefault(k, {})[key] = v

        if fifo_timeout := annotations.get("fifo_timeout"):
            for k, v in fifo_timeout.items():
                fifo_timeout[k] = parse_timedelta(v)

        del annotations["__workers_loose"]
        if not annotations["workers"]:
            del annotations["workers"]
        for k in list(annotations):
            if not annotations[k]:
                del annotations[k]

        keys = list(dsk.keys())
        self._send_update_graph(
            tasks=dict(dsk.layers),
            keys=keys,
            dependencies=dsk.dependencies,
            annotations=annotations,
            **kwargs,
        )

        futures = {key: Future(key, self) for key in dsk}

        # Delete any intermediate futures we no longer need
        intermediate_keys = set(futures) - user_keys
        if intermediate_keys:
            self.release(intermediate_keys)

        return futures

    def _send_update_graph(
        self,
        tasks: Mapping,
        keys: list[Hashable],
        dependencies: Iterable[Hashable] = (),
        client: str | None = None,
        annotations: dict[str, Any] | None = None,
        fifo_timeout: str | timedelta | None = None,
        retries: int | None = None,
        priority: dict[str, int] | None = None,
        resources: dict[str, dict[str, float]] | None = None,
        workers: dict[str, list[str]] | None = None,
        actors: dict[str, list[str]] | None = None,
        submitting_task: Hashable | None = None,
        user_priority: int = 0,
        allow_other_workers: bool = False,
        on_error: Literal["retry", "raise", "ignore"] | None = None,
        **kwargs: Any,
    ):
        """Send graph to scheduler"""
        if annotations is None:
            annotations = {}
        if client is None:
            client = self.id

        tasks2 = {}
        dependencies2 = set()

        # Make sure that all tasks are serializable
        for key, task in tasks.items():
            try:
                if istask(task):
                    tasks2[key] = dumps(task)
                else:
                    tasks2[key] = Serialize(task)
            except Exception as e:
                logger.exception("Failed to serialize task %s", key)
                raise e

        # Collect dependencies from annotations
        annotations_deps = set()
        for v in annotations.values():
            if isinstance(v, dict):
                annotations_deps.update(v)
        dependencies2.update(annotations_deps)

        # Collect dependencies from Futures in task arguments
        for task in tasks.values():
            if istask(task):
                dependencies2.update({f.key for f in task[1:] if isinstance(f, Future)})
            elif isinstance(task, (list, set)):
                dependencies2.update({f.key for f in task if isinstance(f, Future)})

        # Collect dependencies from explicit keywords
        dependencies2.update({dep for dep in dependencies if isinstance(dep, Future)})
        dependencies2.update({dep.key for dep in dependencies if isinstance(dep, Future)})

        # Collect dependencies from keys to compute
        dependencies2.update({k.key for k in keys if isinstance(k, Future)})

        # Create Future states for all keys
        for key in tasks:
            if key not in self.futures:
                self.futures[key] = FutureState()

        # Build message payload
        msg = {
            "op": "update-graph",
            "tasks": tasks2,
            "dependencies": [Serialize(dep) for dep in dependencies2],
            "keys": [to_serialize(key) for key in keys],
            "priority": user_priority,
            "submitting_task": submitting_task,
            "client": client,
            "annotations": {
                k: Serialize(v)
                for k, v in annotations.items()
                if k not in ("fifo_timeout", "retries")
            },
        }
        if fifo_timeout := annotations.get("fifo_timeout"):
            msg["fifo_timeout"] = Serialize(fifo_timeout)
        if retries := annotations.get("retries"):
            msg["retries"] = Serialize(retries)

        # Send message to scheduler
        self._send_to_scheduler_safe(msg)

    def _inc_ref(self, key: Hashable) -> None:
        """Increment the reference count for a key"""
        if self._refcount[key] == 0:
            self._send_to_scheduler_safe(
                {"op": "client-desires-keys", "keys": [to_serialize(key)], "client": self.id}
            )
        self._refcount[key] += 1

    def _dec_ref(self, key: Hashable) -> None:
        """Decrement the reference count for a key"""
        if key not in self._refcount:
            return  # Should not happen in principle
        self._refcount[key] -= 1
        if self._refcount[key] == 0:
            self._send_to_scheduler_safe(
                {"op": "client-releases-keys", "keys": [to_serialize(key)], "client": self.id}
            )
            del self._refcount[key]

    def _get_computation(self, task: tuple, key: Hashable, pure: bool = True) -> tuple:
        """Get a computation from a task tuple"""
        if pure:
            return task
        else:
            # Add a random nonce to ensure the task is unique
            return task + (uuid.uuid4().hex,)

    def _get_futures(self, value: Any) -> list[Future]:
        """Extract futures from a value"""
        if isinstance(value, Future):
            return [value]
        elif isinstance(value, (list, tuple)):
            return list(itertools.chain.from_iterable(self._get_futures(v) for v in value))
        elif isinstance(value, dict):
            return list(
                itertools.chain.from_iterable(
                    self._get_futures(v) for v in value.values()
                )
            )
        else:
            return []

    def gather(
        self,
        futures: Any,
        errors: Literal["raise", "omit", "skip"] = "raise",
        direct: bool | None = None,
        local_worker: bool | None = None,
        asynchronous: bool | None = None,
    ) -> Any:
        """Gather futures from cluster

        Accepts a future, nested container of futures, or dict/list/tuple
        of futures. Returns the results in the same structure.

        Parameters
        ----------
        futures : Collection of Futures or Future
            The futures to gather. Can be a single future, list, tuple, dict
            of futures. Nested structures are supported.
        errors : string
            How to handle errors. Options are 'raise', 'omit', 'skip'.
            If 'raise', raise the exception on the client. Default.
            If 'omit', remove the future from the collection.
            If 'skip', keep the future object itself in the collection.
        direct : bool
            Whether or not to connect directly to the workers; defaults to True
            if the client can connect to the workers, False otherwise.
        local_worker : bool
            Whether or not to prefer using a local worker, if available.
            Defaults to True.
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Returns
        -------
        results : gathered results in the same structure as futures.

        Raises
        ------
        If any remote task raises an exception, that exception will be
        raised here, unless ``errors='omit'`` or ``errors='skip'``.

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> x = client.submit(add, 1, 2)  # doctest: +SKIP
        >>> y = client.submit(add, 10, 20)  # doctest: +SKIP
        >>> client.gather([x, y])  # doctest: +SKIP
        [3, 30]

        >>> client.gather({'x': x, 'y': y})  # doctest: +SKIP
        {'x': 3, 'y': 30}

        Optionally ignore futures that erred

        >>> z = client.submit(lambda: 1 / 0)  # doctest: +SKIP
        >>> client.gather([x, y, z], errors='omit')  # doctest: +SKIP
        [3, 30]
        >>> client.gather([x, y, z], errors='skip')  # doctest: +SKIP
        [3, 30, <Future: error, key: lambda-....>]

        See Also
        --------
        Client.get : Normal synchronous dask function
        Client.compute : Normal synchronous dask function
        Future.result : Wait on a single future
        """
        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._gather(
                futures, errors=errors, direct=direct, local_worker=local_worker
            )
        else:
            return self.sync(
                self._gather,
                futures,
                errors=errors,
                direct=direct,
                local_worker=local_worker,
                asynchronous=True,
            )

    async def _gather(
        self,
        futures: Any,
        errors: Literal["raise", "omit", "skip"] = "raise",
        direct: bool | None = None,
        local_worker: bool | None = None,
    ) -> Any:
        """Asynchronous version of gather"""
        if errors not in ("raise", "omit", "skip"):
            raise ValueError("errors= must be 'raise', 'omit', or 'skip'")

        # Short circuit on trivial cases
        if not futures:
            return futures
        if isinstance(futures, Future):
            futures_list = [futures]
            return_structure = lambda L: L[0]
        else:
            futures_list, return_structure = pack_data(futures, merge_lists=False)

        # Filter out non-futures
        futures_set: set[Future] = set()
        futures_indices: dict[Future, list[int]] = defaultdict(list)
        data = {}
        for i, f in enumerate(futures_list):
            if isinstance(f, Future):
                if f.client is not self:
                    raise ValueError("Inputs contain futures from another client.")
                futures_set.add(f)
                futures_indices[f].append(i)
            else:
                data[i] = f

        # Wait for all futures to complete
        await wait(futures_set)

        # Collect results or errors
        results: dict[int, Any] = {}
        exceptions: dict[int, Exception] = {}
        tracebacks: dict[int, Any] = {}
        bad_futures: set[Future] = set()

        for f in futures_set:
            if f.status == "error":
                if errors == "raise":
                    exc = f.exception
                    tb = f.traceback
                    # Avoid keeping references to large objects through the traceback
                    exc = exc.with_traceback(tb)
                    raise exc
                elif errors == "omit":
                    bad_futures.add(f)
                elif errors == "skip":
                    for i in futures_indices[f]:
                        results[i] = f
                else:
                    assert False, "Not reachable"
            elif f.status == "cancelled":
                if errors == "raise":
                    raise f.exception
                elif errors == "omit":
                    bad_futures.add(f)
                elif errors == "skip":
                    for i in futures_indices[f]:
                        results[i] = f
                else:
                    assert False, "Not reachable"
            else:
                assert f.status == "finished"

        # Collect data from workers
        keys_to_gather = {f.key for f in futures_set if f not in bad_futures}
        if keys_to_gather:
            gathered_results = await self._gather_remote(
                keys_to_gather, direct=direct, local_worker=local_worker
            )
            for f in futures_set:
                if f in bad_futures:
                    continue
                try:
                    result = gathered_results[f.key]
                except KeyError:
                    # Future not in gathered results, assume it was lost
                    f._state.set_lost()
                    if errors == "raise":
                        raise LostKeyError(f.key)
                    elif errors == "omit":
                        bad_futures.add(f)
                    elif errors == "skip":
                        for i in futures_indices[f]:
                            results[i] = f
                    else:
                        assert False, "Not reachable"
                else:
                    for i in futures_indices[f]:
                        results[i] = result

        # Reconstruct original data structure
        if bad_futures:
            unpack_indices = [
                i for i, f in enumerate(futures_list) if f not in bad_futures
            ]
            packed_results = [results[i] for i in unpack_indices]
            return return_structure(packed_results)
        else:
            final_results = [results.get(i, data.get(i)) for i in range(len(futures_list))]
            return return_structure(final_results)

    async def _gather_remote(
        self,
        keys_to_gather: set[Hashable],
        direct: bool | None = None,
        local_worker: bool | None = None,
    ) -> dict[Hashable, Any]:
        """Gather data from workers"""
        who_has = await self._who_has(keys_to_gather)
        data, missing_keys, failed_keys, bad_workers = await gather_from_workers(
            who_has,
            rpc=self.rpc,
            close=False,
            serializers=self._serializers_orig,
            direct=direct,
            local_worker=local_worker,
            gather_semaphore=self._gather_semaphore,
        )

        if missing_keys:
            logger.warning("Couldn't gather keys %s", missing_keys)
            # TODO: Do something about missing keys?

        if failed_keys:
            logger.warning("Failed to gather keys %s", failed_keys)
            # TODO: Do something about failed keys?

        return data

    def scatter(
        self,
        data: Any,
        workers: str | Iterable[str] | None = None,
        broadcast: bool = False,
        direct: bool | None = None,
        local_worker: bool | None = None,
        timeout: float | str = config.timeouts["connect"],
        hash: bool = True,
        asynchronous: bool | None = None,
    ):
        """Scatter data to workers

        Parameters
        ----------
        data : list, dict, or object
            Data to scatter to workers. Output type matches input type.
        workers : list of strings, optional
            List of worker addresses to scatter data to.
            Leave None to scatter to all workers.
        broadcast : bool, optional
            Whether to scatter data to all workers. Default is False.
            If True, ignores the ``workers`` keyword argument.
        direct : bool, optional
            Whether or not to connect directly to the workers; defaults to True
            if the client can connect to the workers, False otherwise.
        local_worker : bool, optional
            Whether or not to prefer using a local worker, if available.
            Defaults to True.
        timeout : number, optional
            Time in seconds after which to raise a ``distributed.TimeoutError``
            if the scatter operation has not completed.
        hash : bool, optional
            Whether to hash data to determine which worker to send it to.
            Default is True. If False, data is sent to workers in a round-robin fashion.
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Returns
        -------
        List, dict, or Futures matching the type of input.

        Examples
        --------
        >>> data = [1, 2, 3]  # doctest: +SKIP
        >>> futures = client.scatter(data)  # doctest: +SKIP

        >>> data = {'x': 1, 'y': 2, 'z': 3}  # doctest: +SKIP
        >>> futures = client.scatter(data)  # doctest: +SKIP

        >>> data = MyObject()  # doctest: +SKIP
        >>> future = client.scatter(data)  # doctest: +SKIP

        See Also
        --------
        Client.gather : gather data back to client
        Client.replicate : replicate data across workers
        """
        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._scatter(
                data,
                workers=workers,
                broadcast=broadcast,
                direct=direct,
                local_worker=local_worker,
                timeout=timeout,
                hash=hash,
            )
        else:
            return self.sync(
                self._scatter,
                data,
                workers=workers,
                broadcast=broadcast,
                direct=direct,
                local_worker=local_worker,
                timeout=timeout,
                hash=hash,
                asynchronous=True,
            )

    async def _scatter(
        self,
        data: Any,
        workers: str | Iterable[str] | None = None,
        broadcast: bool = False,
        direct: bool | None = None,
        local_worker: bool | None = None,
        timeout: float | str = config.timeouts["connect"],
        hash: bool = True,
    ) -> Any:
        """Asynchronous version of scatter"""
        # Input validation
        if broadcast:
            workers = None
        elif workers is not None:
            if isinstance(workers, str):
                workers = [workers]
            workers = list(set(workers))
            # Check if workers are known
            known_workers = set(self.scheduler_info()["workers"])
            if not set(workers).issubset(known_workers):
                raise ValueError(
                    f"Workers {set(workers) - known_workers} not found. "
                    f"Valid workers are: {known_workers}"
                )

        # Prepare data and keys
        if isinstance(data, Mapping):
            keys = list(data)
            data_list = [data[k] for k in keys]
            return_structure = lambda L: dict(zip(keys, L))
        elif isinstance(data, (list, tuple, set)):
            data_list = list(data)
            return_structure = type(data)
        else:
            data_list = [data]
            return_structure = lambda L: L[0]

        keys = [f"scatter-{uuid.uuid4()}" for _ in data_list]
        data_dict = dict(zip(keys, data_list))

        # Scatter data to workers
        futures_dict = await scatter_to_workers(
            data_dict,
            workers=workers,
            broadcast=broadcast,
            rpc=self.rpc,
            serializers=self._serializers_orig,
            report=False,
            direct=direct,
            local_worker=local_worker,
            timeout=timeout,
            hash=hash,
        )

        # Create Futures and update client state
        futures = []
        for key in keys:
            if key in futures_dict:
                # Data successfully scattered
                worker_addr = futures_dict[key]
                future = Future(key, self, inform=False)
                self.futures[key] = FutureState(status="finished")
                self._who_has[key] = {worker_addr}
                futures.append(future)
            else:
                # Scatter failed for this key
                logger.warning("Failed to scatter data for key %s", key)
                # TODO: Handle scatter failure more gracefully?
                futures.append(None)  # Placeholder for failed scatter

        return return_structure(futures)

    def cancel(
        self,
        futures: Future | Collection[Future],
        force: bool = False,
        asynchronous: bool | None = None,
    ) -> None:
        """Cancel running futures

        Parameters
        ----------
        futures : list of Futures
        force : bool, default False
            If True, cancel the future even if other clients or tasks depend on it.
            If False, the future will only be cancelled if the current client is
            the only one holding a reference to it.
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Examples
        --------
        >>> x = client.submit(inc, 1)  # doctest: +SKIP
        >>> client.cancel([x])  # doctest: +SKIP

        See Also
        --------
        Future.cancel
        Client.retry
        """
        if isinstance(futures, Future):
            futures = [futures]
        if not futures:
            return

        keys = [f.key for f in futures]
        logger.debug("Cancel futures %s", keys)

        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._cancel(keys, force=force)
        else:
            return self.sync(self._cancel, keys, force=force, asynchronous=True)

    async def _cancel(self, keys: list[Hashable], force: bool = False) -> None:
        """Asynchronous version of cancel"""
        await self._send_to_scheduler(
            {"op": "cancel", "keys": keys, "client": self.id, "force": force}
        )

    def retry(
        self, futures: Future | Collection[Future], asynchronous: bool | None = None
    ) -> None:
        """Retry failed futures

        Parameters
        ----------
        futures : list of Futures
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Examples
        --------
        >>> x = client.submit(fail)  # doctest: +SKIP
        >>> x.result()  # raises Exception  # doctest: +SKIP
        >>> client.retry([x])  # doctest: +SKIP

        See Also
        --------
        Client.cancel
        """
        if isinstance(futures, Future):
            futures = [futures]
        if not futures:
            return

        keys = [f.key for f in futures]
        logger.debug("Retry futures %s", keys)

        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._retry(keys)
        else:
            return self.sync(self._retry, keys, asynchronous=True)

    async def _retry(self, keys: list[Hashable]) -> None:
        """Asynchronous version of retry"""
        await self._send_to_scheduler({"op": "retry", "keys": keys, "client": self.id})

    def publish_dataset(
        self,
        *args: Any,
        name: str | None = None,
        override: bool = False,
        client: Client | None = None,
        **kwargs: Any,
    ) -> None:
        """Publish data to the cluster under a given name

        This stores data on the cluster under a given name. This data is
        available to all clients. If the data exists on the scheduler then this
        is cheap. If the data exists on the workers then it will be replicated
        on all workers (perhaps inefficiently.) If the data is local then this
        will call ``Client.scatter`` first.

        Warning: If the data is larger than the available memory on the
        scheduler then the scheduler may crash. This is especially likely if
        calling `publish_dataset` on data that has not yet been scattered.
        Consider scattering large datasets manually first.

        Parameters
        ----------
        *args : dask objects or Futures
            Datasets to publish. These must have names attribute
        name : str, optional
            Name under which to publish the dataset. Defaults to the name
            of the dataset.
        override : bool, optional
            Whether to override existing dataset with the same name.
            Default is False.
        client : Client, optional
            Client to use for publishing. Defaults to the current client.
        **kwargs : dict
            Keyword arguments to pass to ``Client.scatter`` if scattering is
            necessary.

        Examples
        --------
        Publish a local Pandas DataFrame

        >>> df = pd.DataFrame({'x': [1, 2, 3]})  # doctest: +SKIP
        >>> client.publish_dataset(my_dataset=df)  # doctest: +SKIP

        Publish a Dask DataFrame

        >>> import dask.dataframe as dd  # doctest: +SKIP
        >>> df = dd.read_csv('s3://...')  # doctest: +SKIP
        >>> df = client.persist(df)  # doctest: +SKIP
        >>> client.publish_dataset(my_dataset=df)  # doctest: +SKIP

        Publishing multiple datasets at once

        >>> client.publish_dataset(dataset1=df1, dataset2=df2)  # doctest: +SKIP

        Retrieving data

        >>> client.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> df_future = client.get_dataset('my_dataset')  # doctest: +SKIP
        >>> df = client.gather(df_future)  # doctest: +SKIP

        See Also
        --------
        Client.list_datasets
        Client.get_dataset
        Client.unpublish_dataset
        Client.scatter
        """
        if not args and not kwargs:
            raise ValueError("Must provide at least one dataset to publish")
        if args:
            if name is not None:
                raise ValueError("Cannot provide name= and unnamed arguments")
            if len(args) > 1:
                raise ValueError("Cannot provide multiple unnamed arguments")
            if not hasattr(args[0], "name"):
                raise ValueError("Unnamed arguments must have a .name attribute")
            name = args[0].name
            data = args[0]
            kwargs[name] = data
        if name is not None:
            if len(kwargs) > 1:
                raise ValueError("Cannot provide name= and multiple datasets")
            if name not in kwargs:
                raise ValueError("Name provided but no matching dataset found")
            data = kwargs[name]

        if client is None:
            client = self

        for name, data in kwargs.items():
            if not isinstance(data, Future) and not is_dask_collection(data):
                # Scatter local data
                logger.info("Scattering data for dataset '%s'...", name)
                if sizeof(data) > 1e6:
                    logger.warning(
                        "Large dataset detected. Consider scattering manually "
                        "using client.scatter for more control."
                    )
                scatter_kwargs = kwargs.copy()
                scatter_kwargs.pop(name)  # Avoid passing dataset as kwarg
                futures = client.scatter(data, **scatter_kwargs)
                data = futures

            # Publish scattered or persisted data
            logger.info("Publishing dataset '%s'...", name)
            if client.asynchronous:
                client.loop.add_callback(
                    client.datasets.publish, data, name=name, override=override
                )
            else:
                client.sync(
                    client.datasets.publish, data, name=name, override=override
                )

    def unpublish_dataset(self, name: str, **kwargs: Any) -> None:
        """Remove a published dataset from the cluster

        Parameters
        ----------
        name : str
            Name of the dataset to unpublish.
        **kwargs : dict
            Options to pass to the remote function.

        Examples
        --------
        >>> client.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> client.unpublish_dataset('my_dataset')  # doctest: +SKIP
        >>> client.list_datasets()  # doctest: +SKIP
        []

        See Also
        --------
        Client.publish_dataset
        Client.list_datasets
        Client.get_dataset
        """
        if self.asynchronous:
            self.loop.add_callback(self.datasets.unpublish, name, **kwargs)
        else:
            self.sync(self.datasets.unpublish, name, **kwargs)

    def list_datasets(self, **kwargs: Any) -> list[str]:
        """List named datasets available on the scheduler

        Parameters
        ----------
        **kwargs : dict
            Options to pass to the remote function.

        Returns
        -------
        List of dataset names

        Examples
        --------
        >>> client.publish_dataset(my_dataset=df)  # doctest: +SKIP
        >>> client.list_datasets()  # doctest: +SKIP
        ['my_dataset']

        See Also
        --------
        Client.publish_dataset
        Client.get_dataset
        Client.unpublish_dataset
        """
        if self.asynchronous:
            # Cannot await here, return a coroutine instead
            raise RuntimeError(
                "list_datasets is awaitable, use `await client.list_datasets()`"
            )
        else:
            return self.sync(self.datasets.list, **kwargs)

    async def _list_datasets(self, **kwargs: Any) -> list[str]:
        """Internal awaitable version of list_datasets"""
        return await self.datasets.list(**kwargs)

    def get_dataset(
        self, name: str, default: Any = no_default, **kwargs: Any
    ) -> Future | Any:
        """Get a named dataset from the scheduler

        This returns a Future. The data represented by this future will be
        replicated on all workers.

        Parameters
        ----------
        name : str
            Name of the dataset to retrieve.
        default : object, optional
            Value to return if the dataset does not exist. If not provided,
            raises KeyError if the dataset is not found.
        **kwargs : dict
            Options to pass to the remote function.

        Returns
        -------
        Future containing the dataset.

        Examples
        --------
        >>> client.publish_dataset(my_dataset=df)  # doctest: +SKIP
        >>> future = client.get_dataset('my_dataset')  # doctest: +SKIP
        >>> result = client.gather(future)  # doctest: +SKIP

        See Also
        --------
        Client.publish_dataset
        Client.list_datasets
        Client.unpublish_dataset
        """
        if self.asynchronous:
            # Cannot await here, return a coroutine instead
            raise RuntimeError(
                "get_dataset is awaitable, use `await client.get_dataset()`"
            )
        else:
            return self.sync(self.datasets.get, name, default=default, **kwargs)

    async def _get_dataset(
        self, name: str, default: Any = no_default, **kwargs: Any
    ) -> Future | Any:
        """Internal awaitable version of get_dataset"""
        return await self.datasets.get(name, default=default, **kwargs)

    def upload_file(
        self,
        filename: str,
        data: bytes | None = None,
        raise_on_error: bool = True,
        **kwargs: Any,
    ) -> None:
        """Upload a local file to all workers

        Parameters
        ----------
        filename : string
            Filename of the file to upload. The file will be placed in the
            worker's local directory with the same name.
        data : bytes, optional
            If provided, the value of `data` is uploaded instead of the
            contents of `filename`. The file will still be named `filename`
            on the worker.
        raise_on_error : bool, optional
            If True (default), raise an exception if the upload fails on any worker.
            If False, log a warning but do not raise an exception.
        **kwargs : dict
            Optional keyword arguments for the remote function.

        Examples
        --------
        >>> client.upload_file('myfile.py')  # doctest: +SKIP

        See Also
        --------
        Client.upload_environment
        Client.register_worker_plugin
        """
        if self.asynchronous:
            self.loop.add_callback(
                self._upload_file, filename, data=data, raise_on_error=raise_on_error
            )
        else:
            self.sync(
                self._upload_file,
                filename,
                data=data,
                raise_on_error=raise_on_error,
            )

    async def _upload_file(
        self, filename: str, data: bytes | None = None, raise_on_error: bool = True
    ) -> None:
        """Asynchronous version of upload_file"""
        key = os.path.basename(filename)
        async with self._file_upload_lock:
            if key in self._pending_uploads:
                # Already uploading this file
                return
            self._pending_uploads.add(key)

        try:
            if data is None:
                with open(filename, "rb") as f:
                    data = f.read()

            logger.info("Uploading file %s (%d bytes)", filename, len(data))
            response = await self._send_to_scheduler(
                {
                    "op": "upload_file",
                    "filename": filename,
                    "data": data,
                    "reply": True,
                    "raise_on_error": raise_on_error,
                }
            )

            if raise_on_error and response["status"] == "error":
                raise response["error"]
            elif response["status"] == "error":
                logger.warning(
                    "Failed to upload file %s to some workers: %s",
                    filename,
                    response["error"],
                )
        finally:
            async with self._file_upload_lock:
                self._pending_uploads.remove(key)

    def upload_environment(self, zipfile: str, **kwargs: Any) -> None:
        """Upload a zipped conda environment to all workers

        Parameters
        ----------
        zipfile : string
            Path to the zipped conda environment file.
        **kwargs : dict
            Optional keyword arguments for the remote function.

        See Also
        --------
        Client.upload_file
        """
        # TODO: Implement environment upload functionality
        raise NotImplementedError("Environment upload not yet implemented")

    def register_worker_plugin(
        self,
        plugin: WorkerPlugin | tuple | dict,
        name: str | None = None,
        nanny: bool = False,
        **kwargs: Any,
    ) -> None:
        """Register a worker plugin on all workers

        A worker plugin is a class or function that is run on the worker
        at startup and periodically throughout its lifetime. Plugins can
        be used to extend worker functionality, monitor performance, or
        integrate with external systems.

        Parameters
        ----------
        plugin : WorkerPlugin, tuple, or dict
            The plugin object, or arguments to construct a plugin.
            If a tuple, it is interpreted as ``(PluginClass, *args, **kwargs)``.
            If a dict, it is interpreted as ``{"plugin": PluginClass, "args": [], "kwargs": {}}``.
        name : str, optional
            A name for the plugin. If not provided, the class name is used.
        nanny : bool, optional
            If True, register the plugin with the Nanny process instead of the Worker.
            Default is False.
        **kwargs : dict
            Optional keyword arguments for the remote function.

        Examples
        --------
        >>> class MyPlugin(WorkerPlugin):
        ...     def setup(self, worker):
        ...         print("Plugin setup on worker", worker.address)
        ...
        >>> client.register_worker_plugin(MyPlugin())  # doctest: +SKIP

        Register a plugin with arguments

        >>> class MyPluginWithArgs(WorkerPlugin):
        ...     def __init__(self, value):
        ...         self.value = value
        ...     def setup(self, worker):
        ...         print("Plugin setup with value", self.value)
        ...
        >>> client.register_worker_plugin(MyPluginWithArgs(10))  # doctest: +SKIP

        Register using a tuple

        >>> client.register_worker_plugin((MyPluginWithArgs, 20))  # doctest: +SKIP

        Register using a dict

        >>> client.register_worker_plugin({"plugin": MyPluginWithArgs, "kwargs": {"value": 30}})  # doctest: +SKIP

        Register with the Nanny process

        >>> client.register_worker_plugin(MyPlugin(), nanny=True)  # doctest: +SKIP

        See Also
        --------
        Client.unregister_worker_plugin
        distributed.diagnostics.plugin.WorkerPlugin
        """
        if self.asynchronous:
            self.loop.add_callback(
                self._register_worker_plugin, plugin, name=name, nanny=nanny, **kwargs
            )
        else:
            self.sync(
                self._register_worker_plugin,
                plugin,
                name=name,
                nanny=nanny,
                **kwargs,
            )

    async def _register_worker_plugin(
        self,
        plugin: WorkerPlugin | tuple | dict,
        name: str | None = None,
        nanny: bool = False,
        **kwargs: Any,
    ) -> None:
        """Asynchronous version of register_worker_plugin"""
        if isinstance(plugin, dict):
            plugin_dict = plugin
        elif isinstance(plugin, tuple):
            plugin_dict = {"plugin": plugin[0]}
            if len(plugin) > 1:
                plugin_dict["args"] = plugin[1:]
            if len(plugin) > 2:
                plugin_dict["kwargs"] = plugin[2]
        else:
            plugin_dict = {"plugin": plugin}

        if name is None:
            if isinstance(plugin_dict["plugin"], type):
                name = plugin_dict["plugin"].__name__
            else:
                name = type(plugin_dict["plugin"]).__name__

        logger.info("Registering worker plugin %s", name)
        await self._send_to_scheduler(
            {
                "op": "register_worker_plugin",
                "plugin": Serialize(plugin_dict),
                "name": name,
                "nanny": nanny,
                **kwargs,
            }
        )

    def unregister_worker_plugin(
        self, name: str, nanny: bool = False, **kwargs: Any
    ) -> None:
        """Unregister a worker plugin on all workers

        Parameters
        ----------
        name : str
            Name of the plugin to unregister.
        nanny : bool, optional
            If True, unregister the plugin from the Nanny process instead of the Worker.
            Default is False.
        **kwargs : dict
            Optional keyword arguments for the remote function.

        See Also
        --------
        Client.register_worker_plugin
        """
        if self.asynchronous:
            self.loop.add_callback(
                self._unregister_worker_plugin, name, nanny=nanny, **kwargs
            )
        else:
            self.sync(self._unregister_worker_plugin, name, nanny=nanny, **kwargs)

    async def _unregister_worker_plugin(
        self, name: str, nanny: bool = False, **kwargs: Any
    ) -> None:
        """Asynchronous version of unregister_worker_plugin"""
        logger.info("Unregistering worker plugin %s", name)
        await self._send_to_scheduler(
            {"op": "unregister_worker_plugin", "name": name, "nanny": nanny, **kwargs}
        )

    def run(
        self,
        function: Callable,
        *args: Any,
        workers: list[str] | None = None,
        nanny: bool = False,
        wait: bool = True,
        on_error: Literal["raise", "return"] = "raise",
        **kwargs: Any,
    ) -> dict:
        """Run a function on all workers

        Parameters
        ----------
        function : callable
            Function to run on each worker.
        *args :
            Positional arguments to pass to the function.
        workers : list of strings, optional
            List of worker addresses to run the function on.
            Defaults to all workers.
        nanny : bool, optional
            Whether to run the function on the Nanny process instead of the Worker.
            Default is False.
        wait : bool, optional
            If True (default), wait for the function to complete on all workers.
            If False, return immediately.
        on_error : 'raise' or 'return'
            If 'raise' (default), raise the first exception encountered on any worker.
            If 'return', return the exception object instead of raising.
        **kwargs :
            Keyword arguments to pass to the function.

        Returns
        -------
        Dictionary mapping worker addresses to the results or exceptions.

        Examples
        --------
        >>> client.run(os.getpid)  # doctest: +SKIP
        {'tcp://127.0.0.1:37985': 14185,
         'tcp://127.0.0.1:40167': 14186,
         'tcp://127.0.0.1:44193': 14187,
         'tcp://127.0.0.1:46169': 14188}

        Restrict to particular workers

        >>> client.run(os.getpid, workers=['tcp://127.0.0.1:37985'])  # doctest: +SKIP
        {'tcp://127.0.0.1:37985': 14185}

        Run on the Nanny process instead

        >>> client.run(os.getpid, nanny=True)  # doctest: +SKIP

        See Also
        --------
        Client.run_on_scheduler
        """
        if self.asynchronous:
            return self._run(
                function,
                *args,
                workers=workers,
                nanny=nanny,
                wait=wait,
                on_error=on_error,
                **kwargs,
            )
        else:
            return self.sync(
                self._run,
                function,
                *args,
                workers=workers,
                nanny=nanny,
                wait=wait,
                on_error=on_error,
                **kwargs,
            )

    async def _run(
        self,
        function: Callable,
        *args: Any,
        workers: list[str] | None = None,
        nanny: bool = False,
        wait: bool = True,
        on_error: Literal["raise", "return"] = "raise",
        **kwargs: Any,
    ) -> dict:
        """Asynchronous version of run"""
        if on_error not in ("raise", "return"):
            raise ValueError("on_error= must be 'raise' or 'return'")

        # Serialize function and arguments
        try:
            serialized_function = dumps(function)
            serialized_args = dumps(args)
            serialized_kwargs = dumps(kwargs)
        except Exception as e:
            logger.exception("Failed to serialize function or arguments")
            raise e

        # Send request to scheduler
        response = await self._send_to_scheduler(
            {
                "op": "run",
                "function": serialized_function,
                "args": serialized_args,
                "kwargs": serialized_kwargs,
                "workers": workers,
                "nanny": nanny,
                "wait": wait,
            }
        )

        # Process results
        results = {}
        for worker, res in response.items():
            if res["status"] == "error":
                exc = loads(res["exception"])
                if on_error == "raise":
                    raise exc
                else:
                    results[worker] = exc
            elif res["status"] == "OK":
                results[worker] = loads(res["result"])
            else:
                logger.error("Unknown status from worker %s: %s", worker, res["status"])

        return results

    def run_on_scheduler(
        self, function: Callable, *args: Any, wait: bool = True, **kwargs: Any
    ) -> Any:
        """Run a function on the scheduler process

        Parameters
        ----------
        function : callable
            Function to run on the scheduler.
        *args :
            Positional arguments to pass to the function.
        wait : bool, optional
            If True (default), wait for the function to complete.
            If False, return immediately (result will be None).
        **kwargs :
            Keyword arguments to pass to the function.

        Returns
        -------
        Result of the function if wait=True, else None.

        Examples
        --------
        >>> def get_log_level():
        ...     return logging.getLogger('distributed.scheduler').level
        ...
        >>> client.run_on_scheduler(get_log_level)  # doctest: +SKIP
        20  # logging.INFO

        See Also
        --------
        Client.run : Run a function on workers
        """
        if self.asynchronous:
            return self._run_on_scheduler(function, *args, wait=wait, **kwargs)
        else:
            return self.sync(
                self._run_on_scheduler, function, *args, wait=wait, **kwargs
            )

    async def _run_on_scheduler(
        self, function: Callable, *args: Any, wait: bool = True, **kwargs: Any
    ) -> Any:
        """Asynchronous version of run_on_scheduler"""
        # Serialize function and arguments
        try:
            serialized_function = dumps(function)
            serialized_args = dumps(args)
            serialized_kwargs = dumps(kwargs)
        except Exception as e:
            logger.exception("Failed to serialize function or arguments")
            raise e

        # Send request to scheduler
        response = await self._send_to_scheduler(
            {
                "op": "run_scheduler",
                "function": serialized_function,
                "args": serialized_args,
                "kwargs": serialized_kwargs,
                "wait": wait,
            }
        )

        # Process result
        if response["status"] == "error":
            raise loads(response["exception"])
        elif response["status"] == "OK":
            return loads(response["result"])
        else:
            logger.error("Unknown status from scheduler: %s", response["status"])
            return None

    def get_metadata(
        self,
        keys: Hashable | Collection[Hashable],
        default: Any = no_default,
        asynchronous: bool | None = None,
    ) -> Any:
        """Get metadata associated with keys

        Parameters
        ----------
        keys : list of keys or single key
            Keys to retrieve metadata for.
        default : object, optional
            Value to return if a key is not found. If not provided,
            raises KeyError if any key is not found.
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Returns
        -------
        Dictionary mapping keys to metadata, or single metadata value if
        a single key was provided.

        Examples
        --------
        >>> arr = da.ones(1000, chunks=100)  # doctest: +SKIP
        >>> fut = client.persist(arr)  # doctest: +SKIP
        >>> client.get_metadata(fut.key)  # doctest: +SKIP
        {'dtype': dtype('float64'), 'shape': (1000,), 'chunks': ((100,) * 10,)}

        See Also
        --------
        Client.set_metadata
        Client.has_what
        """
        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._get_metadata(keys, default=default)
        else:
            return self.sync(self._get_metadata, keys, default=default)

    async def _get_metadata(
        self, keys: Hashable | Collection[Hashable], default: Any = no_default
    ) -> Any:
        """Asynchronous version of get_metadata"""
        if not isinstance(keys, (list, tuple, set)):
            singleton = True
            keys_list = [keys]
        else:
            singleton = False
            keys_list = list(keys)

        # Request metadata from scheduler
        response = await self._send_to_scheduler(
            {"op": "get_metadata", "keys": keys_list}
        )

        # Process results
        results = {}
        missing_keys = set(keys_list)
        for key, metadata in response.items():
            results[key] = metadata
            missing_keys.remove(key)

        if missing_keys:
            if default is no_default:
                raise KeyError(f"Metadata not found for keys: {missing_keys}")
            else:
                for key in missing_keys:
                    results[key] = default

        if singleton:
            return results[keys_list[0]]
        else:
            return results

    def set_metadata(
        self, keys: Hashable | Collection[Hashable], value: Any = None, **kwargs: Any
    ) -> None:
        """Set metadata associated with keys

        Parameters
        ----------
        keys : list of keys or single key
            Keys to set metadata for.
        value : object, optional
            Metadata value to set. If not provided, metadata is set based
            on keyword arguments.
        **kwargs :
            Keyword arguments mapping metadata fields to values.

        Examples
        --------
        Set metadata for a single key

        >>> client.set_metadata('mykey', type='int', description='An integer')  # doctest: +SKIP

        Set metadata for multiple keys

        >>> client.set_metadata(['key1', 'key2'], is_valid=True)  # doctest: +SKIP

        Set a single metadata value for a key

        >>> client.set_metadata('key3', value={'custom': 'info'})  # doctest: +SKIP

        See Also
        --------
        Client.get_metadata
        """
        if self.asynchronous:
            self.loop.add_callback(self._set_metadata, keys, value=value, **kwargs)
        else:
            self.sync(self._set_metadata, keys, value=value, **kwargs)

    async def _set_metadata(
        self, keys: Hashable | Collection[Hashable], value: Any = None, **kwargs: Any
    ) -> None:
        """Asynchronous version of set_metadata"""
        if not isinstance(keys, (list, tuple, set)):
            keys_list = [keys]
        else:
            keys_list = list(keys)

        if value is not None:
            if kwargs:
                raise ValueError("Cannot provide both value= and keyword arguments")
            metadata = value
        else:
            metadata = kwargs

        await self._send_to_scheduler(
            {"op": "set_metadata", "keys": keys_list, "metadata": metadata}
        )

    def get_versions(
        self,
        workers: list[str] | None = None,
        packages: list[str] | None = None,
        check: bool = False,
        asynchronous: bool | None = None,
    ) -> dict:
        """Get package versions from scheduler and workers

        Parameters
        ----------
        workers : list of strings, optional
            List of worker addresses to query. Defaults to all workers.
        packages : list of strings, optional
            List of package names to query versions for. Defaults to packages
            listed in `distributed.versions.required_packages`.
        check : bool, optional
            If True, check for version mismatches and raise an error if found.
            Default is False.
        asynchronous: bool or None
            If True run in asynchronous mode. If False run in synchronous mode.
            If None, defaults to client behavior.

        Returns
        -------
        Dictionary mapping component addresses ('scheduler', 'client', worker addresses)
        to dictionaries mapping package names to version strings.

        Examples
        --------
        >>> client.get_versions()  # doctest: +SKIP
        {'client': {'python': '3.9.7', 'dask': '2022.1.0', ...},
         'scheduler': {'python': '3.9.7', 'dask': '2022.1.0', ...},
         'tcp://127.0.0.1:37985': {'python': '3.9.7', 'dask': '2022.1.0', ...},
         ...}

        Check for version mismatches

        >>> client.get_versions(check=True)  # doctest: +SKIP
        # Raises ImportError if mismatches are found

        See Also
        --------
        versions.get_versions
        """
        if asynchronous is None:
            asynchronous = self.asynchronous
        if asynchronous:
            return self._get_versions(workers=workers, packages=packages, check=check)
        else:
            return self.sync(
                self._get_versions, workers=workers, packages=packages, check=check
            )

    async def _get_versions(
        self,
        workers: list[str] | None = None,
        packages: list[str] | None = None,
        check: bool = False,
    ) -> dict:
        """Asynchronous version of get_versions"""
        # Get versions from scheduler and workers
        response = await self._send_to_scheduler(
            {"op": "versions", "workers": workers, "packages": packages}
        )

        # Add client versions
        client_versions = versions.get_versions(packages=packages)
        response["client"] = client_versions

        # Check for mismatches if requested
        if check:
            versions.error_message(response, versions.required_packages)

        return response

    def get_scheduler_logs(self, n: int | None = None) -> list[tuple]:
        """Get logs from the scheduler

        Parameters
        ----------
        n : int, optional
            Number of log entries to retrieve. Defaults to all logs.

        Returns
        -------
        List of log entries (tuples containing level, message, timestamp).

        Examples
        --------
        >>> client.get_scheduler_logs(n=10)  # doctest: +SKIP

        See Also
        --------
        Client.get_worker_logs
        """
        if self.asynchronous:
            return self._get_scheduler_logs(n=n)
        else:
            return self.sync(self._get_scheduler_logs, n=n)

    async def _get_scheduler_logs(self, n: int | None = None) -> list[tuple]:
        """Asynchronous version of get_scheduler_logs"""
        response = await self._send_to_scheduler({"op": "logs", "n": n})
        return response

    def get_worker_logs(
        self, n: int | None = None, workers: list[str] | None = None, nanny: bool = False
    ) -> dict[str, list[tuple]]:
        """Get logs from workers

        Parameters
        ----------
        n : int, optional
            Number of log entries to retrieve per worker. Defaults to all logs.
        workers : list of strings, optional
            List of worker addresses to retrieve logs from. Defaults to all workers.
        nanny : bool, optional
            Whether to retrieve logs from the Nanny process instead of the Worker.
            Default is False.

        Returns
        -------
        Dictionary mapping worker addresses to lists of log entries.

        Examples
        --------
        >>> client.get_worker_logs(n=10)  # doctest: +SKIP

        See Also
        --------
        Client.get_scheduler_logs
        """
        if self.asynchronous:
            return self._get_worker_logs(n=n, workers=workers, nanny=nanny)
        else:
            return self.sync(self._get_worker_logs, n=n, workers=workers, nanny=nanny)

    async def _get_worker_logs(
        self, n: int | None = None, workers: list[str] | None = None, nanny: bool = False
    ) -> dict[str, list[tuple]]:
        """Asynchronous version of get_worker_logs"""
        response = await self._send_to_scheduler(
            {"op": "worker_logs", "n": n, "workers": workers, "nanny": nanny}
        )
        return response

    @property
    def nthreads(self) -> dict[str, int]:
        """Number of threads per worker"""
        return valmap(lambda w: w["nthreads"], self.scheduler_info()["workers"])

    def scheduler_info(self, n_workers: int = 5, **kwargs: Any) -> SchedulerInfo:
        """Basic information about the workers in the cluster

        Parameters
        ----------
        n_workers : int
            The number of workers for which to fetch information. To fetch all,
            use -1.
        **kwargs : dict
            Optional keyword arguments for the remote function

        Returns
        -------
        SchedulerInfo : dict
            A dictionary containing information about the scheduler and workers.

            Top-level keys include:
                * type: (str) Always "Scheduler".
                * id: (str) Scheduler's unique ID.
                * address: (str) Scheduler's network address (e.g., "tcp://127.0.0.1:8786").
                * services: (dict) Service names mapped to ports (e.g., {'dashboard': 8787}).
                * started: (float) Timestamp when the scheduler started.
                * n_workers: (int) Number of workers included in the 'workers' dict.
                * total_threads: (int) Total threads across the included workers.
                * total_memory: (int) Total memory limit across the included workers (bytes).
                * workers: (dict) A dictionary mapping worker addresses to detailed worker information.

            Keys within the nested `workers` dictionary include:
                * type: (str) Always "Worker".
                * id: (str) Worker's unique ID.
                * host: (str) Hostname or IP address of the worker.
                * resources: (dict) Custom resources assigned to the worker (e.g., {}).
                * local_directory: (str) Path to the worker's local scratch directory.
                * name: (str or Hashable) Worker's name.
                * nthreads: (int) Number of threads on the worker.
                * memory_limit: (int) Memory limit for the worker (bytes).
                * last_seen: (float) Timestamp of the last heartbeat from the worker.
                * services: (dict) Services running on the worker mapped to ports.
                * metrics: (dict) Worker metrics (e.g., memory usage, CPU usage). The specific
                  metrics included can vary based on worker configuration and plugins.
                * status: (str) Current status (e.g., "running", "closing").
                * nanny: (str or None) Address of the worker's Nanny process, if applicable.

        Note
        ----
        Some internal scheduler state regarding workers, such as the total
        size of task data stored on a worker (tracked internally as `nbytes`),
        is not included in this dictionary.
        """
        if not self.asynchronous:
            self.sync(self._update_scheduler_info, n_workers=n_workers)
        return self._scheduler_identity

    def dump_cluster_state(
        self,
        filename: str = "dask-cluster-dump",
        write_from_scheduler: bool | None = None,
        exclude: Collection[str] = (),
        format: Literal["msgpack", "yaml"] = "msgpack",
        **storage_options,
    ):
        """Extract a dump of the entire cluster state and persist to disk or a URL.
        This is intended for debugging purposes only.

        Warning: Memory usage on the scheduler (and client, if writing the dump locally)
        can be large. On a large or long-running cluster, this can take several minutes.

        Parameters
        ----------
        filename: str, default "dask-cluster-dump"
            Path or URL to write the dump to. If no extension is provided, the format
            (e.g., ".msgpack") will be appended.
        write_from_scheduler: bool, optional
            If True, write the dump file from the scheduler process. If False (default),
            write from the client process. Writing from the scheduler may be necessary
            if the client does not have write access to the target location or if the
            dump is too large to fit in the client's memory.
        exclude: Collection[str], optional
            A collection of top-level keys to exclude from the dump (e.g., {"tasks"}).
        format: "msgpack" or "yaml", default "msgpack"
            The format to use for the dump file.
        **storage_options:
            Additional keyword arguments passed to the backend file system implementation
            (e.g., `host`, `port` for HDFS).

        Returns
        -------
        Future | None
            If `write_from_scheduler` is True, returns a Future that completes when the
            dump is finished. Otherwise, returns None.

        See Also
        --------
        distributed.diagnostics.cluster_dump.ClusterDump
        """
        from .diagnostics.cluster_dump import dump_cluster_state

        if self.asynchronous:
            return dump_cluster_state(
                client=self,
                filename=filename,
                write_from_scheduler=write_from_scheduler,
                exclude=exclude,
                format=format,
                **storage_options,
            )
        else:
            return self.sync(
                dump_cluster_state,
                client=self,
                filename=filename,
                write_from_scheduler=write_from_scheduler,
                exclude=exclude,
                format=format,
                **storage_options,
            )

    ###################
    # Close Methods #
    ###################

    def close(
        self, timeout: float | str | None = config.timeouts["closing"], **kwargs: Any
    ) -> None:
        """Close the client connection

        Parameters
        ----------
        timeout : number, optional
            Time in seconds to wait for the client to close gracefully.
        **kwargs : dict
            Optional keyword arguments for the remote function.

        """
        if self.asynchronous:
            # Trigger close, but don't wait for it
            if self._status != Status.closing and self._status != Status.closed:
                # Use ensure_future to avoid blocking the event loop
                asyncio_ensure_future(self._close(timeout=timeout, **kwargs))
        else:
            # Synchronous close
            if self._status != Status.closing and self._status != Status.closed:
                # Ensure the IO loop is running
                if not self._loop_runner.is_runnning:
                    # Cannot close if loop is not running
                    logger.warning("Cannot close client cleanly, event loop stopped.")
                    return

                try:
                    self.sync(self._close, timeout=timeout, **kwargs)
                except RuntimeError as e:
                    if "cannot schedule new futures after shutdown" in str(e):
                        # Loop already closed, cannot close cleanly
                        pass
                    else:
                        raise
                except TimeoutError:
                    logger.warning("Client close timed out.")
                except Exception:
                    logger.exception("Error during client close:")

    async def _close(self, timeout=None, fast=False):
        """Asynchronous version of close"""
        if self._status in (Status.closing, Status.closed):
            # Already closing or closed
            if self._close_future:
                await self._close_future
            return

        logger.info("Closing Dask client: %s", self.id)
        self._status = Status.closing
        self._close_future = asyncio.Future()

        # Stop background tasks
        if self._handle_report_task:
            self._handle_report_task.cancel()
        if self._handle_stream_task:
            self._handle_stream_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._wait_for_workers_task:
            self._wait_for_workers_task.cancel()

        # Clean up periodic callbacks
        for pc in self._periodic_callbacks.values():
            pc.stop()

        # Remove from global list
        cls = type(self)
        with cls._global_clients_lock:
            while self in cls._running_clients_on_this_process:
                cls._running_clients_on_this_process.remove(self)
            if self.id in cls._instances:
                del cls._instances[self.id]
            key = tokenize(self._start_kwargs)
            if key in cls._active_clients and cls._active_clients[key] is self:
                del cls._active_clients[key]
        if _get_global_client() is self:
            _set_global_client(None)

        # Close scheduler connection
        if self._scheduler_conn and not self._scheduler_conn.closed():
            try:
                if not fast:
                    await self._scheduler.unregister_client(client=self.id)
                await self._scheduler_conn.close()
            except CommClosedError:
                pass
            except Exception:
                logger.exception("Error during scheduler connection close:")

        # Close cluster if owned
        if self.cluster and self._shutdown_on_close:
            try:
                await self.cluster.close(timeout=timeout)
            except Exception:
                logger.exception("Error during cluster close:")

        # Shutdown executor if owned
        if self._executor:
            self._executor.shutdown()

        # Mark as closed
        self._status = Status.closed
        if self._close_future:
            self._close_future.set_result(None)
        self._closed = True
        logger.info("Finished closing Dask client: %s", self.id)

    def __del__(self):
        if self._status != Status.closed:
            self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    async def __aenter__(self):
        await self._started
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._close()

    # Configuration methods
    def get_config(self, **kwargs):
        """Get configuration settings from the scheduler"""
        if self.asynchronous:
            return self._get_config(**kwargs)
        else:
            return self.sync(self._get_config, **kwargs)

    async def _get_config(self, **kwargs):
        """Asynchronous version of get_config"""
        response = await self._send_to_scheduler({"op": "get_config", **kwargs})
        return response

    def set_config(self, config: dict, **kwargs):
        """Set configuration settings on the scheduler and workers"""
        if self.asynchronous:
            return self._set_config(config, **kwargs)
        else:
            self.sync(self._set_config, config, **kwargs)

    async def _set_config(self, config: dict, **kwargs):
        """Asynchronous version of set_config"""
        await self._send_to_scheduler({"op": "set_config", "config": config, **kwargs})

    # Utility methods
    def _get_computation_keys(self, computation: Any) -> list[Hashable]:
        """Get the keys associated with a computation"""
        if isinstance(computation, Future):
            return [computation.key]
        elif isinstance(computation, (list, tuple)):
            return list(
                itertools.chain.from_iterable(
                    self._get_computation_keys(c) for c in computation
                )
            )
        elif isinstance(computation, dict):
            return list(
                itertools.chain.from_iterable(
                    self._get_computation_keys(c) for c in computation.values()
                )
            )
        else:
            return []

    def _is_local(self) -> bool:
        """Check if the client is connected to a local cluster"""
        return isinstance(self.cluster, LocalCluster)

    # Internal Future management
    def _inc_client_ref(self, key: Hashable) -> None:
        """Increment client-side reference count for a key"""
        self._refcount[key] += 1

    def _dec_client_ref(self, key: Hashable) -> None:
        """Decrement client-side reference count for a key"""
        if key not in self._refcount:
            return
        self._refcount[key] -= 1
        if self._refcount[key] == 0:
            del self._refcount[key]
            self._release_key(key)

    def _release_key(self, key: Hashable) -> None:
        """Notify scheduler that client no longer needs a key"""
        logger.debug("Client releases key %s", key)
        self._send_to_scheduler_safe(
            {"op": "client-releases-keys", "keys": [to_serialize(key)], "client": self.id}
        )

    def _maybe_remove_future_later(self, key: Hashable, state_ref: weakref.ReferenceType) -> None:
        """Schedule removal of future state if no longer referenced"""
        self.loop.call_later(0.1, self._check_remove_future, key, state_ref)

    def _check_remove_future(self, key: Hashable, state_ref: weakref.ReferenceType) -> None:
        """Remove future state if no longer referenced"""
        if key in self.futures and self.futures[key] is state_ref():
            # Future state still exists and matches the original reference
            # Check if it's still referenced by the client or other futures
            if key not in self._refcount:
                # No longer referenced, remove it
                del self.futures[key]

    # Deprecated aliases
    @property
    def datasets(self):
        warnings.warn(
            "The `Client.datasets` attribute is deprecated. "
            "Use `Client.list_datasets`, `Client.get_dataset`, etc. instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._datasets

    @property
    def _loop(self):
        warnings.warn(
            "The `Client._loop` attribute is deprecated. Use `Client.loop` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.loop

    # Class-level state (for tracking instances)
    _running_clients_on_this_process: ClassVar[list[Client]] = []
    _active_clients: ClassVar[dict[Hashable, Client]] = {}


# Define ClientClosedError after Client class definition
class ClientClosedError(RuntimeError):
    """Raised when trying to use a closed client"""

    pass


# Helper function for creating default client
def default_client(c=None):
    """Return the default client"""
    if c:
        return c
    else:
        return Client.current()


# Helper function for synchronous execution
def sync(loop, func, *args, callback_timeout=None, **kwargs):
    """Run coroutine in loop running in separate thread.
    We use this to run loop.sync() from within the loop thread"""
    # TODO: remove this function when Tornado support is removed
    e = threading.Event()
    result = [None]
    error = [False]

    async def _sync_run():
        try:
            if is_coroutine_function(func):
                res = await func(*args, **kwargs)
            else:
                res = func(*args, **kwargs)
            result[0] = res
        except Exception:
            error[0] = sys.exc_info()
        finally:
            e.set()

    # Ensure the loop is running
    if not loop.asyncio_loop.is_running():
        raise RuntimeError("Event loop is not running.")

    # Schedule the coroutine
    asyncio.run_coroutine_threadsafe(_sync_run(), loop.asyncio_loop)

    # Wait for completion
    if callback_timeout is not None:
        timeout = parse_timedelta(callback_timeout, "s")
        if not e.wait(timeout):
            raise TimeoutError("timed out waiting for callback")
    else:
        e.wait()

    # Raise errors if any
    if error[0]:
        typ, exc, tb = error[0]
        raise exc.with_traceback(tb)
    else:
        return result[0]


# Patch Dask configuration with client-specific settings
dask.config.update(
    dask.config.config,
    {
        "get": Client.get,
        "compute": Client.compute,
        "persist": Client.persist,
        "futures.Future": Future,
        "futures.Client": Client,
        "distributed.Client": Client,
        "distributed.Future": Future,
    },
    priority=10,
)
