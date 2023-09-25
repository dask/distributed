from __future__ import annotations

import asyncio
import atexit
import copy
import inspect
import json
import logging
import os
import pickle
import re
import sys
import threading
import traceback
import uuid
import warnings
import weakref
from collections import defaultdict
from collections.abc import Collection, Coroutine, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import DoneAndNotDoneFutures
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from functools import partial, singledispatchmethod
from importlib.metadata import PackageNotFoundError, version
from numbers import Number
from queue import Queue as pyQueue
from typing import Any, Callable, ClassVar, Literal, NamedTuple, TypedDict, cast

from packaging.version import parse as parse_version
from tlz import first, groupby, merge, partition_all, valmap

import dask
from dask.base import collections_to_dsk, normalize_token, tokenize
from dask.core import flatten, validate_key
from dask.highlevelgraph import HighLevelGraph
from dask.optimization import SubgraphCallable
from dask.utils import (
    apply,
    ensure_dict,
    format_bytes,
    funcname,
    parse_timedelta,
    shorten_traceback,
    typename,
)
from dask.widgets import get_template

from distributed.core import ErrorMessage, OKMessage
from distributed.protocol.serialize import _is_dumpable
from distributed.utils import Deadline, wait_for

try:
    from dask.delayed import single_key
except ImportError:
    single_key = first
from tornado import gen
from tornado.ioloop import IOLoop

import distributed.utils
from distributed import cluster_dump, preloading
from distributed import versions as version_module
from distributed.batched import BatchedSend
from distributed.cfexecutor import ClientExecutor
from distributed.compatibility import PeriodicCallback
from distributed.core import (
    CommClosedError,
    ConnectionPool,
    PooledRPCCall,
    Status,
    clean_exception,
    connect,
    rpc,
)
from distributed.diagnostics.plugin import (
    ForwardLoggingPlugin,
    NannyPlugin,
    SchedulerPlugin,
    SchedulerUploadFile,
    UploadFile,
    WorkerPlugin,
    _get_plugin_name,
)
from distributed.metrics import time
from distributed.objects import HasWhat, SchedulerInfo, WhoHas
from distributed.protocol import to_serialize
from distributed.protocol.pickle import dumps, loads
from distributed.publish import Datasets
from distributed.pubsub import PubSubClientExtension
from distributed.security import Security
from distributed.sizeof import sizeof
from distributed.threadpoolexecutor import rejoin
from distributed.utils import (
    CancelledError,
    LoopRunner,
    NoOpAwaitable,
    SyncMethodMixin,
    TimeoutError,
    format_dashboard_link,
    has_keyword,
    import_term,
    is_python_shutting_down,
    log_errors,
    no_default,
    sync,
    thread_state,
)
from distributed.utils_comm import (
    WrappedKey,
    gather_from_workers,
    pack_data,
    retry_operation,
    scatter_to_workers,
    unpack_remotedata,
)

logger = logging.getLogger(__name__)

class SourceCode(NamedTuple):
    code: str
    lineno_frame: int
    lineno_relative: int
    filename: str

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


def _handle_print(event):
    _, msg = event
    if not isinstance(msg, dict):
        # someone must have manually logged a print event with a hand-crafted
        # payload, rather than by calling worker.print(). In that case simply
        # print the payload and hope it works.
        print(msg)
        return

    args = msg.get("args")
    if not isinstance(args, tuple):
        # worker.print() will always send us a tuple of args, even if it's an
        # empty tuple.
        raise TypeError(
            f"_handle_print: client received non-tuple print args: {args!r}"
        )

    file = msg.get("file")
    if file == 1:
        file = sys.stdout
    elif file == 2:
        file = sys.stderr
    elif file is not None:
        raise TypeError(
            f"_handle_print: client received unsupported file kwarg: {file!r}"
        )

    print(
        *args, sep=msg.get("sep"), end=msg.get("end"), file=file, flush=msg.get("flush")
    )


def _handle_warn(event):
    _, msg = event
    if not isinstance(msg, dict):
        # someone must have manually logged a warn event with a hand-crafted
        # payload, rather than by calling worker.warn(). In that case simply
        # warn the payload and hope it works.
        warnings.warn(msg)
    else:
        if "message" not in msg:
            # TypeError makes sense here because it's analogous to calling a
            # function without a required positional argument
            raise TypeError(
                "_handle_warn: client received a warn event missing the required "
                '"message" argument.'
            )
        if "category" in msg:
            category = pickle.loads(msg["category"])
        else:
            category = None
        warnings.warn(
            pickle.loads(msg["message"]),
            category=category,
        )


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


class VersionsDict(TypedDict):
    scheduler: dict[str, dict[str, Any]]
    workers: dict[str, dict[str, dict[str, Any]]]
    client: dict[str, dict[str, Any]]


class Bridge(SyncMethodMixin):
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
    timeout: int (defaults to configuration ``distributed.comm.timeouts.connect``)
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

    preloads: preloading.PreloadManager
    __loop: IOLoop | None = None

    def __init__(
        self,
        workers=[],
        loop=None,
        scheduler_file=None,
        security=None,
        asynchronous=False,
        name=None,
        timeout=no_default,
        serializers=None,
        deserializers=None,
        direct_to_workers=True,
        **kwargs,
    ):
        if timeout == no_default:
            timeout = dask.config.get("distributed.comm.timeouts.connect")
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")
        self._timeout = timeout

        self._pending_msg_buffer = []
        self.extensions = {}
        self.scheduler_file = scheduler_file
        self._startup_kwargs = kwargs
        self.workers = workers
        self.datasets = Datasets(self)
        self._serializers = serializers
        if deserializers is None:
            deserializers = serializers
        self._deserializers = deserializers
        self.direct_to_workers = direct_to_workers
        self._previous_as_current = None

        self.name = "bridge" if name is None else name

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


        self._stream_handlers = {}

        self._state_handlers = {}

        self.rpc = ConnectionPool(
            serializers=serializers,
            deserializers=deserializers,
            deserialize=True,
            connection_args=self.connection_args,
            timeout=timeout,
            server=self,
        )
        self.start(timeout=timeout)
        Bridge._instances.add(self)


    @property
    def io_loop(self) -> IOLoop | None:
        warnings.warn(
            "The io_loop property is deprecated", DeprecationWarning, stacklevel=2
        )
        return self.loop

    @io_loop.setter
    def io_loop(self, value: IOLoop) -> None:
        warnings.warn(
            "The io_loop property is deprecated", DeprecationWarning, stacklevel=2
        )
        self.loop = value

    @property
    def loop(self) -> IOLoop | None:
        loop = self.__loop
        if loop is None:
            # If the loop is not running when this is called, the LoopRunner.loop
            # property will raise a DeprecationWarning
            # However subsequent calls might occur - eg atexit, where a stopped
            # loop is still acceptable - so we cache access to the loop.
            self.__loop = loop = self._loop_runner.loop
        return loop

    @loop.setter
    def loop(self, value: IOLoop) -> None:
        warnings.warn(
            "setting the loop property is deprecated", DeprecationWarning, stacklevel=2
        )
        self.__loop = value

    def _get_scheduler_info(self):
        from distributed.scheduler import Scheduler

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

 
    def start(self, **kwargs):
        """Start scheduler running in separate thread"""
        self._loop_runner.start()

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

    async def _start(self, timeout=no_default, **kwargs):
        self.status = "connecting"
        await self.rpc.start()
        return self

    def __del__(self):
        # If the loop never got assigned, we failed early in the constructor,
        # nothing to do
        if self.__loop is not None:
            self.close()

    async def _gather(self, keys,  errors="raise"):
        data = {}
        results = await self._gather_remote(keys)
        if results["status"] !=  "OK":
            raise TypeError(
                    "Couldn't gather data from remote worker"
                )
        return results["data"]

    async def _gather_remote(self, keys: list) -> dict[str, Any]:
        """Perform gather with workers or scheduler

        This method exists to limit and batch many concurrent gathers into a
        few.  In controls access using a Tornado semaphore, and picks up keys
        from other requests made recently.
        """
        async with self._gather_semaphore:
            # gather directly from workers
            who_has =  {k: self.workers  for k in keys} # for now 
            data, missing_keys, failed_keys, _ = await gather_from_workers(
                who_has, rpc=self.rpc
            )
            response: dict[str, Any] = {"status": "OK", "data": data}
            return response

    def gather(self, keys, errors="raise",asynchronous=None):
        """Gather data associated with keys from distributed memory
        """
        if isinstance(keys, pyQueue):
            raise TypeError(
                "Dask no longer supports gathering over Iterators and Queues. "
                "Consider using a normal for loop and Client.submit/gather"
            )

        if isinstance(keys, Iterator):
            return (self.gather(f, errors=errors, direct=direct) for f in futures)

        with shorten_traceback():
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
        keys=None,
        workers=None,
        broadcast=False,
        local_worker=None,
        timeout=no_default,
        hash=True,
        external=True,
    ):
        if timeout == no_default:
            timeout = self._timeout
        if isinstance(workers, (str, Number)):
            workers = [workers]

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
            if keys:
                names=[k for k in keys]
            else:
                if hash:
                    names = [type(x).__name__ + "-" + tokenize(x) for x in data]
                else:
                    names = [type(x).__name__ + "-" + uuid.uuid4().hex for x in data]
            data = dict(zip(names, data))

        assert isinstance(data, dict)

        types = valmap(type, data)

        if local_worker:  # running within task
            local_worker.update_data(data=data)
        else:
            data2 = valmap(to_serialize, data)
            nthreads = {w: 1 for w in workers} # for now
            _, who_has, nbytes = await scatter_to_workers(
                nthreads, data2, rpc=self.rpc, external=external,
            )
        if broadcast:
            n = None if broadcast is True else broadcast
            await self._replicate(list(out.values()), workers=workers, n=n)

        return "OK"

    def scatter(
        self,
        data,
        keys=None,
        workers=None,
        broadcast=False,
        hash=True,
        timeout=no_default,
        asynchronous=None,
        external=True,

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
        keys: list, dict, or object
            keys of the data to scatter to the workers.
        workers : list of tuples (optional)
            Optionally constrain locations of data.
            Specify workers as hostname/port pairs, e.g.
            ``('127.0.0.1', 8787)``.
        broadcast : bool (defaults to False)
            Whether to send each data element to all workers.
            By default we round-robin based on number of cores.

            .. note::
               Setting this flag to True is incompatible with the Active Memory
               Manager's :ref:`ReduceReplicas` policy. If you wish to use it, you must
               first disable the policy or disable the AMM entirely.
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
        external: bool 
            If True the data has been generated from an external application

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

        Notes
        -----
        Scattering a dictionary uses ``dict`` keys to create ``Future`` keys.
        The current implementation of a task graph resolution searches for occurrences of ``key``
        and replaces it with a corresponding ``Future`` result. That can lead to unwanted
        substitution of strings passed as arguments to a task if these strings match some ``key``
        that already exists on a cluster. To avoid these situations it is required to use unique
        values if a ``key`` is set manually.
        See https://github.com/dask/dask/issues/9969 to track progress on resolving this issue.

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
        return self.sync(
            self._scatter,
            data,
            keys=keys,
            workers=workers,
            broadcast=broadcast,
            timeout=timeout,
            asynchronous=asynchronous,
            hash=hash,
            external=external,
        )
