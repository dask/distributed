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
    """Connect to and scatter data to a worker
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

        self.name = "bridge" if name is None else name
        self._gather_semaphore = asyncio.Semaphore(5)
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
            print("data, missing_keys, failed_keys" ,data, missing_keys, failed_keys  , flush=True)
            return response

    def gather(self, keys, errors="raise", asynchronous=None):
        """Gather data associated with keys from distributed memory
        """
        if isinstance(keys, pyQueue):
            raise TypeError(
                "Dask no longer supports gathering over Iterators and Queues. "
                "Consider using a normal for loop and Client.submit/gather"
            )

        if isinstance(keys, Iterator):
            return (self.gather(k, errors=errors) for k in keys)

        with shorten_traceback():
            return self.sync(
                self._gather,
                keys,
                errors=errors,
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
