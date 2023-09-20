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
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import DoneAndNotDoneFutures
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from functools import partial
from numbers import Number
from queue import Queue as pyQueue
from typing import TYPE_CHECKING, Awaitable, ClassVar, Sequence

from tlz import first, groupby, keymap, merge, partition_all, valmap

if TYPE_CHECKING:
    from typing_extensions import Literal

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

from . import versions as version_module  # type: ignore
from .batched import BatchedSend
from .cfexecutor import ClientExecutor
from .core import (
    CommClosedError,
    ConnectionPool,
    PooledRPCCall,
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

from .client import Future 

logger = logging.getLogger(__name__)

def done_callback(future, callback):
    """Coroutine that waits on future, then calls callback"""
    while future.status == "pending":
        future._state.wait()
    callback(future)


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

class Bridge(SyncMethodMixin):
    _instances: ClassVar[weakref.WeakSet[Bridge]] = weakref.WeakSet()
    _default_event_handlers = {"print": _handle_print, "warn": _handle_warn}

    def __init__(
        self,
        address=None,
        workers=[], 
        loop=None,
        timeout=no_default,
        security=None,
        asynchronous=False,
        name=None,
        serializers=None,
        deserializers=None,
        connection_limit=512,

        **kwargs,
    ):
        if timeout == no_default:
            timeout = dask.config.get("distributed.comm.timeouts.connect")
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")
        self._timeout = timeout

        self.refcount = defaultdict(lambda: 0)
        self.coroutines = []
        if name is None:
            name = dask.config.get("bridge-name", None)
        self.id = (
            type(self).__name__
            + ("-" + name + "-" if name else "-")
            + str(uuid.uuid1(clock_seq=os.getpid()))
        )
        self.generation = 0
        self.status = "newly-created"
        # A reentrant-lock on the refcounts for futures associated with this
        # bridge. Should be held by individual operations modifying refcounts,
        # or any bulk operation that needs to ensure the set of futures doesn't
        # change during operation.
        self._refcount_lock = threading.RLock()
        self.datasets = Datasets(self)
        self._serializers = serializers
        if deserializers is None:
            deserializers = serializers
        self._deserializers = deserializers

        if address is None:
            address = dask.config.get("scheduler-address", None)
            if address:
                logger.info("Config value `scheduler-address` found: %s", address)

        if address is not None and kwargs:
            raise ValueError(f"Unexpected keyword arguments: {sorted(kwargs)}")

        if security is None:
            security = Security()
        elif isinstance(security, dict):
            security = Security(**security)
        elif security is True:
            security = Security.temporary()
            self._startup_kwargs["security"] = security
        elif not isinstance(security, Security):
            raise TypeError("security must be a Security object")

        self.security = security

        if name == "worker":
            self.connection_args = self.security.get_connection_args("worker")
        else:
            self.connection_args = self.security.get_connection_args("client")

        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.io_loop = self.loop = self._loop_runner.loop

        self._gather_keys = None
        self._gather_future = None

        self._event_handlers = {}

        self.rpc = ConnectionPool(
            limit=connection_limit,
            serializers=serializers,
            deserializers=deserializers,
            deserialize=True,
            connection_args=self.connection_args,
            timeout=timeout,
            server=None,
        )
        self.workers = workers
        self.start()
        Bridge._instances.add(self)

    async def _start(self):
        self.status = "connecting"
        self._gather_semaphore = asyncio.Semaphore(5)
        await self.rpc.start()
        return self

    def start(self, **kwargs):
        """Start scheduler running in separate thread"""
        if self.status != "newly-created":
            return
        self._loop_runner.start()

        if self.asynchronous:
            self._started = asyncio.ensure_future(self._start(**kwargs))
        else:
            sync(self.loop, self._start, **kwargs)

    async def _send(
        self,
        data,
        workers=None,
        broadcast=False,
        local_worker=None,
        timeout=no_default,
        hash=True,
        keys=None,
        external=None
    ):
        if timeout == no_default:
            timeout = self._timeout
        if isinstance(workers, (str, Number)):
            workers = [workers]
        if isinstance(data, dict) and not all(
            isinstance(k, (bytes, str)) for k in data
        ):
            d = await self._send(keymap(stringify, data), workers, broadcast)
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
            if keys==None:
                if hash:
                    names = [type(x).__name__ + "-" + tokenize(x) for x in data]
                else:
                    names = [type(x).__name__ + "-" + uuid.uuid4().hex for x in data]
            else:
                names=[stringify(k) for k in keys]
            data = dict(zip(names, data))
        assert isinstance(data, dict)

        types = valmap(type, data)

        if local_worker:  # running within task
            local_worker.update_data(data=data, report=False)
  
        else:
            data2 = valmap(to_serialize, data)
            nthreads = {w: 1 for w in workers}

            _, who_has, nbytes = await scatter_to_workers(
                nthreads, data2, rpc=self.rpc, 
                external=True, report=True,
            )
        return "OK"

    def send(
        self,
        data,
        workers=None,
        broadcast=False,
        hash=True,
        timeout=no_default,
        asynchronous=None,
        keys=None,
    ):
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
            self._send,
            data,
            workers=workers,
            broadcast=broadcast,
            local_worker=local_worker,
            timeout=timeout,
            asynchronous=asynchronous,
            hash=hash,
            keys=keys,
        )

    async def _gather(self, keys, errors="raise", local_worker=None):
        keys = [stringify(key) for key in keys]
        results = await self._gather_remote(keys)
        if results["status"] !=  "OK":
            raise TypeError(
                    "Couldn't get data"
                )
        return results["data"]


    async def _gather_remote(self, keys):
        """Perform gather with workers or scheduler
        This method exists to limit and batch many concurrent gathers into a
        few.  In controls access using a Tornado semaphore, and picks up keys
        from other requests made recently.
        """
        async with self._gather_semaphore:
            who_has =  {k: self.workers  for k in keys}
            data2, missing_keys, missing_workers = await gather_from_workers(
                who_has, rpc=self.rpc, close=False
            )
            response = {"status": "OK", "data": data2}
        return response

    def gather(self, keys, errors="raise", asynchronous=None):
        if hasattr(thread_state, "execution_state"):  # within worker task
            local_worker = thread_state.execution_state["worker"]
        else:
            local_worker = None
        return self.sync(
            self._gather,
            keys,
            errors=errors,
            local_worker=local_worker,
            asynchronous=asynchronous,
        )