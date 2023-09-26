from __future__ import annotations

import asyncio
import logging
import pickle
import sys
import uuid
import warnings
import weakref
from collections.abc import Iterator
from numbers import Number
from queue import Queue as pyQueue
from typing import Any, ClassVar

from tlz import first, valmap

import dask
from dask.base import tokenize
from dask.utils import parse_timedelta, shorten_traceback

from distributed.protocol import to_serialize

try:
    from dask.delayed import single_key
except ImportError:
    single_key = first

from tornado.ioloop import IOLoop

from distributed import preloading
from distributed.core import ConnectionPool
from distributed.security import Security
from distributed.utils import LoopRunner, SyncMethodMixin, no_default, sync
from distributed.utils_comm import gather_from_workers, scatter_to_workers

logger = logging.getLogger(__name__)

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


class Bridge(SyncMethodMixin):
    """Connect to and scatter data to a worker"""

    _instances: ClassVar[weakref.WeakSet[Bridge]] = weakref.WeakSet()

    _default_event_handlers = {"print": _handle_print, "warn": _handle_warn}

    preloads: preloading.PreloadManager
    __loop: IOLoop | None = None

    def __init__(
        self,
        workers=None,
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

    async def _gather(self, keys, workers, errors="raise"):
        results = await self._gather_remote(keys, workers)
        if results["status"] != "OK":
            raise TypeError("Couldn't gather data from remote worker")
        return results["data"]

    async def _gather_remote(self, keys: list, workers: list) -> dict[str, Any]:
        """Perform gather with workers

        This method exists to limit and batch many concurrent gathers into a
        few.  In controls access using a Tornado semaphore, and picks up keys
        from other requests made recently.
        """
        async with self._gather_semaphore:
            # gather directly from workers
            who_has = {k: workers for k in keys}  # for now
            data, missing_keys, failed_keys, _ = await gather_from_workers(
                who_has, rpc=self.rpc
            )
            response: dict[str, Any] = {"status": "OK", "data": data}
            return response

    def gather(self, keys, workers=None, errors="raise", asynchronous=None):
        """Gather data associated with keys from distributed memory"""
        if isinstance(keys, pyQueue):
            raise TypeError(
                "Dask no longer supports gathering over Iterators and Queues. "
            )
        if workers is None:
            workers = self.workers

        assert isinstance(workers, list)
        if isinstance(keys, Iterator):
            return (
                self.gather(k, w, errors=errors) for k, [w] in dict(zip(keys, workers))
            )

        with shorten_traceback():
            return self.sync(
                self._gather,
                keys,
                workers,
                errors=errors,
                asynchronous=asynchronous,
            )

    async def _scatter(
        self,
        data,
        keys=None,
        workers=None,
        local_worker=None,
        timeout=no_default,
        hash=True,
        external=True,
    ):
        if timeout == no_default:
            timeout = self._timeout

        if workers is None:
            workers = self.workers

        if isinstance(workers, (str, Number)):
            workers = [workers]

        assert isinstance(workers, list)

        if isinstance(data, type(range(0))):
            data = list(data)
        names = False
        if isinstance(data, Iterator):
            data = list(data)
        if isinstance(data, (set, frozenset)):
            data = list(data)
        if not isinstance(data, (dict, list, tuple, set, frozenset)):
            data = [data]
        if isinstance(data, (list, tuple)):
            if keys:
                names = [k for k in keys]
            else:
                if hash:
                    names = [type(x).__name__ + "-" + tokenize(x) for x in data]
                else:
                    names = [type(x).__name__ + "-" + uuid.uuid4().hex for x in data]
            data = dict(zip(names, data))

        assert isinstance(data, dict)

        if local_worker:  # running within task
            local_worker.update_data(data=data)
        else:
            data2 = valmap(to_serialize, data)
            nthreads = {w: 1 for w in workers}  # for now
            _, who_has, nbytes = await scatter_to_workers(
                nthreads,
                data2,
                rpc=self.rpc,
                external=external,
            )
        return "OK"

    def scatter(
        self,
        data,
        keys=None,
        workers=None,
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
            raise TypeError("Dask no longer supports mapping over Iterators or Queues.")
        return self.sync(
            self._scatter,
            data,
            keys=keys,
            workers=workers,
            timeout=timeout,
            asynchronous=asynchronous,
            hash=hash,
            external=external,
        )
