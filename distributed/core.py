from __future__ import annotations

import asyncio
import inspect
import logging
import sys
import threading
import traceback
import types
import uuid
import warnings
import weakref
from collections import defaultdict
from collections.abc import Container
from contextlib import suppress
from enum import Enum
from functools import partial
from typing import Callable, ClassVar, TypedDict, TypeVar

import tblib
from tlz import merge
from tornado import gen
from tornado.ioloop import IOLoop, PeriodicCallback

import dask
from dask.utils import parse_timedelta

from distributed import profile, protocol
from distributed.comm import (
    Comm,
    CommClosedError,
    connect,
    get_address_host_port,
    listen,
    normalize_address,
    unparse_host_port,
)
from distributed.metrics import time
from distributed.system_monitor import SystemMonitor
from distributed.utils import (
    get_traceback,
    has_keyword,
    is_coroutine_function,
    recursive_to_dict,
    truncate_exception,
)


class Status(Enum):
    """
    This Enum contains the various states a cluster, worker, scheduler and nanny can be
    in. Some of the status can only be observed in one of cluster, nanny, scheduler or
    worker but we put them in the same Enum as they are compared with each
    other.
    """

    undefined = "undefined"
    created = "created"
    init = "init"
    starting = "starting"
    running = "running"
    paused = "paused"
    stopping = "stopping"
    stopped = "stopped"
    closing = "closing"
    closing_gracefully = "closing_gracefully"
    closed = "closed"
    failed = "failed"
    dont_reply = "dont_reply"


Status.lookup = {s.name: s for s in Status}  # type: ignore


class RPCClosed(IOError):
    pass


logger = logging.getLogger(__name__)


def raise_later(exc):
    def _raise(*args, **kwargs):
        raise exc

    return _raise


tick_maximum_delay = parse_timedelta(
    dask.config.get("distributed.admin.tick.limit"), default="ms"
)

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")


def _expects_comm(func: Callable) -> bool:
    sig = inspect.signature(func)
    params = list(sig.parameters)
    if params and params[0] == "comm":
        return True
    if params and params[0] == "stream":
        warnings.warn(
            "Calling the first arugment of a RPC handler `stream` is "
            "deprecated. Defining this argument is optional. Either remove the "
            f"arugment or rename it to `comm` in {func}.",
            FutureWarning,
        )
        return True
    return False


class Server:
    """Dask Distributed Server

    Superclass for endpoints in a distributed cluster, such as Worker
    and Scheduler objects.

    **Handlers**

    Servers define operations with a ``handlers`` dict mapping operation names
    to functions.  The first argument of a handler function will be a ``Comm``
    for the communication established with the client.  Other arguments
    will receive inputs from the keys of the incoming message which will
    always be a dictionary.

    >>> def pingpong(comm):
    ...     return b'pong'

    >>> def add(comm, x, y):
    ...     return x + y

    >>> handlers = {'ping': pingpong, 'add': add}
    >>> server = Server(handlers)  # doctest: +SKIP
    >>> server.listen('tcp://0.0.0.0:8000')  # doctest: +SKIP

    **Message Format**

    The server expects messages to be dictionaries with a special key, `'op'`
    that corresponds to the name of the operation, and other key-value pairs as
    required by the function.

    So in the example above the following would be good messages.

    *  ``{'op': 'ping'}``
    *  ``{'op': 'add', 'x': 10, 'y': 20}``

    """

    default_ip = ""
    default_port = 0

    def __init__(
        self,
        handlers,
        blocked_handlers=None,
        stream_handlers=None,
        connection_limit=512,
        deserialize=True,
        serializers=None,
        deserializers=None,
        connection_args=None,
        timeout=None,
        io_loop=None,
    ):
        self._status = Status.init
        self.handlers = {
            "identity": self.identity,
            "echo": self.echo,
            "connection_stream": self.handle_stream,
            "dump_state": self._to_dict,
        }
        self.handlers.update(handlers)
        if blocked_handlers is None:
            blocked_handlers = dask.config.get(
                "distributed.%s.blocked-handlers" % type(self).__name__.lower(), []
            )
        self.blocked_handlers = blocked_handlers
        self.stream_handlers = {}
        self.stream_handlers.update(stream_handlers or {})

        self.id = type(self).__name__ + "-" + str(uuid.uuid4())
        self._address = None
        self._listen_address = None
        self._port = None
        self._comms = {}
        self.deserialize = deserialize
        self.monitor = SystemMonitor()
        self.counters = None
        self.digests = None
        self._ongoing_coroutines = set()
        self._event_finished = asyncio.Event()

        self.listeners = []
        self.io_loop = io_loop or IOLoop.current()
        self.loop = self.io_loop

        if not hasattr(self.io_loop, "profile"):
            ref = weakref.ref(self.io_loop)

            def stop() -> bool:
                loop = ref()
                return loop is None or loop.asyncio_loop.is_closed()

            self.io_loop.profile = profile.watch(
                omit=("profile.py", "selectors.py"),
                interval=dask.config.get("distributed.worker.profile.interval"),
                cycle=dask.config.get("distributed.worker.profile.cycle"),
                stop=stop,
            )

        # Statistics counters for various events
        with suppress(ImportError):
            from distributed.counter import Digest

            self.digests = defaultdict(partial(Digest, loop=self.io_loop))

        from distributed.counter import Counter

        self.counters = defaultdict(partial(Counter, loop=self.io_loop))

        self.periodic_callbacks = dict()

        pc = PeriodicCallback(
            self.monitor.update,
            parse_timedelta(
                dask.config.get("distributed.admin.system-monitor.interval")
            )
            * 1000,
        )
        self.periodic_callbacks["monitor"] = pc

        self._last_tick = time()
        self._tick_counter = 0
        self._tick_count = 0
        self._tick_count_last = time()
        self._tick_interval = parse_timedelta(
            dask.config.get("distributed.admin.tick.interval"), default="ms"
        )
        self._tick_interval_observed = self._tick_interval
        self.periodic_callbacks["tick"] = PeriodicCallback(
            self._measure_tick, self._tick_interval * 1000
        )
        self.periodic_callbacks["ticks"] = PeriodicCallback(
            self._cycle_ticks,
            parse_timedelta(dask.config.get("distributed.admin.tick.cycle")) * 1000,
        )

        self.thread_id = 0

        def set_thread_ident():
            self.thread_id = threading.get_ident()

        self.io_loop.add_callback(set_thread_ident)
        self._startup_lock = asyncio.Lock()
        self.__startup_exc: Exception | None = None
        self.__started = asyncio.Event()

        self.rpc = ConnectionPool(
            limit=connection_limit,
            deserialize=deserialize,
            serializers=serializers,
            deserializers=deserializers,
            connection_args=connection_args,
            timeout=timeout,
            server=self,
        )

        self.__stopped = False

    @property
    def status(self):
        try:
            return self._status
        except AttributeError:
            return Status.undefined

    @status.setter
    def status(self, new_status):
        if not isinstance(new_status, Status):
            raise TypeError(f"Expected Status; got {new_status!r}")
        self._status = new_status

    async def finished(self):
        """Wait until the server has finished"""
        await self._event_finished.wait()

    def __await__(self):
        return self.start().__await__()

    async def start_unsafe(self):
        """Attempt to start the server. This is not idempotent and not protected against concurrent startup attempts.

        This is intended to be overwritten or called by subclasses. For a safe
        startup, please use ``Server.start`` instead.

        If ``death_timeout`` is configured, we will require this coroutine to
        finish before this timeout is reached. If the timeout is reached we will
        close the instance and raise an ``asyncio.TimeoutError``
        """
        await self.rpc.start()
        return self

    async def start(self):
        async with self._startup_lock:
            if self.status == Status.failed:
                assert self.__startup_exc is not None
                raise self.__startup_exc
            elif self.status != Status.init:
                return self
            timeout = getattr(self, "death_timeout", None)

            async def _close_on_failure(exc: Exception):
                await self.close()
                self.status = Status.failed
                self.__startup_exc = exc

            try:
                await asyncio.wait_for(self.start_unsafe(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                await _close_on_failure(exc)
                raise asyncio.TimeoutError(
                    f"{type(self).__name__} start timed out after {timeout}s."
                ) from exc
            except Exception as exc:
                await _close_on_failure(exc)
                raise RuntimeError(f"{type(self).__name__} failed to start.") from exc
            self.status = Status.running
            self.__started.set()
        return self

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def start_periodic_callbacks(self):
        """Start Periodic Callbacks consistently

        This starts all PeriodicCallbacks stored in self.periodic_callbacks if
        they are not yet running.  It does this safely on the IOLoop.
        """
        self._last_tick = time()

        def start_pcs():
            for pc in self.periodic_callbacks.values():
                if not pc.is_running():
                    pc.start()

        self.io_loop.add_callback(start_pcs)

    def stop(self):
        if not self.__stopped:
            self.__stopped = True
            for listener in self.listeners:
                # Delay closing the server socket until the next IO loop tick.
                # Otherwise race conditions can appear if an event handler
                # for an accept() call is already scheduled by the IO loop,
                # raising EBADF.
                # The demonstrator for this is Worker.terminate(), which
                # closes the server socket in response to an incoming message.
                # See https://github.com/tornadoweb/tornado/issues/2069
                self.io_loop.add_callback(listener.stop)

    @property
    def listener(self):
        if self.listeners:
            return self.listeners[0]
        else:
            return None

    def _measure_tick(self):
        now = time()
        diff = now - self._last_tick
        self._last_tick = now
        self._tick_counter += 1
        if diff > tick_maximum_delay:
            logger.info(
                "Event loop was unresponsive in %s for %.2fs.  "
                "This is often caused by long-running GIL-holding "
                "functions or moving large chunks of data. "
                "This can cause timeouts and instability.",
                type(self).__name__,
                diff,
            )
        if self.digests is not None:
            self.digests["tick-duration"].add(diff)

    def _cycle_ticks(self):
        if not self._tick_counter:
            return
        last, self._tick_count_last = self._tick_count_last, time()
        count, self._tick_counter = self._tick_counter, 0
        self._tick_interval_observed = (time() - last) / (count or 1)

    @property
    def address(self) -> str:
        """
        The address this Server can be contacted on.
        If the server is not up, yet, this raises a ValueError.
        """
        if not self._address:
            if self.listener is None:
                raise ValueError("cannot get address of non-running Server")
            self._address = self.listener.contact_address
        return self._address

    @property
    def address_safe(self) -> str:
        """
        The address this Server can be contacted on.
        If the server is not up, yet, this returns a ``"not-running"``.
        """
        try:
            return self.address
        except ValueError:
            return "not-running"

    @property
    def listen_address(self):
        """
        The address this Server is listening on.  This may be a wildcard
        address such as `tcp://0.0.0.0:1234`.
        """
        if not self._listen_address:
            if self.listener is None:
                raise ValueError("cannot get listen address of non-running Server")
            self._listen_address = self.listener.listen_address
        return self._listen_address

    @property
    def port(self):
        """
        The port number this Server is listening on.

        This will raise ValueError if the Server is listening on a
        non-IP based protocol.
        """
        if not self._port:
            _, self._port = get_address_host_port(self.address)
        return self._port

    def identity(self) -> dict[str, str]:
        return {"type": type(self).__name__, "id": self.id}

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Server.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = self.identity()
        extra = {
            "address": self.address,
            "status": self.status.name,
            "thread_id": self.thread_id,
        }
        info.update(extra)
        info = {k: v for k, v in info.items() if k not in exclude}
        return recursive_to_dict(info, exclude=exclude)

    def echo(self, data=None):
        return data

    async def listen(self, port_or_addr=None, allow_offload=True, **kwargs):
        if port_or_addr is None:
            port_or_addr = self.default_port
        if isinstance(port_or_addr, int):
            addr = unparse_host_port(self.default_ip, port_or_addr)
        elif isinstance(port_or_addr, tuple):
            addr = unparse_host_port(*port_or_addr)
        else:
            addr = port_or_addr
            assert isinstance(addr, str)
        listener = await listen(
            addr,
            self.handle_comm,
            deserialize=self.deserialize,
            allow_offload=allow_offload,
            **kwargs,
        )
        self.listeners.append(listener)

    async def handle_comm(self, comm):
        """Dispatch new communications to coroutine-handlers

        Handlers is a dictionary mapping operation names to functions or
        coroutines.

            {'get_data': get_data,
             'ping': pingpong}

        Coroutines should expect a single Comm object.
        """
        if self.__stopped:
            comm.abort()
            return
        address = comm.peer_address
        op = None

        logger.debug("Connection from %r to %s", address, type(self).__name__)
        self._comms[comm] = op

        await self
        try:
            while not self.__stopped:
                try:
                    msg = await comm.read()
                    logger.debug("Message from %r: %s", address, msg)
                except OSError as e:
                    if not sys.is_finalizing():
                        logger.debug(
                            "Lost connection to %r while reading message: %s."
                            " Last operation: %s",
                            address,
                            e,
                            op,
                        )
                    break
                except Exception as e:
                    logger.exception("Exception while reading from %s", address)
                    if comm.closed():
                        raise
                    else:
                        await comm.write(error_message(e, status="uncaught-error"))
                        continue
                if not isinstance(msg, dict):
                    raise TypeError(
                        "Bad message type.  Expected dict, got\n  " + str(msg)
                    )

                try:
                    op = msg.pop("op")
                except KeyError as e:
                    raise ValueError(
                        "Received unexpected message without 'op' key: " + str(msg)
                    ) from e
                if self.counters is not None:
                    self.counters["op"].add(op)
                self._comms[comm] = op
                serializers = msg.pop("serializers", None)
                close_desired = msg.pop("close", False)
                reply = msg.pop("reply", True)
                if op == "close":
                    if reply:
                        await comm.write("OK")
                    break

                result = None
                try:
                    if op in self.blocked_handlers:
                        _msg = (
                            "The '{op}' handler has been explicitly disallowed "
                            "in {obj}, possibly due to security concerns."
                        )
                        exc = ValueError(_msg.format(op=op, obj=type(self).__name__))
                        handler = raise_later(exc)
                    else:
                        handler = self.handlers[op]
                except KeyError:
                    logger.warning(
                        "No handler %s found in %s",
                        op,
                        type(self).__name__,
                        exc_info=True,
                    )
                else:
                    if serializers is not None and has_keyword(handler, "serializers"):
                        msg["serializers"] = serializers  # add back in

                    logger.debug("Calling into handler %s", handler.__name__)
                    try:
                        if _expects_comm(handler):
                            result = handler(comm, **msg)
                        else:
                            result = handler(**msg)
                        if inspect.iscoroutine(result):
                            result = asyncio.create_task(
                                result, name=f"handle-comm-{address}-{op}"
                            )
                            self._ongoing_coroutines.add(result)
                            result.add_done_callback(self._ongoing_coroutines.remove)
                            result = await result
                        elif inspect.isawaitable(result):
                            raise RuntimeError(
                                f"Comm handler returned unknown awaitable. Expected coroutine, instead got {type(result)}"
                            )
                    except CommClosedError:
                        if self.status == Status.running:
                            logger.info("Lost connection to %r", address, exc_info=True)
                        break
                    except Exception as e:
                        logger.exception("Exception while handling op %s", op)
                        if comm.closed():
                            raise
                        else:
                            result = error_message(e, status="uncaught-error")

                if reply and result != Status.dont_reply:
                    try:
                        await comm.write(result, serializers=serializers)
                    except (OSError, TypeError) as e:
                        logger.debug(
                            "Lost connection to %r while sending result for op %r: %s",
                            address,
                            op,
                            e,
                        )
                        break

                self._comms[comm] = None
                msg = result = None
                if close_desired:
                    await comm.close()
                if comm.closed():
                    break

        finally:
            del self._comms[comm]
            if not sys.is_finalizing() and not comm.closed():
                try:
                    comm.abort()
                except Exception as e:
                    logger.error(
                        "Failed while closing connection to %r: %s", address, e
                    )

    async def handle_stream(self, comm, extra=None):
        extra = extra or {}
        logger.info("Starting established connection")

        closed = False
        try:
            while not closed:
                msgs = await comm.read()
                if not isinstance(msgs, (tuple, list)):
                    msgs = (msgs,)

                if not comm.closed():
                    for msg in msgs:
                        if msg == "OK":  # from close
                            break
                        op = msg.pop("op")
                        if op:
                            if op == "close-stream":
                                closed = True
                                break
                            handler = self.stream_handlers[op]
                            if is_coroutine_function(handler):
                                self.loop.add_callback(handler, **merge(extra, msg))
                                await gen.sleep(0)
                            else:
                                handler(**merge(extra, msg))
                        else:
                            logger.error("odd message %s", msg)
                    await asyncio.sleep(0)

        except OSError:
            pass
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        finally:
            await comm.close()
            assert comm.closed()

    async def close(self, timeout=None):
        for pc in self.periodic_callbacks.values():
            pc.stop()

        if not self.__stopped:
            self.__stopped = True
            _stops = set()
            for listener in self.listeners:
                future = listener.stop()
                if inspect.isawaitable(future):
                    _stops.add(future)
            await asyncio.gather(*_stops)

        def _ongoing_tasks():
            return (
                t for t in self._ongoing_coroutines if t is not asyncio.current_task()
            )

        # TODO: Deal with exceptions
        try:
            # Give the handlers a bit of time to finish gracefully
            await asyncio.wait_for(
                asyncio.gather(*_ongoing_tasks(), return_exceptions=True), 1
            )
        except asyncio.TimeoutError:
            # the timeout on gather should've cancelled all the tasks
            await asyncio.gather(*_ongoing_tasks(), return_exceptions=True)

        await self.rpc.close()
        await asyncio.gather(*[comm.close() for comm in list(self._comms)])
        self._event_finished.set()


def pingpong(comm):
    return b"pong"


async def send_recv(
    comm: Comm,
    *,
    reply: bool = True,
    serializers=None,
    deserializers=None,
    **kwargs,
):
    """Send and recv with a Comm.

    Keyword arguments turn into the message

    response = await send_recv(comm, op='ping', reply=True)
    """
    msg = kwargs
    msg["reply"] = reply
    please_close = kwargs.get("close", False)
    force_close = False
    if deserializers is None:
        deserializers = serializers
    if deserializers is not None:
        msg["serializers"] = deserializers

    try:
        await comm.write(msg, serializers=serializers, on_error="raise")
        if reply:
            response = await comm.read(deserializers=deserializers)
        else:
            response = None
    except (asyncio.TimeoutError, OSError):
        # On communication errors, we should simply close the communication
        # Note that OSError includes CommClosedError and socket timeouts
        force_close = True
        raise
    except asyncio.CancelledError:
        # Do not reuse the comm to prevent the next call of send_recv from receiving
        # data from this call and/or accidentally putting multiple waiters on read().
        # Note that this relies on all Comm implementations to allow a write() in the
        # middle of a read().
        please_close = True
        raise
    finally:
        if force_close:
            comm.abort()
        elif please_close:
            await comm.close()

    if isinstance(response, dict) and response.get("status") == "uncaught-error":
        if comm.deserialize:
            typ, exc, tb = clean_exception(**response)
            raise exc.with_traceback(tb)
        else:
            raise Exception(response["exception_text"])
    return response


def addr_from_args(addr=None, ip=None, port=None):
    if addr is None:
        addr = (ip, port)
    else:
        assert ip is None and port is None
    if isinstance(addr, tuple):
        addr = unparse_host_port(*addr)
    return normalize_address(addr)


class rpc:
    """Conveniently interact with a remote server

    >>> remote = rpc(address)  # doctest: +SKIP
    >>> response = await remote.add(x=10, y=20)  # doctest: +SKIP

    One rpc object can be reused for several interactions.
    Additionally, this object creates and destroys many comms as necessary
    and so is safe to use in multiple overlapping communications.

    When done, close comms explicitly.

    >>> remote.close_comms()  # doctest: +SKIP
    """

    active: ClassVar[weakref.WeakSet[rpc]] = weakref.WeakSet()
    comms = ()
    address = None

    def __init__(
        self,
        arg=None,
        comm=None,
        deserialize=True,
        timeout=None,
        connection_args=None,
        serializers=None,
        deserializers=None,
    ):
        self.comms = {}
        self.address = coerce_to_address(arg)
        self.timeout = timeout
        self.status = Status.running
        self.deserialize = deserialize
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers
        self.connection_args = connection_args or {}
        self._created = weakref.WeakSet()
        rpc.active.add(self)

    async def live_comm(self):
        """Get an open communication

        Some comms to the ip/port target may be in current use by other
        coroutines.  We track this with the `comms` dict

            :: {comm: True/False if open and ready for use}

        This function produces an open communication, either by taking one
        that we've already made or making a new one if they are all taken.
        This also removes comms that have been closed.

        When the caller is done with the stream they should set

            self.comms[comm] = True

        As is done in __getattr__ below.
        """
        if self.status == Status.closed:
            raise RPCClosed("RPC Closed")
        to_clear = set()
        open = False
        for comm, open in self.comms.items():
            if comm.closed():
                to_clear.add(comm)
            if open:
                break
        for s in to_clear:
            del self.comms[s]
        if not open or comm.closed():
            comm = await connect(
                self.address,
                self.timeout,
                deserialize=self.deserialize,
                **self.connection_args,
            )
            comm.name = "rpc"
        self.comms[comm] = False  # mark as taken
        return comm

    def close_comms(self):
        async def _close_comm(comm):
            # Make sure we tell the peer to close
            try:
                if not comm.closed():
                    await comm.write({"op": "close", "reply": False})
                    await comm.close()
            except OSError:
                comm.abort()

        tasks = []
        for comm in list(self.comms):
            if comm and not comm.closed():
                # IOLoop.current().add_callback(_close_comm, comm)
                task = asyncio.ensure_future(_close_comm(comm))
                tasks.append(task)
        for comm in list(self._created):
            if comm and not comm.closed():
                # IOLoop.current().add_callback(_close_comm, comm)
                task = asyncio.ensure_future(_close_comm(comm))
                tasks.append(task)

        self.comms.clear()
        return tasks

    def __getattr__(self, key):
        async def send_recv_from_rpc(**kwargs):
            if self.serializers is not None and kwargs.get("serializers") is None:
                kwargs["serializers"] = self.serializers
            if self.deserializers is not None and kwargs.get("deserializers") is None:
                kwargs["deserializers"] = self.deserializers
            comm = None
            try:
                comm = await self.live_comm()
                comm.name = "rpc." + key
                result = await send_recv(comm=comm, op=key, **kwargs)
            except (RPCClosed, CommClosedError) as e:
                if comm:
                    raise type(e)(
                        f"Exception while trying to call remote method {key!r} before comm was established."
                    ) from e
                else:
                    raise type(e)(
                        f"Exception while trying to call remote method {key!r} using comm {comm!r}."
                    ) from e

            self.comms[comm] = True  # mark as open
            return result

        return send_recv_from_rpc

    async def close_rpc(self):
        if self.status != Status.closed:
            rpc.active.discard(self)
        self.status = Status.closed
        return await asyncio.gather(*self.close_comms())

    def __enter__(self):
        warnings.warn(
            "the rpc synchronous context manager is deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        asyncio.ensure_future(self.close_rpc())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close_rpc()

    def __del__(self):
        if self.status != Status.closed:
            rpc.active.discard(self)
            self.status = Status.closed
            still_open = [comm for comm in self.comms if not comm.closed()]
            if still_open:
                logger.warning(
                    "rpc object %s deleted with %d open comms", self, len(still_open)
                )
                for comm in still_open:
                    comm.abort()

    def __repr__(self):
        return "<rpc to %r, %d comms>" % (self.address, len(self.comms))


class PooledRPCCall:
    """The result of ConnectionPool()('host:port')

    See Also:
        ConnectionPool
    """

    def __init__(self, addr, pool, serializers=None, deserializers=None):
        self.addr = addr
        self.pool = pool
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers

    @property
    def address(self):
        return self.addr

    def __getattr__(self, key):
        async def send_recv_from_rpc(**kwargs):
            if self.serializers is not None and kwargs.get("serializers") is None:
                kwargs["serializers"] = self.serializers
            if self.deserializers is not None and kwargs.get("deserializers") is None:
                kwargs["deserializers"] = self.deserializers
            comm = await self.pool.connect(self.addr)
            prev_name, comm.name = comm.name, "ConnectionPool." + key
            try:
                return await send_recv(comm=comm, op=key, **kwargs)
            finally:
                self.pool.reuse(self.addr, comm)
                comm.name = prev_name

        return send_recv_from_rpc

    async def close_rpc(self):
        pass

    # For compatibility with rpc()
    def __enter__(self):
        warnings.warn(
            "the rpc synchronous context manager is deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __repr__(self):
        return f"<pooled rpc to {self.addr!r}>"


class ConnectionPool:
    """A maximum sized pool of Comm objects.

    This provides a connect method that mirrors the normal distributed.connect
    method, but provides connection sharing and tracks connection limits.

    This object provides an ``rpc`` like interface::

        >>> rpc = ConnectionPool(limit=512)
        >>> scheduler = rpc('127.0.0.1:8786')
        >>> workers = [rpc(address) for address in ...]

        >>> info = await scheduler.identity()

    It creates enough comms to satisfy concurrent connections to any
    particular address::

        >>> a, b = await asyncio.gather(scheduler.who_has(), scheduler.has_what())

    It reuses existing comms so that we don't have to continuously reconnect.

    It also maintains a comm limit to avoid "too many open file handle"
    issues.  Whenever this maximum is reached we clear out all idling comms.
    If that doesn't do the trick then we wait until one of the occupied comms
    closes.

    Parameters
    ----------
    limit: int
        The number of open comms to maintain at once
    deserialize: bool
        Whether or not to deserialize data by default or pass it through
    """

    _instances: ClassVar[weakref.WeakSet[ConnectionPool]] = weakref.WeakSet()

    def __init__(
        self,
        limit=512,
        deserialize=True,
        serializers=None,
        allow_offload=True,
        deserializers=None,
        connection_args=None,
        timeout=None,
        server=None,
    ):
        self.limit = limit  # Max number of open comms
        # Invariant: len(available) == open - active
        self.available = defaultdict(set)
        # Invariant: len(occupied) == active
        self.occupied = defaultdict(set)
        self.allow_offload = allow_offload
        self.deserialize = deserialize
        self.serializers = serializers
        self.deserializers = deserializers if deserializers is not None else serializers
        self.connection_args = connection_args or {}
        self.timeout = timeout
        self.server = weakref.ref(server) if server else None
        self._created = weakref.WeakSet()
        self._instances.add(self)
        # _n_connecting and _connecting have subtle different semantics. The set
        # _connecting contains futures actively trying to establish a connection
        # while the _n_connecting also accounts for connection attempts which
        # are waiting due to the connection limit
        self._connecting = set()
        self._pending_count = 0
        self._connecting_count = 0
        self.status = Status.init

    def _validate(self):
        """
        Validate important invariants of this class

        Used only for testing / debugging
        """
        assert self.semaphore._value == self.limit - self.open - self._n_connecting

    @property
    def active(self):
        return sum(map(len, self.occupied.values()))

    @property
    def open(self):
        return self.active + sum(map(len, self.available.values()))

    def __repr__(self):
        return "<ConnectionPool: open=%d, active=%d, connecting=%d>" % (
            self.open,
            self.active,
            len(self._connecting),
        )

    def __call__(self, addr=None, ip=None, port=None):
        """Cached rpc objects"""
        addr = addr_from_args(addr=addr, ip=ip, port=port)
        return PooledRPCCall(
            addr, self, serializers=self.serializers, deserializers=self.deserializers
        )

    def __await__(self):
        async def _():
            await self.start()
            return self

        return _().__await__()

    async def start(self):
        # Invariant: semaphore._value == limit - open - _n_connecting
        self.semaphore = asyncio.Semaphore(self.limit)
        self.status = Status.running

    @property
    def _n_connecting(self) -> int:
        return self._connecting_count

    async def _connect(self, addr, timeout=None):
        self._pending_count += 1
        try:
            await self.semaphore.acquire()
            try:
                self._connecting_count += 1
                comm = await connect(
                    addr,
                    timeout=timeout or self.timeout,
                    deserialize=self.deserialize,
                    **self.connection_args,
                )
                comm.name = "ConnectionPool"
                comm._pool = weakref.ref(self)
                comm.allow_offload = self.allow_offload
                self._created.add(comm)

                self.occupied[addr].add(comm)

                return comm
            except BaseException:
                self.semaphore.release()
                raise
            finally:
                self._connecting_count -= 1
        except asyncio.CancelledError:
            raise CommClosedError("ConnectionPool closing.")
        finally:
            self._pending_count -= 1

    async def connect(self, addr, timeout=None):
        """
        Get a Comm to the given address.  For internal use.
        """
        available = self.available[addr]
        occupied = self.occupied[addr]
        while available:
            comm = available.pop()
            if comm.closed():
                self.semaphore.release()
            else:
                occupied.add(comm)
                return comm

        if self.semaphore.locked():
            self.collect()

        # This construction is there to ensure that cancellation requests from
        # the outside can be distinguished from cancellations of our own.
        # Once the CommPool closes, we'll cancel the connect_attempt which will
        # raise an OSError
        # If the ``connect`` is cancelled from the outside, the Event.wait will
        # be cancelled instead which we'll reraise as a CancelledError and allow
        # it to propagate
        connect_attempt = asyncio.create_task(self._connect(addr, timeout))
        done = asyncio.Event()
        self._connecting.add(connect_attempt)
        connect_attempt.add_done_callback(lambda _: done.set())
        connect_attempt.add_done_callback(self._connecting.discard)

        try:
            await done.wait()
        except asyncio.CancelledError:
            # This is an outside cancel attempt
            connect_attempt.cancel()
            try:
                await connect_attempt
            except CommClosedError:
                pass
            raise
        return await connect_attempt

    def reuse(self, addr, comm):
        """
        Reuse an open communication to the given address.  For internal use.
        """
        # if the pool is asked to re-use a comm it does not know about, ignore
        # this comm: just close it.
        if comm not in self.occupied[addr]:
            IOLoop.current().add_callback(comm.close)
        else:
            self.occupied[addr].remove(comm)
            if comm.closed():
                # Either the user passed the close=True parameter to send_recv, or
                # the RPC call raised OSError or CancelledError
                self.semaphore.release()
            else:
                self.available[addr].add(comm)
                if self.semaphore.locked() and self._pending_count:
                    self.collect()

    def collect(self):
        """
        Collect open but unused communications, to allow opening other ones.
        """
        logger.info(
            "Collecting unused comms.  open: %d, active: %d, connecting: %d",
            self.open,
            self.active,
            len(self._connecting),
        )
        for addr, comms in self.available.items():
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()
            comms.clear()

    def remove(self, addr):
        """
        Remove all Comms to a given address.
        """
        logger.info("Removing comms to %s", addr)
        if addr in self.available:
            comms = self.available.pop(addr)
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()
        if addr in self.occupied:
            comms = self.occupied.pop(addr)
            for comm in comms:
                IOLoop.current().add_callback(comm.close)
                self.semaphore.release()

    async def close(self):
        """
        Close all communications
        """
        self.status = Status.closed
        for conn_fut in self._connecting:
            conn_fut.cancel()
        for d in [self.available, self.occupied]:
            comms = set()
            while d:
                comms.update(d.popitem()[1])

            await asyncio.gather(
                *(comm.close() for comm in comms), return_exceptions=True
            )

            for _ in comms:
                self.semaphore.release()

        while self._connecting:
            await asyncio.sleep(0.005)


def coerce_to_address(o):
    if isinstance(o, (list, tuple)):
        o = unparse_host_port(*o)

    return normalize_address(o)


def collect_causes(e: BaseException) -> list[BaseException]:
    causes = []
    while e.__cause__ is not None:
        causes.append(e.__cause__)
        e = e.__cause__
    return causes


class ErrorMessage(TypedDict):
    status: str
    exception: protocol.Serialize
    traceback: protocol.Serialize | None
    exception_text: str
    traceback_text: str


def error_message(e: BaseException, status="error") -> ErrorMessage:
    """Produce message to send back given an exception has occurred

    This does the following:

    1.  Gets the traceback
    2.  Truncates the exception and the traceback
    3.  Serializes the exception and traceback or
    4.  If they can't be serialized send string versions
    5.  Format a message and return

    See Also
    --------
    clean_exception : deserialize and unpack message into exception/traceback
    """
    MAX_ERROR_LEN = dask.config.get("distributed.admin.max-error-length")
    tblib.pickling_support.install(e, *collect_causes(e))
    tb = get_traceback()
    tb_text = "".join(traceback.format_tb(tb))
    e = truncate_exception(e, MAX_ERROR_LEN)
    try:
        e_bytes = protocol.pickle.dumps(e)
        protocol.pickle.loads(e_bytes)
    except Exception:
        e_bytes = protocol.pickle.dumps(Exception(repr(e)))
    e_serialized = protocol.to_serialize(e_bytes)

    try:
        tb_bytes = protocol.pickle.dumps(tb)
        protocol.pickle.loads(tb_bytes)
    except Exception:
        tb_bytes = protocol.pickle.dumps(tb_text)

    if len(tb_bytes) > MAX_ERROR_LEN:
        tb_serialized = None
    else:
        tb_serialized = protocol.to_serialize(tb_bytes)

    return {
        "status": status,
        "exception": e_serialized,
        "traceback": tb_serialized,
        "exception_text": repr(e),
        "traceback_text": tb_text,
    }


E = TypeVar("E", bound=BaseException)


def clean_exception(
    exception: E, traceback: types.TracebackType | None = None, **kwargs
) -> tuple[type[E], E, types.TracebackType | None]:
    """Reraise exception and traceback. Deserialize if necessary

    See Also
    --------
    error_message : create and serialize errors into message
    """
    if isinstance(exception, bytes) or isinstance(exception, bytearray):
        try:
            exception = protocol.pickle.loads(exception)
        except Exception:
            exception = Exception(exception)
    elif isinstance(exception, str):
        exception = Exception(exception)
    if isinstance(traceback, bytes):
        try:
            traceback = protocol.pickle.loads(traceback)
        except (TypeError, AttributeError):
            traceback = None
    elif isinstance(traceback, str):
        traceback = None  # happens if the traceback failed serializing
    return type(exception), exception, traceback
