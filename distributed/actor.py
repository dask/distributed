import asyncio
import functools
import threading

from .client import Future
from .protocol import to_serialize
from .utils import iscoroutinefunction, sync, thread_state
from .utils_comm import WrappedKey
from .worker import get_client, get_worker


class Actor(WrappedKey):
    """Controls an object on a remote worker

    An actor allows remote control of a stateful object living on a remote
    worker.  Method calls on this object trigger operations on the remote
    object and return ActorFutures on which we can block to get results.

    Examples
    --------
    >>> class Counter:
    ...    def __init__(self):
    ...        self.n = 0
    ...    def increment(self):
    ...        self.n += 1
    ...        return self.n

    >>> from dask.distributed import Client
    >>> client = Client()

    You can create an actor by submitting a class with the keyword
    ``actor=True``.

    >>> future = client.submit(Counter, actor=True)
    >>> counter = future.result()
    >>> counter
    <Actor: Counter, key=Counter-1234abcd>

    Calling methods on this object immediately returns deferred ``ActorFuture``
    objects.  You can call ``.result()`` on these objects to block and get the
    result of the function call.

    >>> future = counter.increment()
    >>> future.result()
    1
    >>> future = counter.increment()
    >>> future.result()
    2
    """

    def __init__(self, cls, address, key, worker=None):
        super().__init__(key)
        self._cls = cls
        self._address = address
        self._future = None
        if worker:
            self._worker = worker
            self._client = None
        else:
            try:
                # TODO: `get_worker` may return the wrong worker instance for async local clusters (most tests)
                # when run outside of a task (when deserializing a key pointing to an Actor, etc.)
                self._worker = get_worker()
            except ValueError:
                self._worker = None
            try:
                self._client = get_client()
                self._future = Future(key, inform=self._worker is None)
                # ^ When running on a worker, only hold a weak reference to the key, otherwise the key could become unreleasable.
            except ValueError:
                self._client = None

    def __repr__(self):
        return f"<Actor: {self._cls.__name__}, key={self.key}>"

    def __reduce__(self):
        return (Actor, (self._cls, self._address, self.key))

    @property
    def _io_loop(self):
        if self._worker:
            return self._worker.io_loop
        else:
            return self._client.io_loop

    @property
    def _scheduler_rpc(self):
        if self._worker:
            return self._worker.scheduler
        else:
            return self._client.scheduler

    @property
    def _worker_rpc(self):
        if self._worker:
            return self._worker.rpc(self._address)
        else:
            if self._client.direct_to_workers:
                return self._client.rpc(self._address)
            else:
                return ProxyRPC(self._client.scheduler, self._address)

    @property
    def _asynchronous(self):
        if self._client:
            return self._client.asynchronous
        else:
            return threading.get_ident() == self._worker.thread_id

    def _sync(self, func, *args, **kwargs):
        if self._client:
            return self._client.sync(func, *args, **kwargs)
        else:
            if self._asynchronous:
                return func(*args, **kwargs)
            return sync(self._worker.loop, func, *args, **kwargs)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(attr for attr in dir(self._cls) if not attr.startswith("_"))
        return sorted(o)

    def __getattr__(self, key):

        if self._future and self._future.status not in ("finished", "pending"):
            raise ValueError(
                "Worker holding Actor was lost.  Status: " + self._future.status
            )

        if (
            self._worker
            and self._worker.address == self._address
            and getattr(thread_state, "actor", False)
        ):
            # actor calls actor on same worker
            actor = self._worker.actors[self.key]
            attr = getattr(actor, key)

            if iscoroutinefunction(attr):
                return attr

            elif callable(attr):
                return lambda *args, **kwargs: ActorFuture(
                    None, self._io_loop, result=attr(*args, **kwargs)
                )
            else:
                return attr

        attr = getattr(self._cls, key)

        if callable(attr):

            @functools.wraps(attr)
            def func(*args, **kwargs):
                async def run_actor_function_on_worker():
                    try:
                        result = await self._worker_rpc.actor_execute(
                            function=key,
                            actor=self.key,
                            args=[to_serialize(arg) for arg in args],
                            kwargs={k: to_serialize(v) for k, v in kwargs.items()},
                        )
                    except OSError:
                        if self._future and not self._future.done():
                            await self._future
                            return await run_actor_function_on_worker()
                        else:
                            raise OSError("Unable to contact Actor's worker")
                    return result

                q = asyncio.Queue(loop=self._io_loop.asyncio_loop)

                async def wait_then_add_to_queue():
                    x = await run_actor_function_on_worker()
                    await q.put(x)

                self._io_loop.add_callback(wait_then_add_to_queue)
                return ActorFuture(q, self._io_loop)

            return func

        else:

            async def get_actor_attribute_from_worker():
                x = await self._worker_rpc.actor_attribute(
                    attribute=key, actor=self.key
                )
                if x["status"] == "OK":
                    return x["result"]
                else:
                    raise x["exception"]

            return self._sync(get_actor_attribute_from_worker)

    @property
    def client(self):
        return self._future.client


class ProxyRPC:
    """
    An rpc-like object that uses the scheduler's rpc to connect to a worker
    """

    def __init__(self, rpc, address):
        self.rpc = rpc
        self._address = address

    def __getattr__(self, key):
        async def func(**msg):
            msg["op"] = key
            result = await self.rpc.proxy(worker=self._address, msg=msg)
            return result

        return func


class ActorFuture:
    """Future to an actor's method call

    Whenever you call a method on an Actor you get an ActorFuture immediately
    while the computation happens in the background.  You can call ``.result``
    to block and collect the full result

    See Also
    --------
    Actor
    """

    def __init__(self, q, io_loop, result=None):
        self.q = q
        self.io_loop = io_loop
        if result:
            self._cached_result = result
        self.status = "pending"

    def __await__(self):
        return self._result().__await__()

    def done(self):
        return self.status != "pending"

    async def _result(self, raiseit=True):
        if not hasattr(self, "_cached_result"):
            out = await self.q.get()
            if out["status"] == "OK":
                self.status = "finished"
                self._cached_result = out["result"]
            else:
                self.status = "error"
                self._cached_result = out["exception"]
        if self.status == "error":
            raise self._cached_result
        return self._cached_result

    def result(self, timeout=None):
        return sync(self.io_loop, self._result, callback_timeout=timeout)

    def __repr__(self):
        return "<ActorFuture>"
