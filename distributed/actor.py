import asyncio
import functools
from inspect import iscoroutinefunction
import threading
from queue import Queue, Empty

from .client import Future, default_client
from .protocol import to_serialize
from .utils import thread_state, sync
from .utils_comm import WrappedKey
from .worker import get_worker, logger


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
        self._cls = cls
        self._address = address
        self.key = key
        self._future = None
        if worker:
            # made by a worker
            self._worker = worker
            self._client = None
            assert self.key in self._worker.actors
            assert self._address == self._worker.address
        else:
            try:
                # instance on a worker, but not made by worker
                self._worker = get_worker()
            except ValueError:
                self._worker = None
            try:
                # claim remote original actor
                self._client = default_client()
                self._future = Future(key)
            except ValueError:
                self._client = None

    def __repr__(self):
        return "<Actor: %s, key=%s>" % (self._cls.__name__, self.key)

    def __reduce__(self):
        return Actor, (self._cls, self._address, self.key)

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

    def set_address(self):
        print("SETTING ADDRESS from", self._address)
        self._address, = self._sync(self._scheduler_rpc.find_actor, actor_key=self.key)
        print(self._address)
        try:
            print(self._sync(self._worker_rpc.get_data, keys=[self.key]))
        except Exception as e:
            print("GET DATA FAIL", e)
        print("synced future")

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
            # TODO support sync operation by checking against thread ident of loop
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
                    None, None, result=attr(*args, **kwargs)
                )
            else:
                return attr

        attr = getattr(self._cls, key)

        if callable(attr):

            @functools.wraps(attr)
            def func(*args, **kwargs):
                async def run_actor_function_on_worker():
                    retry = 3
                    while retry > 0:
                        # warning: error and full stack can leak here
                        try:
                            result = await asyncio.wait_for(self._worker_rpc.actor_execute(
                                function=key,
                                actor=self.key,
                                args=[to_serialize(arg) for arg in args],
                                kwargs={k: to_serialize(v) for k, v in kwargs.items()},
                            ), timeout=2)
                            if "cannot schedule new futures after shutdown" not in str(result.get('exception', "")):
                                break
                        except (OSError, AssertionError) as e:
                            # assertion error is a low-level comm validation error
                            print("ERROR", e)
                            result = {"exception": e}
                            break
                        except Exception as e:
                            logger.debug("Actor execute retriable exception")
                            result = {"exception": e}
                            retry -= 1

                    print(result)

                    return result

                if self._asynchronous:

                    async def unwrap():
                        result = await run_actor_function_on_worker()
                        if "result" in result:
                            return result["result"]
                        raise result["exception"]

                    return asyncio.ensure_future(unwrap())
                else:
                    # TODO: this mechanism is error prone
                    # we should endeavor to make dask's standard code work here
                    q = Queue()

                    async def wait_then_add_to_queue():
                        try:
                            x = await run_actor_function_on_worker()
                            print("2", x)
                            q.put(x)
                        except Exception as e:
                            q.put({"exception": e})

                    self._io_loop.add_callback(wait_then_add_to_queue)

                    return ActorFuture(q, self._io_loop, actor=self, defs=(attr, args, kwargs))

            return func

        else:

            async def get_actor_attribute_from_worker():
                x = await self._worker_rpc.actor_attribute(
                    attribute=key, actor=self.key
                )
                if "result" in x:
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

    def __init__(self, q, io_loop, result=None, actor=None, defs=None):
        self.q = q
        self.io_loop = io_loop
        if result:
            self._cached_result = result
        else:
            self.actor = actor
            self.defs = defs

    def __await__(self):
        return self.result()

    def result(self, timeout=2.5):
        try:
            if isinstance(self._cached_result, Exception):
                raise self._cached_result
            print("Result!", self._cached_result)
            return self._cached_result
        except AttributeError:
            pass
        try:
            out = self.q.get(timeout=timeout)
            print("RECEIVED", out)
            if "result" in out:
                self._cached_result = out["result"]
            else:
                ex = out["exception"]
                self.actor.set_address()
        except Empty:
            self._cached_result = TimeoutError()
        self.actor = None
        self.defs = None
        return self.result()

    def __repr__(self):
        return "<ActorFuture>"
