import asyncio
from collections import defaultdict
from contextlib import suppress
import logging
import uuid

from tlz import merge

from .client import Future, Client
from .utils import tokey, log_errors, TimeoutError, parse_timedelta
from .worker import get_client

logger = logging.getLogger(__name__)


class VariableExtension:
    """ An extension for the scheduler to manage queues

    This adds the following routes to the scheduler

    *  variable-set
    *  variable-get
    *  variable-delete
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.variables = dict()
        self.waiting = defaultdict(set)
        self.waiting_conditions = defaultdict(asyncio.Condition)
        self.started = asyncio.Condition()

        self.scheduler.handlers.update(
            {"variable_set": self.set, "variable_get": self.get}
        )

        self.scheduler.stream_handlers["variable-future-release"] = self.future_release
        self.scheduler.stream_handlers["variable_delete"] = self.delete

        self.scheduler.extensions["variables"] = self

    async def set(self, comm=None, name=None, key=None, data=None, client=None):
        if key is not None:
            record = {"type": "Future", "value": key}
            self.scheduler.client_desires_keys(keys=[key], client="variable-%s" % name)
        else:
            record = {"type": "msgpack", "value": data}
        try:
            old = self.variables[name]
        except KeyError:
            pass
        else:
            if old["type"] == "Future" and old["value"] != key:
                asyncio.ensure_future(self.release(old["value"], name))
        if name not in self.variables:
            async with self.started:
                self.started.notify_all()
        self.variables[name] = record

    async def release(self, key, name):
        while self.waiting[key, name]:
            async with self.waiting_conditions[name]:
                await self.waiting_conditions[name].wait()

        self.scheduler.client_releases_keys(keys=[key], client="variable-%s" % name)
        del self.waiting[key, name]

    async def future_release(self, name=None, key=None, token=None, client=None):
        self.waiting[key, name].remove(token)
        if not self.waiting[key, name]:
            async with self.waiting_conditions[name]:
                self.waiting_conditions[name].notify_all()

    async def get(self, comm=None, name=None, client=None, timeout=None):
        start = self.scheduler.loop.time()
        while name not in self.variables:
            if timeout is not None:
                left = timeout - (self.scheduler.loop.time() - start)
            else:
                left = None
            if left and left < 0:
                raise TimeoutError()
            try:

                async def _():  # Python 3.6 is odd and requires special help here
                    await self.started.acquire()
                    await self.started.wait()

                await asyncio.wait_for(_(), timeout=left)
            finally:
                with suppress(RuntimeError):  # Python 3.6 loses lock on finally clause
                    self.started.release()

        record = self.variables[name]
        if record["type"] == "Future":
            key = record["value"]
            token = uuid.uuid4().hex
            ts = self.scheduler.tasks.get(key)
            state = ts.state if ts is not None else "lost"
            msg = {"token": token, "state": state}
            if state == "erred":
                msg["exception"] = ts.exception_blame.exception
                msg["traceback"] = ts.exception_blame.traceback
            record = merge(record, msg)
            self.waiting[key, name].add(token)
        return record

    async def delete(self, comm=None, name=None, client=None):
        with log_errors():
            try:
                old = self.variables[name]
            except KeyError:
                pass
            else:
                if old["type"] == "Future":
                    await self.release(old["value"], name)
            with suppress(KeyError):
                del self.waiting_conditions[name]
            with suppress(KeyError):
                del self.variables[name]

            self.scheduler.remove_client("variable-%s" % name)


class Variable:
    """ Distributed Global Variable

    This allows multiple clients to share futures and data between each other
    with a single mutable variable.  All metadata is sequentialized through the
    scheduler.  Race conditions can occur.

    Values must be either Futures or msgpack-encodable data (ints, lists,
    strings, etc..)  All data will be kept and sent through the scheduler, so
    it is wise not to send too much.  If you want to share a large amount of
    data then ``scatter`` it and share the future instead.

    .. warning::

       This object is experimental and has known issues in Python 2

    Parameters
    ----------
    name: string (optional)
        Name used by other clients and the scheduler to identify the variable.
        If not given, a random name will be generated.
    client: Client (optional)
        Client used for communication with the scheduler. Defaults to the
        value of ``Client.current()``.

    Examples
    --------
    >>> from dask.distributed import Client, Variable # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> x = Variable('x')  # doctest: +SKIP
    >>> x.set(123)  # docttest: +SKIP
    >>> x.get()  # docttest: +SKIP
    123
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> x.set(future)  # doctest: +SKIP

    See Also
    --------
    Queue: shared multi-producer/multi-consumer queue between clients
    """

    def __init__(self, name=None, client=None, maxsize=0):
        self.client = client or Client.current()
        self.name = name or "variable-" + uuid.uuid4().hex

    async def _set(self, value):
        if isinstance(value, Future):
            await self.client.scheduler.variable_set(
                key=tokey(value.key), name=self.name
            )
        else:
            await self.client.scheduler.variable_set(data=value, name=self.name)

    def set(self, value, **kwargs):
        """ Set the value of this variable

        Parameters
        ----------
        value: Future or object
            Must be either a Future or a msgpack-encodable value
        """
        return self.client.sync(self._set, value, **kwargs)

    async def _get(self, timeout=None):
        d = await self.client.scheduler.variable_get(
            timeout=timeout, name=self.name, client=self.client.id
        )
        if d["type"] == "Future":
            value = Future(d["value"], self.client, inform=True, state=d["state"])
            if d["state"] == "erred":
                value._state.set_error(d["exception"], d["traceback"])
            self.client._send_to_scheduler(
                {
                    "op": "variable-future-release",
                    "name": self.name,
                    "key": d["value"],
                    "token": d["token"],
                }
            )
        else:
            value = d["value"]
        return value

    def get(self, timeout=None, **kwargs):
        """ Get the value of this variable

        Parameters
        ----------
        timeout: number or string or timedelta, optional
            Time in seconds to wait before timing out.
            Instead of number of seconds, it is also possible to specify
            a timedelta in string format, e.g. "200ms".
        """
        timeout = parse_timedelta(timeout)
        return self.client.sync(self._get, timeout=timeout, **kwargs)

    def delete(self):
        """ Delete this variable

        Caution, this affects all clients currently pointing to this variable.
        """
        if self.client.status == "running":  # TODO: can leave zombie futures
            self.client._send_to_scheduler({"op": "variable_delete", "name": self.name})

    def __getstate__(self):
        return (self.name, self.client.scheduler.address)

    def __setstate__(self, state):
        name, address = state
        try:
            client = get_client(address)
            assert client.scheduler.address == address
        except (AttributeError, AssertionError):
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client)
