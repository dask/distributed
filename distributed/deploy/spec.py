from __future__ import annotations

import asyncio
import atexit
import contextlib
import copy
import logging
import math
import weakref
from collections.abc import Awaitable, Generator, Iterable
from contextlib import suppress
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from tornado import gen
from tornado.ioloop import IOLoop

import dask
from dask.utils import parse_bytes, parse_timedelta
from dask.widgets import get_template

from distributed.core import Status, rpc
from distributed.deploy.adaptive import Adaptive
from distributed.deploy.cluster import Cluster
from distributed.scheduler import Scheduler
from distributed.security import Security
from distributed.utils import (
    NoOpAwaitable,
    TimeoutError,
    import_term,
    silence_logging_cmgr,
)

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.11)
    from typing_extensions import Self

    # Circular imports
    from distributed import Nanny, Worker

logger = logging.getLogger(__name__)


class ProcessInterface:
    """
    An interface for Scheduler and Worker processes for use in SpecCluster

    This interface is responsible to submit a worker or scheduler process to a
    resource manager like Kubernetes, Yarn, or SLURM/PBS/SGE/...
    It should implement the methods below, like ``start`` and ``close``
    """

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        if not isinstance(new_status, Status):
            raise TypeError(f"Expected Status; got {new_status!r}")
        self._status = new_status

    def __init__(self, scheduler=None, name=None):
        self.address = getattr(self, "address", None)
        self.external_address = None
        self.lock = asyncio.Lock()
        self.status = Status.created
        self._event_finished = asyncio.Event()

    def __await__(self):
        async def _():
            async with self.lock:
                if self.status == Status.created:
                    await self.start()
                    assert self.status == Status.running
            return self

        return _().__await__()

    async def start(self):
        """Submit the process to the resource manager

        For workers this doesn't have to wait until the process actually starts,
        but can return once the resource manager has the request, and will work
        to make the job exist in the future

        For the scheduler we will expect the scheduler's ``.address`` attribute
        to be available after this completes.
        """
        self.status = Status.running

    async def close(self):
        """Close the process

        This will be called by the Cluster object when we scale down a node,
        but only after we ask the Scheduler to close the worker gracefully.
        This method should kill the process a bit more forcefully and does not
        need to worry about shutting down gracefully
        """
        self.status = Status.closed
        self._event_finished.set()

    async def finished(self):
        """Wait until the server has finished"""
        await self._event_finished.wait()

    def __repr__(self):
        return f"<{dask.utils.typename(type(self))}: status={self.status.name}>"

    def _repr_html_(self):
        return get_template("process_interface.html.j2").render(process_interface=self)

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


_T = TypeVar("_T")


async def _wrap_awaitable(aw: Awaitable[_T]) -> _T:
    return await aw


class SpecCluster(Cluster):
    """Cluster that requires a full specification of workers

    The SpecCluster class expects a full specification of the Scheduler and
    Workers to use.  It removes any handling of user inputs (like threads vs
    processes, number of cores, and so on) and any handling of cluster resource
    managers (like pods, jobs, and so on).  Instead, it expects this
    information to be passed in scheduler and worker specifications.  This
    class does handle all of the logic around asynchronously cleanly setting up
    and tearing things down at the right times.  Hopefully it can form a base
    for other more user-centric classes.

    Terminology
    -----------
    **Spec name**: The string key in the ``worker_spec`` dictionary (e.g., ``"0"``,
    ``"my-worker"``). This identifies a worker specification entry.

    **Worker name**: The actual name a worker reports to the scheduler (e.g., ``"0"``,
    ``"0-0"``, ``"0-1"``). This is what appears in ``scheduler.workers``.

    For **regular workers**: spec name == worker name (one-to-one mapping)
    For **grouped workers**: one spec name → multiple worker names (one-to-many mapping)

    Grouped Workers
    ---------------
    A single spec entry can generate multiple Dask workers by including a ``"group"``
    element with suffixes. This is useful for:
    - HPC systems (e.g., SLURM) where multiple processes are allocated together
    - Any worker class that manages multiple workers as a unit (e.g., MultiWorker)

    >>> cluster.worker_spec = {
    ...     "0": {"cls": MultiWorker, "options": {"processes": 3}, "group": ["-0", "-1", "-2"]},
    ...     "1": {"cls": MultiWorker, "options": {"processes": 2}, "group": ["-0", "-1"]}
    ... }

    The scheduler sees individual workers with concatenated names:

    >>> [ws.name for ws in cluster.scheduler.workers.values()]
    ["0-0", "0-1", "0-2", "1-0", "1-1"]

    When any worker in a group fails, the entire spec is removed so the group
    can be recreated as a unit (important for HPC where the whole allocation fails).

    Parameters
    ----------
    workers: dict[str, dict], optional
        A dictionary mapping spec names (strings) to worker specifications.
        Each worker spec is a dict with 'cls' and optionally 'options' and 'group'.
        Spec names must be strings.
    scheduler: dict, optional
        A specification for the scheduler with 'cls' and 'options' keys
    worker: dict, optional
        A worker specification template used when calling scale().
        This template is used to auto-generate new worker specs.
    asynchronous: bool
        If this is intended to be used directly within an event loop with
        async/await
    silence_logs: bool
        Whether or not we should silence logging when setting up the cluster.
    name: str, optional
        A name to use when printing out the cluster, defaults to type name
    shutdown_on_close: bool
        Whether or not to close the cluster when the program exits
    shutdown_scheduler: bool
        Whether or not to shut down the scheduler when the cluster is closed

    Examples
    --------
    To create a SpecCluster you specify worker specifications and a scheduler spec

    >>> from dask.distributed import Scheduler, Worker, Nanny
    >>> scheduler = {'cls': Scheduler, 'options': {"dashboard_address": ':8787'}}
    >>> worker_spec = {
    ...     'my-worker': {"cls": Worker, "options": {"nthreads": 1}},
    ...     'my-nanny': {"cls": Nanny, "options": {"nthreads": 2}},
    ... }
    >>> cluster = SpecCluster(scheduler=scheduler, workers=worker_spec)

    The worker specs are stored in the ``.worker_spec`` attribute

    >>> cluster.worker_spec
    {
       'my-worker': {"cls": Worker, "options": {"nthreads": 1}},
       'my-nanny': {"cls": Nanny, "options": {"nthreads": 2}},
    }

    The actual Worker instances created from these specs are stored in the
    ``.workers`` attribute

    >>> cluster.workers
    {
        'my-worker': <Worker ...>
        'my-nanny': <Nanny ...>
    }

    Should the worker_spec change, we can await the cluster or call the
    ``._correct_state`` method to align the actual Worker instances to the
    specified state.

    We can also ``.scale(...)`` the cluster, which adds new worker specs using
    the template provided via the ``worker`` parameter.

    >>> worker_template = {'cls': Worker, 'options': {}}
    >>> cluster = SpecCluster(scheduler=scheduler, worker=worker_template)
    >>> cluster.worker_spec
    {}

    >>> cluster.scale(3)
    >>> cluster.worker_spec
    {
        "0": {'cls': Worker, 'options': {}},
        "1": {'cls': Worker, 'options': {}},
        "2": {'cls': Worker, 'options': {}},
    }

    Note that above we are using the standard ``Worker`` and ``Nanny`` classes,
    however in practice other classes could be used that handle resource
    management like ``KubernetesPod`` or ``SLURMJob``.  Worker specs do not need
    to conform to the expectations of the standard Dask Worker class.  They just
    need to be called with the provided options, support ``__await__`` and
    ``close`` methods and the ``worker_address`` property.

    Also note that uniformity of worker specs is not required.  Other API
    could be added externally (in subclasses) that adds worker specs of different
    types into the same worker_spec dictionary.
    """

    _instances: ClassVar[weakref.WeakSet[SpecCluster]] = weakref.WeakSet()

    def __init__(
        self,
        workers=None,
        scheduler=None,
        worker=None,
        asynchronous=False,
        loop=None,
        security=None,
        silence_logs=False,
        name=None,
        shutdown_on_close=True,
        scheduler_sync_interval=1,
        shutdown_scheduler=True,
    ):
        if loop is None and asynchronous:
            loop = IOLoop.current()

        self.__exit_stack = stack = contextlib.ExitStack()
        self._created = weakref.WeakSet()

        self.scheduler_spec = copy.copy(scheduler)
        self.worker_spec: dict[str, dict[str, Any]] = copy.copy(workers) or {}
        self.new_spec: dict[str, Any] | None = copy.copy(worker)
        self.scheduler = None
        self.workers: dict[str, Worker | Nanny] = {}
        self._i = 0
        self.security = security or Security()
        self._futures = set()

        if silence_logs:
            stack.enter_context(silence_logging_cmgr(level=silence_logs))
            stack.enter_context(silence_logging_cmgr(level=silence_logs, root="bokeh"))

        self._instances.add(self)
        self._correct_state_waiting = None
        self._name = name or type(self).__name__
        self.shutdown_on_close = shutdown_on_close
        self.shutdown_scheduler = shutdown_scheduler

        super().__init__(
            asynchronous=asynchronous,
            loop=loop,
            name=name,
            scheduler_sync_interval=scheduler_sync_interval,
        )

        if not self.called_from_running_loop:
            self._loop_runner.start()
            self.sync(self._start)
            try:
                self.sync(self._correct_state)
            except Exception:
                self.sync(self.close)
                self._loop_runner.stop()
                raise

    def close(self, timeout: float | None = None) -> Awaitable[None] | None:
        aw = super().close(timeout)
        if not self.asynchronous:
            self._loop_runner.stop()
        return aw

    async def _start(self):
        while self.status == Status.starting:
            await asyncio.sleep(0.01)
        if self.status == Status.running:
            return
        if self.status == Status.closed:
            raise ValueError("Cluster is closed")

        self._lock = asyncio.Lock()
        self.status = Status.starting

        if self.scheduler_spec is None:
            try:
                import distributed.dashboard  # noqa: F401
            except ImportError:
                pass
            else:
                options = {"dashboard": True}
            self.scheduler_spec = {"cls": Scheduler, "options": options}

        try:
            # Check if scheduler has already been created by a subclass
            if self.scheduler is None:
                cls = self.scheduler_spec["cls"]
                if isinstance(cls, str):
                    cls = import_term(cls)
                self.scheduler = cls(**self.scheduler_spec.get("options", {}))
                self.scheduler = await self.scheduler
            self.scheduler_comm = rpc(
                getattr(self.scheduler, "external_address", None)
                or self.scheduler.address,
                connection_args=self.security.get_connection_args("client"),
            )
            await super()._start()
        except Exception as e:  # pragma: no cover
            self.status = Status.failed
            await self._close()
            raise RuntimeError(f"Cluster failed to start: {e}") from e

    def _correct_state(self):
        if self._correct_state_waiting:
            # If people call this frequently, we only want to run it once
            return self._correct_state_waiting
        else:
            task = asyncio.ensure_future(self._correct_state_internal())
            self._correct_state_waiting = task
            return task

    async def _correct_state_internal(self) -> None:
        async with self._lock:
            self._correct_state_waiting = None

            to_close = set(self.workers) - set(self.worker_spec)
            if to_close:
                if self.scheduler.status == Status.running:
                    await self.scheduler_comm.retire_workers(workers=list(to_close))
                tasks = [
                    asyncio.create_task(self.workers[w].close())
                    for w in to_close
                    if w in self.workers
                ]
                await asyncio.gather(*tasks)
            for name in to_close:
                if name in self.workers:
                    del self.workers[name]

            to_open = set(self.worker_spec) - set(self.workers)
            workers = []
            for name in to_open:
                d = self.worker_spec[name]
                cls, opts = d["cls"], d.get("options", {})
                if "name" not in opts:
                    opts = opts.copy()
                    opts["name"] = name
                if isinstance(cls, str):
                    cls = import_term(cls)
                worker = cls(
                    getattr(self.scheduler, "contact_address", None)
                    or self.scheduler.address,
                    **opts,
                )
                self._created.add(worker)
                workers.append(worker)
            if workers:
                worker_futs = [asyncio.ensure_future(w) for w in workers]
                await asyncio.wait(worker_futs)
                self.workers.update(dict(zip(to_open, workers)))
                for w in workers:
                    w._cluster = weakref.ref(self)
                # Collect exceptions from failed workers. This must happen after all
                # *other* workers have finished initialising, so that we can have a
                # proper teardown.
                await asyncio.gather(*worker_futs)

    def _update_worker_status(self, op, msg):
        if op == "remove":
            name = self.scheduler_info["workers"][msg]["name"]

            def f():
                if (
                    name in self.workers
                    and msg not in self.scheduler_info["workers"]
                    and not any(
                        d["name"] == name
                        for d in self.scheduler_info["workers"].values()
                    )
                ):
                    self._futures.add(asyncio.ensure_future(self.workers[name].close()))
                    del self.workers[name]

            delay = parse_timedelta(
                dask.config.get("distributed.deploy.lost-worker-timeout")
            )

            asyncio.get_running_loop().call_later(delay, f)
        super()._update_worker_status(op, msg)

    def __await__(self: Self) -> Generator[Any, Any, Self]:
        async def _() -> Self:
            if self.status == Status.created:
                await self._start()
            await self.scheduler
            await self._correct_state()
            if self.workers:
                await asyncio.wait(
                    [
                        asyncio.create_task(_wrap_awaitable(w))
                        for w in self.workers.values()
                    ]
                )  # maybe there are more
            return self

        return _().__await__()

    async def _close(self):
        while self.status == Status.closing:
            await asyncio.sleep(0.1)
        if self.status == Status.closed:
            return
        if self.status == Status.running or self.status == Status.failed:
            self.status = Status.closing

            # Need to call stop here before we close all servers to avoid having
            # dangling tasks in the ioloop
            with suppress(AttributeError):
                self._adaptive.stop()

            f = self.scale(0)
            if isawaitable(f):
                await f
            await self._correct_state()
            await asyncio.gather(*self._futures)

            if self.scheduler_comm:
                async with self._lock:
                    if self.shutdown_scheduler:
                        with suppress(OSError):
                            await self.scheduler_comm.terminate()
                    await self.scheduler_comm.close_rpc()
            else:
                logger.warning("Cluster closed without starting up")

            if self.scheduler and self.shutdown_scheduler:
                await self.scheduler.close()
            for w in self._created:
                assert w.status in {
                    Status.closing,
                    Status.closed,
                    Status.failed,
                }, w.status

        self.__exit_stack.__exit__(None, None, None)
        await super()._close()

    async def __aenter__(self):
        try:
            await self
            await self._correct_state()
            assert self.status == Status.running
            return self
        except Exception:
            await self._close()
            raise

    def _threads_per_worker(self) -> int:
        """Return the number of threads per worker for new workers"""
        if not self.new_spec:  # pragma: no cover
            raise ValueError("To scale by cores= you must specify cores per worker")

        for name in ["nthreads", "ncores", "threads", "cores"]:
            with suppress(KeyError):
                return self.new_spec["options"][name]
        raise RuntimeError("unreachable")

    def _memory_per_worker(self) -> int:
        """Return the memory limit per worker for new workers"""
        if not self.new_spec:  # pragma: no cover
            raise ValueError(
                "to scale by memory= your worker definition must include a "
                "memory_limit definition"
            )

        for name in ["memory_limit", "memory"]:
            with suppress(KeyError):
                return parse_bytes(self.new_spec["options"][name])

        raise ValueError(
            "to use scale(memory=...) your worker definition must include a "
            "memory_limit definition"
        )

    def scale(self, n=0, memory=None, cores=None):
        if memory is not None:
            n = max(n, int(math.ceil(parse_bytes(memory) / self._memory_per_worker())))

        if cores is not None:
            n = max(n, int(math.ceil(cores / self._threads_per_worker())))

        if len(self.worker_spec) > n:
            not_yet_launched = set(self.worker_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while len(self.worker_spec) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]

        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        if self.status not in (Status.closing, Status.closed):
            while len(self.worker_spec) < n:
                self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()

    def _new_spec_name(self, spec_number: int) -> str:
        """Returns new spec name (key for worker_spec dict).

        This generates a spec name for auto-created worker specs. For regular
        workers, the spec name will also be the worker name. For grouped workers,
        the spec name is the prefix, and actual worker names will have suffixes
        appended (e.g., spec name "0" with group ["-0", "-1"] creates workers
        "0-0" and "0-1").

        This can be overridden in SpecCluster derived classes to customize spec
        naming.

        Parameters
        ----------
        spec_number : int
            The numeric identifier for this spec (typically from self._i)

        Returns
        -------
        str
            The spec name to use as a key in worker_spec dict
        """
        return str(spec_number)

    def _spec_name_to_worker_names(self, spec_name: str) -> set[str]:
        """Convert a spec name to the set of worker names it generates.

        For regular workers, the spec name equals the worker name (1:1 mapping).
        For grouped workers, one spec name maps to multiple worker names (1:many).

        Parameters
        ----------
        spec_name : str
            The spec name (key in worker_spec dict)

        Returns
        -------
        set[str]
            Set of worker names the scheduler will see for this spec

        Examples
        --------
        Regular worker (no "group" key):
        >>> cluster.worker_spec = {"0": {"cls": Worker, "options": {}}}
        >>> cluster._spec_name_to_worker_names("0")
        {"0"}

        Grouped worker (has "group" key):
        >>> cluster.worker_spec = {
        ...     "0": {"cls": MultiWorker, "options": {}, "group": ["-0", "-1", "-2"]}
        ... }
        >>> cluster._spec_name_to_worker_names("0")
        {"0-0", "0-1", "0-2"}
        """
        if spec_name not in self.worker_spec:
            return set()

        spec = self.worker_spec[spec_name]
        if "group" in spec:
            # Grouped worker: concatenate spec_name with each suffix
            return {spec_name + suffix for suffix in spec["group"]}
        else:
            # Regular worker: spec name == worker name
            return {spec_name}

    def _worker_name_to_spec_name(self, worker_name: str) -> str | None:
        """Convert a worker name to its corresponding spec name.

        For regular workers, the worker name equals the spec name.
        For grouped workers, extract the spec name prefix from the worker name.

        Parameters
        ----------
        worker_name : str
            The worker name (as seen by the scheduler)

        Returns
        -------
        str | None
            The spec name (key in worker_spec dict), or None if not found

        Examples
        --------
        Regular worker:
        >>> cluster.worker_spec = {"0": {"cls": Worker, "options": {}}}
        >>> cluster._worker_name_to_spec_name("0")
        "0"

        Grouped worker:
        >>> cluster.worker_spec = {
        ...     "0": {"cls": MultiWorker, "options": {}, "group": ["-0", "-1", "-2"]}
        ... }
        >>> cluster._worker_name_to_spec_name("0-1")
        "0"

        Not found:
        >>> cluster._worker_name_to_spec_name("nonexistent")
        None
        """
        # First check if worker_name is directly a spec name (regular worker)
        if worker_name in self.worker_spec:
            return worker_name

        # For grouped workers, check each spec to see if this worker belongs to it
        for spec_name in self.worker_spec:
            worker_names = self._spec_name_to_worker_names(spec_name)
            if worker_name in worker_names:
                return spec_name

        return None

    def new_worker_spec(self) -> dict[str, dict[str, Any]]:
        """Return name and spec for the next worker spec

        Returns
        -------
        dict[str, dict]
            A dictionary with a single entry mapping a spec name (string) to
            a worker specification dict

        See Also
        --------
        scale
        """
        spec_name = self._new_spec_name(self._i)
        while spec_name in self.worker_spec:
            self._i += 1
            spec_name = self._new_spec_name(self._i)

        return {spec_name: cast(dict[str, Any], self.new_spec)}

    @property
    def _supports_scaling(self):
        return bool(self.new_spec)

    async def scale_down(self, workers: Iterable[str]) -> None:
        """Scale down by removing worker specs."""
        # We may have groups, if so, map worker addresses to job names
        if not all(w in self.worker_spec for w in workers):
            mapping = {}
            for name, spec in self.worker_spec.items():
                if "group" in spec:
                    for suffix in spec["group"]:
                        mapping[str(name) + suffix] = name
                else:
                    mapping[name] = name

            workers = {mapping.get(w, w) for w in workers}

        for w in workers:
            if w in self.worker_spec:
                del self.worker_spec[w]
        await self

    scale_up = scale  # backwards compatibility

    @property
    def plan(self):
        out = set()
        for name, spec in self.worker_spec.items():
            if "group" in spec:
                out.update({str(name) + suffix for suffix in spec["group"]})
            else:
                out.add(name)
        return out

    @property
    def requested(self):
        out = set()
        for name in self.workers:
            try:
                spec = self.worker_spec[name]
            except KeyError:
                continue
            if "group" in spec:
                out.update({str(name) + suffix for suffix in spec["group"]})
            else:
                out.add(name)
        return out

    def adapt(
        self,
        Adaptive: type[Adaptive] = Adaptive,
        minimum: float = 0,
        maximum: float = math.inf,
        minimum_cores: int | None = None,
        maximum_cores: int | None = None,
        minimum_memory: str | None = None,
        maximum_memory: str | None = None,
        **kwargs: Any,
    ) -> Adaptive:
        """Turn on adaptivity

        This scales Dask clusters automatically based on scheduler activity.

        Parameters
        ----------
        minimum : int
            Minimum number of workers
        maximum : int
            Maximum number of workers
        minimum_cores : int
            Minimum number of cores/threads to keep around in the cluster
        maximum_cores : int
            Maximum number of cores/threads to keep around in the cluster
        minimum_memory : str
            Minimum amount of memory to keep around in the cluster
            Expressed as a string like "100 GiB"
        maximum_memory : str
            Maximum amount of memory to keep around in the cluster
            Expressed as a string like "100 GiB"

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum_memory="100 GiB", interval='500ms')

        See Also
        --------
        dask.distributed.Adaptive : for more keyword arguments
        """
        if minimum_cores is not None:
            minimum = max(
                minimum or 0, math.ceil(minimum_cores / self._threads_per_worker())
            )
        if minimum_memory is not None:
            minimum = max(
                minimum or 0,
                math.ceil(parse_bytes(minimum_memory) / self._memory_per_worker()),
            )
        if maximum_cores is not None:
            maximum = min(
                maximum, math.floor(maximum_cores / self._threads_per_worker())
            )
        if maximum_memory is not None:
            maximum = min(
                maximum,
                math.floor(parse_bytes(maximum_memory) / self._memory_per_worker()),
            )

        return super().adapt(
            Adaptive=Adaptive, minimum=minimum, maximum=maximum, **kwargs
        )

    @classmethod
    def from_name(cls, name: str) -> ProcessInterface:
        """Create an instance of this class to represent an existing cluster by name."""
        raise NotImplementedError()


def init_spec(spec: dict[str, Any], *args: Any) -> dict[str, Worker | Nanny]:
    workers = {}
    for k, d in spec.items():
        cls = d["cls"]
        if isinstance(cls, str):
            cls = import_term(cls)
        workers[k] = cls(*args, **d.get("opts", {}))
    return workers


async def run_spec(spec: dict[str, Any], *args: Any) -> dict[str, Worker | Nanny]:
    workers = init_spec(spec, *args)
    if workers:
        await asyncio.gather(*workers.values())
    return workers


@atexit.register
def close_clusters():
    for cluster in list(SpecCluster._instances):
        if getattr(cluster, "shutdown_on_close", False):
            with suppress(gen.TimeoutError, TimeoutError):
                if getattr(cluster, "status", Status.closed) != Status.closed:
                    cluster.close(timeout=10)
