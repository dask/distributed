import asyncio
import sys
import threading
from contextlib import suppress
from enum import Enum
from typing import Dict

from ..core import CommClosedError, Status, rpc
from ..scheduler import Scheduler
from ..utils import LoopRunner, import_term, sync, thread_state
from ..worker import Worker


class Role(Enum):
    """
    This Enum contains the various roles processes can be.
    """

    worker = "worker"
    scheduler = "scheduler"
    client = "client"


class Runner:
    """Superclass for runner objects.

    This class contains common functionality for Dask cluster runner classes.

    To implement this class, you must provide

    1.  A ``get_role`` method which returns a role from the ``Role`` enum.
    2.  A ``set_scheduler_address`` method for the scheduler process to communicate its address.
    3.  A ``get_scheduler_address`` method for all other processed to recieve the scheduler address.
    4.  Optionally, a ``get_worker_name`` to provide a platform specific name to the workers.
    5.  Optionally, a ``before_scheduler_start`` to perform any actions before the scheduler is created.
    6.  Optionally, a ``before_worker_start`` to perform any actions before the worker is created.
    7.  Optionally, a ``before_client_start`` to perform any actions before the client code continues.
    8.  Optionally, a ``on_scheduler_start`` to perform anything on the scheduler once it has started.
    9.  Optionally, a ``on_worker_start`` to perform anything on the worker once it has started.

    For that, you should get the following:

    A context manager and object which can be used within a script that is run in parallel to decide which processes
    run the scheduler, workers and client code.

    """

    def __init__(
        self,
        scheduler: bool = True,
        scheduler_options: Dict = None,
        worker_class: str = None,
        worker_options: Dict = None,
        client: bool = True,
        asynchronous: bool = False,
        loop: asyncio.BaseEventLoop = None,
    ):
        self.status = Status.created
        self.scheduler = scheduler
        self.scheduler_address = None
        self.scheduler_comm = None
        self.client = client
        self.scheduler_options = (
            scheduler_options if scheduler_options is not None else {}
        )
        self.worker_class = (
            Worker if worker_class is None else import_term(worker_class)
        )
        self.worker_options = worker_options if worker_options is not None else {}
        self.role = None
        self._asynchronous = asynchronous
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        if not self.asynchronous:
            self._loop_runner.start()
            self.sync(self._start)

    async def get_role(self) -> str:
        raise NotImplementedError()

    async def set_scheduler_address(self, scheduler: Scheduler) -> None:
        raise NotImplementedError()

    async def get_scheduler_address(self) -> str:
        raise NotImplementedError()

    async def get_worker_name(self) -> str:
        return None

    async def before_scheduler_start(self) -> None:
        return None

    async def before_worker_start(self) -> None:
        return None

    async def before_client_start(self) -> None:
        return None

    async def on_scheduler_start(self, scheduler: Scheduler) -> None:
        return None

    async def on_worker_start(self, worker: Worker) -> None:
        return None

    @property
    def asynchronous(self):
        return (
            self._asynchronous
            or getattr(thread_state, "asynchronous", False)
            or hasattr(self.loop, "_thread_identity")
            and self.loop._thread_identity == threading.get_ident()
        )

    def sync(self, func, *args, asynchronous=None, callback_timeout=None, **kwargs):
        asynchronous = asynchronous or self.asynchronous
        if asynchronous:
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = asyncio.wait_for(future, callback_timeout)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    def __await__(self):
        async def _await():
            if self.status != Status.running:
                await self._start()
            return self

        return _await().__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, *args):
        await self._close()

    def __enter__(self):
        return self.sync(self.__aenter__)

    def __exit__(self, typ, value, traceback):
        return self.sync(self.__aexit__)

    def __del__(self):
        if self.status != Status.closed:
            with suppress(AttributeError, RuntimeError):  # during closing
                self.loop.add_callback(self.close)

    async def _start(self) -> None:
        self.role = await self.get_role()
        if self.role == Role.scheduler:
            await self.start_scheduler()
            sys.exit(0)
        elif self.role == Role.worker:
            await self.start_worker()
            sys.exit(0)
        elif self.role == Role.client:
            self.scheduler_address = await self.get_scheduler_address()
            self.scheduler_comm = rpc(self.scheduler_address)
            await self.before_client_start()
        self.status = Status.running

    async def start_scheduler(self) -> None:
        await self.before_scheduler_start()
        async with Scheduler(**self.scheduler_options, loop=self.loop) as scheduler:
            await self.set_scheduler_address(scheduler)
            await self.on_scheduler_start(scheduler)
            await scheduler.finished()

    async def start_worker(self) -> None:
        if (
            "scheduler_file" not in self.worker_options
            and "scheduler_ip" not in self.worker_options
        ):
            self.worker_options["scheduler_ip"] = await self.get_scheduler_address()
        worker_name = await self.get_worker_name()
        await self.before_worker_start()
        async with self.worker_class(
            name=worker_name, **self.worker_options, loop=self.loop
        ) as worker:
            await self.on_worker_start(worker)
            await worker.finished()

    async def _close(self) -> None:
        print(f"stopping {self.role}")
        if self.status == Status.running:
            with suppress(CommClosedError):
                await self.scheduler_comm.terminate()
            self.status = Status.closed

    def close(self) -> None:
        return self.sync(self._close)
        self._loop_runner.stop()


class AsyncCommWorld:
    def __init__(self):
        self.roles = {"scheduler": None, "client": None}
        self.role_lock = asyncio.Lock()
        self.scheduler_address = None


class AsyncRunner(Runner):
    def __init__(self, commworld: AsyncCommWorld, *args, **kwargs):
        self.commworld = commworld
        super().__init__(*args, **kwargs)

    async def get_role(self) -> str:
        async with self.commworld.role_lock:
            if self.commworld.roles["scheduler"] is None and self.scheduler:
                self.commworld.roles["scheduler"] = self
                return Role.scheduler
            elif self.commworld.roles["client"] is None and self.client:
                self.commworld.roles["client"] = self
                return Role.client
            else:
                return Role.worker

    async def set_scheduler_address(self, scheduler: Scheduler) -> None:
        self.commworld.scheduler_address = scheduler.address

    async def get_scheduler_address(self) -> str:
        while self.commworld.scheduler_address is None:
            await asyncio.sleep(0.1)
        return self.commworld.scheduler_address
