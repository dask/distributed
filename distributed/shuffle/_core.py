from __future__ import annotations

import abc
import asyncio
import contextlib
import itertools
import time
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, NewType, TypeVar

import dask.config
from dask.typing import Key
from dask.utils import parse_timedelta

from distributed.core import PooledRPCCall
from distributed.exceptions import Reschedule
from distributed.protocol import to_serialize
from distributed.shuffle._comms import CommShardsBuffer
from distributed.shuffle._disk import DiskShardsBuffer
from distributed.shuffle._exceptions import ShuffleClosedError
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._memory import MemoryShardsBuffer
from distributed.utils_comm import retry

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import ParamSpec, TypeAlias

    _P = ParamSpec("_P")

    # circular dependencies
    from distributed.shuffle._scheduler_plugin import ShuffleSchedulerPlugin
    from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin

ShuffleId = NewType("ShuffleId", str)
NDIndex: TypeAlias = tuple[int, ...]


_T_partition_id = TypeVar("_T_partition_id")
_T_partition_type = TypeVar("_T_partition_type")
_T = TypeVar("_T")


class ShuffleRun(Generic[_T_partition_id, _T_partition_type]):
    id: ShuffleId
    run_id: int
    local_address: str
    executor: ThreadPoolExecutor
    rpc: Callable[[str], PooledRPCCall]
    scheduler: PooledRPCCall
    closed: bool
    _disk_buffer: DiskShardsBuffer | MemoryShardsBuffer
    _comm_buffer: CommShardsBuffer
    diagnostics: dict[str, float]

    def __init__(
        self,
        id: ShuffleId,
        run_id: int,
        local_address: str,
        directory: str,
        executor: ThreadPoolExecutor,
        rpc: Callable[[str], PooledRPCCall],
        scheduler: PooledRPCCall,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
        disk: bool,
    ):
        self.id = id
        self.run_id = run_id
        self.local_address = local_address
        self.executor = executor
        self.rpc = rpc
        self.scheduler = scheduler
        self.closed = False
        if disk:
            self._disk_buffer = DiskShardsBuffer(
                directory=directory,
                read=self.read,
                memory_limiter=memory_limiter_disk,
            )
        else:
            self._disk_buffer = MemoryShardsBuffer(deserialize=self.deserialize)

        self._comm_buffer = CommShardsBuffer(
            send=self.send, memory_limiter=memory_limiter_comms
        )
        # TODO: reduce number of connections to number of workers
        # MultiComm.max_connections = min(10, n_workers)

        self.diagnostics: dict[str, float] = defaultdict(float)
        self.transferred = False
        self.received: set[_T_partition_id] = set()
        self.total_recvd = 0
        self.start_time = time.time()
        self._exception: Exception | None = None
        self._closed_event = asyncio.Event()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: id={self.id!r}, run_id={self.run_id!r}, local_address={self.local_address!r}, closed={self.closed!r}, transferred={self.transferred!r}>"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.id}[{self.run_id}]> on {self.local_address}"

    def __hash__(self) -> int:
        return self.run_id

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start

    async def barrier(self, run_ids: Sequence[int]) -> int:
        self.raise_if_closed()
        consistent = all(run_id == self.run_id for run_id in run_ids)
        # TODO: Consider broadcast pinging once when the shuffle starts to warm
        # up the comm pool on scheduler side
        await self.scheduler.shuffle_barrier(
            id=self.id, run_id=self.run_id, consistent=consistent
        )
        return self.run_id

    async def _send(
        self, address: str, shards: list[tuple[_T_partition_id, bytes]]
    ) -> None:
        self.raise_if_closed()
        return await self.rpc(address).shuffle_receive(
            data=to_serialize(shards),
            shuffle_id=self.id,
            run_id=self.run_id,
        )

    async def send(
        self, address: str, shards: list[tuple[_T_partition_id, bytes]]
    ) -> None:
        retry_count = dask.config.get("distributed.p2p.comm.retry.count")
        retry_delay_min = parse_timedelta(
            dask.config.get("distributed.p2p.comm.retry.delay.min"), default="s"
        )
        retry_delay_max = parse_timedelta(
            dask.config.get("distributed.p2p.comm.retry.delay.max"), default="s"
        )
        return await retry(
            partial(self._send, address, shards),
            count=retry_count,
            delay_min=retry_delay_min,
            delay_max=retry_delay_max,
        )

    async def offload(
        self, func: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        self.raise_if_closed()
        with self.time("cpu"):
            return await asyncio.get_running_loop().run_in_executor(
                self.executor, partial(func, *args, **kwargs)
            )

    def heartbeat(self) -> dict[str, Any]:
        comm_heartbeat = self._comm_buffer.heartbeat()
        comm_heartbeat["read"] = self.total_recvd
        return {
            "disk": self._disk_buffer.heartbeat(),
            "comm": comm_heartbeat,
            "diagnostics": self.diagnostics,
            "start": self.start_time,
        }

    async def _write_to_comm(
        self, data: dict[str, tuple[_T_partition_id, bytes]]
    ) -> None:
        self.raise_if_closed()
        await self._comm_buffer.write(data)

    async def _write_to_disk(self, data: dict[NDIndex, bytes]) -> None:
        self.raise_if_closed()
        await self._disk_buffer.write(
            {"_".join(str(i) for i in k): v for k, v in data.items()}
        )

    def raise_if_closed(self) -> None:
        if self.closed:
            if self._exception:
                raise self._exception
            raise ShuffleClosedError(f"{self} has already been closed")

    async def inputs_done(self) -> None:
        self.raise_if_closed()
        self.transferred = True
        await self._flush_comm()
        try:
            self._comm_buffer.raise_on_exception()
        except Exception as e:
            self._exception = e
            raise

    async def _flush_comm(self) -> None:
        self.raise_if_closed()
        await self._comm_buffer.flush()

    async def flush_receive(self) -> None:
        self.raise_if_closed()
        await self._disk_buffer.flush()

    async def close(self) -> None:
        if self.closed:  # pragma: no cover
            await self._closed_event.wait()
            return

        self.closed = True
        await self._comm_buffer.close()
        await self._disk_buffer.close()
        self._closed_event.set()

    def fail(self, exception: Exception) -> None:
        if not self.closed:
            self._exception = exception

    def _read_from_disk(self, id: NDIndex) -> list[Any]:  # TODO: Typing
        self.raise_if_closed()
        return self._disk_buffer.read("_".join(str(i) for i in id))

    async def receive(self, data: list[tuple[_T_partition_id, bytes]]) -> None:
        await self._receive(data)

    async def _ensure_output_worker(self, i: _T_partition_id, key: str) -> None:
        assigned_worker = self._get_assigned_worker(i)

        if assigned_worker != self.local_address:
            result = await self.scheduler.shuffle_restrict_task(
                id=self.id, run_id=self.run_id, key=key, worker=assigned_worker
            )
            if result["status"] == "error":
                raise RuntimeError(result["message"])
            assert result["status"] == "OK"
            raise Reschedule()

    @abc.abstractmethod
    def _get_assigned_worker(self, i: _T_partition_id) -> str:
        """Get the address of the worker assigned to the output partition"""

    @abc.abstractmethod
    async def _receive(self, data: list[tuple[_T_partition_id, bytes]]) -> None:
        """Receive shards belonging to output partitions of this shuffle run"""

    async def add_partition(
        self, data: _T_partition_type, partition_id: _T_partition_id
    ) -> int:
        self.raise_if_closed()
        if self.transferred:
            raise RuntimeError(f"Cannot add more partitions to {self}")
        return await self._add_partition(data, partition_id)

    @abc.abstractmethod
    async def _add_partition(
        self, data: _T_partition_type, partition_id: _T_partition_id
    ) -> int:
        """Add an input partition to the shuffle run"""

    async def get_output_partition(
        self, partition_id: _T_partition_id, key: str, **kwargs: Any
    ) -> _T_partition_type:
        self.raise_if_closed()
        await self._ensure_output_worker(partition_id, key)
        if not self.transferred:
            raise RuntimeError("`get_output_partition` called before barrier task")
        await self.flush_receive()
        return await self._get_output_partition(partition_id, key, **kwargs)

    @abc.abstractmethod
    async def _get_output_partition(
        self, partition_id: _T_partition_id, key: str, **kwargs: Any
    ) -> _T_partition_type:
        """Get an output partition to the shuffle run"""

    @abc.abstractmethod
    def read(self, path: Path) -> tuple[Any, int]:
        """Read shards from disk"""

    @abc.abstractmethod
    def deserialize(self, buffer: bytes) -> Any:
        """Deserialize shards"""


def get_worker_plugin() -> ShuffleWorkerPlugin:
    from distributed import get_worker

    try:
        worker = get_worker()
    except ValueError as e:
        raise RuntimeError(
            "`shuffle='p2p'` requires Dask's distributed scheduler. This task is not running on a Worker; "
            "please confirm that you've created a distributed Client and are submitting this computation through it."
        ) from e
    try:
        return worker.plugins["shuffle"]  # type: ignore
    except KeyError as e:
        raise RuntimeError(
            f"The worker {worker.address} does not have a P2P shuffle plugin."
        ) from e


_BARRIER_PREFIX = "shuffle-barrier-"


def barrier_key(shuffle_id: ShuffleId) -> str:
    return _BARRIER_PREFIX + shuffle_id


def id_from_key(key: Key) -> ShuffleId | None:
    if not isinstance(key, str) or not key.startswith(_BARRIER_PREFIX):
        return None
    return ShuffleId(key[len(_BARRIER_PREFIX) :])


class ShuffleType(Enum):
    DATAFRAME = "DataFrameShuffle"
    ARRAY_RECHUNK = "ArrayRechunk"


@dataclass(frozen=True)
class ShuffleRunSpec(Generic[_T_partition_id]):
    run_id: int = field(init=False, default_factory=partial(next, itertools.count(1)))  # type: ignore
    spec: ShuffleSpec
    worker_for: dict[_T_partition_id, str]

    @property
    def id(self) -> ShuffleId:
        return self.spec.id


@dataclass(frozen=True)
class ShuffleSpec(abc.ABC, Generic[_T_partition_id]):
    id: ShuffleId
    disk: bool

    def create_new_run(
        self,
        plugin: ShuffleSchedulerPlugin,
    ) -> SchedulerShuffleState:
        worker_for = self._pin_output_workers(plugin)
        return SchedulerShuffleState(
            run_spec=ShuffleRunSpec(spec=self, worker_for=worker_for),
            participating_workers=set(worker_for.values()),
        )

    @abc.abstractmethod
    def _pin_output_workers(
        self, plugin: ShuffleSchedulerPlugin
    ) -> dict[_T_partition_id, str]:
        """Pin output tasks to workers and return the mapping of partition ID to worker."""

    @abc.abstractmethod
    def create_run_on_worker(
        self,
        run_id: int,
        worker_for: dict[_T_partition_id, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        """Create the new shuffle run on the worker."""


@dataclass(eq=False)
class SchedulerShuffleState(Generic[_T_partition_id]):
    run_spec: ShuffleRunSpec
    participating_workers: set[str]
    _archived_by: str | None = field(default=None, init=False)

    @property
    def id(self) -> ShuffleId:
        return self.run_spec.id

    @property
    def run_id(self) -> int:
        return self.run_spec.run_id

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.id}[{self.run_id}]>"

    def __hash__(self) -> int:
        return hash(self.run_id)


@contextlib.contextmanager
def handle_transfer_errors(id: ShuffleId) -> Iterator[None]:
    try:
        yield
    except ShuffleClosedError:
        raise Reschedule()
    except Exception as e:
        raise RuntimeError(f"P2P shuffling {id} failed during transfer phase") from e


@contextlib.contextmanager
def handle_unpack_errors(id: ShuffleId) -> Iterator[None]:
    try:
        yield
    except Reschedule as e:
        raise e
    except ShuffleClosedError:
        raise Reschedule()
    except Exception as e:
        raise RuntimeError(f"P2P shuffling {id} failed during unpack phase") from e
