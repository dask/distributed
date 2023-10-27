from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterator, Sized
from typing import Any, Generic, TypeVar

from dask.utils import parse_bytes

from distributed.metrics import time
from distributed.shuffle._disk import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.sizeof import sizeof
from distributed.utils import log_errors


class NewCommBuffer:
    def __init__(self, memory_limiter, max_message_size, send) -> None:
        self._accepts_input = True
        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self._exception = None
        self.concurrency_limit = 20
        self._inputs_done = False
        self.memory_limiter = memory_limiter
        self.diagnostics: dict[str, float] = defaultdict(float)

        self._shards_available = asyncio.Condition()
        self._flush_lock = asyncio.Lock()
        self.max_message_size = max_message_size
        self._send = send

        self.bytes_total = 0
        self.bytes_memory = 0
        self.bytes_written = 0
        self.bytes_read = 0

    def heartbeat(self) -> dict[str, Any]:
        return {
            "memory": self.bytes_memory,
            "total": self.bytes_total,
            "buckets": len(self.shards),
            "written": self.bytes_written,
            "read": self.bytes_read,
            "diagnostics": self.diagnostics,
            "memory_limit": self.memory_limiter.limit,
        }

    async def write(self, data) -> None:
        """
        Writes objects into the local buffers, blocks until ready for more

        Parameters
        ----------
        data: dict
            A dictionary mapping destinations to the object that should
            be written to that destination

        Notes
        -----
        If this buffer has a memory limiter configured, then it will
        apply back-pressure to the sender (blocking further receives)
        if local resource usage hits the limit, until such time as the
        resource usage drops.

        """

        if self._exception:
            raise self._exception
        if not self._accepts_input or self._inputs_done:
            raise RuntimeError(f"Trying to put data in closed {self}.")

        if not data:
            return

        sizes = {worker: sizeof(shard) for worker, shard in data.items()}
        total_batch_size = sum(sizes.values())
        self.bytes_memory += total_batch_size
        self.bytes_total += total_batch_size

        self.memory_limiter.increase(total_batch_size)
        async with self._shards_available:
            for worker, shard in data.items():
                self.shards[worker].append(shard)
                self.sizes[worker] += sizes[worker]
            self._shards_available.notify()
        # await self.memory_limiter.wait_for_available()
        del data
        assert total_batch_size
        while sum(self.sizes.values()) > self.memory_limiter.limit:
            await self.flush_one()

    async def flush_one(self):
        if not self.sizes:
            return
        max_ = max(self.sizes.values())
        for k, size in self.sizes.items():
            if size == max_:
                data = self.shards.pop(k)
                del self.sizes[k]
                try:
                    return await self._send(k, data)
                finally:
                    await self.memory_limiter.decrease(size)
                    self.bytes_memory -= size
                    self.bytes_written += size


class CommShardsBuffer(ShardsBuffer):
    """Accept, buffer, and send many small messages to many workers

    This takes in lots of small messages destined for remote workers, buffers
    those messages in memory, and then sends out batches of them when possible
    to different workers.  This tries to send larger messages when possible,
    while also respecting a memory bound

    **State**

    -   shards: dict[str, list[ShardType]]

        This is our in-memory buffer of data waiting to be sent to other workers.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and send data from that buffer

    State
    -----
    max_message_size: int
        The maximum size in bytes of a single message that we want to send

    Parameters
    ----------
    send : callable
        How to send a list of shards to a worker
        Expects an address of the target worker (string)
        and a payload of shards (list of bytes) to send to that worker
    memory_limiter : ResourceLimiter
        Limiter for memory usage (in bytes). If the incoming data that
        has yet to be processed exceeds this limit, then the buffer will
        block until below the threshold. See :meth:`.write` for the
        implementation of this scheme.
    concurrency_limit : int
        Number of background tasks to run.
    """

    max_message_size = parse_bytes("2 MiB")

    def __init__(
        self,
        send: Callable[[str, list[tuple[Any, bytes]]], Awaitable[None]],
        memory_limiter: ResourceLimiter,
        concurrency_limit: int = 10,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            concurrency_limit=concurrency_limit,
            max_message_size=CommShardsBuffer.max_message_size,
        )
        self.send = send

    async def _process(self, address: str, shards: list[tuple[Any, bytes]]) -> None:
        """Send one message off to a neighboring worker"""
        with log_errors():
            # Consider boosting total_size a bit here to account for duplication
            with self.time("send"):
                await self.send(address, shards)
