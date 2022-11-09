from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Generic, Sized, TypeVar

from distributed.metrics import time
from distributed.shuffle._limiter import ResourceLimiter

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger("distributed.shuffle")

ShardType = TypeVar("ShardType", bound=Sized)


class ShardsBuffer(Generic[ShardType]):

    shards: defaultdict[str, list[ShardType]]
    sizes: defaultdict[str, int]
    _exception: None | Exception

    def __init__(
        self,
        memory_limiter: ResourceLimiter | None,
        # TODO: Set concurrency to 1 for disk since it cannot work with asyncio (yet)
        concurrency_limit: int = 2,
        max_message_size: int = -1,
    ) -> None:
        self._closed = False
        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self._exception = None
        self.concurrency_limit = concurrency_limit
        self._done = False
        self.memory_limiter = memory_limiter
        self.diagnostics: dict[str, float] = defaultdict(float)
        self._tasks = [
            asyncio.create_task(self._background_task())
            for _ in range(concurrency_limit)
        ]
        self._shards_available = asyncio.Condition()
        self._flush_lock = asyncio.Lock()
        self.max_message_size = max_message_size

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
            "memory_limit": self.memory_limiter._maxvalue if self.memory_limiter else 0,
        }

    async def process(self, id: str, shards: list[pa.Table], size: int) -> None:
        try:
            start = time()
            try:
                await self._process(id, shards)
                self.bytes_written += size

            except Exception as e:
                self._exception = e
                self._done = True
            stop = time()

            self.diagnostics["avg_size"] = (
                0.98 * self.diagnostics["avg_size"] + 0.02 * size
            )
            self.diagnostics["avg_duration"] = 0.98 * self.diagnostics[
                "avg_duration"
            ] + 0.02 * (stop - start)
        finally:
            if self.memory_limiter:
                await self.memory_limiter.decrease(size)
            self.bytes_memory -= size

    async def _process(self, id: str, shards: list[ShardType]) -> None:
        raise NotImplementedError()

    def read(self, id: str) -> ShardType:
        raise NotImplementedError()

    @property
    def empty(self) -> bool:
        return not self.shards

    async def _background_task(self) -> None:
        def _continue() -> bool:
            return bool(self.shards or self._done)

        while True:
            async with self._shards_available:
                await self._shards_available.wait_for(_continue)
                if self._done and not self.shards:
                    break
                part_id = max(self.sizes, key=self.sizes.__getitem__)
                if self.max_message_size > 0:
                    size = 0
                    shards = []
                    while size < self.max_message_size:
                        try:
                            shard = self.shards[part_id].pop()
                            shards.append(shard)
                            s = len(shard)
                            size += s
                            self.sizes[part_id] -= s
                        except IndexError:
                            break
                        finally:
                            if not self.shards[part_id]:
                                del self.shards[part_id]
                                assert not self.sizes[part_id]
                                del self.sizes[part_id]
                else:
                    shards = self.shards.pop(part_id)
                    size = self.sizes.pop(part_id)
                self._shards_available.notify_all()
            await self.process(part_id, shards, size)

    async def put(self, data: dict[str, list[ShardType]]) -> None:
        """
        Writes many objects into the local buffers, blocks until ready for more

        Parameters
        ----------
        data: dict
            A dictionary mapping destinations to lists of objects that should
            be written to that destination
        """

        if self._exception:
            raise self._exception
        if self._closed or self._done:
            raise RuntimeError(f"Trying to put data in closed {self}.")

        if not data:
            return

        shards = None
        size = 0

        sizes = {}
        for id_, shards in data.items():
            size = sum(map(len, shards))
            sizes[id_] = size
        total_batch_size = sum(sizes.values())
        self.bytes_memory += total_batch_size
        self.bytes_total += total_batch_size

        if self.memory_limiter:
            self.memory_limiter.increase(total_batch_size)
        async with self._shards_available:
            for id_, shards in data.items():
                self.shards[id_].extend(shards)
                self.sizes[id_] += sizes[id_]
            self._shards_available.notify()
        if self.memory_limiter:
            await self.memory_limiter.wait_for_available()
        del data, shards
        assert size

    def raise_on_exception(self) -> None:
        if self._exception:
            raise self._exception

    async def flush(self) -> None:
        """Wait until all writes are finished"""
        async with self._flush_lock:
            self._closed = True
            async with self._shards_available:
                self._shards_available.notify_all()
                await self._shards_available.wait_for(
                    lambda: not self.shards or self._exception or self._done
                )
                self._done = True
                self._shards_available.notify_all()

            await asyncio.gather(*self._tasks)
            if not self._exception:
                assert not self.bytes_memory, (type(self), self.bytes_memory)

    async def close(self) -> None:
        await self.flush()
        if not self._exception:
            assert not self.bytes_memory, (type(self), self.bytes_memory)
        for t in self._tasks:
            t.cancel()
        self._closed = True
        self._done = True
        self.shards.clear()
        self.bytes_memory = 0
        async with self._shards_available:
            self._shards_available.notify_all()
        await asyncio.gather(*self._tasks)

    async def __aenter__(self) -> "ShardsBuffer":
        return self

    async def __aexit__(self, exc: Any, typ: Any, traceback: Any) -> None:
        await self.close()

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time()
        yield
        stop = time()
        self.diagnostics[name] += stop - start
