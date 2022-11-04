from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import pathlib
import pickle
import shutil
import weakref
from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, BinaryIO

from tornado.ioloop import IOLoop

from dask.sizeof import sizeof

from distributed.metrics import time
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger("distributed.shuffle")


class ShardBuffer:

    shards: defaultdict[str, list[pa.Table]]
    sizes: defaultdict[str, int]
    _exception: None | Exception

    _queues: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()

    @property
    def _instances(self) -> set:
        raise NotImplementedError()

    def __init__(
        self,
        sizeof: Callable,
        memory_limiter: ResourceLimiter | None,
        concurrency_limit: int = 2,
    ) -> None:
        self._closed = False
        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self.sizeof = sizeof
        self._exception = None
        self.concurrency_limit = concurrency_limit
        self.total_received = 0
        self._done = False
        self.total_size = 0
        self.memory_limiter = memory_limiter
        self.diagnostics: dict[str, float] = defaultdict(float)
        self._tasks = [
            asyncio.create_task(self.worker()) for _ in range(concurrency_limit)
        ]
        self._shards_available = asyncio.Condition()
        self._flush_lock = asyncio.Lock()

    async def process(self, id: str, shards: list[pa.Table], size: int) -> None:
        try:
            start = time()
            try:
                await self._process(id, shards, size)

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
                await self.memory_limiter.release(size)
            self.total_size -= size

    async def _process(self, id: str, shards: list[pa.Table], size: int) -> None:
        raise NotImplementedError()

    def read(self, id: int | str) -> pa.Table:
        raise NotImplementedError()

    @property
    def empty(self) -> bool:
        return not self.shards

    async def worker(self) -> None:
        def _continue() -> bool:
            return bool(self.shards or self._done)

        while True:
            async with self._shards_available:
                await self._shards_available.wait_for(_continue)
                if self._done and not self.shards:
                    print("Terminating worker")
                    break
                part_id = max(self.sizes, key=self.sizes.__getitem__)
                shards = self.shards.pop(part_id)
                size = self.sizes.pop(part_id)
                self._shards_available.notify_all()
            await self.process(part_id, shards, size)

    async def put(self, data: dict[str, list]) -> None:
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
            size = self.sizeof(shards)
            sizes[id_] = size
        total_batch_size = sum(sizes.values())
        self.total_size += total_batch_size

        if self.memory_limiter:
            self.memory_limiter.acquire(total_batch_size)
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
                await self._shards_available.wait_for(
                    lambda: not self.shards or self._exception or self._done
                )
                self._done = True
                self._shards_available.notify_all()

            await asyncio.gather(*self._tasks)
            if not self._exception:
                assert not self.total_size, (type(self), self.total_size)

    async def close(self) -> None:
        await self.flush()
        if not self._exception:
            assert not self.total_size, (type(self), self.total_size)
        for t in self._tasks:
            t.cancel()
        self._closed = True
        self._done = True
        self.shards.clear()
        self.total_size = 0
        async with self._shards_available:
            self._shards_available.notify_all()
        await asyncio.gather(*self._tasks)

        # Exceptions may hold a ref to this s.t. it is never clenaned up
        try:
            del type(self)._queues[IOLoop.current()]
        except KeyError:
            pass

    async def __aenter__(self) -> "ShardBuffer":
        return self

    async def __aexit__(self, exc: Any, typ: Any, traceback: Any) -> None:
        await self.close()

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time()
        yield
        stop = time()
        self.diagnostics[name] += stop - start


class MultiFile(ShardBuffer):
    """Accept, buffer, and write many small objects to many files

    This takes in lots of small objects, writes them to a local directory, and
    then reads them back when all writes are complete.  It buffers these
    objects in memory so that it can optimize disk access for larger writes.

    **State**

    -   shards: dict[str, list[bytes]]

        This is our in-memory buffer of data waiting to be written to files.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and write data from that buffer

    State
    -----
    memory_limit: str
        A maximum amount of memory to use, like "1 GiB"

    Parameters
    ----------
    directory: pathlib.Path
        Where to write and read data.  Ideally points to fast disk.
    dump: callable
        Writes an object to a file, like pickle.dump
    load: callable
        Reads an object from that file, like pickle.load
    sizeof: callable
        Measures the size of an object in memory
    """

    _queues: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
    concurrency_limit = 2
    concurrent_files = concurrency_limit

    def __init__(
        self,
        directory: str,
        memory_limiter: ResourceLimiter,
        dump: Callable[[Any, BinaryIO], None] = pickle.dump,
        load: Callable[[BinaryIO], Any] = pickle.load,
        sizeof: Callable[[list[pa.Table]], int] = sizeof,
    ):
        super().__init__(sizeof=sizeof, memory_limiter=memory_limiter)
        self.directory = pathlib.Path(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        self.dump = dump
        self.load = load

        self.bytes_written = 0
        self.bytes_read = 0

    async def _process(self, id: str, shards: list[pa.Table], size: int) -> None:
        """Write one buffer to file

        This function was built to offload the disk IO, but since then we've
        decided to keep this within the event loop (disk bandwidth should be
        prioritized, and writes are typically small enough to not be a big
        deal).

        Most of the logic here is about possibly going back to a separate
        thread, or about diagnostics.  If things don't change much in the
        future then we should consider simplifying this considerably and
        dropping the write into communicate above.
        """

        with log_errors():
            # Consider boosting total_size a bit here to account for duplication
            with self.time("write"):
                with open(
                    self.directory / str(id), mode="ab", buffering=100_000_000
                ) as f:
                    for shard in shards:
                        self.dump(shard, f)
                    # os.fsync(f)  # TODO: maybe?
            self.bytes_written += size

    def read(self, id: int | str) -> pa.Table:
        """Read a complete file back into memory"""
        self.raise_on_exception()
        if not self._done:
            raise RuntimeError("Tried to read from file before done.")
        parts = []

        try:
            with self.time("read"):
                with open(
                    self.directory / str(id), mode="rb", buffering=100_000_000
                ) as f:
                    while True:
                        try:
                            parts.append(self.load(f))
                        except EOFError:
                            break
                    size = f.tell()
        except FileNotFoundError:
            raise KeyError(id)

        # TODO: We could consider deleting the file at this point
        if parts:
            self.bytes_read += size
            assert len(parts) == 1
            return parts[0]
        else:
            raise KeyError(id)

    async def close(self) -> None:
        await super().close()
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.directory)
