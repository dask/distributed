from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import pickle
import shutil
import time
import weakref
from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, BinaryIO

from tornado.ioloop import IOLoop

from dask.sizeof import sizeof
from dask.utils import parse_bytes

from distributed.utils import log_errors

if TYPE_CHECKING:
    import pyarrow as pa


class MultiFile:
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

    memory_limit = parse_bytes("1 GiB")
    _queues: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
    concurrent_files = 2
    total_size = 0

    shards: defaultdict[str, list]
    sizes: defaultdict[str, int]
    _tasks: set[asyncio.Future]
    diagnostics: defaultdict[str, float]
    _exception: Exception | None

    def __init__(
        self,
        directory: str,
        loop: IOLoop,
        dump: Callable[[Any, BinaryIO], None] = pickle.dump,
        load: Callable[[BinaryIO], Any] = pickle.load,
        sizeof: Callable[[list[pa.Table]], int] = sizeof,
    ):
        self.directory = pathlib.Path(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        self.dump = dump
        self.load = load
        self.sizeof = sizeof

        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self.total_size = 0
        self.total_received = 0

        self._wait_on_memory = asyncio.Condition()

        self.bytes_written = 0
        self.bytes_read = 0

        self._done = False
        self._tasks = set()
        self.diagnostics = defaultdict(float)

        self._communicate_future = asyncio.create_task(self.communicate())
        self._loop = loop
        self._exception = None

    @property
    def _queue(self) -> asyncio.Queue[None]:
        try:
            return MultiFile._queues[self._loop]
        except KeyError:
            queue: asyncio.Queue[None] = asyncio.Queue()
            for _ in range(MultiFile.concurrent_files):
                queue.put_nowait(None)
            MultiFile._queues[self._loop] = queue
            return queue

    async def put(self, data: dict[str, list[pa.Table]]) -> None:
        """
        Writes many objects into the local buffers, blocks until ready for more

        Parameters
        ----------
        data: dict
            A dictionary mapping destinations to lists of objects that should
            be written to that destination
        """
        if self._exception:
            await self._maybe_raise_exception()

        this_size = 0
        for id, shards in data.items():
            size = self.sizeof(shards)
            self.shards[id].extend(shards)
            self.sizes[id] += size
            self.total_size += size
            MultiFile.total_size += size
            self.total_received += size
            this_size += size

        del data, shards

        while MultiFile.total_size > MultiFile.memory_limit:
            with self.time("waiting-on-memory"):
                await self._maybe_raise_exception()
                async with self._wait_on_memory:

                    try:
                        await asyncio.wait_for(
                            self._wait_on_memory.wait(), 1
                        )  # Block until memory calms down
                    except asyncio.TimeoutError:
                        continue

    async def communicate(self) -> None:
        """
        Continuously find the largest batch and trigger writes

        We keep ``concurrent_files`` files open, writing while we still have any data
        as an old write finishes, we find the next largest buffer, and write
        its contents to file.

        We do this until we're done.  This coroutine runs in the background.

        See Also
        --------
        process: does the actual writing
        """
        with log_errors():

            while not self._done:
                with self.time("idle"):
                    if not self.shards:
                        await asyncio.sleep(0.1)
                        continue

                    await self._queue.get()
                # Shards must only be mutated below
                assert self.shards, "MultiFile.shards was mutated unexpectedly"
                id = max(self.sizes, key=self.sizes.__getitem__)
                shards = self.shards.pop(id)
                size = self.sizes.pop(id)
                task = asyncio.create_task(self.process(id, shards, size))
                del shards
                self._tasks.add(task)

                def _reset_count(task: asyncio.Task) -> None:
                    self._tasks.discard(task)
                    self._queue.put_nowait(None)

                task.add_done_callback(_reset_count)
                async with self._wait_on_memory:
                    self._wait_on_memory.notify()

    async def process(self, id: str, shards: list[pa.Table], size: int) -> None:
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
            start = time.time()
            try:
                with self.time("write"):
                    with open(
                        self.directory / str(id), mode="ab", buffering=100_000_000
                    ) as f:
                        for shard in shards:
                            self.dump(shard, f)
                        # os.fsync(f)  # TODO: maybe?
            except Exception as e:
                self._exception = e
                self._done = True

            stop = time.time()

            self.diagnostics["avg_size"] = (
                0.98 * self.diagnostics["avg_size"] + 0.02 * size
            )
            self.diagnostics["avg_duration"] = 0.98 * self.diagnostics[
                "avg_duration"
            ] + 0.02 * (stop - start)

            self.bytes_written += size
            self.total_size -= size
            MultiFile.total_size -= size
            async with self._wait_on_memory:
                self._wait_on_memory.notify()

    def read(self, id: int | str) -> pa.Table:
        """Read a complete file back into memory"""
        if self._exception:
            raise self._exception
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

    async def _maybe_raise_exception(self) -> None:
        if self._exception:
            assert self._done
            await self._communicate_future
            await asyncio.gather(*self._tasks)
            raise self._exception

    async def flush(self) -> None:
        """Wait until all writes are finished"""

        while self.shards:
            await self._maybe_raise_exception()
            # If an exception arises while we're sleeping here we deadlock
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._tasks)
        await self._maybe_raise_exception()

        assert not self.total_size

        self._done = True
        await self._communicate_future

    async def close(self) -> None:
        try:
            # XXX If there is an exception this will raise again during
            # teardown. I don't think this is what we want to. Likely raising
            # the exception on flushing is not ideal
            if not self._done:
                await self.flush()
        finally:
            with contextlib.suppress(FileNotFoundError):
                shutil.rmtree(self.directory)

    async def __aenter__(self) -> MultiFile:
        return self

    async def __aexit__(self, exc: Any, typ: Any, traceback: Any) -> None:
        await self.close()

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start
