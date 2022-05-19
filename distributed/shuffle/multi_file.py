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

from dask.sizeof import sizeof
from dask.utils import parse_bytes

from distributed.utils import log_errors


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

    def __init__(
        self,
        directory,
        dump=pickle.dump,
        load=pickle.load,
        sizeof=sizeof,
        loop=None,
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

        self.condition = asyncio.Condition()

        self.bytes_written = 0
        self.bytes_read = 0

        self._done = False
        self._futures = set()
        self.diagnostics = defaultdict(float)

        self._communicate_future = asyncio.create_task(self.communicate())
        self._loop = loop or asyncio.get_event_loop()
        self._exception = None

    @property
    def queue(self):
        try:
            return MultiFile._queues[self._loop]
        except KeyError:
            queue = asyncio.Queue()
            for _ in range(MultiFile.concurrent_files):
                queue.put_nowait(None)
            MultiFile._queues[self._loop] = queue
            return queue

    async def put(self, data: dict[str, list[object]]):
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

        this_size = 0
        for id, shard in data.items():
            size = self.sizeof(shard)
            self.shards[id].extend(shard)
            self.sizes[id] += size
            self.total_size += size
            MultiFile.total_size += size
            self.total_received += size
            this_size += size

        del data, shard

        while MultiFile.total_size > MultiFile.memory_limit:
            with self.time("waiting-on-memory"):
                async with self.condition:

                    try:
                        await asyncio.wait_for(
                            self.condition.wait(), 1
                        )  # Block until memory calms down
                    except asyncio.TimeoutError:
                        continue

    async def communicate(self):
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

                    await self.queue.get()

                id = max(self.sizes, key=self.sizes.get)
                shards = self.shards.pop(id)
                size = self.sizes.pop(id)

                future = asyncio.create_task(self.process(id, shards, size))
                del shards
                self._futures.add(future)
                async with self.condition:
                    self.condition.notify()

    async def process(self, id: str, shards: list, size: int):
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
            async with self.condition:
                self.condition.notify()
            await self.queue.put(None)

    def read(self, id):
        """Read a complete file back into memory"""
        if self._exception:
            raise self._exception
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

    async def flush(self):
        """Wait until all writes are finished"""
        if self._exception:
            await self._communicate_future
            await asyncio.gather(*self._futures)
            raise self._exception
        while self.shards:
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._futures)
        if all(future.done() for future in self._futures):
            self._futures.clear()

        assert not self.total_size

        self._done = True

        await self._communicate_future

    def close(self):
        self._done = True
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.directory)

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, traceback):
        self.close()

    @contextlib.contextmanager
    def time(self, name: str):
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start
