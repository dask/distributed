from __future__ import annotations

import asyncio
import contextlib
import threading
import time
import weakref
from collections import defaultdict
from collections.abc import Iterator
from typing import Any, Awaitable, Callable, Sequence

from tornado.ioloop import IOLoop

from dask.utils import parse_bytes

from distributed.utils import log_errors


class MultiComm:
    """Accept, buffer, and send many small messages to many workers

    This takes in lots of small messages destined for remote workers, buffers
    those messages in memory, and then sends out batches of them when possible
    to different workers.  This tries to send larger messages when possible,
    while also respecting a memory bound

    **State**

    -   shards: dict[str, list[bytes]]

        This is our in-memory buffer of data waiting to be sent to other workers.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and send data from that buffer

    State
    -----
    memory_limit: str
        A maximum amount of memory to use across the process, like "1 GiB"
        This includes both data in shards and also in network communications
    max_connections: int
        The maximum number of connections to have out at once
    max_message_size: str
        The maximum size of a single message that we want to send
    queue: asyncio.Queue
        A queue holding tokens used to limit concurrency

    Parameters
    ----------
    send: callable
        How to send a list of shards to a worker
        Expects an address of the target worker (string)
        and a payload of shards (list of bytes) to send to that worker
    """

    max_message_size = parse_bytes("2 MiB")
    memory_limit = parse_bytes("100 MiB")
    max_connections = 10
    _queues: weakref.WeakKeyDictionary[
        IOLoop, asyncio.Queue[None]
    ] = weakref.WeakKeyDictionary()
    total_size = 0
    lock = threading.Lock()

    def __init__(
        self,
        send: Callable[[str, list[bytes]], Awaitable[None]],
        loop: IOLoop,
    ):
        self.send = send
        self.shards: dict[str, list[bytes]] = defaultdict(list)
        self.sizes: dict[str, int] = defaultdict(int)
        self.total_size = 0
        self.total_moved = 0
        self._wait_on_memory = threading.Condition()
        self._tasks: set[asyncio.Task] = set()
        self._done = False
        self.diagnostics: dict[str, float] = defaultdict(float)
        self._loop = loop
        self._communicate_task = asyncio.create_task(self.communicate())
        self._exception: Exception | None = None

    @property
    def queue(self) -> asyncio.Queue[None]:
        try:
            return MultiComm._queues[self._loop]
        except KeyError:
            queue: asyncio.Queue[None] = asyncio.Queue()
            for _ in range(MultiComm.max_connections):
                queue.put_nowait(None)
            MultiComm._queues[self._loop] = queue
            return queue

    def put(self, data: dict[str, Sequence[bytes]]) -> None:
        """
        Put a dict of shards into our buffers

        This is intended to be run from a worker thread, hence the synchronous
        nature and the lock.

        If we're out of space then we block in order to enforce backpressure.
        """
        if self._exception:
            raise self._exception
        if self._done:
            raise RuntimeError("Putting data on already closed comm.")
        with self.lock:
            for address, shards in data.items():
                size = sum(map(len, shards))
                self.shards[address].extend(shards)
                self.sizes[address] += size
                self.total_size += size
                MultiComm.total_size += size
                self.total_moved += size

        del data

        while MultiComm.total_size > MultiComm.memory_limit:
            with self.time("waiting-on-memory"):
                with self._wait_on_memory:
                    if self._exception:
                        raise self._exception
                    self._wait_on_memory.wait(1)  # Block until memory calms down

    async def communicate(self) -> None:
        """
        Continuously find the largest batch and send from there

        We keep ``max_connections`` comms running while we still have any data
        as an old comm finishes, we find the next largest buffer, pull off
        ``max_message_size`` data from it, and ship it to the target worker.

        We do this until we're done.  This coroutine runs in the background.

        See Also
        --------
        process: does the actual writing
        """

        while not self._done:
            with self.time("idle"):
                await self._maybe_raise_exception()
                if not self.shards:
                    await asyncio.sleep(0.1)
                    continue

                await self.queue.get()

            with self.lock:
                address = max(self.sizes, key=self.sizes.__getitem__)

                size = 0
                shards = []
                while size < self.max_message_size:
                    try:
                        shard = self.shards[address].pop()
                        shards.append(shard)
                        s = len(shard)
                        size += s
                        self.sizes[address] -= s
                    except IndexError:
                        break
                    finally:
                        if not self.shards[address]:
                            del self.shards[address]
                            assert not self.sizes[address]
                            del self.sizes[address]

                assert set(self.sizes) == set(self.shards)
                assert shards
                task = asyncio.create_task(self._process(address, shards, size))
                del shards
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    async def _process(self, address: str, shards: list, size: int) -> None:
        """Send one message off to a neighboring worker"""
        with log_errors():

            # Consider boosting total_size a bit here to account for duplication

            try:
                start = time.time()
                try:
                    with self.time("send"):
                        await self.send(address, [b"".join(shards)])
                except Exception as e:
                    self._exception = e
                    self._done = True
                    raise
                stop = time.time()
                self.diagnostics["avg_size"] = (
                    0.95 * self.diagnostics["avg_size"] + 0.05 * size
                )
                self.diagnostics["avg_duration"] = 0.98 * self.diagnostics[
                    "avg_duration"
                ] + 0.02 * (stop - start)
            finally:
                with self.lock:
                    self.total_size -= size
                    MultiComm.total_size -= size
                with self._wait_on_memory:
                    self._wait_on_memory.notify()
                self.queue.put_nowait(None)

    async def _maybe_raise_exception(self) -> None:
        if self._exception:
            assert self._done
            await self._communicate_task
            await asyncio.gather(*self._tasks)
            raise self._exception

    async def flush(self) -> None:
        """
        We don't expect any more data, wait until everything is flushed through.

        Not thread safe. Caller must ensure this is not called concurrently with
        put
        """
        if self._exception:
            await self._maybe_raise_exception()

        while self.shards:
            await self._maybe_raise_exception()
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._tasks)
        await self._maybe_raise_exception()

        if self.total_size:
            raise RuntimeError("Received additional input after flushing.")
        self._done = True
        await self._communicate_task

    async def __aenter__(self) -> MultiComm:
        return self

    async def __aexit__(self, exc: Any, typ: Any, traceback: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if not self._done:
            await self.flush()
        self._done = True
        await self._communicate_task
        await asyncio.gather(*self._tasks)
        self.shards.clear()
        self.sizes.clear()

    @contextlib.contextmanager
    def time(self, name: str) -> Iterator[None]:
        start = time.time()
        yield
        stop = time.time()
        self.diagnostics[name] += stop - start
