import asyncio
import contextlib
import os
import pathlib
import pickle
import shutil
import time
from collections import defaultdict

from dask.sizeof import sizeof
from dask.utils import parse_bytes

from distributed.utils import log_errors


class MultiFile:
    memory_limit = parse_bytes("1 GiB")
    queue: asyncio.Queue = None
    concurrent_files = 2

    def __init__(
        self,
        directory,
        dump=pickle.dump,
        load=pickle.load,
        join=None,
        sizeof=sizeof,
    ):
        assert join
        self.directory = pathlib.Path(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        self.dump = dump
        self.load = load
        self.join = join
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
        self.active = set()
        self.diagnostics = defaultdict(float)

        if MultiFile.queue is None:
            MultiFile.queue = asyncio.Queue()
            for _ in range(MultiFile.concurrent_files):
                MultiFile.queue.put_nowait(None)

    async def put(self, data: dict):
        this_size = 0
        for id, shard in data.items():
            size = self.sizeof(shard)
            self.shards[id].extend(shard)
            self.sizes[id] += size
            self.total_size += size
            self.total_received += size
            this_size += size

        del data, shard

        while self.total_size > self.memory_limit:
            with self.time("waiting-on-memory"):
                async with self.condition:

                    try:
                        await asyncio.wait_for(
                            self.condition.wait(), 1
                        )  # Block until memory calms down
                    except asyncio.TimeoutError:
                        continue

    async def communicate(self):
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

                future = asyncio.ensure_future(self.process(id, shards, size))
                del shards
                self._futures.add(future)
                async with self.condition:
                    self.condition.notify()

    async def process(self, id: str, shards: list, size: int):
        with log_errors():
            # Consider boosting total_size a bit here to account for duplication
            while id in self.active:
                await asyncio.sleep(0.01)

            self.active.add(id)

            def _():
                with open(
                    self.directory / str(id), mode="ab", buffering=100_000_000
                ) as f:
                    for shard in shards:
                        self.dump(shard, f)
                    # os.fsync(f)  # TODO: maybe?

            start = time.time()
            with self.time("write"):
                _()
            #     await offload(_)
            stop = time.time()

            self.diagnostics["avg_size"] = (
                0.98 * self.diagnostics["avg_size"] + 0.02 * size
            )
            self.diagnostics["avg_duration"] = 0.98 * self.diagnostics[
                "avg_duration"
            ] + 0.02 * (stop - start)

            self.active.remove(id)
            self.bytes_written += size
            self.total_size -= size
            async with self.condition:
                self.condition.notify()
            await self.queue.put(None)

    def read(self, id):
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
            return self.join(parts)
        else:
            raise KeyError(id)

    async def flush(self):
        while self.shards:
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._futures)
        if all(future.done() for future in self._futures):
            self._futures.clear()

        assert not self.total_size

        self._done = True

    def close(self):
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
