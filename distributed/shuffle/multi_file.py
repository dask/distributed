import asyncio
import os
import pathlib
import pickle
import shutil
from collections import defaultdict

from dask.sizeof import sizeof
from dask.utils import parse_bytes

from ..system import MEMORY_LIMIT
from ..utils import log_errors, offload


class MultiFile:
    def __init__(
        self,
        directory,
        dump=pickle.dump,
        load=pickle.load,
        join=None,
        concurrent_files=1,
        memory_limit=MEMORY_LIMIT / 2,
        file_cache=None,
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

        self.memory_limit = parse_bytes(memory_limit)
        self.concurrent_files = concurrent_files
        self.condition = asyncio.Condition()

        self.bytes_written = 0
        self.bytes_read = 0

        self._done = False
        self._futures = set()

    async def put(self, data: dict):
        this_size = 0
        for id, shard in data.items():
            size = self.sizeof(shard)
            self.shards[id].append(shard)
            self.sizes[id] += size
            self.total_size += size
            self.total_received += size
            this_size += size

        del data

        while self.total_size > self.memory_limit:
            async with self.condition:
                from dask.utils import format_bytes

                print(
                    "waiting",
                    format_bytes(self.total_size),
                    "this",
                    format_bytes(this_size),
                )
                try:
                    await asyncio.wait_for(
                        self.condition.wait(), 1
                    )  # Block until memory calms down
                except asyncio.TimeoutError:
                    continue

    async def communicate(self):
        with log_errors():
            self.queue = asyncio.Queue(maxsize=self.concurrent_files)
            for _ in range(self.concurrent_files):
                self.queue.put_nowait(None)

            while not self._done:
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

            def _():
                # TODO: offload
                with open(
                    self.directory / str(id), mode="ab", buffering=100_000_000
                ) as f:
                    for shard in shards:
                        self.dump(shard, f)

            await offload(_)

            self.total_size -= size
            async with self.condition:
                self.condition.notify()
            await self.queue.put(None)

    def read(self, id):
        parts = []

        with open(self.directory / str(id), mode="rb", buffering=100_000_000) as f:
            while True:
                try:
                    parts.append(self.load(f))
                except EOFError:
                    break

        # TODO: We could consider deleting the file at this point
        if parts:
            for part in parts:
                self.bytes_read += sizeof(part)
            return self.join(parts)
        else:
            raise KeyError(id)

    async def flush(self):
        while self.shards:
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._futures)
        assert not self.total_size
        self._done = True

    def close(self):
        shutil.rmtree(self.directory)
        self.file_cache.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, traceback):
        self.close()
