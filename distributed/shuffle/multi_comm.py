import asyncio
import threading
from collections import defaultdict

from dask.utils import parse_bytes

from ..core import rpc
from ..protocol import to_serialize
from ..sizeof import sizeof
from ..system import MEMORY_LIMIT
from ..utils import log_errors, offload


class MultiComm:
    def __init__(
        self,
        memory_limit=MEMORY_LIMIT / 4,
        join=None,
        rpc=rpc,
        sizeof=sizeof,
        max_connections=10,
        shuffle_id=None,
    ):
        self.lock = threading.Lock()
        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self.total_size = 0
        self.memory_limit = parse_bytes(memory_limit)
        self.thread_condition = threading.Condition()
        if join is None:
            import pandas as pd

            join = pd.concat
        self.join = join
        self.max_connections = max_connections
        self.sizeof = sizeof
        self.shuffle_id = shuffle_id
        self._futures = set()
        self._done = False
        self.rpc = rpc

    def put(self, data: dict):
        with self.lock:
            for address, shard in data.items():
                size = self.sizeof(shard)
                self.shards[address].append(shard)
                self.sizes[address] += size
                self.total_size += size

        del data

        while self.total_size > self.memory_limit:
            with self.thread_condition:
                self.thread_condition.wait(0.500)  # Block until memory calms down

    async def communicate(self):
        self.comm_queue = asyncio.Queue(maxsize=self.max_connections)
        for _ in range(self.max_connections):
            self.comm_queue.put_nowait(None)

        while not self._done:
            if not self.shards:
                await asyncio.sleep(0.1)
                continue

            await self.comm_queue.get()

            with self.lock:
                address = max(self.sizes, key=self.sizes.get)
                shards = self.shards.pop(address)
                size = self.sizes.pop(address)

            future = asyncio.ensure_future(self.process(address, shards, size))
            del shards
            self._futures.add(future)
            with self.thread_condition:
                self.thread_condition.notify()

    async def process(self, address: str, shards: list, size: int):
        with log_errors():
            shards = await offload(self.join, shards)
            # Consider boosting total_size a bit here to account for duplication

            try:
                await self.rpc(address).shuffle_receive(
                    data=to_serialize(shards),
                    shuffle_id=self.shuffle_id,
                )
            finally:
                self.total_size -= size
                await self.comm_queue.put(None)

    async def flush(self):
        while self.shards:
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._futures)
        assert not self.total_size
        self._done = True
