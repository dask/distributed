import asyncio
import threading
from collections import defaultdict

from dask.utils import parse_bytes

from ..core import rpc
from ..protocol import to_serialize
from ..sizeof import sizeof
from ..system import MEMORY_LIMIT
from ..utils import log_errors


class MultiComm:
    def __init__(
        self,
        memory_limit=MEMORY_LIMIT / 4,
        join=None,
        rpc=rpc,
        sizeof=sizeof,
        max_connections=10,
        shuffle_id=None,
        max_message_size="128 MiB",
    ):
        self.lock = threading.Lock()
        self.shards = defaultdict(list)
        self.sizes = defaultdict(int)
        self.total_size = 0
        self.total_moved = 0
        self.max_message_size = parse_bytes(max_message_size)
        self.memory_limit = parse_bytes(memory_limit)
        self.thread_condition = threading.Condition()
        assert join
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
                self.total_moved += size

        del data, shard

        from dask.utils import format_bytes

        if self.total_size > self.memory_limit:
            print(
                "waiting comm",
                format_bytes(self.total_size),
                "this",
                format_bytes(size),
            )

        while self.total_size > self.memory_limit:
            with self.thread_condition:
                self.thread_condition.wait(1)  # Block until memory calms down

    async def communicate(self):
        self.comm_queue = asyncio.Queue(maxsize=self.max_connections)
        for _ in range(self.max_connections):
            self.comm_queue.put_nowait(None)

        from dask.utils import format_bytes

        while not self._done:
            if not self.shards:
                await asyncio.sleep(0.1)
                continue

            await self.comm_queue.get()

            with self.lock:
                address = max(self.sizes, key=self.sizes.get)

                size = 0
                shards = []
                while size < self.max_message_size:
                    try:
                        shard = self.shards[address].pop()
                    except IndexError:
                        del self.shards[address]
                        del self.sizes[address]
                        break
                    else:
                        shards.append(shard)
                        s = self.sizeof(shard)
                        size += s
                        self.sizes[address] -= s

            print(
                "Sending",
                format_bytes(size),
                "to comm",
                format_bytes(self.total_size),
                "left in ",
                len(self.shards),
                "buckets",
            )
            future = asyncio.ensure_future(self.process(address, shards, size))
            del shards
            self._futures.add(future)

    async def process(self, address: str, shards: list, size: int):
        with log_errors():
            shards = self.join(shards)
            # shards = await offload(self.join, shards)

            # Consider boosting total_size a bit here to account for duplication

            try:
                await self.rpc(address).shuffle_receive(
                    data=to_serialize(shards),
                    shuffle_id=self.shuffle_id,
                )
            finally:
                self.total_size -= size
                with self.thread_condition:
                    self.thread_condition.notify()
                await self.comm_queue.put(None)

    async def flush(self):
        while self.shards:
            await asyncio.sleep(0.05)

        await asyncio.gather(*self._futures)
        self._futures.clear()

        assert not self.total_size
        from dask.utils import format_bytes

        print("total moved", format_bytes(self.total_moved))

        self._done = True
