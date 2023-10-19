from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Callable

from dask.sizeof import sizeof

from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors


class MemoryShardsBuffer(ShardsBuffer):
    _deserialize: Callable[[bytes], Any]
    _shards: defaultdict[str, deque[bytes]]

    def __init__(self, deserialize: Callable[[bytes], Any]) -> None:
        super().__init__(
            memory_limiter=ResourceLimiter(None),
        )
        self._deserialize = deserialize
        self._shards = defaultdict(deque)

    async def _process(self, id: str, shards: list[bytes]) -> None:
        # TODO: This can be greatly simplified, there's no need for
        # background threads at all.
        with log_errors():
            with self.time("write"):
                self._shards[id].extend(shards)

    def read(self, id: str) -> Any:
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        with self.time("read"):
            shards = self._shards.pop(id)  # Raises KeyError
            self.bytes_read += sum(map(sizeof, shards))
            # Don't keep the serialized and the deserialized shards
            # in memory at the same time
            data = []
            while shards:
                shard = shards.pop()
                data.append(self._deserialize(shard))

        return data
