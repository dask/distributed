from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from dask.sizeof import sizeof

from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._exceptions import DataUnavailable
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors


class MemoryShardsBuffer(ShardsBuffer):
    _shards: defaultdict[str, deque[Any]]

    def __init__(self) -> None:
        super().__init__(memory_limiter=ResourceLimiter(None))
        self._shards = defaultdict(deque)

    @log_errors
    async def _process(self, id: str, shards: list[Any]) -> None:
        self._shards[id].extend(shards)

    def read(self, id: str) -> Any:
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        try:
            shards = self._shards.pop(id)  # Raises KeyError
        except KeyError:
            raise DataUnavailable(id)
        self.bytes_read += sum(map(sizeof, shards))
        # Don't keep the serialized and the deserialized shards
        # in memory at the same time
        data = []
        while shards:
            shard = shards.pop()
            # TODO unpickle dataframes
            data.append(shard)

        return data
