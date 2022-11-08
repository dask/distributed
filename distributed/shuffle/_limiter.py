from __future__ import annotations

import asyncio


class ResourceLimiter:
    def __init__(self, maxvalue: int) -> None:
        self._maxvalue = maxvalue
        self._acquired = 0
        self._condition = asyncio.Condition()
        self._waiters = 0
        self._backlog_size = 0

    def __repr__(self) -> str:
        return f"<ResourceLimiter maxvalue: {self._maxvalue} available: {self.available()}>"

    def available(self) -> int:
        return max(0, self._maxvalue - self._acquired)

    def free(self) -> bool:
        return self._acquired == 0

    async def wait_for_available(self) -> None:
        async with self._condition:
            self._waiters += 1
            await self._condition.wait_for(self.available)
            self._waiters -= 1

    def increase(self, value: int) -> None:
        self._acquired += value

    async def decrease(self, value: int) -> None:
        if value > self._acquired:
            raise RuntimeError(
                f"Cannot release more than what was acquired! release: {value} acquired: {self._acquired}"
            )
        self._acquired -= value
        async with self._condition:
            self._condition.notify_all()
