from __future__ import annotations

import asyncio
import math
from typing import Protocol

from distributed.metrics import time


class AbstractLimiter(Protocol):
    @property
    def _maxvalue(self) -> int | float:
        ...

    def available(self) -> int | float:
        """How far can the value be increased before blocking"""
        ...

    def free(self) -> bool:
        """Return True if nothing has been acquired / the limiter is in a neutral state"""
        ...

    async def wait_for_available(self) -> None:
        """Block until the counter drops below maxvalue"""
        ...

    def increase(self, value: int) -> None:
        """Increase the internal counter by value"""
        ...

    async def decrease(self, value: int) -> None:
        """Decrease the internal counter by value"""
        ...


class ResourceLimiter:
    """Limit an abstract resource

    This allows us to track usage of an abstract resource. If the usage of this
    resources goes beyond a defined maxvalue, we can block further execution

    Example::

        limiter = ResourceLimiter(2)
        limiter.increase(1)
        limiter.increase(2)
        limiter.decrease(1)

        # This will block since we're still not below maxvalue
        await limiter.wait_for_available()
    """

    def __init__(self, maxvalue: int) -> None:
        self._maxvalue = maxvalue
        self._acquired = 0
        self._condition = asyncio.Condition()
        self._waiters = 0
        self.time_blocked_total = 0.0
        self.time_blocked_avg = 0.0

    def __repr__(self) -> str:
        return f"<ResourceLimiter maxvalue: {self._maxvalue} available: {self.available()}>"

    def available(self) -> int:
        """How far can the value be increased before blocking"""
        return max(0, self._maxvalue - self._acquired)

    def free(self) -> bool:
        """Return True if nothing has been acquired / the limiter is in a neutral state"""
        return self._acquired == 0

    async def wait_for_available(self) -> None:
        """Block until the counter drops below maxvalue"""
        start = time()
        duration = 0.0
        try:
            if self.available():
                return
            async with self._condition:
                self._waiters += 1
                await self._condition.wait_for(self.available)
                self._waiters -= 1
                duration = time() - start
        finally:
            self.time_blocked_total += duration
            self.time_blocked_avg = self.time_blocked_avg * 0.9 + duration * 0.1

    def increase(self, value: int) -> None:
        """Increase the internal counter by value"""
        self._acquired += value

    async def decrease(self, value: int) -> None:
        """Decrease the internal counter by value"""
        if value > self._acquired:
            raise RuntimeError(
                f"Cannot release more than what was acquired! release: {value} acquired: {self._acquired}"
            )
        self._acquired -= value
        async with self._condition:
            self._condition.notify_all()


# Used to simplify code in shardsbuffer
class NoopLimiter:
    """A no-op resource limiter."""

    _maxvalue = math.inf

    def __repr__(self) -> str:
        return f"<NoopLimiter maxvalue: {math.inf} available: {math.inf}>"

    def free(self) -> bool:
        return True

    def available(self) -> float:
        return self._maxvalue

    def increase(self, value: int) -> None:
        pass

    async def decrease(self, value: int) -> None:
        pass

    async def wait_for_available(self) -> None:
        """Don't block and return immediately"""
        pass
