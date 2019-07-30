import collections
import math

import toolz

from ..metrics import time
from ..utils import parse_timedelta, PeriodicCallback


class AdaptiveCore:
    def __init__(
        self,
        minimum: float = 0,
        maximum: float = math.inf,
        wait_count: int = 3,
        interval: str = "1s",
    ):
        self.minimum = minimum
        self.maximum = maximum
        self.wait_count = wait_count
        self.interval = parse_timedelta(interval, "seconds") if interval else interval
        self.periodic_callback = None

        self.plan = set()
        self.requested = set()
        self.observed = set()

        # internal state
        self.close_counts = collections.defaultdict(int)
        self._adapting = False
        self.log = collections.deque(maxlen=10000)

    def __await__(self):
        async def f():
            if self.interval:
                self.periodic_callback = PeriodicCallback(
                    self.adapt, self.interval * 1000
                )
                self.periodic_callback.start()
            return self

        return f().__await__()

    def stop(self):
        if self.periodic_callback:
            self.periodic_callback.stop()
            self.periodic_callback = None

    async def target(self) -> int:
        pass

    async def _workers_to_close(self, target: int) -> list:
        """
        Give a list of workers to close that brings us down to target workers
        """
        # TODO, improve me with something that thinks about current load
        return list(self.observed)[target:]

    async def safe_target(self) -> int:
        n = await self.target()
        if n > self.maximum:
            n = self.maximum

        if n < self.minimum:
            n = self.minimum

        return n

    async def recommendations(self, target: int) -> dict:
        plan = self.plan
        requested = self.requested
        observed = self.observed

        if target == len(plan):
            self.close_counts.clear()
            return {"status": "same"}

        elif target > len(plan):
            self.close_counts.clear()
            return {"status": "up", "n": target}

        elif target < len(plan):
            not_yet_arrived = requested - observed
            to_close = set()
            if not_yet_arrived:
                to_close.update((toolz.take(len(plan) - target, not_yet_arrived)))

            if target < len(plan) - len(to_close):
                L = await self._workers_to_close(target=target)
                to_close.update(L)

            firmly_close = set()
            for w in to_close:
                self.close_counts[w] += 1
                if self.close_counts[w] >= self.wait_count:
                    firmly_close.add(w)

            for k in list(self.close_counts):  # clear out unseen keys
                if k in firmly_close or k not in to_close:
                    del self.close_counts[k]

            if firmly_close:
                return {"status": "down", "workers": firmly_close}
            else:
                return {"status": "same"}

    async def adapt(self) -> None:
        if self._adapting:  # Semaphore to avoid overlapping adapt calls
            return
        self._adapting = True

        try:
            target = await self.safe_target()
            recommendations = await self.recommendations(target)

            if recommendations["status"] != "same":
                self.log.append((time(), dict(recommendations)))

            status = recommendations.pop("status")
            if status == "same":
                return
            if status == "up":
                await self.scale_up(**recommendations)
            if status == "down":
                await self.scale_down(**recommendations)
        finally:
            self._adapting = False
