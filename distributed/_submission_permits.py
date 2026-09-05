"""Bounded, generation-scoped submission permits.

This module deliberately has no Scheduler dependency.  The extension that uses it owns
transport and idle-shutdown policy; this registry owns only the one-shot admission state.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from heapq import heapify, heappop, heappush
from math import isfinite
from time import monotonic
from uuid import uuid4

_HEAP_SLACK = 64


class SubmissionPermitError(ValueError):
    """Base class for a permit that cannot be used."""


class UnknownPermitError(SubmissionPermitError):
    pass


class ExpiredPermitError(SubmissionPermitError):
    pass


class AbortedPermitError(SubmissionPermitError):
    pass


class RetiredPermitError(SubmissionPermitError):
    pass


class ClosedPermitError(SubmissionPermitError):
    pass


class PermitCapacityError(SubmissionPermitError):
    pass


@dataclass(frozen=True, slots=True)
class PermitSnapshot:
    sequence: int
    state: str
    duration: float
    remaining: float

    def to_dict(self) -> dict[str, float | int | str]:
        return {
            "sequence": self.sequence,
            "state": self.state,
            "duration": self.duration,
            "remaining": self.remaining,
        }


@dataclass(slots=True)
class _Permit:
    sequence: int
    duration: float
    deadline: float
    state: str = "pending"


@dataclass(slots=True)
class _Generation:
    epoch: str
    high_watermark: int = 0
    active: dict[int, _Permit] = field(default_factory=dict)
    outcomes: OrderedDict[int, _Permit] = field(default_factory=OrderedDict)


class SubmissionPermitRegistry:
    """A finite permit registry for a single scheduler.

    Sequences are monotonic *per registered epoch*.  Tombstones are deliberately
    finite: a sequence older than the high-water mark that is no longer retained is
    ``retired`` and never becomes live again.
    """

    def __init__(
        self,
        max_duration: float,
        max_pending_per_client: int,
        max_pending: int,
        max_outcomes_per_client: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if (
            not isinstance(max_duration, (int, float))
            or isinstance(max_duration, bool)
            or not isfinite(max_duration)
            or max_duration <= 0
        ):
            raise ValueError("max_duration must be a positive finite number")
        for name, value in (
            ("max_pending_per_client", max_pending_per_client),
            ("max_pending", max_pending),
            ("max_outcomes_per_client", max_outcomes_per_client),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        self.max_duration = float(max_duration)
        self.max_pending_per_client = max_pending_per_client
        self.max_pending = max_pending
        self.max_outcomes_per_client = max_outcomes_per_client
        self._clock = clock
        self._generations: dict[str, _Generation] = {}
        # Entries deliberately contain only scalar identity data.  Finished,
        # unregistered, and replaced generations leave stale entries behind;
        # _expire and _compact_heap validate them against active state.
        self._deadlines: list[tuple[float, str, str, int]] = []
        self._pending = 0
        self._closed = False

    def register(self, client: str) -> str:
        self._validate_client(client)
        if self._closed:
            raise ClosedPermitError("submission permit registry is closed")
        # Replacing a generation invalidates, and releases, all old pending permits.
        old = self._generations.get(client)
        if old is not None:
            self._pending -= len(old.active)
        epoch = uuid4().hex
        self._generations[client] = _Generation(epoch)
        self._compact_heap()
        return epoch

    def is_current(self, client: str, epoch: str) -> bool:
        self._validate_client(client)
        self._validate_epoch(epoch)
        generation = self._generations.get(client)
        return generation is not None and generation.epoch == epoch

    def unregister(self, client: str, epoch: str) -> bool:
        self._validate_client(client)
        self._validate_epoch(epoch)
        generation = self._generations.get(client)
        if generation is None or generation.epoch != epoch:
            return False
        self._pending -= len(generation.active)
        del self._generations[client]
        self._compact_heap()
        return True

    def acquire(
        self, client: str, epoch: str, sequence: int, duration: float
    ) -> PermitSnapshot:
        generation = self._current_generation(client, epoch)
        self._validate_sequence(sequence)
        self._validate_duration(duration)
        if self._closed:
            raise ClosedPermitError("submission permit registry is closed")
        now = self._clock()
        self._expire(now)
        existing = self._lookup(generation, sequence)
        if existing is not None:
            return self._snapshot(existing, now)
        self._raise_if_retired(generation, sequence)
        if len(generation.active) >= self.max_pending_per_client:
            raise PermitCapacityError("client pending permit capacity reached")
        if self._pending >= self.max_pending:
            raise PermitCapacityError("registry pending permit capacity reached")
        permit = _Permit(sequence, float(duration), now + float(duration))
        generation.active[sequence] = permit
        generation.high_watermark = sequence
        self._pending += 1
        heappush(self._deadlines, (permit.deadline, client, epoch, sequence))
        self._compact_heap()
        return self._snapshot(permit, now)

    def status(self, client: str, epoch: str, sequence: int) -> PermitSnapshot:
        generation = self._current_generation(client, epoch)
        self._validate_sequence(sequence)
        now = self._clock()
        self._expire(now)
        permit = self._lookup(generation, sequence)
        if permit is not None:
            return self._snapshot(permit, now)
        return PermitSnapshot(
            sequence,
            "retired" if sequence <= generation.high_watermark else "unknown",
            0.0,
            0.0,
        )

    def abort(self, client: str, epoch: str, sequence: int) -> PermitSnapshot:
        generation = self._current_generation(client, epoch)
        self._validate_sequence(sequence)
        now = self._clock()
        self._expire(now)
        permit = self._lookup(generation, sequence)
        if permit is None:
            return self.status(client, epoch, sequence)
        if permit.state == "pending":
            self._finish(generation, permit, "aborted")
        self._compact_heap()
        return self._snapshot(permit, now)

    def transfer(self, client: str, epoch: str, sequence: int) -> bool:
        generation = self._current_generation(client, epoch)
        self._validate_sequence(sequence)
        self._expire(self._clock())
        permit = self._lookup(generation, sequence)
        if permit is None:
            if sequence <= generation.high_watermark:
                raise RetiredPermitError(f"submission permit {sequence} is retired")
            raise UnknownPermitError(f"unknown submission permit {sequence}")
        if permit.state == "accepted":
            return False
        if permit.state == "expired":
            raise ExpiredPermitError(f"submission permit {sequence} expired")
        if permit.state == "aborted":
            raise AbortedPermitError(f"submission permit {sequence} was aborted")
        self._finish(generation, permit, "accepted")
        self._compact_heap()
        return True

    def has_pending(self) -> bool:
        self._expire(self._clock())
        return self._pending > 0

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for generation in self._generations.values():
            for permit in list(generation.active.values()):
                self._finish(generation, permit, "aborted")
        self._deadlines.clear()

    def _current_generation(self, client: str, epoch: str) -> _Generation:
        self._validate_client(client)
        self._validate_epoch(epoch)
        generation = self._generations.get(client)
        if generation is None or generation.epoch != epoch:
            raise UnknownPermitError("unknown or stale submission permit epoch")
        return generation

    def _expire(self, now: float) -> None:
        while self._deadlines and self._deadlines[0][0] <= now:
            deadline, client, epoch, sequence = heappop(self._deadlines)
            generation = self._generations.get(client)
            if generation is None or generation.epoch != epoch:
                continue
            permit = generation.active.get(sequence)
            if permit is not None and permit.deadline == deadline:
                self._finish(generation, permit, "expired")
        self._compact_heap()

    def _compact_heap(self) -> None:
        if len(self._deadlines) <= 2 * self._pending + _HEAP_SLACK:
            return
        self._deadlines = [
            entry for entry in self._deadlines if self._is_live_entry(entry)
        ]
        heapify(self._deadlines)

    def _is_live_entry(self, entry: tuple[float, str, str, int]) -> bool:
        deadline, client, epoch, sequence = entry
        generation = self._generations.get(client)
        if generation is None or generation.epoch != epoch:
            return False
        permit = generation.active.get(sequence)
        return permit is not None and permit.deadline == deadline

    def _finish(self, generation: _Generation, permit: _Permit, state: str) -> None:
        del generation.active[permit.sequence]
        self._pending -= 1
        permit.state = state
        generation.outcomes[permit.sequence] = permit
        generation.outcomes.move_to_end(permit.sequence)
        while len(generation.outcomes) > self.max_outcomes_per_client:
            generation.outcomes.popitem(last=False)

    @staticmethod
    def _snapshot(permit: _Permit, now: float) -> PermitSnapshot:
        remaining = (
            max(0.0, permit.deadline - now) if permit.state == "pending" else 0.0
        )
        return PermitSnapshot(permit.sequence, permit.state, permit.duration, remaining)

    @staticmethod
    def _lookup(generation: _Generation, sequence: int) -> _Permit | None:
        return generation.active.get(sequence) or generation.outcomes.get(sequence)

    @staticmethod
    def _raise_if_retired(generation: _Generation, sequence: int) -> None:
        if sequence <= generation.high_watermark:
            raise RetiredPermitError(f"submission permit {sequence} is retired")

    @staticmethod
    def _validate_client(client: str) -> None:
        if not isinstance(client, str) or not client:
            raise ValueError("client must be a non-empty string")

    @staticmethod
    def _validate_epoch(epoch: str) -> None:
        if not isinstance(epoch, str) or not epoch:
            raise ValueError("epoch must be a non-empty string")

    @staticmethod
    def _validate_sequence(sequence: int) -> None:
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or not 0 < sequence <= 2**63 - 1
        ):
            raise ValueError("sequence must be an integer between 1 and 2**63 - 1")

    def _validate_duration(self, duration: float) -> None:
        if (
            not isinstance(duration, (int, float))
            or isinstance(duration, bool)
            or not isfinite(duration)
            or not 0 < duration <= self.max_duration
        ):
            raise ValueError(
                "duration must be positive, finite, and no greater than max_duration"
            )
