from __future__ import annotations

from distributed.metrics import time


class Deadline:
    """Utility class tracking a deadline and the progress toward it"""

    #: Expiry time of the deadline in seconds since the epoch
    #: or None if the deadline never expires
    expires_at: float | None
    #: Seconds since the epoch when the deadline was created
    started_at: float

    def __init__(self, expires_at: float | None = None):
        self.expires_at = expires_at
        self.started_at = time()

    @classmethod
    def after(cls, duration: float | None = None) -> Deadline:
        """Create a new ``Deadline`` that expires in ``duration`` seconds
        or never if ``duration`` is None"""
        started_at = time()
        expires_at = duration + started_at if duration is not None else duration
        deadline = cls(expires_at)
        deadline.started_at = started_at
        return deadline

    @property
    def duration(self) -> float | None:
        """Seconds between the creation and expiration time of the deadline
        if the deadline expires, None otherwise"""
        if self.expires_at is None:
            return None
        return self.expires_at - self.started_at

    @property
    def expires(self) -> bool:
        """Whether the deadline ever expires"""
        return self.expires_at is not None

    @property
    def elapsed(self) -> float:
        """Seconds that elapsed since the deadline was created"""
        return time() - self.started_at

    @property
    def remaining(self) -> float | None:
        """Seconds remaining until the deadline expires if an expiry time is set,
        None otherwise"""
        if self.expires_at is None:
            return None
        else:
            return max(0, self.expires_at - time())

    @property
    def expired(self) -> bool:
        """Whether the deadline has already expired"""
        return self.remaining == 0
