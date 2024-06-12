from __future__ import annotations

import asyncio
from time import sleep

from distributed.metrics import monotonic
from distributed.utils import Deadline
from distributed.utils_test import gen_test


def test_deadline():
    deadline = Deadline.after(5)

    assert deadline.duration == 5
    assert deadline.expired is False
    assert deadline.expires is True
    assert deadline.expires_at_mono - deadline.started_at_mono == 5
    assert 4 < deadline.expires_at - deadline.started_at < 6
    assert 0 <= deadline.elapsed <= 1
    assert 4 <= deadline.remaining <= 5


def test_infinite_deadline():
    deadline = Deadline(None)
    assert deadline.expires_at_mono is None
    assert deadline.expires_at is None
    assert deadline.expired is False
    assert deadline.expires is False
    assert deadline.remaining is None
    assert deadline.duration is None
    assert 0 <= deadline.elapsed <= 1

    deadline2 = Deadline.after(None)
    assert deadline2.expires_at_mono is None


@gen_test()
async def test_deadline_progress():
    before_start = monotonic()
    deadline = Deadline.after(100)
    after_start = monotonic()

    assert before_start <= deadline.started_at_mono <= after_start
    assert deadline.started_at_mono - monotonic() <= deadline.remaining <= 100

    await asyncio.sleep(0.1)
    before_elapsed = monotonic()
    elapsed = deadline.elapsed
    after_elapsed = monotonic()
    assert (
        before_elapsed - deadline.started_at_mono
        <= elapsed
        <= after_elapsed - deadline.started_at_mono
    )


def test_deadline_expiration():
    deadline = Deadline.after(0.1)
    sleep(0.15)
    assert deadline.expired is True
    assert deadline.remaining == 0
    assert deadline.elapsed >= deadline.duration


@gen_test()
async def test_deadline_expiration_async():
    deadline = Deadline.after(0.1)
    # Asyncio clock is slightly different, therefore sleep a bit longer
    await asyncio.sleep(0.2)
    assert deadline.expired is True
    assert deadline.remaining == 0
    assert deadline.elapsed >= deadline.duration
