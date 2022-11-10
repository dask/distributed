from __future__ import annotations

import asyncio
from time import sleep

from distributed.metrics import time
from distributed.utils import Deadline
from distributed.utils_test import gen_test


def test_deadline():
    before_start = time()
    deadline = Deadline.after(5)
    after_start = time()

    assert deadline.expired is False
    assert deadline.expires is True
    assert deadline.expires_at - deadline.started_at == 5
    assert 4 <= deadline.remaining <= 5

    deadline2 = Deadline(deadline.expires_at)

    assert deadline.expires_at == deadline2.expires_at


def test_infinite_deadline():
    deadline = Deadline(None)

    assert deadline.expires_at is None
    assert deadline.expired is False
    assert deadline.expires is False
    assert deadline.expires_at is None
    assert deadline.remaining is None

    deadline2 = Deadline.after(None)

    assert deadline2.expires_at is None


@gen_test()
async def test_deadline_progress():
    before_start = time()
    deadline = Deadline.after(100)
    after_start = time()

    assert before_start <= deadline.started_at <= after_start
    assert deadline.started_at - time() <= deadline.remaining <= 100

    await asyncio.sleep(0.1)
    before_elapsed = time()
    elapsed = deadline.elapsed
    after_elapsed = time()
    assert (
        before_elapsed - deadline.started_at
        <= elapsed
        <= after_elapsed - deadline.started_at
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
