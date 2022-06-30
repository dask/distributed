"""Tests for tasks raising the Reschedule exception and Scheduler.reschedule().

Note that this functionality is also used by work stealing;
see test_steal.py for additional tests.
"""
from __future__ import annotations

import asyncio
from time import sleep

import pytest

from distributed import Event, Reschedule, get_worker, secede, wait
from distributed.utils_test import captured_logger, gen_cluster, slowinc
from distributed.worker_state_machine import (
    ComputeTaskEvent,
    FreeKeysEvent,
    RescheduleEvent,
    SecedeEvent,
)


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.work-stealing": False},
)
async def test_scheduler_reschedule(c, s, a, b):
    xs = c.map(slowinc, range(100), key="x", delay=0.1)
    while not a.state.tasks or not b.state.tasks:
        await asyncio.sleep(0.01)
    assert len(a.state.tasks) == len(b.state.tasks) == 50

    ys = c.map(slowinc, range(100), key="y", delay=0.1, workers=[a.address])
    while len(a.state.tasks) != 150:
        await asyncio.sleep(0.01)

    # Reschedule the 50 xs that are processing on a
    for x in xs:
        if s.tasks[x.key].processing_on is s.workers[a.address]:
            s.reschedule(x.key, stimulus_id="test")

    # Wait for at least some of the 50 xs that had been scheduled on a to move to b.
    # This happens because you have 100 ys processing on a and 50 xs processing on b,
    # so the scheduler will prefer b for the rescheduled tasks to obtain more equal
    # balancing.
    while len(a.state.tasks) == 150 or len(b.state.tasks) <= 50:
        await asyncio.sleep(0.01)


@gen_cluster()
async def test_scheduler_reschedule_warns(s, a, b):
    with captured_logger("distributed.scheduler") as sched:
        s.reschedule(key="__this-key-does-not-exist__", stimulus_id="test")

    assert "not found on the scheduler" in sched.getvalue()
    assert "Aborting reschedule" in sched.getvalue()


@pytest.mark.parametrize("long_running", [False, True])
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.work-stealing": False},
)
async def test_raise_reschedule(c, s, a, b, long_running):
    """A task raises Reschedule()"""
    a_address = a.address

    def f(x):
        if long_running:
            secede()
        sleep(0.1)
        if get_worker().address == a_address:
            raise Reschedule()

    futures = c.map(f, range(4), key=["x1", "x2", "x3", "x4"])
    futures2 = c.map(slowinc, range(10), delay=0.1, key="clog", workers=[a.address])
    await wait(futures)
    assert any(isinstance(ev, RescheduleEvent) for ev in a.state.stimulus_log)
    assert all(f.key in b.data for f in futures)


@pytest.mark.parametrize("long_running", [False, True])
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_cancelled_reschedule(c, s, a, long_running):
    """A task raises Reschedule(), but the future was released by the client"""
    ev1 = Event()
    ev2 = Event()

    def f(ev1, ev2):
        if long_running:
            secede()
        ev1.set()
        ev2.wait()
        raise Reschedule()

    x = c.submit(f, ev1, ev2, key="x")
    await ev1.wait()
    x.release()
    while "x" in s.tasks:
        await asyncio.sleep(0.01)

    await ev2.set()
    while "x" in a.state.tasks:
        await asyncio.sleep(0.01)


@pytest.mark.parametrize("long_running", [False, True])
def test_cancelled_reschedule_worker_state(ws, long_running):
    """Same as test_cancelled_reschedule"""

    ws.handle_stimulus(ComputeTaskEvent.dummy(key="x", stimulus_id="s1"))
    if long_running:
        ws.handle_stimulus(SecedeEvent(key="x", compute_duration=1.0, stimulus_id="s2"))

    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s3"),
        RescheduleEvent(key="x", stimulus_id="s4"),
    )
    # There's no RescheduleMsg and the task has been forgotten
    assert not instructions
    assert not ws.tasks
