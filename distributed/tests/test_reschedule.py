"""Tests for tasks raising the Reschedule exception and Scheduler._reschedule().

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
    GatherDep,
    RescheduleEvent,
    RescheduleMsg,
)


@gen_cluster()
async def test_scheduler_reschedule_warns(s, a, b):
    with captured_logger("distributed.scheduler") as sched:
        s._reschedule(key="__this-key-does-not-exist__", stimulus_id="test")

    assert "not found on the scheduler" in sched.getvalue()
    assert "Aborting reschedule" in sched.getvalue()


@pytest.mark.parametrize("state", ["executing", "long-running"])
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.work-stealing": False},
)
async def test_raise_reschedule(c, s, a, b, state):
    """A task raises Reschedule()"""
    a_address = a.address

    def f(x):
        if state == "long-running":
            secede()
        sleep(0.1)
        if get_worker().address == a_address:
            raise Reschedule()

    futures = c.map(f, range(4), key=["x1", "x2", "x3", "x4"])
    futures2 = c.map(slowinc, range(10), delay=0.1, key="clog", workers=[a.address])
    await wait(futures)
    assert any(isinstance(ev, RescheduleEvent) for ev in a.state.stimulus_log)
    assert all(f.key in b.data for f in futures)


@pytest.mark.parametrize("state", ["executing", "long-running"])
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_cancelled_reschedule(c, s, a, state):
    """A task raises Reschedule(), but the future was released by the client.
    Same as test_cancelled_reschedule_worker_state"""
    ev1 = Event()
    ev2 = Event()

    def f(ev1, ev2):
        if state == "long-running":
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


def test_cancelled_reschedule_worker_state(ws_with_running_task):
    """Same as test_cancelled_reschedule"""
    ws = ws_with_running_task

    instructions = ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert not instructions
    assert ws.tasks["x"].state == "cancelled"
    assert ws.available_resources == {"R": 0}

    instructions = ws.handle_stimulus(RescheduleEvent(key="x", stimulus_id="s2"))
    assert not instructions  # There's no RescheduleMsg
    assert not ws.tasks  # The task has been forgotten
    assert ws.available_resources == {"R": 1}


def test_reschedule_releases(ws_with_running_task):
    ws = ws_with_running_task

    instructions = ws.handle_stimulus(RescheduleEvent(key="x", stimulus_id="s1"))
    assert instructions == [RescheduleMsg(stimulus_id="s1", key="x")]
    assert ws.available_resources == {"R": 1}
    assert "x" not in ws.tasks


def test_reschedule_cancelled(ws_with_running_task):
    """Test state loop:

    executing -> cancelled -> rescheduled
    executing -> long-running -> cancelled -> rescheduled
    """
    ws = ws_with_running_task
    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        RescheduleEvent(key="x", stimulus_id="s2"),
    )
    assert not instructions
    assert "x" not in ws.tasks


def test_reschedule_resumed(ws_with_running_task):
    """Test state loop:

    executing -> cancelled -> resumed(fetch) -> rescheduled
    executing -> long-running -> cancelled -> resumed(fetch) -> rescheduled
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"

    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        RescheduleEvent(key="x", stimulus_id="s3"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s3")
    ]
    assert ws.tasks["x"].state == "flight"
