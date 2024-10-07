from __future__ import annotations

import asyncio
import time as timemod

import pytest

from distributed._async_taskgroup import AsyncTaskGroup, AsyncTaskGroupClosedError
from distributed.utils_test import gen_test


async def _wait_for_n_loop_cycles(n):
    for _ in range(n):
        await asyncio.sleep(0)


def test_async_task_group_initialization():
    group = AsyncTaskGroup()
    assert not group.closed
    assert len(group) == 0


@gen_test()
async def test_async_task_group_call_soon_executes_task_in_background():
    group = AsyncTaskGroup()
    ev = asyncio.Event()
    flag = False

    async def set_flag():
        nonlocal flag
        await ev.wait()
        flag = True

    assert group.call_soon(set_flag) is None
    assert len(group) == 1
    ev.set()
    await _wait_for_n_loop_cycles(2)
    assert len(group) == 0
    assert flag


@gen_test()
async def test_async_task_group_call_later_executes_delayed_task_in_background():
    group = AsyncTaskGroup()
    ev = asyncio.Event()

    start = timemod.monotonic()
    assert group.call_later(1, ev.set) is None
    assert len(group) == 1
    await ev.wait()
    end = timemod.monotonic()
    # the task must be removed in exactly 1 event loop cycle
    await _wait_for_n_loop_cycles(2)
    assert len(group) == 0
    assert end - start > 1 - timemod.get_clock_info("monotonic").resolution


def test_async_task_group_close_closes():
    group = AsyncTaskGroup()
    group.close()
    assert group.closed

    # Test idempotency
    group.close()
    assert group.closed


@gen_test()
async def test_async_task_group_close_does_not_cancel_existing_tasks():
    group = AsyncTaskGroup()

    ev = asyncio.Event()
    flag = False

    async def set_flag():
        nonlocal flag
        await ev.wait()
        flag = True
        return None

    assert group.call_soon(set_flag) is None

    group.close()

    assert len(group) == 1

    ev.set()
    await _wait_for_n_loop_cycles(2)
    assert len(group) == 0


@gen_test()
async def test_async_task_group_close_prohibits_new_tasks():
    group = AsyncTaskGroup()
    group.close()

    ev = asyncio.Event()
    flag = False

    async def set_flag():
        nonlocal flag
        await ev.wait()
        flag = True
        return True

    with pytest.raises(AsyncTaskGroupClosedError):
        group.call_soon(set_flag)
    assert len(group) == 0

    with pytest.raises(AsyncTaskGroupClosedError):
        group.call_later(1, set_flag)
    assert len(group) == 0

    await asyncio.sleep(0.01)
    assert not flag


@gen_test()
async def test_async_task_group_stop_disallows_shutdown():
    group = AsyncTaskGroup()

    task = None

    async def set_flag():
        nonlocal task
        task = asyncio.current_task()

    assert group.call_soon(set_flag) is None
    assert len(group) == 1
    # tasks are not given a grace period, and are not even allowed to start
    # if the group is closed immediately
    await group.stop()
    assert task is None


@gen_test()
async def test_async_task_group_stop_cancels_long_running():
    group = AsyncTaskGroup()

    task = None
    flag = False
    started = asyncio.Event()

    async def set_flag():
        nonlocal task
        task = asyncio.current_task()
        started.set()
        await asyncio.sleep(10)
        nonlocal flag
        flag = True
        return True

    assert group.call_soon(set_flag) is None
    assert len(group) == 1
    await started.wait()
    await group.stop()
    assert task
    assert task.cancelled()
    assert not flag
