from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

import dask
from dask import delayed

from distributed import Client, Event, Scheduler, Status, Worker
from distributed.utils_test import (
    async_wait_for,
    gen_cluster,
    inc,
    slowinc,
    wait_for_state,
)

dinc = delayed(inc)
dslowinc = delayed(slowinc)
dwait = delayed(lambda ev: ev.wait())


@asynccontextmanager
async def block_worker(
    c: Client,
    s: Scheduler,
    w: Worker,
    pause: bool,
    ntasks_on_scheduler: int | None = None,
    ntasks_on_worker: int | None = None,
) -> AsyncIterator[None]:
    """Make sure that no tasks submitted inside this context manager start running on
    the worker until the context manager exits.
    Must be used together with the ``@gen_blockable_cluster`` test decorator.

    Parameters
    ----------
    pause : bool
        True
            When entering the context manager, pause the worker. At exit, wait for all
            tasks created inside the context manager to be added to ``Scheduler.unrunnable``
            or ``Scheduler.queued`` and then unpause the worker.
        False
            When entering the context manager, send a dummy long task to the worker. At
            exit, wait for all tasks created inside the context manager to reach the
            scheduler and then terminate the dummy task.

    ntasks_on_scheduler : int, optional
        Number of tasks that must appear on the scheduler. Defaults to the number of
        futures held by the client.
    ntasks_on_worker : int, optional
        Number of tasks that must appear on the worker before any task is actually
        started. Defaults to the number of futures held by the client.
    """
    if pause:
        w.status = Status.paused
        await async_wait_for(
            lambda: s.workers[w.address].status == Status.paused, timeout=5
        )
    else:
        ev = Event()
        clog = c.submit(ev.wait, key="block_worker")
        await wait_for_state(clog.key, "executing", w)

    yield

    if ntasks_on_scheduler is None:
        ntasks_on_scheduler = len(c.futures)
    if ntasks_on_worker is None:
        ntasks_on_worker = len(c.futures)
    await async_wait_for(lambda: len(s.tasks) >= ntasks_on_scheduler, timeout=5)

    if pause:
        assert (
            len(s.unrunnable) == ntasks_on_worker or len(s.queued) == ntasks_on_worker
        )
        assert not w.state.tasks
        w.status = Status.running
    else:
        await ev.set()
        await clog
        del clog
        await async_wait_for(lambda: "block_worker" not in s.tasks, timeout=5)


def gen_blockable_cluster(test_func):
    """Generate a cluster with 1 worker and disabled memory monitor,
    to be used together with ``async with block_worker(...):``.
    """
    return pytest.mark.parametrize("pause", [pytest.param(False), pytest.param(True)])(
        gen_cluster(
            client=True,
            nthreads=[("", 1)],
            config={"distributed.worker.memory.pause": False},
        )(test_func)
    )


@gen_blockable_cluster
async def test_submit(c, s, a, pause):
    async with block_worker(c, s, a, pause):
        low = c.submit(inc, 1, key="low", priority=-1)
        ev = Event()
        clog = c.submit(lambda ev: ev.wait(), ev, key="clog")
        high = c.submit(inc, 2, key="high", priority=1)

    await high
    assert s.tasks[clog.key].state == "processing"
    assert s.tasks[low.key].state != "memory"
    await ev.set()
    await low


@gen_blockable_cluster
async def test_map(c, s, a, pause):
    async with block_worker(c, s, a, pause):
        low = c.map(inc, [1, 2, 3], key=["l1", "l2", "l3"], priority=-1)
        ev = Event()
        clog = c.submit(lambda ev: ev.wait(), ev, key="clog")
        high = c.map(inc, [4, 5, 6], key=["h1", "h2", "h3"], priority=1)

    await c.gather(high)
    assert s.tasks[clog.key].state == "processing"
    assert all(s.tasks[fut.key].state != "memory" for fut in low)
    await ev.set()
    await clog
    await c.gather(low)


@gen_blockable_cluster
async def test_compute(c, s, a, pause):
    async with block_worker(c, s, a, pause):
        low = c.compute(dinc(1, dask_key_name="low"), priority=-1)
        ev = Event()
        clog = c.submit(lambda ev: ev.wait(), ev, key="clog")
        high = c.compute(dinc(2, dask_key_name="high"), priority=1)

    await high
    assert s.tasks[clog.key].state == "processing"
    assert s.tasks[low.key].state != "memory"
    await ev.set()
    await clog
    await low


@gen_blockable_cluster
async def test_persist(c, s, a, pause):
    async with block_worker(c, s, a, pause):
        low = dinc(1, dask_key_name="low").persist(priority=-1)
        ev = Event()
        clog = c.submit(lambda ev: ev.wait(), ev, key="clog")
        high = dinc(2, dask_key_name="high").persist(priority=1)

    await high
    assert s.tasks[clog.key].state == "processing"
    assert s.tasks[low.key].state != "memory"
    await ev.set()
    await clog
    await low


@gen_blockable_cluster
async def test_annotate_compute(c, s, a, pause):
    with dask.annotate(priority=-1):
        low = dinc(1, dask_key_name="low")
    ev = Event()
    clog = dwait(ev, dask_key_name="clog")
    with dask.annotate(priority=1):
        high = dinc(2, dask_key_name="high")

    async with block_worker(c, s, a, pause):
        low, clog, high = c.compute([low, clog, high], optimize_graph=False)

    await high
    assert s.tasks[low.key].state != "memory"
    await ev.set()
    await clog
    await low


@gen_blockable_cluster
async def test_annotate_persist(c, s, a, pause):
    with dask.annotate(priority=-1):
        low = dinc(1, dask_key_name="low")
    ev = Event()
    clog = dwait(ev, dask_key_name="clog")
    with dask.annotate(priority=1):
        high = dinc(2, dask_key_name="high")

    async with block_worker(c, s, a, pause):
        low, clog, high = c.persist([low, clog, high], optimize_graph=False)

    await high
    assert s.tasks[low.key].state != "memory"
    await ev.set()
    await clog
    await low


@gen_blockable_cluster
async def test_repeated_persists_same_priority(c, s, a, pause):
    xs = [delayed(slowinc)(i, delay=0.05, dask_key_name=f"x{i}") for i in range(10)]
    ys = [delayed(slowinc)(xs[i], delay=0.05, dask_key_name=f"y{i}") for i in range(10)]
    zs = [delayed(slowinc)(xs[i], delay=0.05, dask_key_name=f"z{i}") for i in range(10)]

    async with block_worker(c, s, a, pause, 30, 10):
        ys = dask.persist(*ys)
        zs = dask.persist(*zs)

    while (
        sum(t.state == "memory" for t in s.tasks.values()) < 5
    ):  # TODO: reduce this number
        await asyncio.sleep(0.01)

    assert 0 < sum(s.tasks[fut.key].state == "memory" for fut in xs) < 10
    assert 0 < sum(s.tasks[fut.key].state == "memory" for fut in ys) < 10
    assert 0 < sum(s.tasks[fut.key].state == "memory" for fut in zs) < 10


@gen_blockable_cluster
async def test_last_in_first_out(c, s, a, pause):
    async with block_worker(c, s, a, pause, 15, 5):
        xs = [c.submit(slowinc, i, delay=0.05, key=f"x{i}") for i in range(5)]
        ys = [c.submit(slowinc, xs[i], delay=0.05, key=f"y{i}") for i in range(5)]
        zs = [c.submit(slowinc, ys[i], delay=0.05, key=f"z{i}") for i in range(5)]

    while not any(s.tasks[z.key].state == "memory" for z in zs):
        await asyncio.sleep(0.01)
    assert not all(s.tasks[x.key].state == "memory" for x in xs)
