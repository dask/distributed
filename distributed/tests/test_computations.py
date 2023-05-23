"""Tests for distributed.scheduler.Computation objects"""
from __future__ import annotations

import pytest

from distributed import Event, Worker, secede
from distributed.utils_test import async_poll_for, gen_cluster, inc, wait_for_state


@gen_cluster(client=True)
async def test_computations(c, s, a, b):
    da = pytest.importorskip("dask.array")

    x = da.ones(100, chunks=(10,))
    y = (x + 1).persist()
    await y

    z = (x - 2).persist()
    await z

    assert len(s.computations) == 2
    assert "add" in str(s.computations[0].groups)
    assert "sub" in str(s.computations[1].groups)
    assert "sub" not in str(s.computations[0].groups)

    assert isinstance(repr(s.computations[1]), str)

    assert s.computations[1].stop == max(tg.stop for tg in s.task_groups.values())

    assert s.computations[0].states["memory"] == y.npartitions


@gen_cluster(client=True)
async def test_computations_futures(c, s, a, b):
    futures = [c.submit(inc, i) for i in range(10)]
    total = c.submit(sum, futures)
    await total

    [computation] = s.computations
    assert "sum" in str(computation.groups)
    assert "inc" in str(computation.groups)


@gen_cluster(client=True, nthreads=[])
async def test_computations_no_workers(c, s):
    """If a computation is stuck due to lack of workers, don't create a new one"""
    x = c.submit(inc, 1, key="x")
    await wait_for_state("x", ("queued", "no-worker"), s)
    y = c.submit(inc, 2, key="y")
    await wait_for_state("y", ("queued", "no-worker"), s)
    assert s.total_occupancy == 0
    async with Worker(s.address):
        assert await x == 2
        assert await y == 3
        [computation] = s.computations
        assert computation.groups == {s.task_groups["x"], s.task_groups["y"]}


@gen_cluster(client=True)
async def test_computations_no_resources(c, s, a, b):
    """If a computation is stuck due to lack of resources, don't create a new one"""
    x = c.submit(inc, 1, key="x", resources={"A": 1})
    await wait_for_state("x", "no-worker", s)
    y = c.submit(inc, 2, key="y")
    assert await y == 3
    assert s.total_occupancy == 0
    async with Worker(s.address, resources={"A": 1}):
        assert await x == 2
        [computation] = s.computations
        assert computation.groups == {s.task_groups["x"], s.task_groups["y"]}


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_computations_long_running(c, s, a):
    """Don't create new computations if there are long-running tasks"""
    ev = Event()

    def func(ev):
        secede()
        ev.wait()

    x = c.submit(func, ev, key="x")
    await wait_for_state("x", "long-running", a)
    await async_poll_for(lambda: s.total_occupancy == 0, timeout=5)
    y = c.submit(inc, 1, key="y")
    assert await y == 2
    await ev.set()
    await x
    [computation] = s.computations
    assert computation.groups == {s.task_groups["x"], s.task_groups["y"]}
