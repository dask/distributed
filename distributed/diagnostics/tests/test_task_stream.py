from __future__ import annotations

import os
from time import sleep

import pytest
from tlz import frequencies

from distributed import get_task_stream
from distributed.client import wait
from distributed.diagnostics.task_stream import TaskStreamPlugin
from distributed.metrics import time
from distributed.utils_test import div, gen_cluster, inc, slowinc


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_TaskStreamPlugin(c, s, *workers):
    es = TaskStreamPlugin(s)
    s.add_plugin(es)
    assert not es.buffer

    tasks = c.map(div, [1] * 10, range(10))
    total = c.submit(sum, tasks[1:])
    await wait(total)

    assert len(es.buffer) == 11

    workers = dict()

    rects = es.rectangles(0, 10, workers)
    assert workers
    assert all(n == "div" for n in rects["name"])
    assert all(d > 0 for d in rects["duration"])
    counts = frequencies(rects["color"])
    assert counts["black"] == 1
    assert set(counts.values()) == {9, 1}
    assert len(set(rects["y"])) == 3

    rects = es.rectangles(2, 5, workers)
    assert all(len(L) == 3 for L in rects.values())

    starts = sorted(rects["start"])
    rects = es.rectangles(
        2, 5, workers=workers, start_boundary=(starts[0] + starts[1]) / 2000
    )
    assert set(rects["start"]).issubset(set(starts[1:]))


@gen_cluster(client=True)
async def test_maxlen(c, s, a, b):
    tasks_plugin = TaskStreamPlugin(s, maxlen=5)
    s.add_plugin(tasks_plugin)
    tasks = c.map(inc, range(10))
    await wait(tasks)
    assert len(tasks_plugin.buffer) == 5


@gen_cluster(client=True)
async def test_collect(c, s, a, b):
    tasks_plugin = TaskStreamPlugin(s)
    s.add_plugin(tasks_plugin)
    start = time()
    tasks = c.map(slowinc, range(10), delay=0.1)
    await wait(tasks)

    L = tasks_plugin.collect()
    assert len(L) == len(tasks)
    L = tasks_plugin.collect(start=start)
    assert len(L) == len(tasks)

    L = tasks_plugin.collect(start=start + 0.2)
    assert 4 <= len(L) <= len(tasks)

    L = tasks_plugin.collect(start="20 s")
    assert len(L) == len(tasks)

    L = tasks_plugin.collect(start="500ms")
    assert 0 < len(L) <= len(tasks)

    L = tasks_plugin.collect(count=3)
    assert len(L) == 3
    assert L == list(tasks_plugin.buffer)[-3:]

    assert tasks_plugin.collect(stop=start + 100, count=3) == tasks_plugin.collect(
        count=3
    )
    assert tasks_plugin.collect(start=start, count=3) == list(tasks_plugin.buffer)[:3]


@gen_cluster(client=True)
async def test_no_startstops(c, s, a, b):
    tasks = TaskStreamPlugin(s)
    s.add_plugin(tasks)
    # just to create the key on the scheduler
    task = c.submit(inc, 1)
    await wait(task)
    assert len(tasks.buffer) == 1

    tasks.transition(task.key, "processing", "erred", stimulus_id="s1")
    # Transition was not recorded because it didn't contain `startstops`
    assert len(tasks.buffer) == 1

    tasks.transition(task.key, "processing", "erred", stimulus_id="s2", startstops=[])
    # Transition was not recorded because `startstops` was empty
    assert len(tasks.buffer) == 1

    tasks.transition(
        task.key,
        "processing",
        "erred",
        stimulus_id="s3",
        startstops=[dict(start=time(), stop=time())],
    )
    assert len(tasks.buffer) == 2


@gen_cluster(client=True)
async def test_client(c, s, a, b):
    L = await c.get_task_stream()
    assert L == ()

    tasks = c.map(slowinc, range(10), delay=0.1)
    await wait(tasks)

    tasks = s.plugins[TaskStreamPlugin.name]
    L = await c.get_task_stream()
    assert L == tuple(tasks.buffer)


def test_client_sync(client):
    with get_task_stream(client=client) as ts:
        sleep(0.1)  # to smooth over time differences on the scheduler
        # to smooth over time differences on the scheduler
        tasks = client.map(inc, range(10))
        wait(tasks)

    assert len(ts.data) == 10


@gen_cluster(client=True)
async def test_get_task_stream_plot(c, s, a, b):
    bkm = pytest.importorskip("bokeh.models")
    await c.get_task_stream()

    tasks = c.map(slowinc, range(10), delay=0.1)
    await wait(tasks)

    data, figure = await c.get_task_stream(plot=True)
    assert isinstance(figure, bkm.Plot)


def test_get_task_stream_save(client, tmp_path):
    bkm = pytest.importorskip("bokeh.models")
    tmpdir = str(tmp_path)
    fn = os.path.join(tmpdir, "foo.html")

    with get_task_stream(plot="save", filename=fn) as ts:
        wait(client.map(inc, range(10)))
    with open(fn) as f:
        data = f.read()
    assert "inc" in data
    assert "bokeh" in data

    assert isinstance(ts.figure, bkm.Plot)
