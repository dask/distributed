from __future__ import absolute_import, division, print_function

import os
from time import sleep

import pytest
from toolz import frequencies

from distributed import Client, get_task_stream
from distributed.utils_test import gen_cluster, div, inc, slowinc, cluster
from distributed.utils_test import loop  # noqa F401
from distributed.client import wait
from distributed.diagnostics.task_stream import TaskStreamPlugin
from distributed.metrics import time


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_TaskStreamPlugin(c, s, *workers):
    es = TaskStreamPlugin(s)
    assert not es.buffer

    futures = c.map(div, [1] * 10, range(10))
    total = c.submit(sum, futures[1:])
    yield wait(total)

    assert len(es.buffer) == 11

    workers = dict()

    rects = es.rectangles(0, 10, workers)
    assert workers
    assert all(n == 'div' for n in rects['name'])
    assert all(d > 0 for d in rects['duration'])
    counts = frequencies(rects['color'])
    assert counts['black'] == 1
    assert set(counts.values()) == {9, 1}
    assert len(set(rects['y'])) == 3

    rects = es.rectangles(2, 5, workers)
    assert all(len(L) == 3 for L in rects.values())

    starts = sorted(rects['start'])
    rects = es.rectangles(2, 5, workers=workers,
                          start_boundary=(starts[0] + starts[1]) / 2000)
    assert set(rects['start']).issubset(set(starts[1:]))


@gen_cluster(client=True)
def test_maxlen(c, s, a, b):
    tasks = TaskStreamPlugin(s, maxlen=5)
    futures = c.map(inc, range(10))
    yield wait(futures)
    assert len(tasks.buffer) == 5


@gen_cluster(client=True)
def test_collect(c, s, a, b):
    tasks = TaskStreamPlugin(s)
    start = time()
    futures = c.map(slowinc, range(10), delay=0.1)
    yield wait(futures)

    L = tasks.collect()
    assert len(L) == len(futures)
    L = tasks.collect(start=start)
    assert len(L) == len(futures)

    L = tasks.collect(start=start + 0.2)
    assert 4 <= len(L) <= len(futures)

    L = tasks.collect(start='20 s')
    assert len(L) == len(futures)

    L = tasks.collect(start='500ms')
    assert 0 < len(L) <= len(futures)

    L = tasks.collect(count=3)
    assert len(L) == 3
    assert L == list(tasks.buffer)[-3:]

    assert tasks.collect(stop=start + 100, count=3) == tasks.collect(count=3)
    assert tasks.collect(start=start, count=3) == list(tasks.buffer)[:3]


@gen_cluster(client=True)
def test_client(c, s, a, b):
    L = yield c.get_task_stream()
    assert L == ()

    futures = c.map(slowinc, range(10), delay=0.1)
    yield wait(futures)

    tasks = [p for p in s.plugins if isinstance(p, TaskStreamPlugin)][0]
    L = yield c.get_task_stream()
    assert L == tuple(tasks.buffer)


def test_client_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with get_task_stream(client=c) as ts:
                sleep(0.1)  # to smooth over time differences on the scheduler
                # to smooth over time differences on the scheduler
                futures = c.map(inc, range(10))
                wait(futures)

            assert len(ts.data) == 10


@gen_cluster(client=True)
def test_get_task_stream_plot(c, s, a, b):
    bokeh = pytest.importorskip('bokeh')
    yield c.get_task_stream()

    futures = c.map(slowinc, range(10), delay=0.1)
    yield wait(futures)

    data, figure = yield c.get_task_stream(plot=True)
    assert isinstance(figure, bokeh.plotting.Figure)


def test_get_task_stream_save(loop, tmpdir):
    bokeh = pytest.importorskip('bokeh')
    tmpdir = str(tmpdir)
    fn = os.path.join(tmpdir, 'foo.html')
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            with get_task_stream(plot='save', filename=fn) as ts:
                wait(c.map(inc, range(10)))
            with open(fn) as f:
                data = f.read()
            assert 'inc' in data
            assert 'bokeh' in data

            assert isinstance(ts.figure, bokeh.plotting.Figure)
