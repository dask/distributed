from __future__ import annotations

import pytest

from dask import delayed

from distributed import Event, SchedulerPlugin
from distributed.metrics import time
from distributed.spans import span
from distributed.utils_test import (
    NoSchedulerDelayWorker,
    async_poll_for,
    gen_cluster,
    inc,
    wait_for_state,
)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_spans(c, s, a):
    x = delayed(inc)(1)  # Default span
    with span("my workflow"):
        with span("p1"):
            y = x + 1

        @span("p2")
        def f(i):
            return i * 2

        z = f(y)

    zp = c.persist(z)
    assert await c.compute(zp) == 6

    ext = s.extensions["spans"]

    assert s.tasks[x.key].annotations == {}
    assert s.tasks[y.key].annotations == {"span": ("my workflow", "p1")}
    assert s.tasks[z.key].annotations == {"span": ("my workflow", "p2")}

    assert a.state.tasks[x.key].annotations == {}
    assert a.state.tasks[y.key].annotations == {"span": ("my workflow", "p1")}
    assert a.state.tasks[z.key].annotations == {"span": ("my workflow", "p2")}

    assert ext.spans.keys() == {
        ("default",),
        ("my workflow",),
        ("my workflow", "p1"),
        ("my workflow", "p2"),
    }
    for k, sp in ext.spans.items():
        assert sp.id == k

    default = ext.spans["default",]
    mywf = ext.spans["my workflow",]
    p1 = ext.spans["my workflow", "p1"]
    p2 = ext.spans["my workflow", "p2"]

    assert default.children == set()
    assert mywf.children == {p1, p2}
    assert p1.children == set()
    assert p2.children == set()

    assert str(default) == "Span('default',)"
    assert str(p1) == "Span('my workflow', 'p1')"
    assert ext.root_spans == {"default": default, "my workflow": mywf}
    assert ext.spans_search_by_tag["my workflow"] == {mywf, p1, p2}

    assert s.tasks[x.key].annotations == {}
    assert s.tasks[y.key].annotations["span"] == ("my workflow", "p1")

    # Test that spans survive their tasks
    del zp
    await async_poll_for(lambda: not s.tasks, timeout=5)
    assert ext.spans.keys() == {
        ("default",),
        ("my workflow",),
        ("my workflow", "p1"),
        ("my workflow", "p2"),
    }


@gen_cluster(client=True)
async def test_submit(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    with span("foo"):
        y = c.submit(inc, 2, key="y")
    assert await x == 2
    assert await y == 3

    assert "span" not in s.tasks["x"].annotations
    assert s.tasks["y"].annotations["span"] == ("foo",)
    assert s.extensions["spans"].spans.keys() == {("default",), ("foo",)}


@gen_cluster(client=True)
async def test_multiple_tags(c, s, a, b):
    with span("foo", "bar"):
        x = c.submit(inc, 1, key="x")
    assert await x == 2

    assert s.tasks["x"].annotations["span"] == ("foo", "bar")
    assert s.extensions["spans"].spans_search_by_tag.keys() == {"foo", "bar"}


@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    scheduler_kwargs={"extensions": {}},
    worker_kwargs={"extensions": {}},
)
async def test_no_extension(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    assert await x == 2
    assert "spans" not in s.extensions
    assert s.tasks["x"].annotations == {}


@pytest.mark.parametrize("release", [False, True])
@gen_cluster(
    client=True,
    Worker=NoSchedulerDelayWorker,
    config={"optimization.fuse.active": False},
)
async def test_task_groups(c, s, a, b, release):
    da = pytest.importorskip("dask.array")
    with span("wf"):
        with span("p1"):
            a = da.ones(10, chunks=5, dtype="int64") + 1
        with span("p2"):
            a = a + 2
        a = a.sum()  # A TaskGroup attached directly to a non-leaf Span
        finalizer = c.compute(a)

    t0 = time()
    assert await finalizer == 40
    t1 = time()

    if release:
        # Test that the information in the Spans survives the tasks
        finalizer.release()
        await async_poll_for(lambda: not s.tasks, timeout=5)
        assert not s.task_groups

    spans = s.extensions["spans"].spans
    root = spans["wf",]
    assert {s.id for s in root.traverse_spans()} == {
        ("wf",),
        ("wf", "p1"),
        ("wf", "p2"),
    }

    # Note: TaskGroup.prefix is reset when a TaskGroup is forgotten
    assert {tg.name.rsplit("-", 1)[0] for tg in root.traverse_groups()} == {
        "sum",
        "add",
        "finalize",
        "sum-aggregate",
    }
    assert {tg.name.rsplit("-", 1)[0] for tg in spans[("wf", "p1")].groups} == {
        "add",
    }

    assert root.nbytes_total == 240
    assert root.duration > 0
    assert root.all_durations["compute"] > 0
    assert t0 < root.enqueued < root.start < root.stop < t1

    if release:
        assert root.states == {
            "erred": 0,
            "forgotten": 8,
            "memory": 0,
            "no-worker": 0,
            "processing": 0,
            "queued": 0,
            "released": 0,
            "waiting": 0,
        }
    else:
        assert root.states == {
            "erred": 0,
            "forgotten": 0,
            "memory": 1,
            "no-worker": 0,
            "processing": 0,
            "queued": 0,
            "released": 7,
            "waiting": 0,
        }


@gen_cluster(client=True, nthreads=[("", 1)], Worker=NoSchedulerDelayWorker)
async def test_before_first_task_finished(c, s, a):
    t0 = time()
    ev = Event()
    x = c.submit(ev.wait, key="x")
    await wait_for_state("x", "executing", a)
    span = s.extensions["spans"].spans["default",]
    t1 = time()
    assert t0 < span.enqueued < t1
    assert span.start == 0
    assert span.stop == 0
    assert span.duration == 0
    assert span.all_durations == {}
    assert span.nbytes_total == 0

    await ev.set()
    await x
    t2 = time()
    assert t0 < span.enqueued < span.start < t1 < span.stop < t2
    assert span.duration > 0
    assert span.all_durations["compute"] > 0
    assert span.nbytes_total > 0


@gen_cluster(client=True)
async def test_duplicate_task_group(c, s, a, b):
    """When a TaskGroup is forgotten, you may end up with multiple TaskGroups with the
    same key attached to the same Span
    """
    for _ in range(2):
        await c.submit(inc, 1, key="x")
        await async_poll_for(lambda: not s.tasks, timeout=5)
    span = s.extensions["spans"].spans["default",]
    assert len(span.groups) == 2
    tg0, tg1 = span.groups
    assert tg0.name == tg1.name
    assert tg0 is not tg1
    assert span.states["forgotten"] == 2


@pytest.mark.parametrize("use_default", [False, True])
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_mismatched_span(c, s, a, use_default):
    """Test use case of 2+ tasks within the same TaskGroup, but different spans.
    All tasks are coerced to the span of the first seen task, and the annotations are
    updated.
    Also test that SchedulerPlugin.update_graph() receives the updated annotations.
    """

    class MyPlugin(SchedulerPlugin):
        name = "my_plugin"
        seen = []

        def update_graph(self, scheduler, *, tasks, annotations, **kwargs):
            self.seen.append((tasks, annotations))

    await c.register_scheduler_plugin(MyPlugin())

    if use_default:
        x0 = delayed(inc)(1, dask_key_name=("x", 0)).persist()
    else:
        with span("p1"):
            x0 = delayed(inc)(1, dask_key_name=("x", 0)).persist()
    await x0

    with span("p2"):
        x1 = delayed(inc)(2, dask_key_name=("x", 1)).persist()
    await x1
    span_id = ("default",) if use_default else ("p1",)

    spans = s.extensions["spans"].spans
    # First task to attach to the TaskGroup sets the span. This is arbitrary.
    assert spans.keys() == {span_id}
    assert len(spans[span_id].groups) == 1
    assert s.task_groups["x"].span == span_id

    sts0 = s.tasks[str(x0.key)]
    sts1 = s.tasks[str(x1.key)]
    wts0 = a.state.tasks[str(x0.key)]
    wts1 = a.state.tasks[str(x1.key)]
    assert sts0.group == sts1.group
    # The annotation on x1 has been changed
    if use_default:
        assert not sts0.annotations
        assert not sts1.annotations
        assert not wts0.annotations
        assert not wts1.annotations
        assert s.plugins["my_plugin"].seen == [(["('x', 0)"], {}), (["('x', 1)"], {})]
    else:
        assert (
            sts0.annotations
            == sts1.annotations
            == wts0.annotations
            == wts1.annotations
            == ({"span": ("p1",)})
        )
        assert s.plugins["my_plugin"].seen == [
            (["('x', 0)"], {"span": {"('x', 0)": ("p1",)}}),
            (["('x', 1)"], {"span": {"('x', 1)": ("p1",)}}),
        ]
