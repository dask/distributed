from __future__ import annotations

import pytest

from dask import delayed

from distributed import Event
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

    @span("p2")
    def f(i):
        return i * 2

    with span("my workflow") as mywf_id:
        with span("p1") as p1_id:
            y = x + 1
        z = f(y)

    zp = c.persist(z)
    assert await c.compute(zp) == 6

    ext = s.extensions["spans"]

    assert mywf_id
    assert p1_id
    assert s.tasks[y.key].group.span_id == p1_id

    for fut in (x, y, z):
        sts = s.tasks[fut.key]
        wts = a.state.tasks[fut.key]
        assert sts.annotations == {}
        assert wts.annotations == {}
        assert sts.group.span_id == wts.span_id
        assert sts.group.span_id in ext.spans
        assert sts.group in ext.spans[sts.group.span_id].groups

    for k, sp in ext.spans.items():
        assert sp.id == k
        assert sp in ext.spans_search_by_name[sp.name]
        for tag in sp.name:
            assert sp in ext.spans_search_by_tag[tag]

    assert list(ext.spans_search_by_name) == [
        # Scheduler._generate_taskstates yields tasks from rightmost leaf to root
        ("my workflow",),
        ("my workflow", "p2"),
        ("my workflow", "p1"),
        ("default",),
    ]

    default = ext.spans_search_by_name["default",][0]
    mywf = ext.spans_search_by_name["my workflow",][0]
    p1 = ext.spans_search_by_name["my workflow", "p1"][0]
    p2 = ext.spans_search_by_name["my workflow", "p2"][0]

    assert default.children == []
    assert mywf.children == [p2, p1]
    assert p1.children == []
    assert p2.children == []
    assert p1.parent is p2.parent is mywf
    assert mywf.parent is None
    assert default.parent is None

    assert str(default).startswith("Span<name=('default',), id=")
    assert ext.root_spans == [mywf, default]
    assert ext.spans_search_by_name["my workflow",] == [mywf]
    assert ext.spans_search_by_tag["my workflow"] == [mywf, p2, p1]

    # Test that spans survive their tasks
    prev_span_ids = set(ext.spans)
    del zp
    await async_poll_for(lambda: not s.tasks, timeout=5)
    assert ext.spans.keys() == prev_span_ids


@gen_cluster(client=True)
async def test_submit(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    with span("foo") as span_id:
        y = c.submit(inc, 2, key="y")
    assert await x == 2
    assert await y == 3

    default = s.extensions["spans"].spans_search_by_name["default",][0]
    assert s.tasks["x"].group.span_id == default.id
    assert s.tasks["y"].group.span_id == span_id


@gen_cluster(client=True)
async def test_multiple_tags(c, s, a, b):
    with span("foo", "bar"):
        x = c.submit(inc, 1, key="x")
    assert await x == 2

    sbn = s.extensions["spans"].spans_search_by_name
    assert s.tasks["x"].group.span_id == sbn["foo", "bar"][0].id
    assert s.tasks["x"].group.span_id == sbn["foo", "bar"][0].id
    assert sbn["foo", "bar"][0].parent is sbn["foo",][0]


@gen_cluster(client=True)
async def test_repeat_span(c, s, a, b):
    """Opening and closing the same span will result in multiple spans with different
    ids and same name
    """

    @span("foo")
    def f(x, key):
        return c.submit(inc, x, key=key)

    with span("foo"):
        x = c.submit(inc, 1, key="x")
    y = f(x, key="y")
    z = f(y, key="z")
    assert await z == 4

    sbn = s.extensions["spans"].spans_search_by_name["foo",]
    assert len(sbn) == 3
    assert sbn[0].id != sbn[1].id != sbn[2].id
    assert sbn[0].name == sbn[1].name == sbn[2].name == ("foo",)

    assert s.tasks["x"].group.span_id == sbn[0].id
    assert s.tasks["y"].group.span_id == sbn[1].id
    assert s.tasks["z"].group.span_id == sbn[2].id


@gen_cluster(client=True, nthreads=[("", 4)])
async def test_default_span(c, s, a):
    """If the user does not explicitly define a span, tasks are attached to the default
    span. The default span is automatically closed and reopened if all of its tasks are
    done.
    """
    ev1 = Event()
    ev2 = Event()
    ev3 = Event()
    # Tasks not attached to the default span are inconsequential
    with span("foo"):
        x = c.submit(ev1.wait, key="x")
    await wait_for_state("x", "processing", s)

    # Create new default span
    y = c.submit(ev2.wait, key="y")
    await wait_for_state("y", "processing", s)
    # Default span has incomplete tasks; attach to the same span
    z = c.submit(inc, 1, key="z")
    await z

    await ev2.set()
    await y

    # All tasks of the previous default span are done; create a new one
    w = c.submit(ev3.wait, key="w")
    await wait_for_state("w", "processing", s)

    defaults = s.extensions["spans"].spans_search_by_name["default",]
    assert len(defaults) == 2
    assert defaults[0].id != defaults[1].id
    assert defaults[0].name == defaults[1].name == ("default",)
    assert s.tasks["y"].group.span_id == defaults[0].id
    assert s.tasks["z"].group.span_id == defaults[0].id
    assert s.tasks["w"].group.span_id == defaults[1].id
    assert defaults[0].done is True
    assert defaults[1].done is False
    await ev3.set()
    await w
    assert defaults[1].done is True

    await ev1.set()  # Let x complete


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
    assert s.tasks["x"].group.span_id is None
    assert a.state.tasks["x"].span_id is None


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

    sbn = s.extensions["spans"].spans_search_by_name
    root = sbn["wf",][0]
    assert [s.name for s in root.traverse_spans()] == [
        ("wf",),
        ("wf", "p2"),
        ("wf", "p1"),
    ]

    # Note: TaskGroup.prefix is reset when a TaskGroup is forgotten
    assert {tg.name.rsplit("-", 1)[0] for tg in root.traverse_groups()} == {
        "sum",
        "add",
        "finalize",
        "sum-aggregate",
    }
    assert {tg.name.rsplit("-", 1)[0] for tg in sbn["wf", "p1"][0].groups} == {
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
    sp = s.extensions["spans"].spans_search_by_name["default",][-1]
    t1 = time()
    assert t0 < sp.enqueued < t1
    assert sp.start == 0
    assert sp.stop == 0
    assert sp.duration == 0
    assert sp.all_durations == {}
    assert sp.nbytes_total == 0

    await ev.set()
    await x
    t2 = time()
    assert t0 < sp.enqueued < sp.start < t1 < sp.stop < t2
    assert sp.duration > 0
    assert sp.all_durations["compute"] > 0
    assert sp.nbytes_total > 0


@gen_cluster(client=True)
async def test_duplicate_task_group(c, s, a, b):
    """When a TaskGroup is forgotten, you may end up with multiple TaskGroups with the
    same key attached to the same Span
    """
    with span("foo"):
        for _ in range(2):
            await c.submit(inc, 1, key="x")
            await async_poll_for(lambda: not s.tasks, timeout=5)
    sp = s.extensions["spans"].spans_search_by_name["foo",][-1]
    assert len(sp.groups) == 2
    tg0, tg1 = sp.groups
    assert tg0.name == tg1.name
    assert tg0 is not tg1
    assert sp.states["forgotten"] == 2


@pytest.mark.parametrize("use_default", [False, True])
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_mismatched_span(c, s, a, use_default):
    """Test use case of 2+ tasks within the same TaskGroup, but different spans.
    All tasks are coerced to the span of the first seen task, and the annotations are
    updated.
    """
    if use_default:
        x0 = delayed(inc)(1, dask_key_name=("x", 0)).persist()
    else:
        with span("p1"):
            x0 = delayed(inc)(1, dask_key_name=("x", 0)).persist()
    await x0

    with span("p2"):
        x1 = delayed(inc)(2, dask_key_name=("x", 1)).persist()
    await x1
    span_name = ("default",) if use_default else ("p1",)

    sbn = s.extensions["spans"].spans_search_by_name
    # First task to attach to the TaskGroup sets the span. This is arbitrary.
    assert sbn.keys() == {span_name}
    assert len(sbn[span_name][0].groups) == 1
    assert s.task_groups["x"].span_id == sbn[span_name][0].id

    sts0 = s.tasks[str(x0.key)]
    sts1 = s.tasks[str(x1.key)]
    wts0 = a.state.tasks[str(x0.key)]
    wts1 = a.state.tasks[str(x1.key)]
    assert sts0.group is sts1.group
    assert wts0.span_id == wts1.span_id


def test_no_tags():
    with pytest.raises(ValueError, match="at least one"):
        with span():
            pass
