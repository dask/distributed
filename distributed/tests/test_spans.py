from __future__ import annotations

import asyncio

import pytest

import dask
from dask import delayed

import distributed
from distributed import Client, Event, Future, Worker, span, wait
from distributed.compatibility import WINDOWS
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.metrics import time
from distributed.utils_test import (
    NoSchedulerDelayWorker,
    async_poll_for,
    gen_cluster,
    inc,
    slowinc,
    wait_for_state,
)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_spans(c, s, a):
    x = delayed(inc)(1)  # Default span

    with span("my workflow") as mywf_id:
        with span("p1") as p1_id:
            y = x + 1
        with span("p2") as p2_id:
            z = y * 2

    zp = c.persist(z)
    assert await c.compute(zp) == 6

    ext = s.extensions["spans"]

    assert mywf_id
    assert p1_id
    assert p2_id
    assert s.tasks[y.key].group.span_id == p1_id
    assert s.tasks[z.key].group.span_id == p2_id
    assert mywf_id != p1_id != p2_id

    expect_annotations = {
        x: {},
        y: {"span": {"name": ("my workflow", "p1"), "ids": (mywf_id, p1_id)}},
        z: {"span": {"name": ("my workflow", "p2"), "ids": (mywf_id, p2_id)}},
    }

    for fut in (x, y, z):
        sts = s.tasks[fut.key]
        wts = a.state.tasks[fut.key]
        assert sts.annotations == expect_annotations[fut]
        assert wts.annotations == expect_annotations[fut]
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

    assert default.annotation is None
    assert mywf.annotation == {"name": ("my workflow",), "ids": (mywf.id,)}
    assert p1.annotation == {"name": ("my workflow", "p1"), "ids": (mywf.id, p1.id)}

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
    with span("foo"):
        x = c.submit(inc, 1, key="x")
    with span("foo"):
        y = c.submit(inc, x, key="y")
    with span("foo"):
        z = c.submit(inc, y, key="z")
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
async def test_task_groups(c, s, a, b, release, no_time_resync):
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
async def test_before_first_task_finished(c, s, a, no_time_resync):
    t0 = time()
    ev = Event()
    x = c.submit(ev.wait, key="x")
    await wait_for_state("x", "executing", a)
    sp = s.extensions["spans"].spans_search_by_name["default",][-1]
    t1 = time()
    assert t0 < sp.enqueued < t1
    assert sp.start == 0
    assert t1 < sp.stop < t1 + 1
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
    updated. This includes scheduler plugins.
    """

    class MyPlugin(SchedulerPlugin):
        annotations = []

        def update_graph(self, scheduler, annotations, **kwargs):
            self.annotations.append(annotations)

    s.add_plugin(MyPlugin(), name="my-plugin")

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

    sts0 = s.tasks[x0.key]
    sts1 = s.tasks[x1.key]
    wts0 = a.state.tasks[x0.key]
    wts1 = a.state.tasks[x1.key]
    assert sts0.group is sts1.group
    assert wts0.span_id == wts1.span_id

    if use_default:
        assert s.plugins["my-plugin"].annotations == [{}, {}]
        for ts in (sts0, sts1, wts0, wts1):
            assert "span" not in ts.annotations
    else:
        expect = {"ids": (wts0.span_id,), "name": ("p1",)}
        assert s.plugins["my-plugin"].annotations == [
            {"span": {("x", 0): expect}},
            {"span": {("x", 1): expect}},
        ]
        for ts in (sts0, sts1, wts0, wts1):
            assert ts.annotations["span"] == expect


def test_no_tags():
    with pytest.raises(ValueError, match="at least one"):
        with span():
            pass


@gen_cluster(client=True)
async def test_client_desires_keys_creates_ts(c, s, a, b):
    """A TaskState object is created by client_desires_keys, and
    is only later submitted with submit/compute by a different client

    See also
    --------
    test_scheduler.py::test_client_desires_keys_creates_ts
    test_spans.py::test_client_desires_keys_creates_tg
    test_spans.py::test_scatter_creates_ts
    test_spans.py::test_scatter_creates_tg
    """
    x = Future(key="x")
    await wait_for_state("x", "released", s)
    assert s.tasks["x"].group.span_id is None
    async with Client(s.address, asynchronous=True) as c2:
        c2.submit(inc, 1, key="x")
        assert await x == 2
    assert s.tasks["x"].group.span_id is not None


@gen_cluster(client=True)
async def test_client_desires_keys_creates_tg(c, s, a, b):
    """A TaskGroup object is created by client_desires_keys, and
    only later gains runnable tasks

    See also
    --------
    test_spans.py::test_client_desires_keys_creates_ts
    test_spans.py::test_scatter_creates_ts
    test_spans.py::test_scatter_creates_tg
    """
    x0 = Future(key="x-0")
    await wait_for_state("x-0", "released", s)
    assert s.tasks["x-0"].group.span_id is None
    x1 = c.submit(inc, 1, key="x-1")
    assert await x1 == 2
    assert s.tasks["x-0"].group.span_id is not None


@gen_cluster(client=True)
async def test_scatter_creates_ts(c, s, a, b):
    """A TaskState object is created by scatter, and only later becomes runnable

    See also
    --------
    test_scheduler.py::test_scatter_creates_ts
    test_spans.py::test_client_desires_keys_creates_ts
    test_spans.py::test_client_desires_keys_creates_tg
    test_spans.py::test_scatter_creates_tg
    """
    x1 = (await c.scatter({"x": 1}, workers=[a.address]))["x"]
    await wait_for_state("x", "memory", s)
    assert s.tasks["x"].group.span_id is None
    async with Client(s.address, asynchronous=True) as c2:
        x2 = c2.submit(inc, 1, key="x")
        assert await x2 == 1
        await a.close()
        assert await x2 == 2
    assert s.tasks["x"].group.span_id is not None


@gen_cluster(client=True)
async def test_scatter_creates_tg(c, s, a, b):
    """A TaskGroup object is created by scatter, and only later gains runnable tasks

    See also
    --------
    test_spans.py::test_client_desires_keys_creates_ts
    test_spans.py::test_client_desires_keys_creates_tg
    test_spans.py::test_scatter_creates_ts
    """
    x0 = (await c.scatter({"x-0": 1}))["x-0"]
    await wait_for_state("x-0", "memory", s)
    assert s.tasks["x-0"].group.span_id is None
    x1 = c.submit(inc, 1, key="x-1")
    assert await x1 == 2
    assert s.tasks["x-0"].group.span_id is not None


@gen_cluster(client=True)
async def test_worker_metrics(c, s, a, b):
    """Test that Scheduler.cumulative_worker_metrics and Span.cumulative_worker_metrics
    are sync'ed for all 'execute' activities

    See also
    --------
    test_worker_metrics.py::test_send_metrics_to_scheduler
    """
    bar_sid = []
    with span("foo") as foo_sid:
        # Make sure that cumulative sync over multiple heartbeats works as expected
        for _ in range(2):
            # Span 'foo' remains the same across for iterations
            x0 = c.submit(inc, 1, key=("x", 0), workers=[a.address])
            x1 = c.submit(inc, 1, key=("x", 1), workers=[b.address])
            await wait([x0, x1])

            # Span 'bar' is opened and closed between for iterations
            with span("bar") as bar_sid_i:
                bar_sid.append(bar_sid_i)
                await c.submit(inc, 1, key=("y", 0))

            # Populate gather_dep and get_data metrics.
            # They will be ignored by the spans extension because they have no span_id.
            x2 = c.submit(inc, x0, key=("x", 2), workers=[b.address])
            x3 = c.submit(inc, x1, key=("x", 3), workers=[a.address])
            await wait([x2, x3])

            # Flush metrics from workers to scheduler
            await a.heartbeat()
            await b.heartbeat()

            # Cleanup
            del x0, x1, x2, x3
            await async_poll_for(lambda: not s.tasks, timeout=5)

            # Have metrics with 'y' task prefix in foo too
            await c.submit(inc, 1, key=("y", 1))

    ext = s.extensions["spans"]

    foo_metrics = ext.spans[foo_sid].cumulative_worker_metrics
    bar0_metrics = ext.spans[bar_sid[0]].cumulative_worker_metrics
    bar1_metrics = ext.spans[bar_sid[1]].cumulative_worker_metrics

    # metrics for foo include self and its child bar
    assert list(foo_metrics) == [
        ("execute", "x", "thread-cpu", "seconds"),
        ("execute", "x", "thread-noncpu", "seconds"),
        ("execute", "x", "executor", "seconds"),
        ("execute", "x", "other", "seconds"),
        ("execute", "x", "memory-read", "count"),
        ("execute", "x", "memory-read", "bytes"),
        ("execute", "y", "thread-cpu", "seconds"),
        ("execute", "y", "thread-noncpu", "seconds"),
        ("execute", "y", "executor", "seconds"),
        ("execute", "y", "other", "seconds"),
        ("execute", "N/A", "idle or other spans", "seconds"),
    ]
    assert (
        list(bar0_metrics)
        == list(bar1_metrics)
        == [
            ("execute", "y", "thread-cpu", "seconds"),
            ("execute", "y", "thread-noncpu", "seconds"),
            ("execute", "y", "executor", "seconds"),
            ("execute", "y", "other", "seconds"),
            ("execute", "N/A", "idle or other spans", "seconds"),
        ]
    )

    if not WINDOWS:
        for metrics in (foo_metrics, bar0_metrics, bar1_metrics):
            assert all(v > 0 for v in metrics.values()), metrics

    # Metrics have been synchronized from scheduler to spans
    for k, v in foo_metrics.items():
        if k[2] != "idle or other spans":
            assert s.cumulative_worker_metrics[k] == pytest.approx(v), k

    # Metrics for foo contain the sum of metrics from itself and for bar
    for k in bar0_metrics:
        if k[2] != "idle or other spans":
            assert foo_metrics[k] == pytest.approx(
                bar0_metrics[k]
                + bar1_metrics[k]
                + ext.spans[foo_sid]._cumulative_worker_metrics[k]
            ), k


@gen_cluster(client=True)
async def test_merge_by_tags(c, s, a, b):
    with span("foo") as foo1:
        await c.submit(inc, 1, key="x1")
        with span("bar") as bar1:  # foo, bar
            await c.submit(inc, 2, key="x2")
            with span("foo") as foo2:  # foo, bar, foo
                await c.submit(inc, 3, key="x3")
        with span("foo") as foo3:  # foo, foo
            await c.submit(inc, 4, key="x4")
    with span("bar") as bar2:  # bar
        await c.submit(inc, 5, key="x5")

    ext = s.extensions["spans"]
    assert {s.id for s in ext.find_by_tags("foo")} == {foo1}
    assert {s.id for s in ext.find_by_tags("foo", "bar")} == {foo1, bar2}
    assert {s.id for s in ext.find_by_tags("bar", "foo")} == {foo1, bar2}
    assert {s.id for s in ext.find_by_tags("bar")} == {bar1, bar2}

    def tgnames(*tags):
        return [tg.name for tg in ext.merge_by_tags(*tags).traverse_groups()]

    assert tgnames("foo") == ["x1", "x2", "x3", "x4"]
    assert tgnames("foo", "bar") == ["x1", "x2", "x3", "x4", "x5"]
    assert tgnames("bar", "foo") == ["x5", "x1", "x2", "x3", "x4"]
    assert tgnames("bar") == ["x5", "x2", "x3"]


@gen_cluster(client=True)
async def test_merge_by_tags_metrics(c, s, a, b):
    with span("foo") as foo1:
        await c.submit(slowinc, 1, delay=0.05, key="x-1")
    await async_poll_for(lambda: not s.task_groups, timeout=5)

    with span("foo") as foo2:
        await c.submit(slowinc, 2, delay=0.06, key="x-2")
    await async_poll_for(lambda: not s.task_groups, timeout=5)

    with span("bar") as bar1:
        await c.submit(slowinc, 3, delay=0.07, key="x-3")
    await async_poll_for(lambda: not s.task_groups, timeout=5)

    await a.heartbeat()
    await b.heartbeat()

    ext = s.extensions["spans"]
    k = ("execute", "x", "thread-noncpu", "seconds")
    t_foo = ext.merge_by_tags("foo").cumulative_worker_metrics[k]
    t_bar = ext.merge_by_tags("bar").cumulative_worker_metrics[k]
    t_foo1 = ext.spans[foo1]._cumulative_worker_metrics[k]
    t_foo2 = ext.spans[foo2]._cumulative_worker_metrics[k]
    t_bar1 = ext.spans[bar1]._cumulative_worker_metrics[k]
    assert t_foo1 > 0
    assert t_foo2 > 0
    assert t_bar1 > 0
    assert t_foo == t_foo1 + t_foo2
    assert t_bar == t_bar1

    assert ext.merge_by_tags("foo").enqueued == min(
        ext.spans[foo1].enqueued, ext.spans[foo2].enqueued
    )


@gen_cluster(client=True, config={"distributed.diagnostics.computations.nframes": 10})
async def test_code(c, s, a, b):
    with span("foo") as foo:
        for subspan in ("bar", "baz"):
            with span(subspan):
                for key in ("x", "y"):
                    # Call update-graph four times in two different subspans,
                    # all with the same code stack
                    await c.submit(inc, 1, key=key)

        await c.submit(inc, 2, key="z")
        await c.submit(inc, 3, key="w")

    code = s.extensions["spans"].spans[foo].code

    # Identical code stacks have been deduplicated
    assert len(code) == 3
    assert "inc, 1" in code[0][-1].code
    assert code[0][-1].lineno_relative == 10
    assert code[1][-1].lineno_relative == 11
    assert code[2][-1].lineno_relative == 8


@gen_cluster(client=True)
async def test_no_code_by_default(c, s, a, b):
    with span("foo") as foo:
        await c.submit(inc, 1, key="x")
    assert s.extensions["spans"].spans[foo].code == []


@gen_cluster(client=True)
async def test_merge_all(c, s, a, b):
    with span("foo"):
        await c.submit(inc, 1, key="x")
        with span("bar"):
            await c.submit(inc, 1, key="y")
    await c.submit(inc, 1, key="z")
    sp = s.extensions["spans"].merge_all()
    assert [tg.name for tg in sp.traverse_groups()] == ["x", "y", "z"]


@gen_cluster(nthreads=[])
async def test_merge_nothing(s):
    ext = s.extensions["spans"]
    with pytest.raises(ValueError):
        ext.merge_by_tags()
    with pytest.raises(ValueError):
        ext.merge_all()


@gen_cluster(client=True)
async def test_active_cpu_seconds_trivial(c, s, a, b):
    await c.submit(slowinc, 1, delay=0.1, key="x")
    await a.heartbeat()
    await b.heartbeat()
    span = s.extensions["spans"].spans_search_by_name["default",][0]

    assert span.done
    assert span.nthreads_intervals == [(span.enqueued, span.stop, 3)]
    assert span.active_cpu_seconds == (span.stop - span.enqueued) * 3
    k = "execute", "N/A", "idle or other spans", "seconds"
    assert 0.15 < span.cumulative_worker_metrics[k] < span.active_cpu_seconds


@pytest.mark.parametrize("some_done", [False, True])
@gen_cluster(client=True, nthreads=[("", 2)], Worker=NoSchedulerDelayWorker)
async def test_active_cpu_seconds_not_done(c, s, a, some_done, no_time_resync):
    ev = Event()
    x0 = c.submit(ev.wait, key="x-0", workers=[a.address])
    if some_done:
        await c.submit(inc, 1, key="x-1")
    await wait_for_state("x-0", "executing", a)
    await a.heartbeat()

    span = s.extensions["spans"].spans_search_by_name["default",][0]
    assert not span.done
    now = time()

    intervals = span.nthreads_intervals
    assert len(intervals) == 1
    assert intervals[0][0] == span.enqueued
    assert now < intervals[0][1] < now + 1
    assert intervals[0][2] == 2

    expect = (now - span.enqueued) * 2
    assert expect < span.active_cpu_seconds < expect + 1
    k = "execute", "N/A", "idle or other spans", "seconds"
    assert 0 < span.cumulative_worker_metrics[k] < expect + 1

    await ev.set()
    await x0


@gen_cluster(client=True, Worker=NoSchedulerDelayWorker)
async def test_active_cpu_seconds_change_nthreads(c, s, a, b, no_time_resync):
    ev = Event()
    x = c.submit(ev.wait, key="x", workers=[a.address])
    await wait_for_state("x", "executing", a)

    assert s.total_nthreads == 3
    await asyncio.sleep(0.01)
    async with Worker(s.address, nthreads=4):
        assert s.total_nthreads == 7
        await asyncio.sleep(0.01)
    assert s.total_nthreads == 3
    await asyncio.sleep(0.01)
    await ev.set()
    await x
    async with Worker(s.address, nthreads=2):
        assert s.total_nthreads == 5
        await asyncio.sleep(0.01)
    assert s.total_nthreads == 3
    await asyncio.sleep(0.01)

    await a.heartbeat()
    await b.heartbeat()

    span = s.extensions["spans"].spans_search_by_name["default",][0]
    assert span.done

    # There were two more changes in nthreads_total, from 3 to 5 and then back to 3, but
    # they do not appear in nthreads_intervals because they happened past the lifetime
    # of the span
    assert len(span.nthreads_intervals) == 3
    assert span.nthreads_intervals[0][2] == 3
    assert span.nthreads_intervals[1][2] == 7
    assert span.nthreads_intervals[2][2] == 3
    t0, t1, t2, t3, t4, t5 = (
        span.nthreads_intervals[0][0],
        span.nthreads_intervals[0][1],
        span.nthreads_intervals[1][0],
        span.nthreads_intervals[1][1],
        span.nthreads_intervals[2][0],
        span.nthreads_intervals[2][1],
    )
    assert t0 == span.enqueued
    assert t1 == t2
    assert t3 == t4
    assert t5 == span.stop
    assert t0 < t1 < t3 < t5

    assert span.active_cpu_seconds == pytest.approx(
        (t1 - t0) * 3 + (t3 - t2) * 7 + (t5 - t4) * 3
    )


@gen_cluster(client=True, nthreads=[("", 2)], Worker=NoSchedulerDelayWorker)
async def test_active_cpu_seconds_merged(c, s, a, no_time_resync):
    """Overlapping input spans are not double-counted
    Empty gap between input spans is not counted
    """
    ev1 = Event()
    ev2 = Event()

    with span("root") as root_id:
        with span("foo") as foo_id:
            x = c.submit(ev1.wait, key="x")
        await wait_for_state("x", "executing", a)
        await asyncio.sleep(0.1)

        with span("bar") as bar_id:
            y = c.submit(ev2.wait, key="y")
        await wait_for_state("y", "executing", a)

        await asyncio.sleep(0.1)
        await ev1.set()
        await x
        await asyncio.sleep(0.1)
        await ev2.set()
        await y

        await asyncio.sleep(0.1)
        with span("baz") as baz_id:
            await c.submit(inc, 1, key="z")

    await a.heartbeat()
    ext = s.extensions["spans"]
    root = ext.spans[root_id]
    foo = ext.spans[foo_id]
    bar = ext.spans[bar_id]
    baz = ext.spans[baz_id]
    merged = ext.merge_by_tags("foo", "bar", "baz")

    assert foo.enqueued - 0.1 < root.enqueued < foo.enqueued
    assert merged.enqueued == foo.enqueued
    assert merged.stop == root.stop == baz.stop
    assert bar.stop < baz.enqueued
    assert root.nthreads_intervals == [(root.enqueued, baz.stop, 2)]
    assert merged.nthreads_intervals == [
        (foo.enqueued, bar.stop, 2),
        (baz.enqueued, baz.stop, 2),
    ]
    assert root.active_cpu_seconds == pytest.approx((baz.stop - root.enqueued) * 2)
    assert merged.active_cpu_seconds == pytest.approx(
        (bar.stop - foo.enqueued + baz.stop - baz.enqueued) * 2
    )


@gen_cluster(client=True)
async def test_spans_are_visible_from_tasks(c, s, a, b):
    def f():
        client = distributed.get_client()
        with span("bar"):
            return client.submit(inc, 1).result()

    with span("foo") as foo_id:
        annotations = await c.submit(dask.get_annotations)
        assert annotations == {"span": {"name": ("foo",), "ids": (foo_id,)}}
        assert await c.submit(f) == 2

    ext = s.extensions["spans"]
    assert list(ext.spans_search_by_name) == [("foo",), ("foo", "bar")]

    # No annotation is created for the default span
    assert await c.submit(dask.get_annotations) == {}
