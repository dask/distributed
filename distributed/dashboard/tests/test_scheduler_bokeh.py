from __future__ import annotations

import asyncio
import json
import re
import ssl
import sys

import pytest

pytest.importorskip("bokeh")
from bokeh.server.server import BokehTornado
from tlz import first
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

import dask
from dask.core import flatten

from distributed import Event
from distributed.client import wait
from distributed.core import Status
from distributed.dashboard import scheduler
from distributed.dashboard.components.scheduler import (
    AggregateAction,
    ClusterMemory,
    ComputePerKey,
    Contention,
    CurrentLoad,
    Events,
    FinePerformanceMetrics,
    Hardware,
    MemoryByKey,
    MemoryColor,
    Occupancy,
    ProcessingHistogram,
    ProfileServer,
    Shuffling,
    StealingEvents,
    StealingTimeSeries,
    SystemMonitor,
    SystemTimeseries,
    TaskGraph,
    TaskGroupGraph,
    TaskProgress,
    TaskStream,
    WorkerNetworkBandwidth,
    WorkersMemory,
    WorkersMemoryHistogram,
    WorkerTable,
)
from distributed.dashboard.components.worker import Counters
from distributed.dashboard.scheduler import applications
from distributed.diagnostics.task_stream import TaskStreamPlugin
from distributed.metrics import context_meter, time
from distributed.spans import span
from distributed.utils import format_dashboard_link
from distributed.utils_test import (
    block_on_event,
    dec,
    div,
    gen_cluster,
    get_cert,
    inc,
    slowinc,
)
from distributed.worker import Worker

# Imported from distributed.dashboard.utils
scheduler.PROFILING = False  # type: ignore


blocklist_apps = {
    # /hardware performs a hardware benchmarks. Particularly on CI this is quite
    # stressful and can cause timeouts.
    "/hardware",
}


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_simple(c, s, a, b):
    port = s.http_server.port
    ev = Event()
    future = c.submit(block_on_event, ev)
    await asyncio.sleep(0.1)

    http_client = AsyncHTTPClient()
    for suffix in applications:
        if suffix in blocklist_apps:
            continue
        response = await http_client.fetch(f"http://localhost:{port}{suffix}")
        body = response.body.decode()
        assert "bokeh" in body.lower()
        assert not re.search("href=./", body)  # no absolute links

    response = await http_client.fetch(
        "http://localhost:%d/individual-plots.json" % port
    )
    response = json.loads(response.body.decode())
    assert response

    await ev.set()
    await future


@gen_cluster(client=True, worker_kwargs={"dashboard": True})
async def test_basic(c, s, a, b):
    for component in [
        TaskStream,
        SystemMonitor,
        Occupancy,
        StealingTimeSeries,
        Contention,
    ]:
        ss = component(s)

        ss.update()
        data = ss.source.data
        assert len(first(data.values()))
        if component is Occupancy:
            assert all("127.0.0.1" in addr for addr in data["escaped_worker"])


@gen_cluster(client=True)
async def test_counters(c, s, a, b):
    pytest.importorskip("crick")
    while "tick-duration" not in s.digests:
        await asyncio.sleep(0.01)
    ss = Counters(s)

    ss.update()
    await asyncio.sleep(0.1)
    ss.update()

    while not len(ss.digest_sources["tick-duration"][0].data["x"]):
        await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_stealing_events(c, s, a, b):
    se = StealingEvents(s)

    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )

    await wait(futures)
    se.update()
    assert len(first(se.source.data.values()))
    assert b.state.tasks
    assert sum(se.source.data["count"]) >= len(b.state.tasks)


@gen_cluster(client=True)
async def test_events(c, s, a, b):
    e = Events(s, "all")

    futures = c.map(
        slowinc, range(100), delay=0.1, workers=a.address, allow_other_workers=True
    )

    while not b.state.tasks:
        await asyncio.sleep(0.01)

    e.update()
    d = dict(e.source.data)
    assert sum(a == "add-worker" for a in d["action"]) == 2


@gen_cluster(client=True)
async def test_task_stream(c, s, a, b):
    ts = TaskStream(s)

    futures = c.map(slowinc, range(10), delay=0.001)

    await wait(futures)

    ts.update()
    d = dict(ts.source.data)

    assert all(len(L) == 10 for L in d.values())
    assert min(d["start"]) == 0  # zero based

    ts.update()
    d = dict(ts.source.data)
    assert all(len(L) == 10 for L in d.values())

    total = c.submit(sum, futures)
    await wait(total)

    ts.update()
    d = dict(ts.source.data)
    assert len(set(map(len, d.values()))) == 1


@gen_cluster(client=True)
async def test_task_stream_n_rectangles(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10)
    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)
    ts.update()

    assert len(ts.source.data["start"]) == 10


@gen_cluster(client=True)
async def test_task_stream_second_plugin(c, s, a, b):
    ts = TaskStream(s, n_rectangles=10, clear_interval=10)
    ts.update()
    futures = c.map(inc, range(10))
    await wait(futures)
    ts.update()

    ts2 = TaskStream(s, n_rectangles=5, clear_interval=10)
    ts2.update()


@gen_cluster(client=True)
async def test_task_stream_clear_interval(c, s, a, b):
    ts = TaskStream(s, clear_interval=200)

    await wait(c.map(inc, range(10)))
    ts.update()
    await asyncio.sleep(0.01)
    await wait(c.map(dec, range(10)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 10

    await asyncio.sleep(0.3)
    await wait(c.map(inc, range(10, 20)))
    ts.update()

    assert len(set(map(len, ts.source.data.values()))) == 1
    assert ts.source.data["name"].count("inc") == 10
    assert ts.source.data["name"].count("dec") == 0


@gen_cluster(client=True)
async def test_TaskProgress(c, s, a, b):
    tp = TaskProgress(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 1 for L in d.values())
    assert d["name"] == ["slowinc"]

    futures2 = c.map(dec, range(5))
    await wait(futures2)

    tp.update()
    d = dict(tp.source.data)
    assert all(len(L) == 2 for L in d.values())
    assert d["name"] == ["slowinc", "dec"]

    del futures, futures2

    while s.tasks:
        await asyncio.sleep(0.01)

    tp.update()
    assert not tp.source.data["all"]


@gen_cluster(client=True)
async def test_TaskProgress_empty(c, s, a, b):
    tp = TaskProgress(s)
    tp.update()

    futures = [c.submit(inc, i, key="f-" + "a" * i) for i in range(20)]
    await wait(futures)
    tp.update()

    del futures
    while s.tasks:
        await asyncio.sleep(0.01)
    tp.update()

    assert not any(len(v) for v in tp.source.data.values())


@gen_cluster(client=True)
async def test_CurrentLoad(c, s, a, b):
    cl = CurrentLoad(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    cl.update()
    d = dict(cl.source.data)

    assert all(len(l) == 2 for l in d.values())
    assert cl.cpu_figure.x_range.end == 200


@gen_cluster(client=True)
async def test_ProcessingHistogram(c, s, a, b):
    ph = ProcessingHistogram(s)
    ph.update()
    assert (ph.source.data["top"] != 0).sum() == 1
    assert ph.source.data["right"][-1] < 2

    futures = c.map(slowinc, range(10), delay=0.050)
    while not s.tasks:
        await asyncio.sleep(0.01)

    ph.update()
    assert ph.source.data["right"][-1] >= 2


@gen_cluster(client=True)
async def test_WorkersMemory(c, s, a, b):
    cl = WorkersMemory(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    cl.update()
    d = dict(cl.source.data)
    llens = {len(l) for l in d.values()}
    # There are 2 workers. There is definitely going to be managed and
    # unmanaged_old; there may be unmanaged_new. There won't be spilled.
    # Empty rects are removed.
    assert llens in ({4}, {5}, {6})
    assert all(d["width"])


@gen_cluster(
    client=True,
    config={
        "distributed.worker.memory.target": 0.7,
        "distributed.worker.memory.spill": False,
        "distributed.worker.memory.pause": False,
    },
    worker_kwargs={"memory_limit": 100},
)
async def test_FinePerformanceMetrics(c, s, a, b):
    cl = FinePerformanceMetrics(s)

    # Test with no metrics
    cl.update()
    assert not cl.visible_functions
    assert not cl.visible_activities
    assert not cl.span_tag_selector.options
    assert not cl.function_selector.options
    assert cl.unit_selector.options == ["seconds"]

    # execute on default span; multiple tasks in same TaskGroup
    x0 = c.submit(inc, 0, key="x-0", workers=[a.address])
    x1 = c.submit(inc, 1, key="x-1", workers=[a.address])

    # execute with spill (output size is individually larger than target)
    w = c.submit(lambda: "x" * 75, key="w", workers=[a.address])

    await wait([x0, x1, w])

    a.data.evict()
    a.data.evict()
    assert not a.data.fast
    assert set(a.data.slow) == {"x-0", "x-1", "w"}

    with span("foo"):
        # execute on named span, with unspill
        y0 = c.submit(inc, x0, key="y-0", workers=[a.address])
        # get_data with unspill + gather_dep + execute on named span
        y1 = c.submit(inc, x1, key="y-1", workers=[b.address])

    # execute on named span (duplicate name, different id)
    with span("foo"):
        z = c.submit(inc, 3, key="z")

    # Custom metric with non-string label and custom unit
    def f():
        context_meter.digest_metric(None, 1, "seconds")
        context_meter.digest_metric(("foo", 1), 2, "seconds")
        context_meter.digest_metric("hideme", 1.1, "custom")

    v = c.submit(f, key="v")
    await wait([y0, y1, z, v])

    await a.heartbeat()
    await b.heartbeat()

    cl.update()
    assert sorted(cl.visible_functions) == ["N/A", "v", "w", "x", "y", "z"]
    assert sorted(cl.function_selector.options) == ["N/A", "v", "w", "x", "y", "z"]
    assert sorted(cl.unit_selector.options) == ["bytes", "count", "custom", "seconds"]
    assert "thread-cpu" in cl.visible_activities
    assert "('foo', 1)" in cl.visible_activities
    assert "None" in cl.visible_activities
    assert "hideme" not in cl.visible_activities
    assert "idle" in cl.visible_activities
    assert "idle or other spans" not in cl.visible_activities
    assert sorted(cl.span_tag_selector.options) == ["default", "foo"]

    orig_activities = cl.visible_activities[:]

    cl.unit_selector.value = "bytes"
    cl.update()
    assert sorted(cl.visible_activities) == ["disk-read", "disk-write", "memory-read"]

    cl.unit_selector.value = "count"
    cl.update()
    assert sorted(cl.visible_activities) == ["disk-read", "disk-write", "memory-read"]

    cl.unit_selector.value = "custom"
    cl.update()
    assert sorted(cl.visible_activities) == ["hideme"]

    cl.unit_selector.value = "seconds"
    cl.update()
    assert cl.visible_activities == orig_activities

    cl.span_tag_selector.value = ["foo"]
    cl.update()
    assert sorted(cl.visible_functions) == ["N/A", "y", "z"]
    assert sorted(cl.function_selector.options) == ["N/A", "v", "w", "x", "y", "z"]
    assert "idle" not in cl.visible_activities
    assert "idle or other spans" in cl.visible_activities


@gen_cluster(
    client=True,
    scheduler_kwargs={"extensions": {}},
    worker_kwargs={"extensions": {}},
)
async def test_FinePerformanceMetrics_no_spans(c, s, a, b):
    cl = FinePerformanceMetrics(s)

    # Test with no metrics
    cl.update()
    assert not cl.visible_functions
    await c.submit(inc, 0, key="x-0")
    await a.heartbeat()
    await b.heartbeat()

    cl.update()
    assert sorted(cl.visible_functions) == ["x"]
    assert sorted(cl.unit_selector.options) == ["bytes", "count", "seconds"]
    assert "thread-cpu" in cl.visible_activities

    cl.unit_selector.value = "bytes"
    cl.update()
    assert sorted(cl.visible_activities) == ["memory-read"]


@gen_cluster(client=True)
async def test_ClusterMemory(c, s, a, b):
    cl = ClusterMemory(s)

    futures = c.map(slowinc, range(10), delay=0.001)
    await wait(futures)

    cl.update()
    d = dict(cl.source.data)
    llens = {len(l) for l in d.values()}
    # Unlike WorkersMemory, empty rects here aren't pruned away.
    assert llens == {4}
    # There is definitely going to be managed and
    # unmanaged_old; there may be unmanaged_new. There won't be spilled.
    assert any(d["width"])
    assert not all(d["width"])


def test_memory_color():
    def config(**kwargs):
        return dask.config.set(
            {f"distributed.worker.memory.{k}": v for k, v in kwargs.items()}
        )

    with config(target=0.6, spill=0.7, pause=0.8, terminate=0.95):
        c = MemoryColor()
        assert c._memory_color(50, 100, Status.running) == "blue"
        assert c._memory_color(60, 100, Status.running) == "orange"
        assert c._memory_color(75, 100, Status.running) == "orange"
        # Pause is not impacted by the paused threshold, but by the worker status
        assert c._memory_color(85, 100, Status.running) == "orange"
        assert c._memory_color(0, 100, Status.paused) == "red"
        assert c._memory_color(0, 100, Status.closing_gracefully) == "red"
        # Passing the terminate threshold will turn the bar red, regardless of pause
        assert c._memory_color(95, 100, Status.running) == "red"
        # Disabling memory limit disables all threshold-related color changes
        assert c._memory_color(100, 0, Status.running) == "blue"
        assert c._memory_color(100, 0, Status.paused) == "red"

    # target disabled
    with config(target=False, spill=0.7):
        c = MemoryColor()
        assert c._memory_color(60, 100, Status.running) == "blue"
        assert c._memory_color(75, 100, Status.running) == "orange"

    # spilling disabled
    with config(target=False, spill=False, pause=0.8, terminate=0.95):
        c = MemoryColor()
        assert c._memory_color(94, 100, Status.running) == "blue"
        assert c._memory_color(0, 100, Status.closing_gracefully) == "red"
        assert c._memory_color(95, 100, Status.running) == "red"

    # terminate disabled; fall back to 100%
    with config(target=False, spill=False, terminate=False):
        c = MemoryColor()
        assert c._memory_color(99, 100, Status.running) == "blue"
        assert c._memory_color(100, 100, Status.running) == "red"
        assert c._memory_color(110, 100, Status.running) == "red"


@gen_cluster(client=True)
async def test_WorkersMemoryHistogram(c, s, a, b):
    nh = WorkersMemoryHistogram(s)
    nh.update()
    assert any(nh.source.data["top"] != 0)

    futures = c.map(inc, range(10))
    await wait(futures)

    nh.update()
    assert nh.source.data["right"][-1] > 5 * 20


@gen_cluster(client=True)
async def test_WorkerTable(c, s, a, b):
    wt = WorkerTable(s)
    wt.update()
    assert all(wt.source.data.values())
    assert all(
        not v or isinstance(v, (str, int, float))
        for L in wt.source.data.values()
        for v in L
    ), {type(v).__name__ for L in wt.source.data.values() for v in L}

    assert all(len(v) == 3 for v in wt.source.data.values())
    assert wt.source.data["name"][0] == "Total (2)"

    nthreads = wt.source.data["nthreads"]
    assert all(nthreads)
    assert nthreads[0] == nthreads[1] + nthreads[2]


@gen_cluster(client=True)
async def test_WorkerTable_custom_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    def metric_address(worker):
        return worker.address

    metrics = {"metric_port": metric_port, "metric_address": metric_address}

    for w in [a, b]:
        for name, func in metrics.items():
            w.metrics[name] = func

    await asyncio.gather(a.heartbeat(), b.heartbeat())

    for w in [a, b]:
        assert s.workers[w.address].metrics["metric_port"] == w.port
        assert s.workers[w.address].metrics["metric_address"] == w.address

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    for name in metrics:
        assert name in data

    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_port"][i] for i in my_index] == [a.port, b.port]
    assert [data["metric_address"][i] for i in my_index] == [a.address, b.address]


@gen_cluster(client=True)
async def test_WorkerTable_different_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert "metric_b" in data
    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]
    assert [data["metric_b"][i] for i in my_index] == [None, b.port]


@gen_cluster(client=True)
async def test_WorkerTable_metrics_with_different_metric_2(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    data = wt.source.data

    assert "metric_a" in data
    assert all(data.values())
    assert all(len(v) == 3 for v in data.values())
    my_index = data["address"].index(a.address), data["address"].index(b.address)
    assert [data["metric_a"][i] for i in my_index] == [a.port, None]


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
async def test_WorkerTable_add_and_remove_metrics(c, s, a, b):
    def metric_port(worker):
        return worker.port

    a.metrics["metric_a"] = metric_port
    b.metrics["metric_b"] = metric_port
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["metric_a"] == a.port
    assert s.workers[b.address].metrics["metric_b"] == b.port

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data
    assert "metric_b" in wt.source.data

    # Remove 'metric_b' from worker b
    del b.metrics["metric_b"]
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" in wt.source.data

    del a.metrics["metric_a"]
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    wt = WorkerTable(s)
    wt.update()
    assert "metric_a" not in wt.source.data


@gen_cluster(client=True)
async def test_WorkerTable_custom_metric_overlap_with_core_metric(c, s, a, b):
    def metric(worker):
        return -999

    a.metrics["cpu"] = metric
    a.metrics["metric"] = metric
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    assert s.workers[a.address].metrics["cpu"] != -999
    assert s.workers[a.address].metrics["metric"] == -999


@gen_cluster(client=True, worker_kwargs={"memory_limit": 0})
async def test_WorkerTable_with_memory_limit_as_0(c, s, a, b):
    wt = WorkerTable(s)
    wt.update()
    assert all(wt.source.data.values())
    assert wt.source.data["name"][0] == "Total (2)"
    assert wt.source.data["memory_limit"][0] == 0
    assert wt.source.data["memory_percent"][0] == ""


@gen_cluster(client=True)
async def test_WorkerNetworkBandwidth(c, s, a, b):
    nb = WorkerNetworkBandwidth(s)
    nb.update()

    assert all(len(v) == 2 for v in nb.source.data.values())

    assert nb.source.data["y_read"] == [0.75, 1.85]
    assert nb.source.data["y_write"] == [0.25, 1.35]


@gen_cluster(client=True)
async def test_WorkerNetworkBandwidth_metrics(c, s, a, b):
    # Disable system monitor periodic callback to allow us to manually control
    # when it is called below
    with dask.config.set({"distributed.admin.system-monitor.disk": False}):
        async with Worker(s.address) as w:
            a.periodic_callbacks["monitor"].stop()
            b.periodic_callbacks["monitor"].stop()
            w.periodic_callbacks["monitor"].stop()

            # Update worker system monitors and send updated metrics to the scheduler
            a.monitor.update()
            b.monitor.update()
            w.monitor.update()
            await asyncio.gather(a.heartbeat(), b.heartbeat())
            await asyncio.gather(a.heartbeat(), b.heartbeat(), w.heartbeat())

            nb = WorkerNetworkBandwidth(s)
            nb.update()

            for idx, ws in enumerate(s.workers.values()):
                assert (
                    ws.metrics["host_net_io"]["read_bps"]
                    == nb.source.data["x_read"][idx]
                )
                assert (
                    ws.metrics["host_net_io"]["write_bps"]
                    == nb.source.data["x_write"][idx]
                )
                assert (
                    ws.metrics.get("host_disk_io", {}).get("read_bps", 0)
                    == nb.source.data["x_read_disk"][idx]
                )
                assert (
                    ws.metrics.get("host_disk_io", {}).get("write_bps", 0)
                    == nb.source.data["x_write_disk"][idx]
                )


@gen_cluster(client=True)
async def test_SystemTimeseries(c, s, a, b):
    # Disable system monitor periodic callback to allow us to manually control
    # when it is called below
    a.periodic_callbacks["monitor"].stop()
    b.periodic_callbacks["monitor"].stop()

    # Update worker system monitors and send updated metrics to the scheduler
    a.monitor.update()
    b.monitor.update()
    await asyncio.gather(a.heartbeat(), b.heartbeat())

    systs = SystemTimeseries(s)
    workers = s.workers.values()

    assert all(len(v) == 1 for v in systs.source.data.values())
    assert systs.source.data["host_net_io.read_bps"][0] == sum(
        ws.metrics["host_net_io"]["read_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_net_io.write_bps"][0] == sum(
        ws.metrics["host_net_io"]["write_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["cpu"][0] == sum(
        ws.metrics["cpu"] for ws in workers
    ) / len(workers)
    assert systs.source.data["memory"][0] == sum(
        ws.metrics["memory"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_disk_io.read_bps"][0] == sum(
        ws.metrics["host_disk_io"]["read_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_disk_io.write_bps"][0] == sum(
        ws.metrics["host_disk_io"]["write_bps"] for ws in workers
    ) / len(workers)
    assert (
        systs.source.data["time"][0]
        == sum(ws.metrics["time"] for ws in workers) / len(workers) * 1000
    )

    # Update worker system monitors and send updated metrics to the scheduler
    a.monitor.update()
    b.monitor.update()
    await asyncio.gather(a.heartbeat(), b.heartbeat())
    systs.update()

    assert all(len(v) == 2 for v in systs.source.data.values())
    assert systs.source.data["host_net_io.read_bps"][1] == sum(
        ws.metrics["host_net_io"]["read_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_net_io.write_bps"][1] == sum(
        ws.metrics["host_net_io"]["write_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["cpu"][1] == sum(
        ws.metrics["cpu"] for ws in workers
    ) / len(workers)
    assert systs.source.data["memory"][1] == sum(
        ws.metrics["memory"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_disk_io.read_bps"][1] == sum(
        ws.metrics["host_disk_io"]["read_bps"] for ws in workers
    ) / len(workers)
    assert systs.source.data["host_disk_io.write_bps"][1] == sum(
        ws.metrics["host_disk_io"]["write_bps"] for ws in workers
    ) / len(workers)
    assert (
        systs.source.data["time"][1]
        == sum(ws.metrics["time"] for ws in workers) / len(workers) * 1000
    )


@gen_cluster(client=True)
async def test_TaskGraph(c, s, a, b):
    gp = TaskGraph(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    await total

    gp.update()
    assert set(map(len, gp.node_source.data.values())) == {6}
    assert set(map(len, gp.edge_source.data.values())) == {5}
    json.dumps(gp.edge_source.data)
    json.dumps(gp.node_source.data)

    da = pytest.importorskip("dask.array")
    x = da.random.random((20, 20), chunks=(10, 10)).persist()
    y = (x + x.T) - x.mean(axis=0)
    y = y.persist()
    await wait(y)

    gp.update()
    gp.update()

    await c.compute((x + y).sum())

    gp.update()

    future = c.submit(inc, 10)
    future2 = c.submit(inc, future)
    await wait(future2)
    key = future.key
    del future, future2
    while key in s.tasks:
        await asyncio.sleep(0.01)

    assert "memory" in gp.node_source.data["state"]

    gp.update()
    gp.update()

    assert not all(x == "False" for x in gp.edge_source.data["visible"])


@gen_cluster(client=True)
async def test_TaskGraph_clear(c, s, a, b):
    gp = TaskGraph(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    await total

    gp.update()

    del total, futures

    while s.tasks:
        await asyncio.sleep(0.01)

    gp.update()
    gp.update()

    start = time()
    while any(gp.node_source.data.values()) or any(gp.edge_source.data.values()):
        await asyncio.sleep(0.1)
        gp.update()
        assert time() < start + 5


@gen_cluster(client=True, config={"distributed.dashboard.graph-max-items": 2})
async def test_TaskGraph_limit(c, s, a, b):
    gp = TaskGraph(s)

    def func(x):
        return x

    f1 = c.submit(func, 1)
    await wait(f1)
    gp.update()
    assert len(gp.node_source.data["x"]) == 1
    f2 = c.submit(func, 2)
    await wait(f2)
    gp.update()
    assert len(gp.node_source.data["x"]) == 2
    f3 = c.submit(func, 3)
    await wait(f3)
    # Breached task limit, clearing graph
    gp.update()
    assert len(gp.node_source.data["x"]) == 0


@gen_cluster(client=True)
async def test_TaskGraph_complex(c, s, a, b):
    da = pytest.importorskip("dask.array")
    gp = TaskGraph(s)
    x = da.random.random((2000, 2000), chunks=(1000, 1000))
    y = ((x + x.T) - x.mean(axis=0)).persist()
    await wait(y)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    z = (x - y).sum().persist()
    await wait(z)
    gp.update()
    assert len(gp.layout.index) == len(gp.node_source.data["x"])
    assert len(gp.layout.index) == len(s.tasks)
    del z
    await asyncio.sleep(0.2)
    gp.update()
    assert len(gp.layout.index) == sum(
        v == "True" for v in gp.node_source.data["visible"]
    )
    assert len(gp.layout.index) == len(s.tasks)
    assert max(gp.layout.index.values()) < len(gp.node_source.data["visible"])
    assert gp.layout.next_index == len(gp.node_source.data["visible"])
    gp.update()
    assert set(gp.layout.index.values()) == set(range(len(gp.layout.index)))
    visible = gp.node_source.data["visible"]
    keys = list(flatten(y.__dask_keys__()))
    assert all(visible[gp.layout.index[key]] == "True" for key in keys)


@gen_cluster(client=True)
async def test_TaskGraph_order(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(div, 1, 0)
    await wait(y)

    gp = TaskGraph(s)
    gp.update()

    assert gp.node_source.data["state"][gp.layout.index[y.key]] == "erred"


@gen_cluster(client=True)
async def test_TaskGroupGraph(c, s, a, b):
    tgg = TaskGroupGraph(s)
    futures = c.map(inc, range(10))
    await wait(futures)

    tgg.update()
    assert all(len(L) == 1 for L in tgg.nodes_source.data.values())
    assert tgg.nodes_source.data["name"] == ["inc"]
    assert tgg.nodes_source.data["tot_tasks"] == [10]

    assert all(len(L) == 0 for L in tgg.arrows_source.data.values())

    futures2 = c.map(dec, range(5))
    await wait(futures2)

    tgg.update()
    assert all(len(L) == 2 for L in tgg.nodes_source.data.values())
    assert tgg.nodes_source.data["name"] == ["inc", "dec"]
    assert tgg.nodes_source.data["tot_tasks"] == [10, 5]

    del futures, futures2
    while s.task_groups:
        await asyncio.sleep(0.01)

    tgg.update()
    assert not any(tgg.nodes_source.data.values())


@gen_cluster(client=True)
async def test_TaskGroupGraph_arrows(c, s, a, b):
    tgg = TaskGroupGraph(s)

    futures = c.map(inc, range(10))
    await wait(futures)

    tgg.update()
    assert all(len(L) == 1 for L in tgg.nodes_source.data.values())
    assert tgg.nodes_source.data["name"] == ["inc"]
    assert tgg.nodes_source.data["tot_tasks"] == [10]

    assert all(len(L) == 0 for L in tgg.arrows_source.data.values())

    futures2 = c.map(dec, futures)
    await wait(futures2)

    tgg.update()
    assert all(len(L) == 2 for L in tgg.nodes_source.data.values())
    assert tgg.nodes_source.data["name"] == ["inc", "dec"]
    assert tgg.nodes_source.data["tot_tasks"] == [10, 10]

    assert all(len(L) == 1 for L in tgg.arrows_source.data.values())

    del futures, futures2
    while s.task_groups:
        await asyncio.sleep(0.01)

    tgg.update()  # for some reason after deleting the futures the tgg.node_source.data.values are not clear.
    assert not any(tgg.nodes_source.data.values())
    assert not any(tgg.arrows_source.data.values())


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.enabled": True,
        "distributed.worker.profile.interval": "10ms",
        "distributed.worker.profile.cycle": "50ms",
    },
)
async def test_profile_server(c, s, a, b):
    ptp = ProfileServer(s)
    assert "disabled" not in ptp.subtitle.text
    start = time()
    await asyncio.sleep(0.100)
    while len(ptp.ts_source.data["time"]) < 2:
        await asyncio.sleep(0.100)
        ptp.trigger_update()
        assert time() < start + 2


@pytest.mark.slow
@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.enabled": False,
        "distributed.worker.profile.interval": "5ms",
        "distributed.worker.profile.cycle": "10ms",
    },
)
async def test_profile_server_disabled(c, s, a, b):
    ptp = ProfileServer(s)
    assert "disabled" in ptp.subtitle.text
    start = time()
    await asyncio.sleep(0.1)
    ptp.trigger_update()
    assert len(ptp.ts_source.data["time"]) == 0


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_root_redirect(c, s, a, b):
    http_client = AsyncHTTPClient()
    response = await http_client.fetch("http://localhost:%d/" % s.http_server.port)
    assert response.code == 200
    assert "/status" in response.effective_url


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    worker_kwargs={"dashboard": True},
    timeout=180,
)
async def test_proxy_to_workers(c, s, a, b):
    try:
        import jupyter_server_proxy  # noqa: F401

        proxy_exists = True
    except ImportError:
        proxy_exists = False

    dashboard_port = s.http_server.port
    http_client = AsyncHTTPClient()
    response = await http_client.fetch("http://localhost:%d/" % dashboard_port)
    assert response.code == 200
    assert "/status" in response.effective_url

    for w in [a, b]:
        host = w.ip
        port = w.http_server.port
        proxy_url = "http://localhost:%d/proxy/%s/%s/status" % (
            dashboard_port,
            port,
            host,
        )
        direct_url = "http://localhost:%s/status" % port
        http_client = AsyncHTTPClient()
        response_proxy = await http_client.fetch(proxy_url)
        response_direct = await http_client.fetch(direct_url)

        assert response_proxy.code == 200
        if proxy_exists:
            assert b"System" in response_proxy.body
        else:
            assert b"python -m pip install jupyter-server-proxy" in response_proxy.body
        assert response_direct.code == 200
        assert b"System" in response_direct.body


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.tasks.task-stream-length": 10,
        "distributed.scheduler.dashboard.status.task-stream-length": 10,
    },
)
async def test_lots_of_tasks(c, s, a, b):
    import tlz as toolz

    ts = TaskStream(s)
    ts.update()
    futures = c.map(toolz.identity, range(100))
    await wait(futures)

    tsp = s.plugins[TaskStreamPlugin.name]
    assert len(tsp.buffer) == 10
    ts.update()
    assert len(ts.source.data["start"]) == 10
    assert "identity" in str(ts.source.data)

    futures = c.map(lambda x: x, range(100), pure=False)
    await wait(futures)
    ts.update()
    assert "lambda" in str(ts.source.data)


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.tls.key": get_cert("tls-key.pem"),
        "distributed.scheduler.dashboard.tls.cert": get_cert("tls-cert.pem"),
        "distributed.scheduler.dashboard.tls.ca-file": get_cert("tls-ca-cert.pem"),
    },
)
async def test_https_support(c, s, a, b):
    port = s.http_server.port

    assert (
        format_dashboard_link("localhost", port) == "https://localhost:%d/status" % port
    )

    ctx = ssl.create_default_context()
    ctx.load_verify_locations(get_cert("tls-ca-cert.pem"))

    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "https://localhost:%d/individual-plots.json" % port, ssl_options=ctx
    )
    response = json.loads(response.body.decode())

    for suffix in [
        "system",
        "counters",
        "workers",
        "status",
        "tasks",
        "stealing",
        "graph",
    ] + [url.strip("/") for url in response.values()]:
        req = HTTPRequest(
            url="https://localhost:%d/%s" % (port, suffix), ssl_options=ctx
        )
        response = await http_client.fetch(req)
        assert response.code < 300
        body = response.body.decode()
        assert not re.search("href=./", body)  # no absolute links


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_memory_by_key(c, s, a, b):
    mbk = MemoryByKey(s)

    da = pytest.importorskip("dask.array")
    x = (da.random.random((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)
    await x

    y = await dask.delayed(inc)(1).persist()

    mbk.update()
    assert mbk.source.data["name"] == ["add", "inc"]
    assert mbk.source.data["nbytes"] == [x.nbytes, sys.getsizeof(1)]


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_aggregate_action(c, s, a, b):
    mbk = AggregateAction(s)

    da = pytest.importorskip("dask.array")
    x = (da.ones((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)

    await x
    y = await dask.delayed(inc)(1).persist()
    z = (x + x.T) - x.mean(axis=0)
    await c.compute(z.sum())

    mbk.update()
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "http://localhost:%d/individual-aggregate-time-per-action" % s.http_server.port
    )
    assert response.code == 200

    assert ("transfer") in mbk.action_source.data["names"]
    assert ("compute") in mbk.action_source.data["names"]

    [title_line] = [
        line for line in response.body.decode().split("\n") if "<title>" in line
    ]
    assert "AggregateAction" in title_line
    assert "Bokeh" not in title_line


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_compute_per_key(c, s, a, b):
    mbk = ComputePerKey(s)

    da = pytest.importorskip("dask.array")
    x = (da.ones((20, 20), chunks=(10, 10)) + 1).persist(optimize_graph=False)

    await x
    y = await dask.delayed(inc)(1).persist()
    z = (x + x.T) - x.mean(axis=0)
    zsum = z.sum()
    await c.compute(zsum)

    mbk.update()
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        "http://localhost:%d/individual-compute-time-per-key" % s.http_server.port
    )
    assert response.code == 200
    assert ("sum-aggregate") in mbk.compute_source.data["names"]
    assert ("add") in mbk.compute_source.data["names"]
    assert "angles" in mbk.compute_source.data.keys()


@gen_cluster(scheduler_kwargs={"http_prefix": "foo-bar", "dashboard": True})
async def test_prefix_bokeh(s, a, b):
    prefix = "foo-bar"
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(
        f"http://localhost:{s.http_server.port}/{prefix}/status"
    )
    assert response.code == 200
    assert (
        f'<script type="text/javascript" src="/{prefix}/static/'
        in response.body.decode()
    )

    bokeh_app = s.http_application.applications[0]
    assert isinstance(bokeh_app, BokehTornado)
    assert bokeh_app.prefix == f"/{prefix}"


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True})
async def test_shuffling(c, s, a, b):
    pytest.importorskip("pyarrow")
    dd = pytest.importorskip("dask.dataframe")
    ss = Shuffling(s)

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-02-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    df2 = dd.shuffle.shuffle(df, "x", shuffle="p2p").persist()
    start = time()
    while not ss.source.data["comm_written"]:
        ss.update()
        await asyncio.sleep(0)
        assert time() < start + 5
    await df2


@gen_cluster(client=True, scheduler_kwargs={"dashboard": True}, timeout=60)
async def test_hardware(c, s, *workers):
    plot = Hardware(s)
    while not plot.disk_data:
        await asyncio.sleep(0.1)
        plot.update()


@gen_cluster(client=True, nthreads=[], scheduler_kwargs={"dashboard": True})
async def test_hardware_endpoint(c, s):
    port = s.http_server.port
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(f"http://localhost:{port}/hardware")
    body = response.body.decode()
    assert "bokeh" in body.lower()
