from __future__ import annotations

import json
from unittest import mock

import pytest
from tornado.httpclient import AsyncHTTPClient

from distributed import Event, Worker, wait
from distributed.sizeof import sizeof
from distributed.utils_test import (
    async_poll_for,
    fetch_metrics,
    fetch_metrics_body,
    fetch_metrics_sample_names,
    gen_cluster,
)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_prometheus(c, s, a):
    pytest.importorskip("prometheus_client")

    # We need *some* tasks or dask_worker_tasks won't appear
    fut = c.submit(lambda: 1)
    await wait(fut)

    a.data.evict()

    active_metrics = await fetch_metrics_sample_names(
        a.http_server.port, prefix="dask_worker_"
    )
    expected_metrics = {
        "dask_worker_concurrent_fetch_requests",
        "dask_worker_gil_contention_total",
        "dask_worker_latency_seconds",
        "dask_worker_memory_bytes",
        "dask_worker_spill_bytes_total",
        "dask_worker_spill_count_total",
        "dask_worker_spill_time_seconds_total",
        "dask_worker_tasks",
        "dask_worker_threads",
        "dask_worker_tick_count_total",
        "dask_worker_tick_duration_maximum_seconds",
        "dask_worker_transfer_incoming_bytes",
        "dask_worker_transfer_incoming_count",
        "dask_worker_transfer_incoming_count_total",
        "dask_worker_transfer_outgoing_bytes",
        "dask_worker_transfer_outgoing_count",
        "dask_worker_transfer_outgoing_count_total",
        "dask_worker_transfer_outgoing_bytes_total",
    }

    try:
        import crick  # noqa: F401
    except ImportError:
        pass
    else:
        expected_metrics.update(
            {
                "dask_worker_tick_duration_median_seconds",
                "dask_worker_task_duration_median_seconds",
                "dask_worker_transfer_bandwidth_median_bytes",
            }
        )

    assert active_metrics == expected_metrics

    # request data twice since there once was a case where metrics got registered
    # multiple times resulting in prometheus_client errors
    await fetch_metrics(a.http_server.port, prefix="dask_worker_")


@pytest.fixture
def prometheus_not_available():
    import sys

    with mock.patch.dict("sys.modules", {"prometheus_client": None}):
        sys.modules.pop("distributed.http.worker.prometheus", None)
        yield


@gen_cluster(client=True, nthreads=[])
async def test_metrics_when_prometheus_client_not_installed(
    c, s, prometheus_not_available
):
    async with Worker(s.address) as w:
        body = await fetch_metrics_body(w.http_server.port)
        assert "Prometheus metrics are not available" in body


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_prometheus_collect_task_states(c, s, a):
    pytest.importorskip("prometheus_client")

    async def assert_metrics(**expect):
        families = await fetch_metrics(a.http_server.port, prefix="dask_worker_")
        actual = {
            sample.labels["state"]: sample.value
            for sample in families["dask_worker_tasks"].samples
        }

        assert actual == expect

    assert not a.state.tasks
    await assert_metrics()
    ev = Event()

    # submit a task which should show up in the prometheus scraping
    fut1 = c.submit(ev.wait)
    await async_poll_for(lambda: a.state.executing, timeout=5)

    await assert_metrics(executing=1)

    await ev.set()
    await wait(fut1)

    await assert_metrics(memory=1)

    fut2 = c.submit(lambda: 1)
    await wait(fut2)
    await assert_metrics(memory=2)

    a.data.evict()
    await assert_metrics(memory=1, disk=1)
    a.data.evict()
    await assert_metrics(disk=2)

    fut1.release()
    fut2.release()

    await async_poll_for(lambda: not a.state.tasks, timeout=5)
    await assert_metrics()


@gen_cluster(client=True)
async def test_health(c, s, a, b):
    http_client = AsyncHTTPClient()

    response = await http_client.fetch(
        "http://localhost:%d/health" % a.http_server.port
    )
    assert response.code == 200
    assert response.headers["Content-Type"] == "text/plain"

    txt = response.body.decode("utf8")
    assert txt == "ok"


@gen_cluster()
async def test_sitemap(s, a, b):
    http_client = AsyncHTTPClient()

    response = await http_client.fetch(
        "http://localhost:%d/sitemap.json" % a.http_server.port
    )
    out = json.loads(response.body.decode())
    assert "paths" in out
    assert "/sitemap.json" in out["paths"]
    assert "/health" in out["paths"]
    assert "/statics/css/base.css" in out["paths"]


async def fetch_memory_metrics(w: Worker) -> dict[str, float]:
    families = await fetch_metrics(w.http_server.port, prefix="dask_worker_")
    active_metrics = {
        sample.labels["type"]: sample.value
        for sample in families["dask_worker_memory_bytes"].samples
    }
    assert active_metrics.keys() == {"managed", "unmanaged", "spilled"}
    return active_metrics


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_prometheus_collect_memory_metrics(c, s, a):
    pytest.importorskip("prometheus_client")

    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == 0
    assert metrics["spilled"] == 0
    assert metrics["unmanaged"] > 50 * 2**20

    x = c.submit(lambda: "foo", key="x")
    await wait(x)
    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == sizeof("foo")
    assert metrics["spilled"] == 0

    a.data.evict()
    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == 0
    assert metrics["spilled"] > 0
    assert metrics["spilled"] != sizeof("foo")  # pickled bytes


@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    config={
        "distributed.worker.memory.target": False,
        "distributed.worker.memory.spill": False,
    },
)
async def test_prometheus_collect_memory_metrics_spill_disabled(c, s, a):
    pytest.importorskip("prometheus_client")
    assert isinstance(a.data, dict)

    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == 0
    assert metrics["spilled"] == 0
    assert metrics["unmanaged"] > 50 * 2**20

    x = c.submit(lambda: "foo", key="x")
    await wait(x)
    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == sizeof("foo")
    assert metrics["spilled"] == 0


@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    config={
        "distributed.worker.memory.target": False,
        "distributed.worker.memory.spill": False,
    },
)
async def test_prometheus_collect_memory_metrics_bogus_sizeof(c, s, a):
    """Test that managed memory never goes above process memory and that unmanaged
    memory never goes below 0, no matter how large the output of sizeof() is
    """
    pytest.importorskip("prometheus_client")

    metrics = await fetch_memory_metrics(a)
    assert metrics["managed"] == 0
    assert metrics["unmanaged"] > 50 * 2**20
    assert metrics["spilled"] == 0

    class C:
        def __sizeof__(self):
            return 2**40

    x = c.submit(C, key="x")
    await wait(x)
    assert a.state.nbytes > 2**40

    metrics = await fetch_memory_metrics(a)
    assert 50 * 2**20 < metrics["managed"] < 100 * 2**30  # capped to process memory
    assert metrics["unmanaged"] == 0  # floored to 0
    assert metrics["spilled"] == 0
