from __future__ import annotations

import asyncio
import json

import pytest
from tornado.httpclient import AsyncHTTPClient

import dask.config

from distributed import Event, Worker, wait
from distributed.sizeof import sizeof
from distributed.utils_test import fetch_metrics, gen_cluster


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_prometheus(c, s, a):
    pytest.importorskip("prometheus_client")

    active_metrics = await fetch_metrics(a.http_server.port, prefix="dask_worker_")
    expected_metrics = {
        "dask_worker_tasks",
        "dask_worker_concurrent_fetch_requests",
        "dask_worker_threads",
        "dask_worker_latency_seconds",
        "dask_worker_memory",
        "dask_worker_transfer_incoming_bytes",
        "dask_worker_transfer_incoming_count",
        "dask_worker_transfer_incoming_count_total",
        "dask_worker_transfer_outgoing_bytes",
        "dask_worker_transfer_outgoing_count",
        "dask_worker_transfer_outgoing_count_total",
    }

    try:
        import crick  # noqa: F401
    except ImportError:
        pass
    else:
        expected_metrics = expected_metrics.union(
            {
                "dask_worker_tick_duration_median_seconds",
                "dask_worker_task_duration_median_seconds",
                "dask_worker_transfer_bandwidth_median_bytes",
            }
        )

    assert set(active_metrics) == expected_metrics

    # request data twice since there once was a case where metrics got registered
    # multiple times resulting in prometheus_client errors
    await fetch_metrics(a.http_server.port, prefix="dask_worker_")


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_prometheus_collect_task_states(c, s, a):
    pytest.importorskip("prometheus_client")

    async def fetch_state_metrics():
        families = await fetch_metrics(a.http_server.port, prefix="dask_worker_")
        active_metrics = {
            sample.labels["state"]: sample.value
            for sample in families["dask_worker_tasks"].samples
        }
        return active_metrics

    expected_metrics = {"stored", "executing", "ready", "waiting"}
    assert not a.state.tasks
    active_metrics = await fetch_state_metrics()
    assert active_metrics == {
        "stored": 0.0,
        "executing": 0.0,
        "ready": 0.0,
        "waiting": 0.0,
    }

    ev = Event()

    # submit a task which should show up in the prometheus scraping
    future = c.submit(ev.wait)
    while not a.state.executing:
        await asyncio.sleep(0.001)

    active_metrics = await fetch_state_metrics()
    assert active_metrics == {
        "stored": 0.0,
        "executing": 1.0,
        "ready": 0.0,
        "waiting": 0.0,
    }

    await ev.set()
    await c.gather(future)

    future.release()

    while future.key in a.state.tasks:
        await asyncio.sleep(0.001)

    active_metrics = await fetch_state_metrics()
    assert active_metrics == {
        "stored": 0.0,
        "executing": 0.0,
        "ready": 0.0,
        "waiting": 0.0,
    }


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


@pytest.mark.parametrize("spill", [False, True])
@gen_cluster(client=True, nthreads=[])
async def test_prometheus_collect_memory_metrics(c, s, spill):
    pytest.importorskip("prometheus_client")

    async def fetch_memory_metrics(w):
        families = await fetch_metrics(w.http_server.port, prefix="dask_worker_")
        active_metrics = {
            sample.labels["type"]: sample.value
            for sample in families["dask_worker_memory"].samples
        }
        assert active_metrics.keys() == {"managed", "unmanaged", "spilled"}
        return active_metrics

    if spill:
        config = {}
    else:
        config = {
            "distributed.worker.memory.target": False,
            "distributed.worker.memory.spill": False,
        }

    with dask.config.set(config):
        async with Worker(s.address) as w:
            metrics = await fetch_memory_metrics(w)
            assert metrics["managed"] == 0
            assert metrics["spilled"] == 0
            assert metrics["unmanaged"] > 0

            x = c.submit(lambda: "foo", key="x")
            await wait(x)
            metrics = await fetch_memory_metrics(w)
            assert metrics["managed"] == sizeof("foo")
            assert metrics["spilled"] == 0

            if spill:
                w.data.evict()
                metrics = await fetch_memory_metrics(w)
                assert metrics["managed"] == 0
                assert metrics["spilled"] > 0
                assert metrics["spilled"] != sizeof("foo")  # pickled bytes

            else:
                assert isinstance(w.data, dict)
