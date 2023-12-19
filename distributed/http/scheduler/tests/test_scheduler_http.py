from __future__ import annotations

import asyncio
import json
import re
from unittest import mock

import pytest
from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient, HTTPClientError

import dask.config
from dask.sizeof import sizeof

from distributed import Event, Lock, Scheduler
from distributed.client import wait
from distributed.core import Status
from distributed.utils import is_valid_xml
from distributed.utils_test import (
    async_poll_for,
    div,
    fetch_metrics,
    fetch_metrics_body,
    gen_cluster,
    gen_test,
    inc,
    lock_inc,
    slowinc,
    wait_for_state,
)

DEFAULT_ROUTES = dask.config.get("distributed.scheduler.http.routes")


@gen_cluster(client=True)
async def test_connect(c, s, a, b):
    lock = Lock()
    async with lock:
        future = c.submit(lambda x: x + 1, 1)
        x = c.submit(lock_inc, 1, lock=lock, retries=5)
        await future
        http_client = AsyncHTTPClient()
        for suffix in [
            "info/main/workers.html",
            "info/worker/" + url_escape(a.address) + ".html",
            "info/task/" + url_escape(future.key) + ".html",
            "info/main/logs.html",
            "info/logs/" + url_escape(a.address) + ".html",
            "info/call-stack/" + url_escape(x.key) + ".html",
            "info/call-stacks/" + url_escape(a.address) + ".html",
            "json/counts.json",
            "json/identity.json",
            "json/index.html",
            "individual-plots.json",
            "sitemap.json",
        ]:
            response = await http_client.fetch(
                "http://localhost:%d/%s" % (s.http_server.port, suffix)
            )
            assert response.code == 200
            body = response.body.decode()
            if suffix.endswith(".json"):
                json.loads(body)
            else:
                assert is_valid_xml(body)
                assert not re.search("href=./", body)  # no absolute links


@gen_cluster(client=True, nthreads=[])
async def test_worker_404(c, s):
    http_client = AsyncHTTPClient()
    with pytest.raises(HTTPClientError) as err:
        await http_client.fetch(
            "http://localhost:%d/info/worker/unknown" % s.http_server.port
        )
    assert err.value.code == 404
    with pytest.raises(HTTPClientError) as err:
        await http_client.fetch(
            "http://localhost:%d/info/task/unknown" % s.http_server.port
        )
    assert err.value.code == 404


@gen_cluster(client=True, scheduler_kwargs={"http_prefix": "/foo", "dashboard": True})
async def test_prefix(c, s, a, b):
    pytest.importorskip("bokeh")

    http_client = AsyncHTTPClient()
    for suffix in ["foo/info/main/workers.html", "foo/json/index.html", "foo/system"]:
        response = await http_client.fetch(
            "http://localhost:%d/%s" % (s.http_server.port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert is_valid_xml(body)


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.admin.system-monitor.gil.enabled": True,
    },
)
async def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")

    active_metrics = await fetch_metrics(s.http_server.port, "dask_scheduler_")

    expected_metrics = {
        "dask_scheduler_clients",
        "dask_scheduler_desired_workers",
        "dask_scheduler_workers",
        "dask_scheduler_last_time",
        "dask_scheduler_tasks",
        "dask_scheduler_tasks_suspicious",
        "dask_scheduler_tasks_forgotten",
        "dask_scheduler_tasks_output_bytes",
        "dask_scheduler_tasks_compute_seconds",
        "dask_scheduler_tasks_transfer_seconds",
        "dask_scheduler_prefix_state_totals",
        "dask_scheduler_tick_count",
        "dask_scheduler_tick_duration_maximum_seconds",
    }

    try:
        import gilknocker  # noqa: F401

    except ImportError:
        pass  # pragma: nocover
    else:
        expected_metrics.add("dask_scheduler_gil_contention")

    assert set(active_metrics.keys()) == expected_metrics
    assert active_metrics["dask_scheduler_clients"].samples[0].value == 1.0

    # request data twice since there once was a case where metrics got registered multiple times resulting in
    # prometheus_client errors
    await fetch_metrics(s.http_server.port, "dask_scheduler_")


@pytest.fixture
def prometheus_not_available():
    import sys

    with mock.patch.dict("sys.modules", {"prometheus_client": None}):
        sys.modules.pop("distributed.http.scheduler.prometheus", None)
        yield


@gen_test()
async def test_metrics_when_prometheus_client_not_installed(prometheus_not_available):
    async with Scheduler(dashboard_address=":0") as s:
        body = await fetch_metrics_body(s.http_server.port)
        assert "Prometheus metrics are not available" in body


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_prometheus_collect_task_states(c, s, a, b):
    pytest.importorskip("prometheus_client")

    async def fetch_state_metrics():
        families = await fetch_metrics(s.http_server.port, prefix="dask_scheduler_")

        active_metrics = {
            sample.labels["state"]: sample.value
            for sample in families["dask_scheduler_tasks"].samples
        }
        forgotten_tasks = [
            sample.value
            for sample in families["dask_scheduler_tasks_forgotten"].samples
        ]
        return active_metrics, forgotten_tasks

    expected = {
        "memory",
        "released",
        "queued",
        "processing",
        "waiting",
        "no-worker",
        "erred",
    }

    # Ensure that we get full zero metrics for all states even though the
    # scheduler did nothing, yet
    assert not s.tasks
    active_metrics, forgotten_tasks = await fetch_state_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 0.0
    assert sum(forgotten_tasks) == 0.0

    # submit a task which should show up in the prometheus scraping
    future = c.submit(slowinc, 1, delay=0.5)
    while not any(future.key in w.state.tasks for w in [a, b]):
        await asyncio.sleep(0.001)

    active_metrics, forgotten_tasks = await fetch_state_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 1.0
    assert sum(forgotten_tasks) == 0.0

    res = await c.gather(future)
    assert res == 2

    future.release()

    while any(future.key in w.state.tasks for w in [a, b]):
        await asyncio.sleep(0.001)

    active_metrics, forgotten_tasks = await fetch_state_metrics()
    assert active_metrics.keys() == expected
    assert sum(active_metrics.values()) == 0.0
    assert sum(forgotten_tasks) == 0.0


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_prometheus_collect_task_prefix_counts(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    async def fetch_metrics():
        port = s.http_server.port
        response = await http_client.fetch(f"http://localhost:{port}/metrics")
        txt = response.body.decode("utf8")
        families = {
            family.name: family for family in text_string_to_metric_families(txt)
        }

        prefix_state_counts = {
            (sample.labels["task_prefix_name"], sample.labels["state"]): sample.value
            for sample in families["dask_scheduler_prefix_state_totals"].samples
        }

        return prefix_state_counts

    # do some compute and check the counts for each prefix and state
    futures = c.map(inc, range(10))
    await c.gather(futures)

    prefix_state_counts = await fetch_metrics()
    assert prefix_state_counts.get(("inc", "memory")) == 10
    assert prefix_state_counts.get(("inc", "erred"), 0) == 0

    f = c.submit(div, 1, 0)
    await wait(f)

    prefix_state_counts = await fetch_metrics()
    assert prefix_state_counts.get(("div", "erred")) == 1


@gen_cluster(
    client=True,
    config={"distributed.worker.memory.monitor-interval": "10ms"},
    timeout=3,
)
async def test_prometheus_collect_worker_states(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    async def fetch_metrics():
        port = s.http_server.port
        response = await http_client.fetch(f"http://localhost:{port}/metrics")
        txt = response.body.decode("utf8")
        families = {
            family.name: family for family in text_string_to_metric_families(txt)
        }
        return {
            sample.labels["state"]: sample.value
            for sample in families["dask_scheduler_workers"].samples
        }

    assert await fetch_metrics() == {
        "idle": 2,
        "partially_saturated": 0,
        "saturated": 0,
        "paused_or_retiring": 0,
    }

    ev = Event()
    x = c.submit(lambda ev: ev.wait(), ev, key="x", workers=[a.address])
    await wait_for_state("x", "processing", s)
    assert await fetch_metrics() == {
        "idle": 1,
        "partially_saturated": 1,
        "saturated": 0,
        "paused_or_retiring": 0,
    }

    y = c.submit(lambda ev: ev.wait(), ev, key="y", workers=[a.address])
    z = c.submit(lambda ev: ev.wait(), ev, key="z", workers=[a.address])
    await wait_for_state("y", "processing", s)
    await wait_for_state("z", "processing", s)

    assert await fetch_metrics() == {
        "idle": 1,
        "partially_saturated": 0,
        "saturated": 1,
        "paused_or_retiring": 0,
    }

    a.monitor.get_process_memory = lambda: 2**40
    sa = s.workers[a.address]
    await async_poll_for(lambda: sa.status == Status.paused, timeout=2)
    assert await fetch_metrics() == {
        "idle": 1,
        "partially_saturated": 0,
        "saturated": 0,
        "paused_or_retiring": 1,
    }

    await ev.set()


@gen_cluster(nthreads=[])
async def test_health(s):
    aiohttp = pytest.importorskip("aiohttp")

    async with (
        aiohttp.ClientSession() as session,
        session.get(f"http://localhost:{s.http_server.port}/health") as resp,
    ):
        assert resp.status == 200
        assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert (await resp.text()) == "ok"


@gen_cluster()
async def test_sitemap(s, a, b):
    http_client = AsyncHTTPClient()

    response = await http_client.fetch(
        "http://localhost:%d/sitemap.json" % s.http_server.port
    )
    out = json.loads(response.body.decode())
    assert "paths" in out
    assert "/sitemap.json" in out["paths"]
    assert "/health" in out["paths"]
    assert "/statics/css/base.css" in out["paths"]


KEY_EDGE_CASES = [
    1,
    "1",
    "x",
    "a+b",
    "a b",
    ("x", 1),
    ("a+b", 1),
    ("a b", 1),
    ((1, 2), ("x", "y")),
    "(",
    "[",
    "'",
    '"',
    b"123",
    (b"123", 1),
    ("[", 1),
    ("(", 1),
    ("'", 1),
    ('"', 1),
]


@pytest.mark.parametrize("key", KEY_EDGE_CASES)
@gen_cluster(client=True)
async def test_task_page(c, s, a, b, key):
    http_client = AsyncHTTPClient()
    skey = url_escape(str(key), plus=False)
    url = f"http://localhost:{s.http_server.port}/info/task/{skey}.html"

    response = await http_client.fetch(url, raise_error=False)
    assert response.code == 404

    future = c.submit(lambda: 1, key=key, workers=a.address)
    await future
    response = await http_client.fetch(url)
    assert response.code == 200
    body = response.body.decode()

    assert a.address in body
    assert str(sizeof(1)) in body
    assert "int" in body
    assert "memory" in body


@pytest.mark.parametrize("key", KEY_EDGE_CASES)
@gen_cluster(client=True)
async def test_call_stack_page(c, s, a, b, key):
    http_client = AsyncHTTPClient()
    skey = url_escape(str(key), plus=False)
    url = f"http://localhost:{s.http_server.port}/info/call-stack/{skey}.html"

    response = await http_client.fetch(url, raise_error=False)
    assert response.code == 404

    ev1 = Event()
    ev2 = Event()

    def f(ev1, ev2):
        ev1.set()
        ev2.wait()

    future = c.submit(f, ev1, ev2, key=key)
    await ev1.wait()

    response = await http_client.fetch(url)
    assert response.code == 200
    body = response.body.decode()
    assert "test_scheduler_http.py" in body

    await ev2.set()
    await future
    response = await http_client.fetch(url)
    assert response.code == 200
    body = response.body.decode()
    assert "Task not actively running" in body


@gen_cluster(
    client=True,
    scheduler_kwargs={"dashboard": True},
    config={
        "distributed.scheduler.dashboard.bokeh-application.allow_websocket_origin": [
            "good.invalid"
        ]
    },
)
async def test_allow_websocket_origin(c, s, a, b):
    pytest.importorskip("bokeh")

    from tornado.httpclient import HTTPRequest
    from tornado.websocket import websocket_connect

    url = (
        "ws://localhost:%d/status/ws?bokeh-protocol-version=1.0&bokeh-session-id=1"
        % s.http_server.port
    )
    with pytest.raises(HTTPClientError) as err:
        await websocket_connect(
            HTTPRequest(url, headers={"Origin": "http://evil.invalid"})
        )
    assert err.value.code == 403


@gen_cluster(client=True)
async def test_eventstream(c, s, a, b):
    from tornado.websocket import websocket_connect

    ws_client = await websocket_connect(
        "ws://localhost:%d/%s" % (s.http_server.port, "eventstream")
    )
    assert "websocket" in str(s.plugins).lower()
    ws_client.close()


def test_api_disabled_by_default():
    assert "distributed.http.scheduler.api" not in dask.config.get(
        "distributed.scheduler.http.routes"
    )


@gen_cluster(
    nthreads=[],
    config={
        "distributed.scheduler.http.routes": DEFAULT_ROUTES
        + ["distributed.http.scheduler.api"]
    },
)
async def test_api(s):
    aiohttp = pytest.importorskip("aiohttp")

    async with (
        aiohttp.ClientSession() as session,
        session.get(f"http://localhost:{s.http_server.port}/api/v1") as resp,
    ):
        assert resp.status == 200
        assert resp.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert (await resp.text()) == "API V1"


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.scheduler.http.routes": DEFAULT_ROUTES
        + ["distributed.http.scheduler.api"]
    },
)
async def test_retire_workers(c, s, a, b):
    aiohttp = pytest.importorskip("aiohttp")

    async with aiohttp.ClientSession() as session:
        params = {"workers": [a.address, b.address]}
        async with session.post(
            "http://localhost:%d/api/v1/retire_workers" % s.http_server.port,
            json=params,
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "application/json"
            retired_workers_info = json.loads(await resp.text())
            assert len(retired_workers_info) == 2


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.scheduler.http.routes": DEFAULT_ROUTES
        + ["distributed.http.scheduler.api"]
    },
)
async def test_get_workers(c, s, a, b):
    aiohttp = pytest.importorskip("aiohttp")

    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:%d/api/v1/get_workers" % s.http_server.port
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "application/json"
            workers_info = json.loads(await resp.text())["workers"]
            workers_address = [worker.get("address") for worker in workers_info]
            assert set(workers_address) == {a.address, b.address}


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.scheduler.http.routes": DEFAULT_ROUTES
        + ["distributed.http.scheduler.api"]
    },
)
async def test_adaptive_target(c, s, a, b):
    aiohttp = pytest.importorskip("aiohttp")

    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:%d/api/v1/adaptive_target" % s.http_server.port
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "application/json"
            num_workers = json.loads(await resp.text())["workers"]
            assert num_workers == 0


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.scheduler.http.routes": DEFAULT_ROUTES
        + ["distributed.http.scheduler.api"]
    },
)
async def test_check_idle(c, s, a, b):
    aiohttp = pytest.importorskip("aiohttp")

    async with aiohttp.ClientSession() as session:
        async with session.get(
            "http://localhost:%d/api/v1/check_idle" % s.http_server.port
        ) as resp:
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "application/json"
            response = json.loads(await resp.text())
            assert (
                isinstance(response["idle_since"], float)
                or response["idle_since"] is None
            )
