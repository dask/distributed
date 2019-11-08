import json
import re

import pytest

pytest.importorskip("bokeh")

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient, HTTPClientError, HTTPRequest
from tornado.websocket import websocket_connect

from dask.sizeof import sizeof
from distributed.utils import is_valid_xml
from distributed.utils_test import gen_cluster, slowinc, inc
from distributed.dashboard import BokehScheduler, BokehWorker


@gen_cluster(
    client=True,
    scheduler_kwargs={"services": {("dashboard", 0): BokehScheduler}},
    worker_kwargs={"services": {"dashboard": BokehWorker}},
)
def test_connect(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1)
    x = c.submit(slowinc, 1, delay=1, retries=5)
    yield future
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
    ]:
        response = yield http_client.fetch(
            "http://localhost:%d/%s" % (s.services["dashboard"].port, suffix)
        )
        assert response.code == 200
        body = response.body.decode()
        if suffix.endswith(".json"):
            json.loads(body)
        else:
            assert is_valid_xml(body)
            assert not re.search("href=./", body)  # no absolute links


@gen_cluster(
    client=True,
    nthreads=[],
    scheduler_kwargs={"services": {("dashboard", 0): BokehScheduler}},
)
def test_worker_404(c, s):
    http_client = AsyncHTTPClient()
    with pytest.raises(HTTPClientError) as err:
        yield http_client.fetch(
            "http://localhost:%d/info/worker/unknown" % s.services["dashboard"].port
        )
    assert err.value.code == 404
    with pytest.raises(HTTPClientError) as err:
        yield http_client.fetch(
            "http://localhost:%d/info/task/unknown" % s.services["dashboard"].port
        )
    assert err.value.code == 404


@gen_cluster(
    client=True,
    scheduler_kwargs={
        "services": {("dashboard", 0): (BokehScheduler, {"prefix": "/foo"})}
    },
)
def test_prefix(c, s, a, b):
    http_client = AsyncHTTPClient()
    for suffix in ["foo/info/main/workers.html", "foo/json/index.html", "foo/system"]:
        response = yield http_client.fetch(
            "http://localhost:%d/%s" % (s.services["dashboard"].port, suffix)
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
    scheduler_kwargs={"services": {("dashboard", 0): BokehScheduler}},
)
def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")
    from prometheus_client.parser import text_string_to_metric_families

    http_client = AsyncHTTPClient()

    # request data twice since there once was a case where metrics got registered multiple times resulting in
    # prometheus_client errors
    for _ in range(2):
        response = yield http_client.fetch(
            "http://localhost:%d/metrics" % s.services["dashboard"].port
        )
        assert response.code == 200
        assert response.headers["Content-Type"] == "text/plain; version=0.0.4"

        txt = response.body.decode("utf8")
        families = {familiy.name for familiy in text_string_to_metric_families(txt)}
        assert "dask_scheduler_workers" in families


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    scheduler_kwargs={"services": {("dashboard", 0): BokehScheduler}},
)
def test_health(c, s, a, b):
    http_client = AsyncHTTPClient()

    response = yield http_client.fetch(
        "http://localhost:%d/health" % s.services["dashboard"].port
    )
    assert response.code == 200
    assert response.headers["Content-Type"] == "text/plain"

    txt = response.body.decode("utf8")
    assert txt == "ok"


@gen_cluster(
    client=True, scheduler_kwargs={"services": {("dashboard", 0): BokehScheduler}}
)
def test_task_page(c, s, a, b):
    future = c.submit(lambda x: x + 1, 1, workers=a.address)
    x = c.submit(inc, 1)
    yield future
    http_client = AsyncHTTPClient()

    "info/task/" + url_escape(future.key) + ".html",
    response = yield http_client.fetch(
        "http://localhost:%d/info/task/" % s.services["dashboard"].port
        + url_escape(future.key)
        + ".html"
    )
    assert response.code == 200
    body = response.body.decode()

    assert str(sizeof(1)) in body
    assert "int" in body
    assert a.address in body
    assert "memory" in body


@gen_cluster(
    client=True,
    scheduler_kwargs={
        "services": {
            ("dashboard", 0): (
                BokehScheduler,
                {"allow_websocket_origin": ["good.invalid"]},
            )
        }
    },
)
def test_allow_websocket_origin(c, s, a, b):
    url = (
        "ws://localhost:%d/status/ws?bokeh-protocol-version=1.0&bokeh-session-id=1"
        % s.services["dashboard"].port
    )
    with pytest.raises(HTTPClientError) as err:
        yield websocket_connect(
            HTTPRequest(url, headers={"Origin": "http://evil.invalid"})
        )
    assert err.value.code == 403
