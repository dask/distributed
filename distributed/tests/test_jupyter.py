from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from time import perf_counter

import pytest

from distributed.core import Status

pytest.importorskip("requests")

import requests

from distributed import Client

pytest.importorskip("jupyter_server")

from tornado.httpclient import AsyncHTTPClient

from distributed import Scheduler
from distributed.utils import open_port
from distributed.utils_test import gen_test, popen

pytestmark = pytest.mark.filterwarnings("ignore:Jupyter is migrating its paths")


@gen_test()
async def test_jupyter_server():
    async with Scheduler(jupyter=True) as s:
        http_client = AsyncHTTPClient()
        response = await http_client.fetch(
            f"http://localhost:{s.http_server.port}/jupyter/api/status"
        )
        assert response.code == 200


@pytest.mark.slow
def test_jupyter_cli(loop):
    port = open_port()
    with popen(
        [
            "dask",
            "scheduler",
            "--jupyter",
            "--no-dashboard",
            "--host",
            f"127.0.0.1:{port}",
        ],
        capture_output=True,
    ):
        with Client(f"127.0.0.1:{port}", loop=loop):
            response = requests.get("http://127.0.0.1:8787/jupyter/api/status")
            assert response.status_code == 200


@gen_test()
async def test_jupyter_idle_timeout():
    "An active Jupyter session should prevent idle timeout"
    async with Scheduler(jupyter=True, idle_timeout=0.2) as s:
        web_app = s._jupyter_server_application.web_app

        # Jupyter offers a place for extensions to provide updates on their last-active
        # time, which is used to determine idleness. Instead of a full e2e test
        # launching a kernel and running commands on it, we'll just hook into this:
        # https://github.com/jupyter-server/jupyter_server/blob/e582e555/jupyter_server/serverapp.py#L385-L387
        extension_last_activty = web_app.settings["last_activity_times"]

        for _ in range(10):
            last = perf_counter()
            extension_last_activty["test"] = datetime.now(timezone.utc)

            await asyncio.sleep(s.idle_timeout / 2)
            if (d := perf_counter() - last) >= s.idle_timeout:
                pytest.fail(f"Event loop too slow to test idle timeout: {d:.4f}s")

            assert s.status not in (Status.closed, Status.closing)

        await asyncio.sleep(s.idle_timeout)
        assert s.status in (Status.closed, Status.closing)


@gen_test()
async def test_jupyter_idle_timeout_returned():
    "`check_idle` should return the last Jupyter idle time. Used in dask-kubernetes."
    async with Scheduler(jupyter=True) as s:
        web_app = s._jupyter_server_application.web_app
        extension_last_activty = web_app.settings["last_activity_times"]

        extension_last_activty["test"] = datetime.now(timezone.utc)
        last_idle = s.check_idle()
        assert last_idle is not None
        extension_last_activty["test"] = datetime.now(timezone.utc) + timedelta(
            seconds=1
        )
        next_idle = s.check_idle()
        assert next_idle is not None
        assert next_idle > last_idle

        assert s.check_idle() is None
        # ^ NOTE: this probably should be `== next_idle`;
        # see discussion in https://github.com/dask/distributed/pull/7687#discussion_r1145095196
