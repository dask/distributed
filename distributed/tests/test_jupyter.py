from __future__ import annotations

import asyncio
import concurrent.futures
import subprocess
from datetime import datetime, timedelta, timezone
from time import perf_counter

import pytest
from tornado.httpclient import AsyncHTTPClient

from distributed import Client, Scheduler
from distributed.compatibility import MACOS, WINDOWS
from distributed.core import Status
from distributed.utils import open_port
from distributed.utils_test import gen_test, popen

pytest.importorskip("requests")
import requests

pytest.importorskip("jupyter_server")

pytestmark = pytest.mark.filterwarnings("ignore:Jupyter is migrating its paths")

if WINDOWS:
    try:
        import jupyter_server  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip(
            allow_module_level=True,
            reason="Windows struggles running these tests w/ jupyter server",
        )


@gen_test()
async def test_jupyter_server():
    async with Scheduler(jupyter=True, dashboard_address=":0") as s:
        http_client = AsyncHTTPClient()
        response = await http_client.fetch(
            f"http://localhost:{s.http_server.port}/jupyter/api/status"
        )
        assert response.code == 200


@pytest.mark.slow
def test_jupyter_cli(loop, requires_default_ports):
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
        terminate_timeout=120,
        kill_timeout=60,
    ):
        with Client(f"127.0.0.1:{port}", loop=loop):
            response = requests.get("http://127.0.0.1:8787/jupyter/api/status")
            assert response.status_code == 200


@gen_test()
async def test_jupyter_idle_timeout():
    "An active Jupyter session should prevent idle timeout"
    async with Scheduler(jupyter=True, idle_timeout=0.2, dashboard_address=":0") as s:
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

        # small bit of extra time to catch up
        await asyncio.sleep(s.idle_timeout + 0.5)
        assert s.status in (Status.closed, Status.closing)


@gen_test()
async def test_jupyter_idle_timeout_returned():
    "`check_idle` should return the last Jupyter idle time. Used in dask-kubernetes."
    async with Scheduler(jupyter=True, dashboard_address=":0") as s:
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

        assert s.check_idle() is next_idle


@pytest.mark.slow
@pytest.mark.xfail(WINDOWS, reason="Subprocess launching scheduler TimeoutError")
@pytest.mark.xfail(MACOS, reason="Client fails to connect on OSX")
def test_shutsdown_cleanly(requires_default_ports):
    port = open_port()
    with concurrent.futures.ThreadPoolExecutor() as tpe:
        subprocess_fut = tpe.submit(
            subprocess.run,
            [
                "dask",
                "scheduler",
                "--jupyter",
                "--no-dashboard",
                "--host",
                f"127.0.0.1:{port}",
            ],
            check=True,
            capture_output=True,
            timeout=10,
            encoding="utf8",
        )

        # wait until scheduler is running
        with Client(f"127.0.0.1:{port}"):
            pass

        with requests.Session() as session:
            session.get("http://127.0.0.1:8787/jupyter/lab").raise_for_status()
            session.post(
                "http://127.0.0.1:8787/jupyter/api/shutdown",
                headers={"X-XSRFToken": session.cookies["_xsrf"]},
            ).raise_for_status()

        stderr = subprocess_fut.result().stderr
        assert "Traceback" not in stderr
        assert (
            "distributed.scheduler - INFO - Closing scheduler. Reason: jupyter-requested-shutdown"
            in stderr
        )
        assert "Shutting down on /api/shutdown request.\n" in stderr
        assert (
            "distributed.scheduler - INFO - Stopped scheduler at "
            f"'tcp://127.0.0.1:{port}'\n" in stderr
        )
        assert stderr.endswith("distributed.scheduler - INFO - End scheduler\n")
