from __future__ import annotations

import pytest
import requests

from distributed import Client

pytest.importorskip("jupyter_server")

from tornado.httpclient import AsyncHTTPClient

from distributed import Scheduler
from distributed.utils import open_port
from distributed.utils_test import gen_test, popen


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
