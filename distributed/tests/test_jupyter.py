from __future__ import annotations

import pytest

pytest.importorskip("jupyter_server")

from tornado.httpclient import AsyncHTTPClient

from distributed import Scheduler
from distributed.utils_test import gen_test


@gen_test()
async def test_jupyter_server():
    async with Scheduler(jupyter=True) as s:
        http_client = AsyncHTTPClient()
        response = await http_client.fetch(
            f"http://localhost:{s.http_server.port}/jupyter/api/status"
        )
        assert response.code == 200
