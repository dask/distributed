from __future__ import annotations

from tornado.httpclient import AsyncHTTPClient

from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_scheduler(c, s, a, b):
    client = AsyncHTTPClient()
    response = await client.fetch(f"http://localhost:{s.http_server.port}/health")
    assert response.code == 200
