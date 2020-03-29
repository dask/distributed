from distributed.utils_test import gen_cluster
from tornado.httpclient import AsyncHTTPClient


@gen_cluster(client=True)
async def test_scheduler(c, s, a, b):
    client = AsyncHTTPClient()
    response = await client.fetch("http://localhost:8080/health")
    assert response.code == 200
