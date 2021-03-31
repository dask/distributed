from tornado.httpclient import AsyncHTTPClient

from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_scheduler(c, s, a, b):
    client = AsyncHTTPClient()
    response = await client.fetch(
        "http://localhost:{}/health".format(s.http_server.port)
    )
    assert response.code == 200
