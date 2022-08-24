from __future__ import annotations

import pytest
from tornado import web
from tornado.httpclient import AsyncHTTPClient, HTTPClientError

from distributed.http.routing import RoutingApplication
from distributed.utils_test import gen_test


class OneHandler(web.RequestHandler):
    def get(self):
        self.write("one")


class TwoHandler(web.RequestHandler):
    def get(self):
        self.write("two")


@gen_test()
async def test_basic():
    application = RoutingApplication([(r"/one", OneHandler)])
    two = web.Application([(r"/two", TwoHandler)])
    server = application.listen(1234)

    client = AsyncHTTPClient("http://localhost:1234")
    response = await client.fetch("http://localhost:1234/one")
    assert response.body.decode() == "one"

    with pytest.raises(HTTPClientError):
        response = await client.fetch("http://localhost:1234/two")

    application.applications.append(two)

    response = await client.fetch("http://localhost:1234/two")
    assert response.body.decode() == "two"

    application.add_handlers(".*", [(r"/three", OneHandler, {})])
    response = await client.fetch("http://localhost:1234/three")
    assert response.body.decode() == "one"
