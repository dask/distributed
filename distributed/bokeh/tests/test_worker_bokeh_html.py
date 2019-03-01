import pytest
pytest.importorskip('bokeh')

from tornado.httpclient import AsyncHTTPClient
from distributed.utils_test import gen_cluster
from distributed.bokeh.worker import BokehWorker


@gen_cluster(client=True,
             worker_kwargs={'services': {('bokeh', 0):  BokehWorker}})
def test_prometheus(c, s, a, b):
    pytest.importorskip('prometheus_client')
    http_client = AsyncHTTPClient()

    # request data twice since there once was a case where metrics got registered multiple times resulting in
    # prometheus_client errors
    for _ in range(2):
        response = yield http_client.fetch('http://localhost:%d/metrics'
                                           % a.services['bokeh'].port)
        assert response.code == 200
