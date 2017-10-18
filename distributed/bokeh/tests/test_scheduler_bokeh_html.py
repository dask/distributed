from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('bokeh')

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient

from distributed.utils_test import gen_cluster, inc
from distributed.bokeh.scheduler import BokehScheduler


@gen_cluster(client=True,
             scheduler_kwargs={'services': {('bokeh', 0):  BokehScheduler}})
def test_connect(c, s, a, b):
    future = c.submit(inc, 1)
    yield future
    http_client = AsyncHTTPClient()
    for suffix in ['scheduler/workers.html',
                   'scheduler/worker/' + url_escape(a.address) + '.html',
                   'scheduler/task/' + url_escape(future.key) + '.html']:
        response = yield http_client.fetch('http://localhost:%d/%s'
                                           % (s.services['bokeh'].port, suffix))
        assert response.code == 200
        body = response.body.decode()
