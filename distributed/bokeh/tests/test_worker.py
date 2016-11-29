from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep

from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.utils_test import gen_cluster, inc, dec
from distributed.bokeh.worker import BokehWorkerServer, ExecutingTable


@gen_cluster(client=True,
             worker_kwargs={'services': {('bokeh', 0):  BokehWorkerServer}})
def test_simple(c, s, a, b):
    assert s.worker_info[a.address]['services'] == {'bokeh': a.services['bokeh'].port}
    assert s.worker_info[b.address]['services'] == {'bokeh': b.services['bokeh'].port}

    future = c.submit(sleep, 1)
    yield gen.sleep(0.1)

    http_client = AsyncHTTPClient()
    response = yield http_client.fetch('http://localhost:%d/' %
                                       a.services['bokeh'].port)
    assert 'bokeh' in response.body.decode().lower()


@gen_cluster(client=True)
def test_ExecutingTable(c, s, a, b):
    ta = ExecutingTable(a)
    tb = ExecutingTable(b)

    xs = c.map(inc, range(10))
    ys = c.map(dec, range(10))
    z = c.submit(add, 1, 2)

    def slowall(*args):
        sleep(1)
        pass

    future = c.submit(slowall, xs, ys, z)
    yield gen.sleep(0.2)

    ta.update()
    tb.update()
    assert ta.source.data['Task'] or tb.source.data['Task']
    for t in [ta, tb]:
        if t.source.data['Task']:
            assert t.source.data['Task'] == [future.key]
            assert '10 x ' in t.source.data['Dependencies'][0]


@gen_cluster(client=True)
def test_port_overlap(c, s, a, b):
    sa = BokehWorkerServer(a)
    sa.listen(57384)
    sb = BokehWorkerServer(b)
    sb.listen(57384)
    assert sa.port
    assert sb.port
    assert sa.port != sb.port
