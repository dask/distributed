from __future__ import print_function, division, absolute_import

from operator import add, sub
from time import sleep

from toolz import first
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from distributed.client import _wait
from distributed.utils_test import gen_cluster, inc, dec
from distributed.bokeh.worker import (BokehWorker, StateTable,
        CommunicatingStream, ExecutingTimeSeries, CommunicatingTimeSeries)


@gen_cluster(client=True,
             worker_kwargs={'services': {('bokeh', 0):  BokehWorker}})
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
def test_basic(c, s, a, b):
    for component in [StateTable, ExecutingTimeSeries,
            CommunicatingTimeSeries]:

        aa = component(a)
        bb = component(b)

        xs = c.map(inc, range(10), workers=a.address)
        ys = c.map(dec, range(10), workers=b.address)

        def slowall(*args):
            sleep(1)
            pass

        x = c.submit(slowall, xs, ys, 1, workers=a.address)
        y = c.submit(slowall, xs, ys, 2, workers=b.address)
        yield gen.sleep(0.1)

        aa.update()
        bb.update()

        assert (len(first(aa.source.data.values())) and
                len(first(bb.source.data.values())))



@gen_cluster(client=True)
def test_CommunicatingStream(c, s, a, b):
    aa = CommunicatingStream(a)
    bb = CommunicatingStream(b)

    xs = c.map(inc, range(10), workers=a.address)
    ys = c.map(dec, range(10), workers=b.address)
    adds = c.map(add, xs, ys, workers=a.address)
    subs = c.map(sub, xs, ys, workers=b.address)

    yield _wait([adds, subs])

    aa.update()
    bb.update()

    assert (len(first(aa.outgoing.data.values())) and
            len(first(bb.outgoing.data.values())))
    assert (len(first(aa.incoming.data.values())) and
            len(first(bb.incoming.data.values())))


@gen_cluster(client=True)
def test_port_overlap(c, s, a, b):
    sa = BokehWorker(a)
    sa.listen(57384)
    sb = BokehWorker(b)
    sb.listen(57384)
    assert sa.port
    assert sb.port
    assert sa.port != sb.port
