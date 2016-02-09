import json
import tornado

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed.utils_test import gen_cluster, gen_test
from distributed import Worker
from distributed.http.worker import HTTPWorker
from distributed import Executor


@gen_cluster()
def test_simple(s, a, b):
    port = 9898
    server = HTTPWorker(a)
    server.listen(port)
    client = AsyncHTTPClient()

    response = yield client.fetch('http://localhost:%d/info.json' % port)
    response = json.loads(response.body.decode())
    assert response['ncores'] == a.ncores
    assert response['status'] == a.status

    response = yield client.fetch('http://localhost:%d/resources.json' % port)
    response = json.loads(response.body.decode())
    try:
        import psutil
        assert 0 < response['memory_percent'] < 100
    except ImportError:
        assert response == {}

    endpoints = ['/data.json', '/value/none.json', '/active.json',
                 '/files.json']
    for endpoint in endpoints:
        response = yield client.fetch(('http://localhost:%d' % port)
                                      + endpoint)
        response = json.loads(response.body.decode())
        print(response)
        assert response


@gen_cluster()
def test_services(s, a, b):
    c = Worker(s.ip, s.port, ncores=1, ip='127.0.0.1',
               services={'http': HTTPWorker})
    yield c._start()
    assert isinstance(c.services['http'], HTTPServer)
    assert c.service_ports['http'] == c.services['http'].port
    assert s.worker_services[c.address]['http'] == c.service_ports['http']


@gen_cluster()
def test_with_data(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield tornado.gen.sleep(0.5)
    yield e._start()
    yield e._scatter([1])
    yield tornado.gen.sleep(0.5)
    client = AsyncHTTPClient()
    d = yield e.scheduler.identity()
    sch_hport = services['http']
    (wip, wport) = list(e.workers)[0]
    w_hport = e.workers[(wip, wport)]['http']
    response = yield client.fetch("http://{ip}:{port}/data.json".format(ip=wip,
                                  port=w_hport))
    keys = json.loads(response.body.decode())['keys']
    assert len(keys) == 1
    key = keys[0]
    response = yield client.fetch('http://{ip}:{port}/value/{key}.json'.format(
                                  ip=wip, port=w_hport, key=key))
    out = json.loads(response.body.decode())
    assert out[key] == 1
