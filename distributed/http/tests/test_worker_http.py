import json

from tornado.ioloop import IOLoop
from tornado import web
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer

from distributed.utils_test import gen_cluster, gen_test
from distributed import Worker
from distributed.http.worker import HTTPWorker
from distributed import Executor

import requests


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
    yield e._start()
    e.scatter([1])
    sch_hport = e.services['http']
    (wip, wport) = list(e.workers)[0]
    w_hport = e.workers[(wip, wport)]['http']
    keys = requests.get("http://{ip}:{port}/data.json".format(ip=wip,
                        port=w_hport)).json()['keys']
    assert len(keys) == 1
    key = keys[0]
    out = requests.get('http://192.168.20.132:60004/value/{}.json'.format(
                       key)).json()
    assert out[key] = 1
    
    