
import json
import logging
import os

from tornado import web, gen
from tornado.httpclient import AsyncHTTPClient

from .core import RequestHandler, MyApp, Resources, Proxy


logger = logging.getLogger(__name__)
here = os.path.split(os.path.abspath(__file__))[0]


class Info(RequestHandler):
    def get(self):
        resp = {'ncores': {'%s:%d' % k: n for k, n in self.server.ncores.items()},
                'status': self.server.status}
        self.write(resp)


class HasWhat(RequestHandler):
    def get(self):
        self.write({'data': [(list(k), list(v)) for k, v in
                    self.server.has_what.items()]})


class RenderJSON(RequestHandler):
    # Credit to https://github.com/caldwell/renderjson
    data = open(os.path.join(here, 'renderjson.js')).read()

    def get(self):
        self.set_header("Content-Type", 'application/javascript')
        self.write(self.data)


class Render(RequestHandler):
    """Give data in formatted JSON. Calls other endpoints"""
    template = open(os.path.join(here, 'template.html')).read()

    def get(self):
        self.write(self.template)


class Broadcast(RequestHandler):
    @gen.coroutine
    def get(self, rest):
        addresses = [(ip, port, d['http'])
                     for (ip, port), d in self.server.worker_services.items()
                     if 'http' in d]
        client = AsyncHTTPClient()
        responses = {'%s:%d' % (ip, tcp_port): client.fetch("http://%s:%d/%s" %
                     (ip, http_port, rest))
                     for ip, tcp_port, http_port in addresses}
        responses2 = yield responses
        responses3 = {k: json.loads(v.body.decode())
                      for k, v in responses2.items()}
        self.write(responses3)  # TODO: capture more data of response


def HTTPScheduler(scheduler):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': scheduler}),
        (r'/resources.json', Resources, {'server': scheduler}),
        (r'/haswhat.json', HasWhat, {'server': scheduler}),
        (r'/proxy/([\w.-]+):(\d+)/(.+)', Proxy),
        (r'/broadcast/(.+)', Broadcast, {'server': scheduler}),
        (r'/render', Render),
        (r'/renderjson.js', RenderJSON)
        ]))
    return application
