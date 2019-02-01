import os

from tornado import web
from prometheus_client import Gauge, generate_latest

dirname = os.path.dirname(__file__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, 'templates')


class PrometheusHandler(RequestHandler):
    # Construct Prometheus metrics
    # https://prometheus.io/docs/introduction/overview/
    def get(self):
        #workers = Gauge('memory_bytes',
        #    'Total memory.',
        #    namespace='worker')
        #workers.set(0.)

        self.write(generate_latest())


routes = [
        (r'metrics', PrometheusHandler),
]


def get_handlers(server):
    return [(url, cls, {'server': server}) for url, cls in routes]
