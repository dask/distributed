from urllib import parse

from tornado import web

from notebook.utils import url_path_join
from jupyter_server_proxy.handlers import ProxyHandler

import tornado.ioloop
import tornado.web
from bokeh.server.server import BaseServer
from tornado.httpserver import HTTPServer


def get_host():
    return "127.0.0.1"


class GlobalProxyHandler(ProxyHandler):
    """
    A tornado request handler that proxies HTTP and websockets
    from a port to any valid endpoint'.
    """
    def initialize(self, server=None, extra=None):
        self.scheduler = server
        self.extra = extra or {}

    async def http_get(self, port, proxied_path):
        # route here first
        # incoming URI /proxy/{proxy}/port
        host = self.get_argument("host", None)
        if not host:
            host = "127.0.0.1"
        self.host = host
        worker = '%s:%s' % (self.host, str(port))
        if not check_worker_bokeh_exits(self.scheduler, worker):
            async def _noop():
                return None
            msg = "Worker <%s> does not exist" % worker
            self.set_status(400)
            self.finish(msg)
            return
        return await self.proxy(port, proxied_path)

    async def open(self, port, proxied_path):
        # finally, proxy to other address/port
        host = self.get_argument("host", None)
        if not host:
            host = "127.0.0.1"
        return await self.proxy_open(host, port, proxied_path)

    def post(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def put(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def delete(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def head(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def patch(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def options(self, port, proxied_path):
        return self.proxy(port, proxied_path)

    def proxy(self, port, proxied_path):
        # router here second
        # returns ProxyHandler coroutine
        return super().proxy(self.host, port, proxied_path)

def check_worker_bokeh_exits(scheduler, worker):
    """Check addr:port exists as a worker in scheduler list

    Parameters
    ----------
    worker : str
        addr:port

    Returns
    -------
    bool
    """
    addr, port = worker.split(':')
    workers = list(scheduler.workers.values())
    for w in workers:
        bokeh_port = w.services.get('bokeh', '')
        if addr == w.host and port == str(bokeh_port):
            return True
    return False


class Proxy(tornado.web.Application):
    def __init__(self, scheduler, io_loop=None, prefix="", **kwargs):
        self.scheduler = scheduler
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.server_kwargs = kwargs
        self.server_kwargs["prefix"] = prefix or None

        self.loop = io_loop or scheduler.loop
        self.server = None

    def listen(self, *args, **kwargs):
        addr, port = args[0]
        if not addr:
            # proxy should run with the same host as
            # scheduler
            addr = self.scheduler.ip
        self.ip = addr
        self.port = port

        handlers = [(r"/proxy/(\d+)(.*)", GlobalProxyHandler,
            {"server": self.scheduler},)]
        self.application = tornado.web.Application(handlers)

        self.server = HTTPServer(self.application)
        self.server.listen(port=self.port, address=self.ip)
