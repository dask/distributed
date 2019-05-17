from urllib import parse

from tornado import web

from notebook.utils import url_path_join
from jupyter_server_proxy.handlers import ProxyHandler

import tornado.ioloop
import tornado.web
from bokeh.server.server import BaseServer
from tornado.httpserver import HTTPServer


def get_host():
    return '10.31.241.45'

class GlobalProxyHandler(ProxyHandler):
    """
    A tornado request handler that proxies HTTP and websockets
    from a port to any valid endpoint'.
    """
    async def http_get(self, port, proxied_path):
        # valid_address_check?
        return await self.proxy(port, proxied_path)

    async def open(self, port, proxied_path):
        # proxy to other address
        host = get_host()
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
        host = get_host()
        return super().proxy(host, port, proxied_path)

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
        self.port = port

        handlers = [(r'/proxy/(\d+)(.*)', GlobalProxyHandler)]
        self.application = tornado.web.Application(handlers)

        self.server = HTTPServer(self.application)

        self.server.listen(self.port)
