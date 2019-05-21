
from jupyter_server_proxy.handlers import ProxyHandler

class GlobalProxyHandler(ProxyHandler):
    """
    A tornado request handler that proxies HTTP and websockets
    from a port to any valid endpoint'.
    """

    def initialize(self, server=None, extra=None):
        self.scheduler = server
        self.extra = extra or {}

    async def http_get(self, port, host, proxied_path):
        # route here first
        # incoming URI /proxy/{port}/{host}/{proxied_path}

        self.host = host

        # rewrite uri for jupyter-server-proxy handling
        uri = "/proxy/%s/%s" % (str(port), proxied_path)
        self.request.uri = uri

        # slash is removed during regex in handler
        proxied_path = "/%s" % proxied_path

        worker = "%s:%s" % (self.host, str(port))
        if not check_worker_bokeh_exits(self.scheduler, worker):

            async def _noop():
                return None

            msg = "Worker <%s> does not exist" % worker
            self.set_status(400)
            self.finish(msg)
            return
        return await self.proxy(port, proxied_path)

    async def open(self, port, host, proxied_path):
        # finally, proxy to other address/port
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
    addr, port = worker.split(":")
    workers = list(scheduler.workers.values())
    for w in workers:
        bokeh_port = w.services.get("bokeh", "")
        if addr == w.host and port == str(bokeh_port):
            return True
    return False
