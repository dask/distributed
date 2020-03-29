import functools
import logging
import os

from bokeh.themes import Theme
from tlz import merge

from .components.worker import (
    status_doc,
    crossfilter_doc,
    systemmonitor_doc,
    counters_doc,
    profile_doc,
    profile_server_doc,
)
from .core import BokehServer


logger = logging.getLogger(__name__)

with open(
    os.path.join(os.path.dirname(__file__), "..", "http", "templates", "base.html")
) as f:
    template_source = f.read()

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "theme.yaml"))

template_variables = {
    "pages": ["status", "system", "profile", "crossfilter", "profile-server"]
}


class BokehWorker(BokehServer):
    def __init__(self, worker, io_loop=None, prefix="", **kwargs):
        self.worker = worker
        self.server_kwargs = kwargs
        self.server_kwargs["prefix"] = prefix or None
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.apps = {
            "/status": status_doc,
            "/counters": counters_doc,
            "/crossfilter": crossfilter_doc,
            "/system": systemmonitor_doc,
            "/profile": profile_doc,
            "/profile-server": profile_server_doc,
        }
        self.apps = {
            k: functools.partial(v, worker, self.extra) for k, v in self.apps.items()
        }

        self.loop = io_loop or worker.loop
        self.server = None

    @property
    def extra(self):
        return merge({"prefix": self.prefix}, template_variables)

    @property
    def my_server(self):
        return self.worker

    def listen(self, *args, **kwargs):
        from ..http.worker import routes

        super(BokehWorker, self).listen(*args, **kwargs)

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)
