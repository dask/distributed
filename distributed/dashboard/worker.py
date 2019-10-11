from functools import partial
import logging
import os

from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application
from bokeh.themes import Theme
from toolz import merge

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

with open(os.path.join(os.path.dirname(__file__), "templates", "base.html")) as f:
    template_source = f.read()

from jinja2 import Environment, FileSystemLoader

env = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
)

BOKEH_THEME = Theme(os.path.join(os.path.dirname(__file__), "theme.yaml"))

template_variables = {"pages": ["status", "system", "profile", "crossfilter"]}


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

        extra = {"prefix": prefix}

        extra.update(template_variables)

        status = Application(FunctionHandler(partial(status_doc, worker, extra)))
        crossfilter = Application(
            FunctionHandler(partial(crossfilter_doc, worker, extra))
        )
        systemmonitor = Application(
            FunctionHandler(partial(systemmonitor_doc, worker, extra))
        )
        counters = Application(FunctionHandler(partial(counters_doc, worker, extra)))
        profile = Application(FunctionHandler(partial(profile_doc, worker, extra)))
        profile_server = Application(
            FunctionHandler(partial(profile_server_doc, worker, extra))
        )

        self.apps = {
            "/status": status,
            "/counters": counters,
            "/crossfilter": crossfilter,
            "/system": systemmonitor,
            "/profile": profile,
            "/profile-server": profile_server,
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
        super(BokehWorker, self).listen(*args, **kwargs)

        from .worker_html import routes

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)
