from urllib import parse

from tornado import web

from notebook.utils import url_path_join
from jupyter_server_proxy.handlers import ProxyHandler

import tornado.ioloop
import tornado.web

class Proxy(BokehServer):
    def __init__(self, scheduler, io_loop=None, prefix="", **kwargs):
        self.scheduler = scheduler
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.server_kwargs = kwargs
        self.server_kwargs["prefix"] = prefix or None

        self.apps = {
            "/proxies": systemmonitor_doc,
            "/stealing": stealing_doc,
            "/workers": workers_doc,
            "/events": events_doc,
            "/counters": counters_doc,
            "/tasks": tasks_doc,
            "/status": status_doc,
            "/profile": profile_doc,
            "/profile-server": profile_server_doc,
            "/graph": graph_doc,
            "/individual-task-stream": individual_task_stream_doc,
            "/individual-progress": individual_progress_doc,
            "/individual-graph": individual_graph_doc,
            "/individual-profile": individual_profile_doc,
            "/individual-profile-server": individual_profile_server_doc,
            "/individual-nbytes": individual_nbytes_doc,
            "/individual-nprocessing": individual_nprocessing_doc,
            "/individual-workers": individual_workers_doc,
        }

        self.apps = {k: partial(v, scheduler, self.extra) for k, v in self.apps.items()}

        self.loop = io_loop or scheduler.loop
        self.server = None

    @property
    def extra(self):
        return merge({"prefix": self.prefix}, template_variables)

    @property
    def my_server(self):
        return self.scheduler

    def listen(self, *args, **kwargs):
        super(BokehScheduler, self).listen(*args, **kwargs)

        from .scheduler_html import routes

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)
