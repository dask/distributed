from datetime import datetime
import json
import logging
import os
import os.path

from dask.utils import format_bytes

from tornado import escape, web
from tornado.websocket import WebSocketHandler
from tlz import first, merge, merge_with

from .proxy import GlobalProxyHandler
from .utils import RequestHandler, redirect
from ..diagnostics.websocket import WebsocketPlugin
from ..metrics import time
from ..scheduler import ALL_TASK_STATES
from ..utils import log_errors, format_time

ns = {
    func.__name__: func
    for func in [format_bytes, format_time, datetime.fromtimestamp, time]
}

rel_path_statics = {"rel_path_statics": "../../"}


logger = logging.getLogger(__name__)


class Workers(RequestHandler):
    def get(self):
        with log_errors():
            self.render(
                "workers.html",
                title="Workers",
                scheduler=self.server,
                **merge(self.server.__dict__, ns, self.extra, rel_path_statics),
            )


class Worker(RequestHandler):
    def get(self, worker):
        worker = escape.url_unescape(worker)
        if worker not in self.server.workers:
            self.send_error(404)
            return
        with log_errors():
            self.render(
                "worker.html",
                title="Worker: " + worker,
                scheduler=self.server,
                Worker=worker,
                **merge(self.server.__dict__, ns, self.extra, rel_path_statics),
            )


class Task(RequestHandler):
    def get(self, task):
        task = escape.url_unescape(task)
        if task not in self.server.tasks:
            self.send_error(404)
            return
        with log_errors():
            self.render(
                "task.html",
                title="Task: " + task,
                Task=task,
                scheduler=self.server,
                **merge(self.server.__dict__, ns, self.extra, rel_path_statics),
            )


class Logs(RequestHandler):
    def get(self):
        with log_errors():
            logs = self.server.get_logs()
            self.render(
                "logs.html",
                title="Logs",
                logs=logs,
                **merge(self.extra, rel_path_statics),
            )


class WorkerLogs(RequestHandler):
    async def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            logs = await self.server.get_worker_logs(workers=[worker])
            logs = logs[worker]
            self.render(
                "logs.html",
                title="Logs: " + worker,
                logs=logs,
                **merge(self.extra, rel_path_statics),
            )


class WorkerCallStacks(RequestHandler):
    async def get(self, worker):
        with log_errors():
            worker = escape.url_unescape(worker)
            keys = self.server.processing[worker]
            call_stack = await self.server.get_call_stack(keys=keys)
            self.render(
                "call-stack.html",
                title="Call Stacks: " + worker,
                call_stack=call_stack,
                **merge(self.extra, rel_path_statics),
            )


class TaskCallStack(RequestHandler):
    async def get(self, key):
        with log_errors():
            key = escape.url_unescape(key)
            call_stack = await self.server.get_call_stack(keys=[key])
            if not call_stack:
                self.write(
                    "<p>Task not actively running. "
                    "It may be finished or not yet started</p>"
                )
            else:
                self.render(
                    "call-stack.html",
                    title="Call Stack: " + key,
                    call_stack=call_stack,
                    **merge(self.extra, rel_path_statics),
                )


class CountsJSON(RequestHandler):
    def get(self):
        scheduler = self.server
        erred = 0
        nbytes = 0
        nthreads = 0
        memory = 0
        processing = 0
        released = 0
        waiting = 0
        waiting_data = 0
        desired_workers = scheduler.adaptive_target()

        for ts in scheduler.tasks.values():
            if ts.exception_blame is not None:
                erred += 1
            elif ts.state == "released":
                released += 1
            if ts.waiting_on:
                waiting += 1
            if ts.waiters:
                waiting_data += 1
        for ws in scheduler.workers.values():
            nthreads += ws.nthreads
            memory += len(ws.has_what)
            nbytes += ws.nbytes
            processing += len(ws.processing)

        response = {
            "bytes": nbytes,
            "clients": len(scheduler.clients),
            "cores": nthreads,
            "erred": erred,
            "hosts": len(scheduler.host_info),
            "idle": len(scheduler.idle),
            "memory": memory,
            "processing": processing,
            "released": released,
            "saturated": len(scheduler.saturated),
            "tasks": len(scheduler.tasks),
            "unrunnable": len(scheduler.unrunnable),
            "waiting": waiting,
            "waiting_data": waiting_data,
            "workers": len(scheduler.workers),
            "desired_workers": desired_workers,
        }
        self.write(response)


class IdentityJSON(RequestHandler):
    def get(self):
        self.write(self.server.identity())


class IndexJSON(RequestHandler):
    def get(self):
        with log_errors():
            r = [url for url, _ in dask_routes if url.endswith(".json")]
            self.render(
                "json-index.html", routes=r, title="Index of JSON routes", **self.extra
            )


class IndividualPlots(RequestHandler):
    def get(self):
        from bokeh.server.tornado import BokehTornado

        bokeh_application = first(
            app
            for app in self.server.http_application.applications
            if isinstance(app, BokehTornado)
        )
        individual_bokeh = {
            uri.strip("/").replace("-", " ").title(): uri
            for uri in bokeh_application.app_paths
            if uri.lstrip("/").startswith("individual-") and not uri.endswith(".json")
        }
        individual_static = {
            uri.strip("/").replace(".html", "").replace("-", " ").title(): "/statics/"
            + uri
            for uri in os.listdir(os.path.join(os.path.dirname(__file__), "static"))
            if uri.lstrip("/").startswith("individual-") and uri.endswith(".html")
        }
        result = {**individual_bokeh, **individual_static}
        self.write(result)


class _PrometheusCollector:
    def __init__(self, server):
        self.server = server

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

        yield GaugeMetricFamily(
            "dask_scheduler_clients",
            "Number of clients connected.",
            value=len(self.server.clients),
        )

        yield GaugeMetricFamily(
            "dask_scheduler_desired_workers",
            "Number of workers scheduler needs for task graph.",
            value=self.server.adaptive_target(),
        )

        worker_states = GaugeMetricFamily(
            "dask_scheduler_workers",
            "Number of workers known by scheduler.",
            labels=["state"],
        )
        worker_states.add_metric(["connected"], len(self.server.workers))
        worker_states.add_metric(["saturated"], len(self.server.saturated))
        worker_states.add_metric(["idle"], len(self.server.idle))
        yield worker_states

        tasks = GaugeMetricFamily(
            "dask_scheduler_tasks",
            "Number of tasks known by scheduler.",
            labels=["state"],
        )

        task_counter = merge_with(
            sum, (tp.states for tp in self.server.task_prefixes.values())
        )

        suspicious_tasks = CounterMetricFamily(
            "dask_scheduler_tasks_suspicious",
            "Total number of times a task has been marked suspicious",
            labels=["task_prefix_name"],
        )

        for tp in self.server.task_prefixes.values():
            suspicious_tasks.add_metric([tp.name], tp.suspicious)
        yield suspicious_tasks

        yield CounterMetricFamily(
            "dask_scheduler_tasks_forgotten",
            (
                "Total number of processed tasks no longer in memory and already "
                "removed from the scheduler job queue. Note task groups on the "
                "scheduler which have all tasks in the forgotten state are not included."
            ),
            value=task_counter.get("forgotten", 0.0),
        )

        for state in ALL_TASK_STATES:
            tasks.add_metric([state], task_counter.get(state, 0.0))
        yield tasks


class PrometheusHandler(RequestHandler):
    _collector = None

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        if PrometheusHandler._collector:
            # Especially during testing, multiple schedulers are started
            # sequentially in the same python process
            PrometheusHandler._collector.server = self.server
            return

        PrometheusHandler._collector = _PrometheusCollector(self.server)
        prometheus_client.REGISTRY.register(PrometheusHandler._collector)

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")


class HealthHandler(RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain")


class EventstreamHandler(WebSocketHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}
        self.plugin = WebsocketPlugin(self, server)
        self.server.add_plugin(self.plugin)

    def send(self, name, data):
        data["name"] = name
        for k in list(data):
            # Drop bytes objects for now
            if isinstance(data[k], bytes):
                del data[k]
        self.write_message(data)

    def open(self):
        for worker in self.server.workers:
            self.plugin.add_worker(self.server, worker)

    def on_message(self, message):
        message = json.loads(message)
        if message["name"] == "ping":
            self.send("pong", {"timestamp": str(datetime.now())})

    def on_close(self):
        self.server.remove_plugin(self.plugin)


dask_routes = [
    (r"info", redirect("info/main/workers.html")),
    (r"info/main/workers.html", Workers),
    (r"info/worker/(.*).html", Worker),
    (r"info/task/(.*).html", Task),
    (r"info/main/logs.html", Logs),
    (r"info/call-stacks/(.*).html", WorkerCallStacks),
    (r"info/call-stack/(.*).html", TaskCallStack),
    (r"info/logs/(.*).html", WorkerLogs),
    (r"json/counts.json", CountsJSON),
    (r"json/identity.json", IdentityJSON),
    (r"json/index.html", IndexJSON),
    (r"individual-plots.json", IndividualPlots),
    (r"metrics", PrometheusHandler),
    (r"health", HealthHandler),
    (r"eventstream", EventstreamHandler),
    (r"proxy/(\d+)/(.*?)/(.*)", GlobalProxyHandler),
]

plain_routes = [
    (
        r"/statics/(.*)",
        web.StaticFileHandler,
        {"path": os.path.join(os.path.dirname(__file__), "static")},
    ),
]


def get_handlers(server, prefix="/"):
    prefix = "/" + prefix.strip("/")
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    return [
        (prefix + url, cls, {"server": server}) for url, cls in dask_routes
    ] + plain_routes
