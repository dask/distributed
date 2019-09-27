from datetime import datetime

from dask.utils import format_bytes
import toolz
from tornado import escape

from ..utils import log_errors, format_time
from .proxy import GlobalProxyHandler
from .utils import RequestHandler, redirect

ns = {
    func.__name__: func for func in [format_bytes, format_time, datetime.fromtimestamp]
}

rel_path_statics = {"rel_path_statics": "../../"}


class Workers(RequestHandler):
    def get(self):
        with log_errors():
            self.render(
                "workers.html",
                title="Workers",
                scheduler=self.server,
                **toolz.merge(self.server.__dict__, ns, self.extra, rel_path_statics),
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
                **toolz.merge(self.server.__dict__, ns, self.extra, rel_path_statics),
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
                **toolz.merge(self.server.__dict__, ns, self.extra, rel_path_statics),
            )


class Logs(RequestHandler):
    def get(self):
        with log_errors():
            logs = self.server.get_logs()
            self.render(
                "logs.html",
                title="Logs",
                logs=logs,
                **toolz.merge(self.extra, rel_path_statics),
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
                **toolz.merge(self.extra, rel_path_statics),
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
                **toolz.merge(self.extra, rel_path_statics),
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
                    **toolz.merge(self.extra, rel_path_statics),
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
        }
        self.write(response)


class IdentityJSON(RequestHandler):
    def get(self):
        self.write(self.server.identity())


class IndexJSON(RequestHandler):
    def get(self):
        with log_errors():
            r = [url for url, _ in routes if url.endswith(".json")]
            self.render(
                "json-index.html", routes=r, title="Index of JSON routes", **self.extra
            )


class IndividualPlots(RequestHandler):
    def get(self):
        bokeh_server = self.server.services["dashboard"]
        result = {
            uri.strip("/").replace("-", " ").title(): uri
            for uri in bokeh_server.apps
            if uri.lstrip("/").startswith("individual-") and not uri.endswith(".json")
        }
        self.write(result)


class _PrometheusCollector(object):
    def __init__(self, server):
        self.server = server

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily

        yield GaugeMetricFamily(
            "dask_scheduler_workers",
            "Number of workers connected.",
            value=len(self.server.workers),
        )
        yield GaugeMetricFamily(
            "dask_scheduler_clients",
            "Number of clients connected.",
            value=len(self.server.clients),
        )
        yield GaugeMetricFamily(
            "dask_scheduler_received_tasks",
            "Number of tasks received at scheduler",
            value=len(self.server.tasks),
        )
        yield GaugeMetricFamily(
            "dask_scheduler_unrunnable_tasks",
            "Number of unrunnable tasks at scheduler",
            value=len(self.server.unrunnable),
        )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        if PrometheusHandler._initialized:
            return

        prometheus_client.REGISTRY.register(_PrometheusCollector(self.server))

        PrometheusHandler._initialized = True

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")


class HealthHandler(RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain")


routes = [
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
    (r"proxy/(\d+)/(.*?)/(.*)", GlobalProxyHandler),
]


def get_handlers(server):
    return [(url, cls, {"server": server}) for url, cls in routes]
