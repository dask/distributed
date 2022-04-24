from __future__ import annotations

import json
import logging
import os
import os.path
from datetime import datetime

from tlz import first, merge
from tornado import escape
from tornado.websocket import WebSocketHandler

from dask.utils import format_bytes, format_time

from distributed.diagnostics.websocket import WebsocketPlugin
from distributed.http.utils import RequestHandler, redirect
from distributed.metrics import time
from distributed.utils import log_errors

ns = {
    func.__name__: func
    for func in [format_bytes, format_time, datetime.fromtimestamp, time]
}

rel_path_statics = {"rel_path_statics": "../../.."}


logger = logging.getLogger(__name__)


class Workers(RequestHandler):
    @log_errors
    def get(self):
        self.render(
            "workers.html",
            title="Workers",
            scheduler=self.server,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Worker(RequestHandler):
    @log_errors
    def get(self, worker):
        worker = escape.url_unescape(worker)
        if worker not in self.server.workers:
            self.send_error(404)
            return

        self.render(
            "worker.html",
            title="Worker: " + worker,
            scheduler=self.server,
            Worker=worker,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Task(RequestHandler):
    @log_errors
    def get(self, task):
        task = escape.url_unescape(task)
        if task not in self.server.tasks:
            self.send_error(404)
            return

        self.render(
            "task.html",
            title="Task: " + task,
            Task=task,
            scheduler=self.server,
            **merge(
                self.server.__dict__,
                self.server.__pdict__,
                ns,
                self.extra,
                rel_path_statics,
            ),
        )


class Logs(RequestHandler):
    @log_errors
    def get(self):
        logs = self.server.get_logs()
        self.render(
            "logs.html",
            title="Logs",
            logs=logs,
            **merge(self.extra, rel_path_statics),
        )


class WorkerLogs(RequestHandler):
    @log_errors
    async def get(self, worker):
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
    @log_errors
    async def get(self, worker):
        worker = escape.url_unescape(worker)
        keys = {ts.key for ts in self.server.workers[worker].processing}
        call_stack = await self.server.get_call_stack(keys=keys)
        self.render(
            "call-stack.html",
            title="Call Stacks: " + worker,
            call_stack=call_stack,
            **merge(self.extra, rel_path_statics),
        )


class TaskCallStack(RequestHandler):
    @log_errors
    async def get(self, key):
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


class IndividualPlots(RequestHandler):
    def get(self):
        try:
            from bokeh.server.tornado import BokehTornado

            bokeh_application = first(
                app
                for app in self.server.http_application.applications
                if isinstance(app, BokehTornado)
            )
            individual_bokeh = {
                uri.strip("/").replace("-", " ").title(): uri
                for uri in bokeh_application.app_paths
                if uri.lstrip("/").startswith("individual-")
                and not uri.endswith(".json")
            }
            individual_static = {
                uri.strip("/")
                .replace(".html", "")
                .replace("-", " ")
                .title(): "/statics/"
                + uri
                for uri in os.listdir(
                    os.path.join(os.path.dirname(__file__), "..", "static")
                )
                if uri.lstrip("/").startswith("individual-") and uri.endswith(".html")
            }
            result = {**individual_bokeh, **individual_static}
            self.write(result)
        except (ImportError, StopIteration):
            self.write({})


class EventstreamHandler(WebSocketHandler):
    def initialize(self, dask_server=None, extra=None):
        self.server = dask_server
        self.extra = extra or {}
        self.plugin = WebsocketPlugin(self, self.server)
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
        self.server.remove_plugin(name=self.plugin.name)


routes: list[tuple] = [
    (r"info", redirect("info/main/workers.html"), {}),
    (r"info/main/workers.html", Workers, {}),
    (r"info/worker/(.*).html", Worker, {}),
    (r"info/task/(.*).html", Task, {}),
    (r"info/main/logs.html", Logs, {}),
    (r"info/call-stacks/(.*).html", WorkerCallStacks, {}),
    (r"info/call-stack/(.*).html", TaskCallStack, {}),
    (r"info/logs/(.*).html", WorkerLogs, {}),
    (r"individual-plots.json", IndividualPlots, {}),
    (r"eventstream", EventstreamHandler, {}),
]
