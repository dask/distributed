import functools

import dask

from tlz import merge

try:
    import numpy as np
except ImportError:
    np = False

from .components.worker import counters_doc
from .components.scheduler import (
    systemmonitor_doc,
    stealing_doc,
    workers_doc,
    events_doc,
    tasks_doc,
    status_doc,
    profile_doc,
    profile_server_doc,
    graph_doc,
    individual_task_stream_doc,
    individual_progress_doc,
    individual_graph_doc,
    individual_profile_doc,
    individual_profile_server_doc,
    individual_nbytes_doc,
    individual_cpu_doc,
    individual_nprocessing_doc,
    individual_workers_doc,
    individual_bandwidth_types_doc,
    individual_bandwidth_workers_doc,
    individual_memory_by_key_doc,
)
from .core import BokehServer
from .worker import counters_doc


template_variables = {
    "pages": ["status", "workers", "tasks", "system", "profile", "graph", "info"]
}


class BokehScheduler(BokehServer):
    def __init__(self, scheduler, io_loop=None, prefix="", **kwargs):
        self.scheduler = scheduler
        prefix = prefix or ""
        prefix = prefix.rstrip("/")
        if prefix and not prefix.startswith("/"):
            prefix = "/" + prefix
        self.prefix = prefix

        self.server_kwargs = kwargs

        # TLS configuration
        http_server_kwargs = kwargs.setdefault("http_server_kwargs", {})
        tls_key = dask.config.get("distributed.scheduler.dashboard.tls.key")
        tls_cert = dask.config.get("distributed.scheduler.dashboard.tls.cert")
        tls_ca_file = dask.config.get("distributed.scheduler.dashboard.tls.ca-file")
        if tls_cert and "ssl_options" not in http_server_kwargs:
            import ssl

            ctx = ssl.create_default_context(
                cafile=tls_ca_file, purpose=ssl.Purpose.SERVER_AUTH
            )
            ctx.load_cert_chain(tls_cert, keyfile=tls_key)
            # Unlike the client/scheduler/worker TLS handling, we don't care
            # about authenticating the user's webclient, TLS here is just for
            # encryption. Disable these checks.
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            http_server_kwargs["ssl_options"] = ctx

        self.server_kwargs["prefix"] = prefix or None

        self.apps = applications
        self.apps = {
            k: functools.partial(v, scheduler, self.extra) for k, v in self.apps.items()
        }

        self.loop = io_loop or scheduler.loop
        self.server = None

    @property
    def extra(self):
        return merge({"prefix": self.prefix}, template_variables)

    @property
    def my_server(self):
        return self.scheduler

    def listen(self, *args, **kwargs):
        from ..http.scheduler import routes

        super(BokehScheduler, self).listen(*args, **kwargs)

        handlers = [
            (
                self.prefix + "/" + url,
                cls,
                {"server": self.my_server, "extra": self.extra},
            )
            for url, cls in routes
        ]

        self.server._tornado.add_handlers(r".*", handlers)


applications = {
    "/system": systemmonitor_doc,
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
    "/individual-cpu": individual_cpu_doc,
    "/individual-nprocessing": individual_nprocessing_doc,
    "/individual-workers": individual_workers_doc,
    "/individual-bandwidth-types": individual_bandwidth_types_doc,
    "/individual-bandwidth-workers": individual_bandwidth_workers_doc,
    "/individual-memory-by-key": individual_memory_by_key_doc,
}

try:
    import pynvml  # noqa: 1708
except ImportError:
    pass
else:
    from .components.nvml import gpu_memory_doc, gpu_utilization_doc  # noqa: 1708

    applications["/individual-gpu-memory"] = gpu_memory_doc
    applications["/individual-gpu-utilization"] = gpu_utilization_doc
