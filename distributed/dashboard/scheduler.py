from urllib.parse import urljoin

from tornado import web
from tornado.ioloop import IOLoop

try:
    import numpy as np
except ImportError:
    np = False

from .components.nvml import gpu_doc  # noqa: 1708
from .components.nvml import NVML_ENABLED, gpu_memory_doc, gpu_utilization_doc
from .components.scheduler import (
    AggregateAction,
    BandwidthTypes,
    BandwidthWorkers,
    ComputePerKey,
    CurrentLoad,
    MemoryByKey,
    NBytes,
    NBytesCluster,
    SystemMonitor,
    TaskGraph,
    TaskProgress,
    TaskStream,
    WorkerTable,
    events_doc,
    graph_doc,
    individual_doc,
    individual_profile_doc,
    individual_profile_server_doc,
    profile_doc,
    profile_server_doc,
    status_doc,
    stealing_doc,
    systemmonitor_doc,
    tasks_doc,
    workers_doc,
)
from .core import BokehApplication
from .worker import counters_doc

template_variables = {
    "pages": ["status", "workers", "tasks", "system", "profile", "graph", "info"]
}

if NVML_ENABLED:
    template_variables["pages"].insert(4, "gpu")


def connect(application, http_server, scheduler, prefix=""):
    bokeh_app = BokehApplication(
        applications, scheduler, prefix=prefix, template_variables=template_variables
    )
    application.add_application(bokeh_app)
    bokeh_app.initialize(IOLoop.current())

    bokeh_app.add_handlers(
        r".*",
        [
            (
                r"/",
                web.RedirectHandler,
                {"url": urljoin((prefix or "").strip("/") + "/", r"status")},
            )
        ],
    )


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
    "/gpu": gpu_doc,
    "/individual-task-stream": individual_doc(
        TaskStream, 100, n_rectangles=1000, clear_interval="10s"
    ),
    "/individual-progress": individual_doc(TaskProgress, 100, height=160),
    "/individual-graph": individual_doc(TaskGraph, 200),
    "/individual-nbytes": individual_doc(NBytes, 100),
    "/individual-nbytes-cluster": individual_doc(NBytesCluster, 100),
    "/individual-cpu": individual_doc(CurrentLoad, 100, fig_attr="cpu_figure"),
    "/individual-nprocessing": individual_doc(
        CurrentLoad, 100, fig_attr="processing_figure"
    ),
    "/individual-workers": individual_doc(WorkerTable, 500),
    "/individual-bandwidth-types": individual_doc(BandwidthTypes, 500),
    "/individual-bandwidth-workers": individual_doc(BandwidthWorkers, 500),
    "/individual-memory-by-key": individual_doc(MemoryByKey, 500),
    "/individual-compute-time-per-key": individual_doc(ComputePerKey, 500),
    "/individual-aggregate-time-per-action": individual_doc(AggregateAction, 500),
    "/individual-scheduler-system": individual_doc(SystemMonitor, 500),
    "/individual-profile": individual_profile_doc,
    "/individual-profile-server": individual_profile_server_doc,
    "/individual-gpu-memory": gpu_memory_doc,
    "/individual-gpu-utilization": gpu_utilization_doc,
}
