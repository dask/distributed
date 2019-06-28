from .utils import RequestHandler, redirect


class _PrometheusCollector(object):
    def __init__(self, server, prometheus_client):
        self.worker = server

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily

        yield GaugeMetricFamily(
            "dask_worker_tasks_stored",
            "Number of tasks stored",
            value=len(self.worker.data),
        )
        yield GaugeMetricFamily(
            "dask_worker_tasks_ready",
            "Number of tasks ready",
            value=len(self.worker.ready),
        )
        yield GaugeMetricFamily(
            "dask_worker_tasks_waiting",
            "Number of tasks waiting",
            value=len(self.worker.waiting_for_data),
        )
        yield GaugeMetricFamily(
            "dask_worker_connections",
            "Number of task connections",
            value=len(self.worker.in_flight_workers),
        )
        yield GaugeMetricFamily(
            "dask_worker_tasks_serving",
            "Number of tasks serving",
            value=len(self.worker._comms),
        )
        yield GaugeMetricFamily(
            "dask_worker_nthreads", "Number of threads", value=self.worker.nthreads
        )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super(PrometheusHandler, self).__init__(*args, **kwargs)

        if PrometheusHandler._initialized:
            return

        prometheus_client.REGISTRY.register(
            _PrometheusCollector(self.server, prometheus_client)
        )

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
    (r"metrics", PrometheusHandler),
    (r"health", HealthHandler),
    (r"main", redirect("/status")),
]


def get_handlers(server):
    return [(url, cls, {"server": server}) for url, cls in routes]
