import logging

from distributed.http.prometheus import PrometheusCollector
from distributed.http.utils import RequestHandler


class WorkerMetricCollector(PrometheusCollector):
    def __init__(self, server):
        super().__init__(server)
        self.logger = logging.getLogger("distributed.dask_worker")
        self.subsystem = "worker"
        self.crick_available = True
        try:
            import crick  # noqa: F401
        except ImportError:
            self.crick_available = False
            self.logger.info(
                "Not all prometheus metrics available are exported. Digest-based metrics require crick to be installed"
            )

    def collect(self):
        from prometheus_client.core import GaugeMetricFamily

        tasks = GaugeMetricFamily(
            self.build_name("tasks"),
            "Number of tasks at worker.",
            labels=["state"],
        )
        tasks.add_metric(["stored"], len(self.server.data))
        tasks.add_metric(["executing"], self.server.executing_count)
        tasks.add_metric(["ready"], len(self.server.ready))
        tasks.add_metric(["waiting"], self.server.waiting_for_data_count)
        yield tasks

        yield GaugeMetricFamily(
            self.build_name("concurrent_fetch_requests"),
            "Number of open fetch requests to other workers.",
            value=len(self.server.in_flight_workers),
        )

        yield GaugeMetricFamily(
            self.build_name("threads"),
            "Number of worker threads.",
            value=self.server.nthreads,
        )

        yield GaugeMetricFamily(
            self.build_name("latency_seconds"),
            "Latency of worker connection.",
            value=self.server.latency,
        )

        # all metrics using digests require crick to be installed
        # the following metrics will export NaN, if the corresponding digests are None
        if self.crick_available:
            yield GaugeMetricFamily(
                self.build_name("tick_duration_median_seconds"),
                "Median tick duration at worker.",
                value=self.server.digests["tick-duration"].components[1].quantile(50),
            )

            yield GaugeMetricFamily(
                self.build_name("task_duration_median_seconds"),
                "Median task runtime at worker.",
                value=self.server.digests["task-duration"].components[1].quantile(50),
            )

            yield GaugeMetricFamily(
                self.build_name("transfer_bandwidth_median_bytes"),
                "Bandwidth for transfer at worker in Bytes.",
                value=self.server.digests["transfer-bandwidth"]
                .components[1]
                .quantile(50),
            )


class PrometheusHandler(RequestHandler):
    _initialized = False

    def __init__(self, *args, **kwargs):
        import prometheus_client

        super().__init__(*args, **kwargs)

        if PrometheusHandler._initialized:
            return

        prometheus_client.REGISTRY.register(WorkerMetricCollector(self.server))

        PrometheusHandler._initialized = True

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
