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
            labels=["state", "worker"],
        )
        tasks.add_metric(["stored", self.server.name], len(self.server.data))
        tasks.add_metric(["executing", self.server.name], self.server.executing_count)
        tasks.add_metric(["ready", self.server.name], len(self.server.ready))
        tasks.add_metric(
            ["waiting", self.server.name], self.server.waiting_for_data_count
        )
        yield tasks

        concurrent_fetch_requests = GaugeMetricFamily(
            self.build_name("concurrent_fetch_requests"),
            "Number of open fetch requests to other workers.",
            labels=["worker"],
        )
        concurrent_fetch_requests.add_metric(
            [self.server.name], len(self.server.in_flight_workers)
        )
        yield concurrent_fetch_requests

        threads = GaugeMetricFamily(
            self.build_name("threads"),
            "Number of worker threads.",
            labels=["worker"],
        )
        threads.add_metric([self.server.name], self.server.nthreads)
        yield threads

        latency_seconds = GaugeMetricFamily(
            self.build_name("latency_seconds"),
            "Latency of worker connection.",
            labels=["worker"],
        )
        latency_seconds.add_metric([self.server.name], self.server.latency)
        yield latency_seconds

        # all metrics using digests require crick to be installed
        # the following metrics will export NaN, if the corresponding digests are None
        if self.crick_available:
            tick_duration_median_seconds = GaugeMetricFamily(
                self.build_name("tick_duration_median_seconds"),
                "Median tick duration at worker.",
                labels=["worker"],
            )
            tick_duration_median_seconds.add_metric(
                [self.server.name],
                self.server.digests["tick-duration"].components[1].quantile(50),
            )
            yield tick_duration_median_seconds

            task_duration_median_seconds = GaugeMetricFamily(
                self.build_name("task_duration_median_seconds"),
                "Median task runtime at worker.",
                labels=["worker"],
            )
            task_duration_median_seconds.add_metric(
                [self.server.name],
                self.server.digests["task-duration"].components[1].quantile(50),
            )
            yield task_duration_median_seconds

            transfer_bandwidth_median_bytes = GaugeMetricFamily(
                self.build_name("transfer_bandwidth_median_bytes"),
                "Bandwidth for transfer at worker in Bytes.",
                labels=["worker"],
            )
            transfer_bandwidth_median_bytes.add_metric(
                [self.server.name],
                self.server.digests["transfer-bandwidth"].components[1].quantile(50),
            )
            yield transfer_bandwidth_median_bytes


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
