from __future__ import annotations

import logging
from typing import ClassVar

from distributed.http.prometheus import PrometheusCollector
from distributed.http.utils import RequestHandler
from distributed.worker import Worker


class WorkerMetricCollector(PrometheusCollector):
    server: Worker

    def __init__(self, server: Worker):
        super().__init__(server)
        self.logger = logging.getLogger("distributed.dask_worker")
        self.subsystem = "worker"
        self.crick_available = True
        try:
            import crick  # noqa: F401
        except ImportError:
            self.crick_available = False
            self.logger.info(
                "Not all prometheus metrics available are exported. "
                "Digest-based metrics require crick to be installed"
            )

    def collect(self):
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

        ws = self.server.state

        tasks = GaugeMetricFamily(
            self.build_name("tasks"),
            "Number of tasks at worker.",
            labels=["state"],
        )
        for k, n in ws.task_counts.items():
            tasks.add_metric([k], n)
        yield tasks

        yield GaugeMetricFamily(
            self.build_name("concurrent_fetch_requests"),
            (
                "[Deprecated: This metric has been renamed to transfer_incoming_count.] "
                "Number of open fetch requests to other workers."
            ),
            value=ws.transfer_incoming_count,
        )

        yield GaugeMetricFamily(
            self.build_name("threads"),
            "Number of worker threads.",
            value=ws.nthreads,
        )

        yield GaugeMetricFamily(
            self.build_name("latency_seconds"),
            "Latency of worker connection.",
            value=self.server.latency,
        )

        try:
            spilled_memory, spilled_disk = self.server.data.spilled_total
        except AttributeError:
            spilled_memory, spilled_disk = 0, 0  # spilling is disabled
        process_memory = self.server.monitor.get_process_memory()
        managed_memory = min(process_memory, ws.nbytes - spilled_memory)

        memory = GaugeMetricFamily(
            self.build_name("memory_bytes"),
            "Memory breakdown",
            labels=["type"],
        )
        memory.add_metric(["managed"], managed_memory)
        memory.add_metric(["unmanaged"], process_memory - managed_memory)
        memory.add_metric(["spilled"], spilled_disk)
        yield memory

        yield GaugeMetricFamily(
            self.build_name("transfer_incoming_bytes"),
            "Total size of open data transfers from other workers.",
            value=ws.transfer_incoming_bytes,
        )
        yield GaugeMetricFamily(
            self.build_name("transfer_incoming_count"),
            "Number of open data transfers from other workers.",
            value=ws.transfer_incoming_count,
        )

        yield CounterMetricFamily(
            self.build_name("transfer_incoming_count_total"),
            (
                "Total number of data transfers from other workers "
                "since the worker was started."
            ),
            value=ws.transfer_incoming_count_total,
        )

        yield GaugeMetricFamily(
            self.build_name("transfer_outgoing_bytes"),
            "Total size of open data transfers to other workers.",
            value=self.server.transfer_outgoing_bytes,
        )
        yield GaugeMetricFamily(
            self.build_name("transfer_outgoing_count"),
            "Number of open data transfers to other workers.",
            value=self.server.transfer_outgoing_count,
        )

        yield CounterMetricFamily(
            self.build_name("transfer_outgoing_count_total"),
            (
                "Total number of data transfers to other workers "
                "since the worker was started."
            ),
            value=self.server.transfer_outgoing_count_total,
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
    _collector: ClassVar[WorkerMetricCollector | None] = None

    def __init__(self, *args, dask_server=None, **kwargs):
        import prometheus_client

        super().__init__(*args, dask_server=dask_server, **kwargs)

        if PrometheusHandler._collector:
            # Especially during testing, multiple workers are started
            # sequentially in the same python process
            PrometheusHandler._collector.server = self.server
            return

        PrometheusHandler._collector = WorkerMetricCollector(self.server)
        # Register collector
        prometheus_client.REGISTRY.register(PrometheusHandler._collector)

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
