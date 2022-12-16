from __future__ import annotations

import logging
from collections.abc import Iterator
from time import time
from typing import ClassVar

import prometheus_client
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric

from distributed.http.prometheus import PrometheusCollector
from distributed.http.utils import RequestHandler
from distributed.worker import Worker

logger = logging.getLogger("distributed.prometheus.worker")


class WorkerMetricCollector(PrometheusCollector):
    server: Worker

    def __init__(self, server: Worker):
        super().__init__(server)
        self.subsystem = "worker"
        self.crick_available = True
        try:
            import crick  # noqa: F401
        except ImportError:
            self.crick_available = False
            logger.debug(
                "Not all prometheus metrics available are exported. "
                "Digest-based metrics require crick to be installed."
            )

    def collect(self) -> Iterator[Metric]:
        ws = self.server.state

        yield GaugeMetricFamily(
            self.build_name("concurrent_fetch_requests"),
            (
                "Deprecated: This metric has been renamed to transfer_incoming_count.\n"
                "Number of open fetch requests to other workers"
            ),
            value=ws.transfer_incoming_count,
        )

        yield GaugeMetricFamily(
            self.build_name("threads"),
            "Number of worker threads",
            value=ws.nthreads,
        )

        try:
            spilled_memory, spilled_disk = self.server.data.spilled_total  # type: ignore
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

        yield from self.collect_crick()

        now = time()
        max_tick_duration = max(
            self.server.digests_max["tick_duration"],
            now - self.server._last_tick,
        )
        yield GaugeMetricFamily(
            self.build_name("tick_duration_maximum"),
            "Maximum tick duration observed since Prometheus last scraped metrics",
            unit="seconds",
            value=max_tick_duration,
        )

        yield CounterMetricFamily(
            self.build_name("tick_count"),
            "Total number of ticks observed since the server started",
            value=self.server._tick_counter,
        )
        self.server.digests_max.clear()

    def collect_crick(self) -> Iterator[Metric]:
        # All metrics using digests require crick to be installed.
        # The following metrics will export NaN, if the corresponding digests are None
        if not self.crick_available:
            return

        yield GaugeMetricFamily(
            self.build_name("tick_duration_median"),
            "Median tick duration at worker",
            unit="seconds",
            value=self.server.digests["tick-duration"].components[1].quantile(50),
        )

        yield GaugeMetricFamily(
            self.build_name("task_duration_median"),
            "Median task runtime at worker",
            unit="seconds",
            value=self.server.digests["task-duration"].components[1].quantile(50),
        )

        yield GaugeMetricFamily(
            self.build_name("transfer_bandwidth_median"),
            "Bandwidth for transfer at worker",
            unit="bytes",
            value=self.server.digests["transfer-bandwidth"].components[1].quantile(50),
        )


class PrometheusHandler(RequestHandler):
    _collector: ClassVar[WorkerMetricCollector | None] = None

    def __init__(self, *args, dask_server=None, **kwargs):
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
        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
