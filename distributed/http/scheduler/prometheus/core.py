import toolz

from distributed.http.prometheus import PrometheusCollector
from distributed.http.scheduler.prometheus.semaphore import SemaphoreMetricCollector
from distributed.http.utils import RequestHandler
from distributed.scheduler import ALL_TASK_STATES


class SchedulerMetricCollector(PrometheusCollector):
    def __init__(self, server):
        super().__init__(server)
        self.subsystem = "scheduler"

    def collect(self):
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

        yield GaugeMetricFamily(
            self.build_name("clients"),
            "Number of clients connected.",
            value=len([k for k in self.server.clients if k != "fire-and-forget"]),
        )

        yield GaugeMetricFamily(
            self.build_name("desired_workers"),
            "Number of workers scheduler needs for task graph.",
            value=self.server.adaptive_target(),
        )

        worker_states = GaugeMetricFamily(
            self.build_name("workers"),
            "Number of workers known by scheduler.",
            labels=["state"],
        )
        worker_states.add_metric(["connected"], len(self.server.workers))
        worker_states.add_metric(["saturated"], len(self.server.saturated))
        worker_states.add_metric(["idle"], len(self.server.idle))
        yield worker_states

        tasks = GaugeMetricFamily(
            self.build_name("tasks"),
            "Number of tasks known by scheduler.",
            labels=["state"],
        )

        task_counter = toolz.merge_with(
            sum, (tp.states for tp in self.server.task_prefixes.values())
        )

        suspicious_tasks = CounterMetricFamily(
            self.build_name("tasks_suspicious"),
            "Total number of times a task has been marked suspicious",
            labels=["task_prefix_name"],
        )

        for tp in self.server.task_prefixes.values():
            suspicious_tasks.add_metric([tp.name], tp.suspicious)
        yield suspicious_tasks

        yield CounterMetricFamily(
            self.build_name("tasks_forgotten"),
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


COLLECTORS = [SchedulerMetricCollector, SemaphoreMetricCollector]


class PrometheusHandler(RequestHandler):
    _collectors = None

    def __init__(self, *args, dask_server=None, **kwargs):
        import prometheus_client

        super().__init__(*args, dask_server=dask_server, **kwargs)

        if PrometheusHandler._collectors:
            # Especially during testing, multiple schedulers are started
            # sequentially in the same python process
            for _collector in PrometheusHandler._collectors:
                _collector.server = self.server
            return

        PrometheusHandler._collectors = tuple(
            collector(self.server) for collector in COLLECTORS
        )
        # Register collectors
        for instantiated_collector in PrometheusHandler._collectors:
            prometheus_client.REGISTRY.register(instantiated_collector)

    def get(self):
        import prometheus_client

        self.write(prometheus_client.generate_latest())
        self.set_header("Content-Type", "text/plain; version=0.0.4")
