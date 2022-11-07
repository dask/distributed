from __future__ import annotations

from distributed.http.prometheus import PrometheusCollector
from distributed.stealing import WorkStealing


class WorkStealingMetricCollector(PrometheusCollector):
    def __init__(self, server):
        super().__init__(server)
        self.subsystem = "stealing"

    def collect(self):
        from prometheus_client.core import CounterMetricFamily

        try:
            stealing: WorkStealing = self.server.extensions["stealing"]
        except KeyError:
            return

        stealing_request_count_total = CounterMetricFamily(
            self.build_name("request_count_total"),
            "Total number of stealing requests per cost multiplier.",
            labels=["cost_multiplier"],
        )

        stealing_request_cost_total = CounterMetricFamily(
            self.build_name("request_cost_total"),
            "Total cost of stealing requests per cost multiplier.",
            labels=["cost_multiplier"],
        )

        for level, multiplier in enumerate(stealing.cost_multipliers):
            stealing_request_count_total.add_metric(
                [str(multiplier)], stealing.metrics["request_count_total"][level]
            )

            stealing_request_cost_total.add_metric(
                [str(multiplier)], stealing.metrics["request_cost_total"][level]
            )

        yield stealing_request_count_total
        yield stealing_request_cost_total
