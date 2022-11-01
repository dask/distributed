from __future__ import annotations

from distributed.http.prometheus import get_metrics_handler

routes: list[tuple] = [
    (
        "/metrics",
        get_metrics_handler(
            "distributed.http.scheduler.prometheus.core", "PrometheusHandler"
        ),
        {},
    )
]
