from __future__ import annotations

from distributed.http.scheduler.prometheus.core import PrometheusHandler

routes: list[tuple] = [("/metrics", PrometheusHandler, {})]
