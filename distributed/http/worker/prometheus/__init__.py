from __future__ import annotations

from distributed.http.worker.prometheus.core import PrometheusHandler

routes: list[tuple] = [("/metrics", PrometheusHandler, {})]
