from __future__ import annotations

from .core import PrometheusHandler

routes: list[tuple] = [("/metrics", PrometheusHandler, {})]
