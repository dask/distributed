from .core import PrometheusHandler

routes = [("/metrics", PrometheusHandler, {})]
