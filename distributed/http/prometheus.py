import dask.config


class PrometheusCollector:
    def __init__(self, server):
        self.server = server
        self.namespace = dask.config.get("distributed.dashboard.prometheus.namespace")
        self.subsystem = None

    def build_name(self, name):
        full_name = []
        if self.namespace:
            full_name.append(self.namespace)
        if self.subsystem:
            full_name.append(self.subsystem)
        full_name.append(name)
        return "_".join(full_name)
