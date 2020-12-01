from typing import Callable, Dict, AsyncIterator, Tuple
from contextlib import suppress
import pkg_resources
import warnings

from tornado.ioloop import IOLoop

from distributed.deploy.spec import SpecCluster


def list_discovery_methods() -> Dict[str, Callable]:
    discovery_methods = {}
    for ep in pkg_resources.iter_entry_points(group="dask_cluster_discovery"):
        with suppress(AttributeError):
            discovery_methods.update(
                {
                    ep.name: {
                        "discover": ep.load(),
                        "package": ep.dist.key,
                        "version": ep.dist.version,
                        "path": ep.dist.location,
                    }
                }
            )
    return discovery_methods


async def discover_cluster_names(
    discovery: str = None,
) -> AsyncIterator[Tuple[str, Callable]]:
    discovery_methods = list_discovery_methods()
    for discovery_method in discovery_methods:
        try:
            if discovery is None or discovery == discovery_method:
                async for cluster_name, cluster_class in discovery_methods[
                    discovery_method
                ]["discover"]():
                    yield (cluster_name, cluster_class)
                if discovery is not None:
                    return
        except Exception as e:  # We are calling code that is out of our control here, so handling broad exceptions
            if discovery is None:
                warnings.warn(f"Cluster discovery for {discovery_method} failed.")
            else:
                raise e


async def discover_clusters(discovery=None) -> AsyncIterator[SpecCluster]:
    async for cluster_name, cluster_class in discover_cluster_names(discovery):
        with suppress(Exception):
            yield cluster_class.from_name(cluster_name)


def list_clusters():
    loop = IOLoop.current()

    async def _list_clusters():
        clusters = []
        async for cluster in discover_clusters():
            clusters.append(cluster)
        return clusters

    return loop.run_sync(_list_clusters)


def get_cluster(name):
    loop = IOLoop.current()

    async def _get_cluster():
        async for cluster_name, cluster_class in discover_cluster_names():
            if cluster_name == name:
                return cluster_class.from_name(name)
        raise RuntimeError("No such cluster %s", name)

    return loop.run_sync(_get_cluster)


def scale_cluster(name, n_workers):
    loop = IOLoop.current()

    async def _scale_cluster():
        async for cluster_name, cluster_class in discover_cluster_names():
            if cluster_name == name:
                return cluster_class.from_name(name).scale(n_workers)
        raise RuntimeError("No such cluster %s", name)

    return loop.run_sync(_scale_cluster)


def delete_cluster(name):
    loop = IOLoop.current()

    async def _delete_cluster():
        async for cluster_name, cluster_class in discover_cluster_names():
            if cluster_name == name:
                return cluster_class.from_name(name).close()
        raise RuntimeError("No such cluster %s", name)

    return loop.run_sync(_delete_cluster)
