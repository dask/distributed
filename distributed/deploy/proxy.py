import asyncio

from .cluster import Cluster
from ..core import rpc, Status, CommClosedError
from ..client import Client
from ..utils import LoopRunner


class ProxyCluster(Cluster):
    """A ProxyCluster is a representation of a cluster with a locally running scheduler.

    If a Dask Scheduler is running locally it is generally assumed that the process is tightly
    coupled to a parent process and therefore the cluster manager type cannot be reconstructed.

    The ProxyCluster object allows you limited interactivity with a local cluster in the same way
    you would with a regular cluster, allowing you to retrieve logs, get stats, etc.

    """

    @classmethod
    def from_name(cls, name, loop=None, asynchronous=False):
        cluster = cls(asynchronous=asynchronous)
        cluster.name = name
        port = name.split("-")[-1]

        cluster.scheduler_comm = rpc(f"tcp://localhost:{port}")

        cluster._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        cluster.loop = cluster._loop_runner.loop
        if not asynchronous:
            cluster._loop_runner.start()

        cluster.status = Status.starting
        cluster.sync(cluster._start)
        return cluster


async def discover():
    open_ports = [8786]  # TODO Scan high ports for Dask schedulers
    dask_ports = []

    for port in open_ports:
        try:
            async with Client(
                f"tcp://localhost:{port}", asynchronous=True, timeout=0.5
            ):
                dask_ports.append(port)
        except OSError:
            pass

    for port in dask_ports:
        yield (
            f"proxycluster-{port}",
            ProxyCluster,
        )
