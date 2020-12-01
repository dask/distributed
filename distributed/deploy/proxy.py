import asyncio
import contextlib

import psutil

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
    open_ports = []

    try:
        connections = psutil.net_connections()
        for connection in connections:
            if (
                connection.status == "LISTEN"
                and connection.family.name == "AF_INET"
                and connection.laddr.port not in open_ports
            ):
                open_ports.append(connection.laddr.port)
    except psutil.AccessDenied:
        # On macOS this needs to be run as root to work but we can still try the default port
        open_ports = [8786]

    async def try_connect(port):
        with contextlib.suppress(OSError):
            async with Client(
                f"tcp://localhost:{port}", asynchronous=True, timeout=0.5
            ):
                return port
        return

    for port in await asyncio.gather(*[try_connect(port) for port in open_ports]):
        if port:
            yield (
                f"proxycluster-{port}",
                ProxyCluster,
            )
