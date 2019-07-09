import asyncio
import logging
import sys
import weakref

import asyncssh

from .spec import SpecCluster

logger = logging.getLogger(__name__)


class Process:
    """ A superclass for SSH Workers and Nannies

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.connection = None
        self.proc = None
        self.status = "created"

    def __await__(self):
        async def _():
            async with self.lock:
                if not self.connection:
                    await self.start()
                    assert self.connection
                    weakref.finalize(self, self.proc.terminate)
            return self

        return _().__await__()

    async def close(self):
        self.proc.terminate()
        self.connection.close()
        self.status = "closed"

    def __repr__(self):
        return "<SSH %s: status=%s>" % (type(self).__name__, self.status)

async def _create_connection(address, validate_host):
    if not validate_host:
        connection = await asyncssh.connect(address, known_hosts=None)
    else:
        try:
            connection = await asyncssh.connect(address)
        except asyncssh.misc.HostKeyNotVerifiable:
            # improve debug message when there is more support for host validation
            logger.debug("Explicitly validating host")
            raise asyncssh.misc.HostKeyNotVerifiable

    return connection


class Worker(Process):
    """ A Remote Dask Worker controled by SSH

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    address: str
        The hostname where we should run this worker
    kwargs:
        validate_host: bool
            Validate host key is trusted (default is False)
    """

    def __init__(self, scheduler: str, address: str, **kwargs):
        self.address = address
        self.scheduler = scheduler
        self.kwargs = kwargs
        self.validate_host = self.kwargs.get('validate_host', False)


        super().__init__()

    async def start(self):
        self.connection = await _create_connection(self.address, self.validate_host)
        self.proc = await self.connection.create_process(
            " ".join(
                [
                    sys.executable,
                    "-m",
                    "distributed.cli.dask_worker",
                    self.scheduler,
                    "--name",  # we need to have name for SpecCluster
                    str(self.kwargs["name"]),
                ]
            )
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if "worker at" in line:
                self.address = line.split("worker at:")[1].strip()
                self.status = "running"
                break
        logger.debug("%s", line)


class Scheduler(Process):
    """ A Remote Dask Scheduler controled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    kwargs:
        TODO
        validate_host: bool
            Validate host key is trusted (default is False)
    """

    def __init__(self, address: str, **kwargs):
        self.address = address
        self.kwargs = kwargs
        self.validate_host = self.kwargs.get('validate_host', False)

        super().__init__()

    async def start(self):
        logger.debug("Created Scheduler Connection")

        self.connection = await _create_connection(self.address, self.validate_host)

        self.proc = await self.connection.create_process(
            " ".join([sys.executable, "-m", "distributed.cli.dask_scheduler"])
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if "Scheduler at" in line:
                self.address = line.split("Scheduler at:")[1].strip()
                break
        logger.debug("%s", line)


def SSHCluster(hosts, **kwargs):
    """ Deploy a Dask cluster using SSH

    Parameters
    ----------
    hosts: List[str]
        List of hostnames or addresses on which to launch our cluster
        The first will be used for the scheduler and the rest for workers

    TODO
    ----
    This doesn't handle any keyword arguments yet.  It is a proof of concept
    """
    scheduler = {"cls": Scheduler, "options": {"address": hosts[0], "validate_host": False}}
    workers = {
        i: {"cls": Worker, "options": {"address": host, "validate_host": False}}
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, **kwargs)
