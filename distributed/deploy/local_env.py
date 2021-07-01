import asyncio
import logging
import weakref
from typing import List, Union

import dask
import dask.config

from ..core import Status
from ..scheduler import Scheduler as _Scheduler
from ..utils import cli_keywords
from ..worker import Worker as _Worker
from .spec import ProcessInterface, SpecCluster

logger = logging.getLogger(__name__)


class Process(ProcessInterface):
    """A superclass for SSH Workers and Nannies

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self, **kwargs):
        self.proc = None
        super().__init__(**kwargs)

    async def start(self):
        weakref.finalize(self, self.proc.kill)
        await super().start()

    async def close(self):
        self.proc.kill()
        await super().close()

    def __repr__(self):
        return "<SSH %s: status=%s>" % (type(self).__name__, self.status)


class Worker(Process):
    """A Remote Dask Worker run by a specified Python executable

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    python_exe: str
        Full path to Python executable to run this worker
    connect_options: dict
        kwargs to be passed to asyncio subprocess connections
    kwargs: dict
        These will be passed through the dask-worker CLI to the
        dask.distributed.Worker class
    worker_module: str
        The python module to run to start the worker
    name: str
        Optionally specify a name for this worker
    """

    def __init__(
        self,
        scheduler: str,
        python_exe: str,
        connect_options: dict,
        kwargs: dict,
        worker_module="distributed.cli.dask_worker",
        name=None,
    ):
        super().__init__()

        self.scheduler = scheduler
        self.python_exe = python_exe
        self.connect_options = connect_options
        self.kwargs = kwargs
        self.worker_module = worker_module
        self.name = name

    async def start(self):
        set_env = await _set_env_helper(self.connect_options)

        cmd = " ".join(
            [
                set_env,
                self.python_exe,
                "-m",
                self.worker_module,
                self.scheduler,
                "--name",
                str(self.name),
            ]
            + cli_keywords(self.kwargs, cls=_Worker, cmd=self.worker_module)
        )
        self.proc = await asyncio.create_subprocess_shell(
            cmd, stderr=asyncio.subprocess.PIPE, **self.connect_options
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if not line.decode("ascii").strip():
                raise Exception("Worker failed to start")
            else:
                line = line.decode("ascii").strip()
            logger.info(line)
            if "worker at" in line:
                self.address = line.split("worker at:")[1].strip()
                self.status = Status.running
                break
        logger.debug("%s", line)
        await super().start()


class Scheduler(Process):
    """A Remote Dask Scheduler run by a specified Python executable

    Parameters
    ----------
    python_exe: str
        Full path to Python executable to run this scheduler
    connect_options: dict
        kwargs to be passed to asyncio subprocess connections
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(self, python_exe: str, connect_options: dict, kwargs: dict):
        super().__init__()

        self.python_exe = python_exe
        self.kwargs = kwargs
        self.connect_options = connect_options

    async def start(self):
        logger.debug("Created Scheduler")

        set_env = await _set_env_helper(self.connect_options)

        cmd = " ".join(
            [set_env, self.python_exe, "-m", "distributed.cli.dask_scheduler"]
            + cli_keywords(self.kwargs, cls=_Scheduler)
        )
        self.proc = await asyncio.create_subprocess_shell(
            cmd, stderr=asyncio.subprocess.PIPE, **self.connect_options
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if not line.decode("ascii").strip():
                raise Exception("Scheduler failed to start")
            else:
                line = line.decode("ascii").strip()
            logger.info(line)
            if "Scheduler at" in line:
                self.address = line.split("Scheduler at:")[1].strip()
                break
        logger.debug("%s", line)
        await super().start()


async def _set_env_helper(connect_options: dict):
    """Helper function to locate existing dask internal config for the remote
    scheduler and workers to inherit when started.

    Parameters
    ----------
    connect_options : dict
        Connection options to pass to the async subprocess shell.

    Returns
    -------
        Dask config to inherit, if any.
    """
    proc = await asyncio.create_subprocess_shell("uname", **connect_options)
    await proc.communicate()
    if proc.returncode == 0:
        set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
            dask.config.serialize(dask.config.global_config)
        )
    else:
        proc = await asyncio.create_subprocess_shell("cmd /c ver", **connect_options)
        await proc.communicate()
        if proc.returncode == 0:
            set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                dask.config.serialize(dask.config.global_config)
            )
        else:
            raise Exception(
                "Scheduler failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
            )
    return set_env


def LocalEnvCluster(
    python_exe: str,
    connect_options: Union[List[dict], dict] = {},
    worker_options: dict = {},
    scheduler_options: dict = {},
    worker_module: str = "distributed.cli.dask_worker",
    **kwargs,
):
    """Deploy a Dask cluster that utilises a different Python executable

    The LocalEnvCluster function deploys a Dask Scheduler and Workers for you on
    your local machine, running in the Python environment specified by
    `python_exe`. This allows you to run a Dask cluster in a different Python
    environment to the host Python environment; particularly useful when
    combined with `dask-labextension` as you can run a Dask Scheduler and
    Workers in a different Python env to the env hosting JupyterLab.

    You may configure the scheduler and workers by passing
    ``scheduler_options`` and ``worker_options`` dictionary keywords.  See the
    ``dask.distributed.Scheduler`` and ``dask.distributed.Worker`` classes for
    details on the available options, but the defaults should work in most
    situations.

    You may configure how to connect to the local Scheduler and Workers using
    the ``connect_options`` keyword, which passes values to the
    ``asyncio.create_subprocess_shell`` function.  For more information on this
    see the documentation on the
    [asyncio library](https://docs.python.org/3/library/asyncio-subprocess.html#asyncio-subprocess).

    Parameters
    ----------
    python_exe : str
        Full path to another Python executable, for example from another
        conda env or Python venv.
    connect_options : dict or list of dict, optional
        Keywords to pass through to :func:`asyncssh.connect`.
        This could include things such as ``port``, ``username``, ``password``
        or ``known_hosts``. See docs for :func:`asyncssh.connect` and
        :class:`asyncssh.SSHClientConnectionOptions` for full information.
        If a list it must have the same length as ``hosts``.
    worker_options : dict, optional
        Keywords to pass on to workers.
    scheduler_options : dict, optional
        Keywords to pass on to scheduler.
    worker_module : str, optional
        Python module to call to start the worker.

    Example
    -------
    >>> from dask.distributed import Client, LocalEnvCluster
    >>> cluster = LocalEnvCluster(
    ...     "/Users/user/miniconda3/envs/myenv/bin/python",
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 1},
    ... )
    >>> client = Client(cluster)

    See Also
    --------
    dask.distributed.Scheduler
    dask.distributed.Worker
    asyncio.create_subprocess_shell
    """
    scheduler = {
        "cls": Scheduler,
        "options": {
            "python_exe": python_exe,
            "connect_options": connect_options,
            "kwargs": scheduler_options,
        },
    }
    workers = {
        0: {
            "cls": Worker,
            "options": {
                "python_exe": python_exe,
                "connect_options": connect_options,
                "kwargs": worker_options,
                "worker_module": worker_module,
            },
        }
    }
    return SpecCluster(workers, scheduler, name="LocalEnvCluster", **kwargs)
