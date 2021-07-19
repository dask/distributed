import asyncio
import logging
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
    """A superclass for Workers and Nannies run by a specified Python executable

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self, **kwargs):
        self.proc = None
        super().__init__(**kwargs)

    async def start(self):
        await super().start()

    async def close(self):
        self.proc.kill()
        await super().close()

    def __repr__(self):
        return "<LocalEnv %s: status=%s>" % (type(self).__name__, self.status)

    async def _set_env_helper(self):
        """Helper function to locate existing dask internal config for the remote
        Scheduler and Workers to inherit when started.

        Returns
        -------
            Dask config to inherit, if any.
        """
        proc = await asyncio.create_subprocess_shell("uname", **self.connect_options)
        await proc.communicate()
        if proc.returncode == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                dask.config.serialize(dask.config.global_config)
            )
        else:
            proc = await asyncio.create_subprocess_shell(
                "cmd /c ver", **self.connect_options
            )
            await proc.communicate()
            if proc.returncode == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    dask.config.serialize(dask.config.global_config)
                )
            else:
                name = self.__class__.__name__
                emsg = f"{name} failed to set DASK_INTERNAL_INHERIT_CONFIG variable"
                raise Exception(emsg)
        return set_env

    async def _get_address(self, search_str):
        # We watch stderr in order to get the address, then we return
        name = self.__class__.__name__
        while True:
            line = await self.proc.stderr.readline()
            if not line.decode("ascii").strip():
                raise Exception(f"{name} failed to start")
            else:
                line = line.decode("ascii").strip()
            logger.info(line)
            if search_str in line:
                self.address = line.split(f"{search_str}:")[1].strip()
                if name == "Worker":
                    self.status = Status.running
                break
        logger.debug("%s", line)


class Worker(Process):
    """A Remote Dask Worker run by a specified Python executable

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    python_executable: str
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
        python_executable: str,
        connect_options: dict,
        kwargs: dict,
        worker_module="distributed.cli.dask_worker",
        name=None,
    ):
        super().__init__()

        self.scheduler = scheduler
        self.python_executable = python_executable
        self.connect_options = connect_options
        self.kwargs = kwargs
        self.worker_module = worker_module
        self.name = name

    async def start(self):
        set_env = await self._set_env_helper()

        cmd = " ".join(
            [
                set_env,
                self.python_executable,
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

        search_string = "worker at"
        await self._get_address(search_string)
        await super().start()


class Scheduler(Process):
    """A Remote Dask Scheduler run by a specified Python executable

    Parameters
    ----------
    python_executable: str
        Full path to Python executable to run this scheduler
    connect_options: dict
        kwargs to be passed to asyncio subprocess connections
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(self, python_executable: str, connect_options: dict, kwargs: dict):
        super().__init__()

        self.python_executable = python_executable
        self.kwargs = kwargs
        self.connect_options = connect_options

    async def start(self):
        logger.debug("Created Scheduler")

        set_env = await self._set_env_helper()

        cmd = " ".join(
            [set_env, self.python_executable, "-m", "distributed.cli.dask_scheduler"]
            + cli_keywords(self.kwargs, cls=_Scheduler)
        )
        self.proc = await asyncio.create_subprocess_shell(
            cmd, stderr=asyncio.subprocess.PIPE, **self.connect_options
        )

        search_string = "Scheduler at"
        await self._get_address(search_string)
        await super().start()


def LocalEnvCluster(
    python_executable: str,
    n_workers: int = 1,
    connect_options: Union[List[dict], dict] = {},
    worker_options: dict = {},
    scheduler_options: dict = {},
    worker_module: str = "distributed.cli.dask_worker",
    **kwargs,
):
    """Deploy a Dask cluster that utilises a different Python executable

    The LocalEnvCluster function deploys a Dask Scheduler and Workers for you on
    your local machine, running in the Python environment specified by
    `python_executable`. This allows you to run a Dask cluster in a different
    Python environment to the host Python environment; particularly useful when
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
    python_executable : str
        Full path to a Python executable, for example from another
        conda env or Python venv.
    n_workers : int
        Number of workers to start
    connect_options : dict, optional
        Keywords to pass through to :func:`asyncio.create_subprocess_shell`.
        See docs for :func:`asyncio.create_subprocess_shell` for full information.
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
            "python_executable": python_executable,
            "connect_options": connect_options,
            "kwargs": scheduler_options,
        },
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {
                "python_executable": python_executable,
                "connect_options": connect_options,
                "kwargs": worker_options,
                "worker_module": worker_module,
            },
        }
        for i in range(n_workers)
    }
    return SpecCluster(workers, scheduler, name="LocalEnvCluster", **kwargs)
