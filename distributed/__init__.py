from . import config  # isort:skip; load distributed configuration first
import dask
from dask.config import config
from dask.utils import import_required

from ._version import get_versions
from .actor import Actor, ActorFuture
from .client import (
    Client,
    CompatibleExecutor,
    Executor,
    Future,
    as_completed,
    default_client,
    fire_and_forget,
    futures_of,
    get_task_metadata,
    get_task_stream,
    performance_report,
    wait,
)
from .core import Status, connect, rpc
from .deploy import Adaptive, LocalCluster, SpecCluster, SSHCluster
from .diagnostics.plugin import PipInstall, SchedulerPlugin, WorkerPlugin
from .diagnostics.progressbar import progress
from .event import Event
from .lock import Lock
from .multi_lock import MultiLock
from .nanny import Nanny
from .pubsub import Pub, Sub
from .queues import Queue
from .scheduler import Scheduler
from .security import Security
from .semaphore import Semaphore
from .threadpoolexecutor import rejoin
from .utils import CancelledError, TimeoutError, sync
from .variable import Variable
from .worker import Reschedule, Worker, get_client, get_worker, secede
from .worker_client import local_client, worker_client

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions

if dask.config.get("distributed.admin.event-loop") in ("asyncio", "tornado"):
    pass
elif dask.config.get("distributed.admin.event-loop") == "uvloop":
    import_required(
        "uvloop",
        "The distributed.admin.event-loop configuration value "
        "is set to 'uvloop' but the uvloop module is not installed"
        "\n\n"
        "Please either change the config value or install one of the following\n"
        "    conda install uvloop\n"
        "    pip install uvloop",
    )
    import uvloop

    uvloop.install()
else:
    raise ValueError(
        "Expected distributed.admin.event-loop to be in ('asyncio', 'tornado', 'uvloop'), got %s"
        % dask.config.get("distributed.admin.event-loop")
    )
