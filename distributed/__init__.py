from . import config  # isort:skip; load distributed configuration first
from . import widgets  # isort:skip; load distributed widgets second


import dask
from dask.config import config  # type: ignore

from ._version import get_versions
from .actor import Actor, ActorFuture, BaseActorFuture
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
from .diagnostics.plugin import (
    Environ,
    NannyPlugin,
    PipInstall,
    SchedulerPlugin,
    UploadDirectory,
    UploadFile,
    WorkerPlugin,
)
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
from .worker import Reschedule, Worker, get_client, get_worker, print, secede, warn
from .worker_client import local_client, worker_client


def __getattr__(name):
    global __version__, __git_revision__

    if name == "__version__":
        from importlib.metadata import version

        __version__ = version("distributed")
        return __version__

    if name == "__git_revision__":
        from ._version import get_versions

        __git_revision__ = get_versions()["full-revisionid"]
        return __git_revision__

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
