from __future__ import print_function, division, absolute_import

from . import config
from dask.config import config
from .actor import Actor, ActorFuture
from .core import connect, rpc
from .deploy import LocalCluster, Adaptive, SpecCluster
from .diagnostics import progress
from .client import (
    Client,
    Executor,
    CompatibleExecutor,
    wait,
    as_completed,
    default_client,
    fire_and_forget,
    Future,
    futures_of,
    get_task_stream,
)
from .lock import Lock
from .nanny import Nanny
from .pubsub import Pub, Sub
from .queues import Queue
from .scheduler import Scheduler
from .threadpoolexecutor import rejoin
from .utils import sync
from .variable import Variable
from .worker import Worker, get_worker, get_client, secede, Reschedule
from .worker_client import local_client, worker_client

from tornado.gen import TimeoutError

from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions

import sys, threading
import time
from .profile import call_stack


def f(idx):
    while True:
        time.sleep(1)
        frame = sys._current_frames()[idx]
        print("-" * 100)
        print("".join(call_stack(frame)))


thread = threading.Thread(target=f, args=(threading.get_ident(),), daemon=True)
# thread.start()
