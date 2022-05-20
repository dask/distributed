from __future__ import annotations

import logging
import os
import socket
import subprocess
import sys
import uuid
import zipfile
from collections.abc import Awaitable
from typing import TYPE_CHECKING

from dask.utils import funcname, tmpfile

if TYPE_CHECKING:
    from distributed.scheduler import Scheduler  # circular import

logger = logging.getLogger(__name__)


class SchedulerPlugin:
    """Interface to extend the Scheduler

    The scheduler operates by triggering and responding to events like
    ``task_finished``, ``update_graph``, ``task_erred``, etc..

    A plugin enables custom code to run at each of those same events.  The
    scheduler will run the analogous methods on this class when each event is
    triggered.  This runs user code within the scheduler thread that can
    perform arbitrary operations in synchrony with the scheduler itself.

    Plugins are often used for diagnostics and measurement, but have full
    access to the scheduler and could in principle affect core scheduling.

    To implement a plugin:

    1. subclass this class
    2. override some of its methods
    3. add the plugin to the scheduler with ``Scheduler.add_plugin(myplugin)``.

    Examples
    --------
    >>> class Counter(SchedulerPlugin):
    ...     def __init__(self):
    ...         self.counter = 0
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if start == 'processing' and finish == 'memory':
    ...             self.counter += 1
    ...
    ...     def restart(self, scheduler):
    ...         self.counter = 0

    >>> plugin = Counter()
    >>> scheduler.add_plugin(plugin)  # doctest: +SKIP
    """

    async def start(self, scheduler: Scheduler) -> None:
        """Run when the scheduler starts up

        This runs at the end of the Scheduler startup process
        """

    async def before_close(self) -> None:
        """Runs prior to any Scheduler shutdown logic"""

    async def close(self) -> None:
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully
        """

    def update_graph(
        self,
        scheduler: Scheduler,
        keys: set[str],
        restrictions: dict[str, float],
        **kwargs,
    ) -> None:
        """Run when a new graph / tasks enter the scheduler"""

    def restart(self, scheduler: Scheduler) -> None:
        """Run when the scheduler restarts itself"""

    def transition(self, key: str, start: str, finish: str, *args, **kwargs) -> None:
        """Run whenever a task changes state

        Parameters
        ----------
        key : string
        start : string
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish : string
            Final state of the transition.
        *args, **kwargs : More options passed when transitioning
            This may include worker ID, compute time, etc.
        """

    def add_worker(self, scheduler: Scheduler, worker: str) -> None | Awaitable[None]:
        """Run when a new worker enters the cluster"""

    def remove_worker(
        self, scheduler: Scheduler, worker: str
    ) -> None | Awaitable[None]:
        """Run when a worker leaves the cluster"""

    def add_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a new client connects"""

    def remove_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a client disconnects"""

    def log_event(self, name, msg) -> None:
        """Run when an event is logged"""


class WorkerPlugin:
    """Interface to extend the Worker

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle: at setup, during task state transitions, when a task or dependency
    is released, and at teardown.

    A plugin enables custom code to run at each of step of a Workers's life. Whenever such
    an event happens, the corresponding method on this class will be called. Note that the
    user code always runs within the Worker's main thread.

    To implement a plugin implement some of the methods of this class and register
    the plugin to your client in order to have it attached to every existing and
    future workers with ``Client.register_worker_plugin``.

    Examples
    --------
    >>> class ErrorLogger(WorkerPlugin):
    ...     def __init__(self, logger):
    ...         self.logger = logger
    ...
    ...     def setup(self, worker):
    ...         self.worker = worker
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if finish == 'error':
    ...             ts = self.worker.tasks[key]
    ...             exc_info = (type(ts.exception), ts.exception, ts.traceback)
    ...             self.logger.error(
    ...                 "Error during computation of '%s'.", key,
    ...                 exc_info=exc_info
    ...             )

    >>> import logging
    >>> plugin = ErrorLogger(logging)
    >>> client.register_worker_plugin(plugin)  # doctest: +SKIP
    """

    def setup(self, worker):
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """

    def teardown(self, worker):
        """Run when the worker to which the plugin is attached to is closed"""

    def transition(self, key, start, finish, **kwargs):
        """
        Throughout the lifecycle of a task (see :doc:`Worker <worker>`), Workers are
        instructed by the scheduler to compute certain tasks, resulting in transitions
        in the state of each task. The Worker owning the task is then notified of this
        state transition.

        Whenever a task changes its state, this method will be called.

        Parameters
        ----------
        key : string
        start : string
            Start state of the transition.
            One of waiting, ready, executing, long-running, memory, error.
        finish : string
            Final state of the transition.
        kwargs : More options passed when transitioning
        """


class NannyPlugin:
    """Interface to extend the Nanny

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle. A nanny plugin does the same thing, but benefits from being able
    to run code before the worker is started, or to restart the worker if
    necessary.

    To implement a plugin implement some of the methods of this class and register
    the plugin to your client in order to have it attached to every existing and
    future nanny by passing ``nanny=True`` to
    :meth:`Client.register_worker_plugin<distributed.Client.register_worker_plugin>`.

    The ``restart`` attribute is used to control whether or not a running ``Worker``
    needs to be restarted when registering the plugin.

    See Also
    --------
    WorkerPlugin
    SchedulerPlugin
    """

    restart = False

    def setup(self, nanny):
        """
        Run when the plugin is attached to a nanny. This happens when the plugin is registered
        and attached to existing nannies, or when a nanny is created after the plugin has been
        registered.
        """

    def teardown(self, nanny):
        """Run when the nanny to which the plugin is attached to is closed"""


def _get_plugin_name(plugin) -> str:
    """Return plugin name.

    If plugin has no name attribute a random name is used.

    """
    if hasattr(plugin, "name"):
        return plugin.name
    else:
        return funcname(type(plugin)) + "-" + str(uuid.uuid4())


class PipInstall(WorkerPlugin):
    """A Worker Plugin to pip install a set of packages

    This accepts a set of packages to install on all workers.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

       Additional issues may arise if multiple workers share the same
       file system. Each worker might try to install the packages
       simultaneously.

    Parameters
    ----------
    packages : List[str]
        A list of strings to place after "pip install" command
    pip_options : List[str]
        Additional options to pass to pip.
    restart : bool, default False
        Whether or not to restart the worker after pip installing
        Only functions if the worker has an attached nanny process

    Examples
    --------
    >>> from dask.distributed import PipInstall
    >>> plugin = PipInstall(packages=["scikit-learn"], pip_options=["--upgrade"])

    >>> client.register_worker_plugin(plugin)
    """

    name = "pip"

    def __init__(self, packages, pip_options=None, restart=False):
        self.packages = packages
        self.restart = restart
        if pip_options is None:
            pip_options = []
        self.pip_options = pip_options

    async def setup(self, worker):
        from distributed.lock import Lock

        async with Lock(socket.gethostname()):  # don't clobber one installation
            logger.info("Pip installing the following packages: %s", self.packages)
            proc = subprocess.Popen(
                [sys.executable, "-m", "pip", "install"]
                + self.pip_options
                + self.packages,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = proc.communicate()
            returncode = proc.wait()

            if returncode:
                logger.error("Pip install failed with '%s'", stderr.decode().strip())
                return

            if self.restart and worker.nanny:
                lines = stdout.strip().split(b"\n")
                if not all(
                    line.startswith(b"Requirement already satisfied") for line in lines
                ):
                    worker.loop.add_callback(
                        worker.close_gracefully, restart=True
                    )  # restart


# Adapted from https://github.com/dask/distributed/issues/3560#issuecomment-596138522
class UploadFile(WorkerPlugin):
    """A WorkerPlugin to upload a local file to workers.

    Parameters
    ----------
    filepath: str
        A path to the file (.py, egg, or zip) to upload

    Examples
    --------
    >>> from distributed.diagnostics.plugin import UploadFile

    >>> client.register_worker_plugin(UploadFile("/path/to/file.py"))  # doctest: +SKIP
    """

    name = "upload_file"

    def __init__(self, filepath):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        self.filename = os.path.basename(filepath)
        with open(filepath, "rb") as f:
            self.data = f.read()

    async def setup(self, worker):
        response = await worker.upload_file(
            comm=None, filename=self.filename, data=self.data, load=True
        )
        assert len(self.data) == response["nbytes"]


class Environ(NannyPlugin):
    restart = True

    def __init__(self, environ={}):
        self.environ = {k: str(v) for k, v in environ.items()}

    async def setup(self, nanny):
        nanny.env.update(self.environ)


class UploadDirectory(NannyPlugin):
    """A NannyPlugin to upload a local file to workers.

    Parameters
    ----------
    path: str
        A path to the directory to upload

    Examples
    --------
    >>> from distributed.diagnostics.plugin import UploadDirectory
    >>> client.register_worker_plugin(UploadDirectory("/path/to/directory"), nanny=True)  # doctest: +SKIP
    """

    def __init__(
        self,
        path,
        restart=False,
        update_path=False,
        skip_words=(".git", ".github", ".pytest_cache", "tests", "docs"),
        skip=(lambda fn: os.path.splitext(fn)[1] == ".pyc",),
    ):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        path = os.path.expanduser(path)
        self.path = os.path.split(path)[-1]
        self.restart = restart
        self.update_path = update_path

        self.name = "upload-directory-" + os.path.split(path)[-1]

        with tmpfile(extension="zip") as fn:
            with zipfile.ZipFile(fn, "w", zipfile.ZIP_DEFLATED) as z:
                for root, dirs, files in os.walk(path):
                    for file in files:
                        filename = os.path.join(root, file)
                        if any(predicate(filename) for predicate in skip):
                            continue
                        dirs = filename.split(os.sep)
                        if any(word in dirs for word in skip_words):
                            continue

                        archive_name = os.path.relpath(
                            os.path.join(root, file), os.path.join(path, "..")
                        )
                        z.write(filename, archive_name)

            with open(fn, "rb") as f:
                self.data = f.read()

    async def setup(self, nanny):
        fn = os.path.join(nanny.local_directory, f"tmp-{uuid.uuid4()}.zip")
        with open(fn, "wb") as f:
            f.write(self.data)

        import zipfile

        with zipfile.ZipFile(fn) as z:
            z.extractall(path=nanny.local_directory)

        if self.update_path:
            path = os.path.join(nanny.local_directory, self.path)
            if path not in sys.path:
                sys.path.insert(0, path)

        os.remove(fn)
