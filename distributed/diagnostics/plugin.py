import logging
import os
import socket
import subprocess
import sys

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

    To implement a plugin implement some of the methods of this class and add
    the plugin to the scheduler with ``Scheduler.add_plugin(myplugin)``.

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

    async def start(self, scheduler):
        """Run when the scheduler starts up

        This runs at the end of the Scheduler startup process
        """
        pass

    async def close(self):
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully
        """
        pass

    def update_graph(self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs):
        """ Run when a new graph / tasks enter the scheduler """

    def restart(self, scheduler, **kwargs):
        """ Run when the scheduler restarts itself """

    def transition(self, key, start, finish, *args, **kwargs):
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

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a new worker enters the cluster """

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a worker leaves the cluster """

    def add_client(self, scheduler=None, client=None, **kwargs):
        """ Run when a new client connects """

    def remove_client(self, scheduler=None, client=None, **kwargs):
        """ Run when a client disconnects """


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
    ...             exc = self.worker.exceptions[key]
    ...             self.logger.error("Task '%s' has failed with exception: %s" % (key, str(exc)))

    >>> plugin = ErrorLogger()
    >>> client.register_worker_plugin(plugin)  # doctest: +SKIP
    """

    def setup(self, worker):
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """

    def teardown(self, worker):
        """ Run when the worker to which the plugin is attached to is closed """

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

    def release_key(self, key, state, cause, reason, report):
        """
        Called when the worker releases a task.

        Parameters
        ----------
        key : string
        state : string
            State of the released task.
            One of waiting, ready, executing, long-running, memory, error.
        cause : string or None
            Additional information on what triggered the release of the task.
        reason : None
            Not used.
        report : bool
            Whether the worker should report the released task to the scheduler.
        """

    def release_dep(self, dep, state, report):
        """
        Called when the worker releases a dependency.

        Parameters
        ----------
        dep : string
        state : string
            State of the released dependency.
            One of waiting, flight, memory.
        report : bool
            Whether the worker should report the released dependency to the scheduler.
        """


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
        from ..lock import Lock

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
