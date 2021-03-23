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
    local_packages : str, List[str]
        Paths to local pip-installable files (``.tar.gz``, ``.whl``, etc.).

        When uploading a wheel, note that pip will not re-install it if that
        module name and version number is already installed (even if the contents are
        different). Therefore, when iterating on a module under local development,
        or replacing a module already installed in the environment, uploading the
        source distribution (``.tar.gz``) is generally preferred.
    pip_options : List[str]
        Additional options to pass to pip.
    restart : bool, default False
        Whether or not to restart the worker after pip installing
        Only functions if the worker has an attached nanny process
        and no ``local_packages`` were given.
    log_output : bool, int, default False
        Whether to log all pip output. Pass the
        `log level <https://docs.python.org/3/library/logging.html#logging-levels>`_
        to use, or for convenience, set ``log_output=True`` to log at ``INFO`` level.
        If False (default), pip output won't be logged.

    Examples
    --------
    >>> from dask.distributed import PipInstall
    >>> plugin = PipInstall(packages=["scikit-learn"], pip_options=["--upgrade"])
    >>> client.register_worker_plugin(plugin)

    >>> local_module_plugin = PipInstall(local_packages="dist/mypackage-0.1.0.tar.gz")
    >>> client.register_worker_plugin(local_module_plugin)
    """

    name = "pip"

    def __init__(
        self,
        packages=None,
        local_packages=None,
        pip_options=None,
        restart=False,
        log_output=False,
    ):
        self.packages = packages if packages is not None else []
        self.restart = restart
        self.log_output = logging.INFO if log_output is True else log_output
        if pip_options is None:
            pip_options = []
        self.pip_options = pip_options

        self.local_packages = {}
        if isinstance(local_packages, str):
            local_packages = [local_packages]
        for path in local_packages:
            if os.path.isdir(path):
                raise ValueError(
                    f"{path} is a directory; you must instead give a single file that `pip install` works on. "
                    "Consider building a distribution archive of your module: "
                    "https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives. "
                    "Then pass the path to the `.tar.gz` or `.whl` file here."
                )
            with open(path, "rb") as f:
                self.local_packages[os.path.basename(path)] = f.read()

    async def setup(self, worker):
        from ..lock import Lock

        async with Lock(socket.gethostname()):  # don't clobber one installation
            local_packages = []
            for filename, data in self.local_packages.items():
                response = await worker.upload_file(
                    comm=None, filename=filename, data=data, load=False
                )
                assert response == {"status": "OK", "nbytes": len(data)}

                out_filename = os.path.join(worker.local_directory, filename)
                local_packages.append(out_filename)

            logger.info(
                "Pip installing the following packages: %s",
                self.packages + list(self.local_packages),
            )
            proc = subprocess.Popen(
                [sys.executable, "-m", "pip", "install"]
                + self.pip_options
                + self.packages
                + local_packages,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = proc.communicate()
            returncode = proc.wait()

            if self.log_output:
                # This may be ugly, but don't want to make assumtions
                # about the log formatter and log line-by-line.
                logger.log(self.log_output, "Pip stderr:\n" + stderr.decode().strip())
                logger.log(self.log_output, "Pip stdout:\n" + stdout.decode().strip())

            if returncode:
                logger.error("Pip install failed with '%s'", stderr.decode().strip())
                return

            if self.restart and worker.nanny and not local_packages:
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
