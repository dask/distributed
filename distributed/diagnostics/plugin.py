from __future__ import annotations

import abc
import asyncio
import contextlib
import functools
import logging
import os
import socket
import subprocess
import sys
import uuid
import zipfile
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, ClassVar

from dask.typing import Key
from dask.utils import funcname, tmpfile

from distributed.protocol.pickle import dumps

if TYPE_CHECKING:
    # circular imports
    from distributed.scheduler import Scheduler
    from distributed.scheduler import TaskStateState as SchedulerTaskStateState
    from distributed.worker import Worker
    from distributed.worker_state_machine import TaskStateState as WorkerTaskStateState

logger = logging.getLogger(__name__)


class SchedulerPlugin:
    """Interface to extend the Scheduler

    A plugin enables custom hooks to run when specific events occur.  The    scheduler will run the methods of this plugin whenever the corresponding
    method of the scheduler is run.  This runs user code within the scheduler
    thread that can perform arbitrary operations in synchrony with the scheduler
    itself.

    Plugins are often used for diagnostics and measurement, but have full
    access to the scheduler and could in principle affect core scheduling.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

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
        *,
        client: str,
        keys: set[Key],
        tasks: list[Key],
        annotations: dict[str, dict[Key, Any]],
        priority: dict[Key, tuple[int | float, ...]],
        dependencies: dict[Key, set[Key]],
        **kwargs: Any,
    ) -> None:
        """Run when a new graph / tasks enter the scheduler

        Parameters
        ----------
            scheduler:
                The `Scheduler` instance.
            client:
                The unique Client id.
            keys:
                The keys the Client is interested in when calling `update_graph`.
            tasks:
                The
            annotations:
                Fully resolved annotations as applied to the tasks in the format::

                    {
                        "annotation": {
                            "key": "value,
                            ...
                        },
                        ...
                    }
            priority:
                Task calculated priorities as assigned to the tasks.
            dependencies:
                A mapping that maps a key to its dependencies.
            **kwargs:
                It is recommended to allow plugins to accept more parameters to
                ensure future compatibility.
        """

    def restart(self, scheduler: Scheduler) -> None:
        """Run when the scheduler restarts itself"""

    def transition(
        self,
        key: Key,
        start: SchedulerTaskStateState,
        finish: SchedulerTaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        """Run whenever a task changes state

        For a description of the transition mechanism and the available states,
        see :ref:`Scheduler task states <scheduler-task-state>`.

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish :
            Final state of the transition.
        stimulus_id :
            ID of stimulus causing the transition.
        *args, **kwargs :
            More options passed when transitioning
            This may include worker ID, compute time, etc.
        """

    def add_worker(self, scheduler: Scheduler, worker: str) -> None | Awaitable[None]:
        """Run when a new worker enters the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.add_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.add_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.add_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None | Awaitable[None]:
        """Run when a worker leaves the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.remove_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.remove_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.remove_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """

    def add_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a new client connects"""

    def remove_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a client disconnects"""

    def log_event(self, topic: str, msg: Any) -> None:
        """Run when an event is logged"""


class WorkerPlugin:
    """Interface to extend the Worker

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle.

    A plugin enables custom code to run at each of step of a Workers's life. Whenever such
    an event happens, the corresponding method on this class will be called. Note that the
    user code always runs within the Worker's main thread.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

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
    >>> client.register_plugin(plugin)  # doctest: +SKIP
    """

    def setup(self, worker: Worker) -> None | Awaitable[None]:
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """

    def teardown(self, worker: Worker) -> None | Awaitable[None]:
        """Run when the worker to which the plugin is attached is closed, or
        when the plugin is removed."""

    def transition(
        self,
        key: Key,
        start: WorkerTaskStateState,
        finish: WorkerTaskStateState,
        **kwargs: Any,
    ) -> None:
        """
        Throughout the lifecycle of a task (see :doc:`Worker State
        <worker-state>`), Workers are instructed by the scheduler to compute
        certain tasks, resulting in transitions in the state of each task. The
        Worker owning the task is then notified of this state transition.

        Whenever a task changes its state, this method will be called.

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of waiting, ready, executing, long-running, memory, error.
        finish :
            Final state of the transition.
        kwargs :
            More options passed when transitioning
        """


class NannyPlugin:
    """Interface to extend the Nanny

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle. A nanny plugin does the same thing, but benefits from being able
    to run code before the worker is started, or to restart the worker if
    necessary.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

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


def _get_plugin_name(plugin: SchedulerPlugin | WorkerPlugin | NannyPlugin) -> str:
    """Return plugin name.

    If plugin has no name attribute a random name is used.

    """
    if hasattr(plugin, "name"):
        return plugin.name
    else:
        return funcname(type(plugin)) + "-" + str(uuid.uuid4())


class SchedulerUploadFile(SchedulerPlugin):
    name = "upload_file"

    def __init__(self, filepath: str, load: bool = True):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        self.filename = os.path.basename(filepath)
        self.load = load
        with open(filepath, "rb") as f:
            self.data = f.read()

    async def start(self, scheduler: Scheduler) -> None:
        await scheduler.upload_file(self.filename, self.data, load=self.load)


class PackageInstall(SchedulerPlugin, abc.ABC):
    """Abstract parent class for a scheduler plugin to install a set of packages

    This accepts a set of packages to install on the scheduler and all workers.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    packages
        A list of packages (with optional versions) to install
    restart_workers
        Whether or not to restart the worker after installing the packages
        Only functions if the worker has an attached nanny process

    See Also
    --------
    CondaInstall
    PipInstall
    """

    _lock: ClassVar[asyncio.Lock | None] = None
    WORKER_PLUGIN_CLASS: ClassVar[type]

    installer: _PackageInstaller
    name: str
    restart_workers: bool
    _scheduler: Scheduler

    def __init__(
        self,
        installer: _PackageInstaller,
        restart_workers: bool,
    ):
        self.installer = installer
        self.restart_workers = restart_workers
        self.name = f"{self.installer.INSTALLER}-install-{uuid.uuid4()}"

    async def start(self, scheduler: Scheduler) -> None:
        self._scheduler = scheduler

        if PackageInstall._lock is None:
            PackageInstall._lock = asyncio.Lock()

        async with PackageInstall._lock:
            if not self._is_installed(scheduler):
                logger.info(
                    "%s installing the following packages on the scheduler: %s",
                    self.installer.INSTALLER,
                    self.installer.packages,
                )
                self.installer.install()
                self._set_installed(scheduler)

                worker_plugin = _PackageInstallWorker(
                    self.installer, self.restart_workers, self.name
                )
                await scheduler.register_worker_plugin(
                    comm=None, plugin=dumps(worker_plugin), name=self.name
                )
            else:
                logger.info(
                    "The following packages have already been installed on the scheduler: %s",
                    self.installer.packages,
                )

    async def close(self) -> None:
        assert PackageInstall._lock is not None

        async with PackageInstall._lock:
            await self._scheduler.unregister_worker_plugin(comm=None, name=self.name)

    def _is_installed(self, scheduler: Scheduler) -> bool:
        return scheduler.get_metadata(self._compose_installed_key(), default=False)

    def _set_installed(self, scheduler: Scheduler) -> None:
        scheduler.set_metadata(
            self._compose_installed_key(),
            True,
        )

    def _compose_installed_key(self) -> list[Key]:
        return [
            self.name,
            "installed",
            socket.gethostname(),
        ]


class _PackageInstallWorker(WorkerPlugin):
    """Worker plugin to install a set of packages

    This accepts an installer which will install a set of packages on all workers.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    installer
        Installer that installs a set of packages
    restart
        Whether or not to restart the worker after installing the packages
        Only functions if the worker has an attached nanny process
    name:
        Name of the plugin

    See Also
    --------
    PackageInstall
    """

    installer: _PackageInstaller
    name: str
    restart: bool

    def __init__(self, installer: _PackageInstaller, restart: bool, name: str):
        self.installer = installer
        self.restart = restart
        self.name = name

    async def setup(self, worker):
        from distributed.semaphore import Semaphore

        async with (
            await Semaphore(
                max_leases=1,
                name=socket.gethostname(),
                register=True,
                scheduler_rpc=worker.scheduler,
                loop=worker.loop,
            )
        ):
            if not await self._is_installed(worker):
                logger.info(
                    "%s installing the following packages: %s",
                    self.installer.INSTALLER,
                    self.installer.packages,
                )
                await self._set_installed(worker)
                self.installer.install()
            else:
                logger.info(
                    "The following packages have already been installed: %s",
                    self.installer.packages,
                )

            if self.restart and worker.nanny and not await self._is_restarted(worker):
                logger.info("Restarting worker to refresh interpreter.")
                await self._set_restarted(worker)
                worker.loop.add_callback(
                    worker.close_gracefully, restart=True, reason=f"{self.name}-setup"
                )

    async def _is_installed(self, worker):
        return await worker.client.get_metadata(
            self._compose_installed_key(), default=False
        )

    async def _set_installed(self, worker):
        await worker.client.set_metadata(
            self._compose_installed_key(),
            True,
        )

    def _compose_installed_key(self):
        return [
            self.name,
            "installed",
            socket.gethostname(),
        ]

    async def _is_restarted(self, worker):
        return await worker.client.get_metadata(
            self._compose_restarted_key(worker),
            default=False,
        )

    async def _set_restarted(self, worker):
        await worker.client.set_metadata(
            self._compose_restarted_key(worker),
            True,
        )

    def _compose_restarted_key(self, worker):
        return [self.name, "restarted", worker.nanny]


class _PackageInstaller(abc.ABC):
    INSTALLER: ClassVar[str]
    packages: list[str]

    def __init__(self, packages: list[str]):
        self.packages = packages

    @abc.abstractmethod
    def install(self) -> None:
        """Install the requested packages"""


class CondaInstall(PackageInstall):
    """A Worker Plugin to conda install a set of packages

    This accepts a set of packages to install on all workers as well as
    options to use when installing.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    packages
        A list of packages (with optional versions) to install using conda
    conda_options
        Additional options to pass to conda
    restart_workers
        Whether or not to restart the worker after installing the packages
        Only functions if the worker has an attached nanny process

    Examples
    --------
    >>> from dask.distributed import CondaInstall
    >>> plugin = CondaInstall(packages=["scikit-learn"], conda_options=["--update-deps"])

    >>> client.register_plugin(plugin)

    See Also
    --------
    PackageInstall
    PipInstall
    """

    def __init__(
        self,
        packages: list[str],
        conda_options: list[str] | None = None,
        restart_workers: bool = False,
    ):
        installer = _CondaInstaller(packages, conda_options)
        super().__init__(installer, restart_workers=restart_workers)


class _CondaInstaller(_PackageInstaller):
    INSTALLER = "conda"

    conda_options: list[str]

    def __init__(self, packages: list[str], conda_options: list[str] | None = None):
        super().__init__(packages)
        self.conda_options = conda_options or []

    def install(self) -> None:
        try:
            from conda.cli.python_api import Commands, run_command
        except ModuleNotFoundError as e:  # pragma: nocover
            msg = (
                "conda install failed because conda could not be found. "
                "Please make sure that conda is installed."
            )
            logger.error(msg)
            raise RuntimeError(msg) from e
        try:
            _, stderr, returncode = run_command(
                Commands.INSTALL, self.conda_options + self.packages
            )
        except Exception as e:
            msg = "conda install failed"
            logger.error(msg)
            raise RuntimeError(msg) from e

        if returncode != 0:
            msg = f"conda install failed with '{stderr.decode().strip()}'"
            logger.error(msg)
            raise RuntimeError(msg)


class PipInstall(PackageInstall):
    """A Worker Plugin to pip install a set of packages

    This accepts a set of packages to install on all workers as well as
    options to use when installing.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    packages
        A list of packages (with optional versions) to install using pip
    pip_options
        Additional options to pass to pip
    restart_workers
        Whether or not to restart the worker after installing the packages
        Only functions if the worker has an attached nanny process

    Examples
    --------
    >>> from dask.distributed import PipInstall
    >>> plugin = PipInstall(packages=["scikit-learn"], pip_options=["--upgrade"])

    >>> client.register_plugin(plugin)

    See Also
    --------
    PackageInstall
    CondaInstall
    """

    def __init__(
        self,
        packages: list[str],
        pip_options: list[str] | None = None,
        restart_workers: bool = False,
    ):
        installer = _PipInstaller(packages, pip_options)
        super().__init__(installer, restart_workers=restart_workers)


class _PipInstaller(_PackageInstaller):
    INSTALLER = "pip"

    pip_options: list[str]

    def __init__(self, packages: list[str], pip_options: list[str] | None = None):
        super().__init__(packages)
        self.pip_options = pip_options or []

    def install(self) -> None:
        proc = subprocess.Popen(
            [sys.executable, "-m", "pip", "install"] + self.pip_options + self.packages,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr = proc.communicate()
        returncode = proc.wait()
        if returncode != 0:
            msg = f"pip install failed with '{stderr.decode().strip()}'"
            logger.error(msg)
            raise RuntimeError(msg)


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

    >>> client.register_plugin(UploadFile("/path/to/file.py"))  # doctest: +SKIP
    """

    name = "upload_file"

    def __init__(self, filepath: str, load: bool = True):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        self.filename = os.path.basename(filepath)
        self.load = load
        with open(filepath, "rb") as f:
            self.data = f.read()

    async def setup(self, worker):
        response = await worker.upload_file(
            filename=self.filename, data=self.data, load=self.load
        )
        assert len(self.data) == response["nbytes"]


class ForwardLoggingPlugin(WorkerPlugin):
    """
    A ``WorkerPlugin`` to forward python logging records from worker to client.
    See :meth:`Client.forward_logging` for full documentation and usage. Needs
    to be used in coordination with :meth:`Client.subscribe_topic`, the details
    of which :meth:`Client.forward_logging` handles for you.

    Parameters
    ----------
    logger_name : str
        The name of the logger to begin forwarding.

    level : str | int
        Optionally restrict forwarding to ``LogRecord``s of this level or
        higher, even if the forwarded logger's own level is lower.

    topic : str
        The name of the topic to which to the worker should log the forwarded log
        records.
    """

    def __init__(self, logger_name, level, topic):
        self.logger_name = logger_name
        self.level = level
        self.topic = topic
        self.handler = None

    def setup(self, worker):
        self.handler = _ForwardingLogHandler(worker, self.topic, level=self.level)
        logger = logging.getLogger(self.logger_name)
        logger.addHandler(self.handler)

    def teardown(self, worker):
        if self.handler is not None:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)


class _ForwardingLogHandler(logging.Handler):
    """
    Handler class that gets installed inside workers by
    :class:`ForwardLoggingPlugin`. Not intended to be instantiated by the user
    directly.

    In each affected worker, ``ForwardLoggingPlugin`` adds an instance of this
    handler to one or more loggers (possibly the root logger). Tasks running on
    the worker may then use the affected logger as normal, with the side effect
    that any ``LogRecord``s handled by the logger (or by a logger below it in
    the hierarchy) will be published to the dask client as a
    ``topic`` event.
    """

    def __init__(self, worker, topic, level=logging.NOTSET):
        super().__init__(level)
        self.worker = worker
        self.topic = topic

    def prepare_record_attributes(self, record):
        # Adapted from the CPython standard library's
        # logging.handlers.SocketHandler.makePickle; see its source at:
        # https://github.com/python/cpython/blob/main/Lib/logging/handlers.py
        ei = record.exc_info
        if ei:
            # just to get traceback text into record.exc_text ...
            _ = self.format(record)
        # If msg or args are objects, they may not be available on the receiving
        # end. So we convert the msg % args to a string, save it as msg and zap
        # the args.
        d = dict(record.__dict__)
        d["msg"] = record.getMessage()
        d["args"] = None
        d["exc_info"] = None
        # delete 'message' if present: redundant with 'msg'
        d.pop("message", None)
        return d

    def emit(self, record):
        attributes = self.prepare_record_attributes(record)
        self.worker.log_event(self.topic, attributes)


class Environ(NannyPlugin):
    restart = True

    def __init__(self, environ: dict | None = None):
        environ = environ or {}
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
    >>> client.register_plugin(UploadDirectory("/path/to/directory"), nanny=True)  # doctest: +SKIP
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


class forward_stream:
    def __init__(self, stream, worker):
        self._worker = worker
        self._original_methods = {}
        self._stream = getattr(sys, stream)
        if stream == "stdout":
            self._file = 1
        elif stream == "stderr":
            self._file = 2
        else:
            raise ValueError(
                f"Expected stream to be 'stdout' or 'stderr'; got '{stream}'"
            )

        self._file = 1 if stream == "stdout" else 2
        self._buffer = []

    def _write(self, write_fn, data):
        self._forward(data)
        write_fn(data)

    def _forward(self, data):
        self._buffer.append(data)
        # Mimic line buffering
        if "\n" in data or "\r" in data:
            self._send()

    def _send(self):
        msg = {"args": self._buffer, "file": self._file, "sep": "", "end": ""}
        self._worker.log_event("print", msg)
        self._buffer = []

    def _flush(self, flush_fn):
        self._send()
        flush_fn()

    def _close(self, close_fn):
        self._send()
        close_fn()

    def _intercept(self, method_name, interceptor):
        original_method = getattr(self._stream, method_name)
        self._original_methods[method_name] = original_method
        setattr(
            self._stream, method_name, functools.partial(interceptor, original_method)
        )

    def __enter__(self):
        self._intercept("write", self._write)
        self._intercept("flush", self._flush)
        self._intercept("close", self._close)
        return self._stream

    def __exit__(self, exc_type, exc_value, traceback):
        self._stream.flush()
        for attr, original in self._original_methods.items():
            setattr(self._stream, attr, original)
        self._original_methods = {}


class ForwardOutput(WorkerPlugin):
    """A Worker Plugin that forwards ``stdout`` and ``stderr`` from workers to clients

    This plugin forwards all output sent to ``stdout`` and ``stderr` on all workers
    to all clients where it is written to the respective streams. Analogous to the
    terminal, this plugin uses line buffering. To ensure that an output is written
    without a newline, make sure to flush the stream.

    .. warning::

        Using this plugin will forward **all** output in ``stdout`` and ``stderr`` from
        every worker to every client. If the output is very chatty, this will add
        significant strain on the scheduler. Proceed with caution!

    Examples
    --------
    >>> from dask.distributed import ForwardOutput
    >>> plugin = ForwardOutput()

    >>> client.register_plugin(plugin)
    """

    def setup(self, worker):
        self._exit_stack = contextlib.ExitStack()
        self._exit_stack.enter_context(forward_stream("stdout", worker=worker))
        self._exit_stack.enter_context(forward_stream("stderr", worker=worker))

    def teardown(self, worker):
        self._exit_stack.close()
