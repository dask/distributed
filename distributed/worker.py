from __future__ import annotations

import asyncio
import bisect
import builtins
import errno
import heapq
import logging
import os
import random
import sys
import threading
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Container,
    Iterable,
    Mapping,
    MutableMapping,
)
from concurrent.futures import Executor
from contextlib import suppress
from datetime import timedelta
from inspect import isawaitable
from pickle import PicklingError
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from tlz import first, keymap, merge, pluck  # noqa: F401
from tornado.ioloop import IOLoop, PeriodicCallback

import dask
from dask.core import istask
from dask.system import CPU_COUNT
from dask.utils import (
    apply,
    format_bytes,
    funcname,
    parse_bytes,
    parse_timedelta,
    stringify,
    typename,
)

from distributed import comm, preloading, profile, utils
from distributed.batched import BatchedSend
from distributed.comm import connect, get_address_host
from distributed.comm.addressing import address_from_user_args, parse_address
from distributed.comm.utils import OFFLOAD_THRESHOLD
from distributed.core import (
    CommClosedError,
    Status,
    coerce_to_address,
    error_message,
    pingpong,
    send_recv,
)
from distributed.diagnostics import nvml
from distributed.diagnostics.plugin import _get_plugin_name
from distributed.diskutils import WorkDir, WorkSpace
from distributed.http import get_handlers
from distributed.metrics import time
from distributed.node import ServerNode
from distributed.proctitle import setproctitle
from distributed.protocol import pickle, to_serialize
from distributed.pubsub import PubSubWorkerExtension
from distributed.security import Security
from distributed.shuffle import ShuffleWorkerExtension
from distributed.sizeof import safe_sizeof as sizeof
from distributed.threadpoolexecutor import ThreadPoolExecutor
from distributed.threadpoolexecutor import secede as tpe_secede
from distributed.utils import (
    LRU,
    TimeoutError,
    _maybe_complex,
    get_ip,
    has_arg,
    import_file,
    in_async_call,
    iscoroutinefunction,
    json_load_robust,
    key_split,
    log_errors,
    offload,
    parse_ports,
    recursive_to_dict,
    silence_logging,
    thread_state,
    warn_on_duration,
)
from distributed.utils_comm import gather_from_workers, pack_data, retry_operation
from distributed.utils_perf import disable_gc_diagnosis, enable_gc_diagnosis
from distributed.versions import get_versions
from distributed.worker_memory import (
    DeprecatedMemoryManagerAttribute,
    DeprecatedMemoryMonitor,
    WorkerMemoryManager,
)
from distributed.worker_state_machine import Instruction  # noqa: F401
from distributed.worker_state_machine import (
    PROCESSING,
    READY,
    AddKeysMsg,
    InvalidTransition,
    LongRunningMsg,
    ReleaseWorkerDataMsg,
    RescheduleMsg,
    SendMessageToScheduler,
    SerializedTask,
    TaskErredMsg,
    TaskFinishedMsg,
    TaskState,
    UniqueTaskHeap,
)

if TYPE_CHECKING:
    # TODO move to typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

    from distributed.actor import Actor
    from distributed.client import Client
    from distributed.diagnostics.plugin import WorkerPlugin
    from distributed.nanny import Nanny
    from distributed.worker_state_machine import TaskStateState

    # {TaskState -> finish: TaskStateState | (finish: TaskStateState, transition *args)}
    Recs: TypeAlias = "dict[TaskState, TaskStateState | tuple]"
    Instructions: TypeAlias = "list[Instruction]"


logger = logging.getLogger(__name__)

LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")

no_value = "--no-value-sentinel--"

DEFAULT_EXTENSIONS: list[type] = [PubSubWorkerExtension, ShuffleWorkerExtension]

DEFAULT_METRICS: dict[str, Callable[[Worker], Any]] = {}

DEFAULT_STARTUP_INFORMATION: dict[str, Callable[[Worker], Any]] = {}


class Worker(ServerNode):
    """Worker node in a Dask distributed cluster

    Workers perform two functions:

    1.  **Serve data** from a local dictionary
    2.  **Perform computation** on that data and on data from peers

    Workers keep the scheduler informed of their data and use that scheduler to
    gather data from other workers when necessary to perform a computation.

    You can start a worker with the ``dask-worker`` command line application::

        $ dask-worker scheduler-ip:port

    Use the ``--help`` flag to see more options::

        $ dask-worker --help

    The rest of this docstring is about the internal state the the worker uses
    to manage and track internal computations.

    **State**

    **Informational State**

    These attributes don't change significantly during execution.

    * **nthreads:** ``int``:
        Number of nthreads used by this worker process
    * **executors:** ``dict[str, concurrent.futures.Executor]``:
        Executors used to perform computation. Always contains the default
        executor.
    * **local_directory:** ``path``:
        Path on local machine to store temporary files
    * **scheduler:** ``rpc``:
        Location of scheduler.  See ``.ip/.port`` attributes.
    * **name:** ``string``:
        Alias
    * **services:** ``{str: Server}``:
        Auxiliary web servers running on this worker
    * **service_ports:** ``{str: port}``:
    * **total_out_connections**: ``int``
        The maximum number of concurrent outgoing requests for data
    * **total_in_connections**: ``int``
        The maximum number of concurrent incoming requests for data
    * **comm_threshold_bytes**: ``int``
        As long as the total number of bytes in flight is below this threshold
        we will not limit the number of outgoing connections for a single tasks
        dependency fetch.
    * **batched_stream**: ``BatchedSend``
        A batched stream along which we communicate to the scheduler
    * **log**: ``[(message)]``
        A structured and queryable log.  See ``Worker.story``

    **Volatile State**

    These attributes track the progress of tasks that this worker is trying to
    complete.  In the descriptions below a ``key`` is the name of a task that
    we want to compute and ``dep`` is the name of a piece of dependent data
    that we want to collect from others.

    * **tasks**: ``{key: TaskState}``
        The tasks currently executing on this worker (and any dependencies of those tasks)
    * **data_needed**: UniqueTaskHeap
        The tasks which still require data in order to execute, prioritized as a heap
    * **ready**: [keys]
        Keys that are ready to run.  Stored in a LIFO stack
    * **constrained**: [keys]
        Keys for which we have the data to run, but are waiting on abstract
        resources like GPUs.  Stored in a FIFO deque
    * **executing_count**: ``int``
        A count of tasks currently executing on this worker
    * **executed_count**: int
        A number of tasks that this worker has run in its lifetime
    * **long_running**: {keys}
        A set of keys of tasks that are running and have started their own
        long-running clients.
    * **has_what**: ``{worker: {deps}}``
        The data that we care about that we think a worker has
    * **pending_data_per_worker**: ``{worker: UniqueTaskHeap}``
        The data on each worker that we still want, prioritized as a heap
    * **in_flight_tasks**: ``int``
        A count of the number of tasks that are coming to us in current
        peer-to-peer connections
    * **in_flight_workers**: ``{worker: {task}}``
        The workers from which we are currently gathering data and the
        dependencies we expect from those connections
    * **comm_bytes**: ``int``
        The total number of bytes in flight
    * **threads**: ``{key: int}``
        The ID of the thread on which the task ran
    * **active_threads**: ``{int: key}``
        The keys currently running on active threads
    * **waiting_for_data_count**: ``int``
        A count of how many tasks are currently waiting for data
    * **generation**: ``int``
        Counter that decreases every time the compute-task handler is invoked by the
        Scheduler. It is appended to TaskState.priority and acts as a tie-breaker
        between tasks that have the same priority on the Scheduler, determining a
        last-in-first-out order between them.

    Parameters
    ----------
    scheduler_ip: str, optional
    scheduler_port: int, optional
    scheduler_file: str, optional
    ip: str, optional
    data: MutableMapping, type, None
        The object to use for storage, builds a disk-backed LRU dict by default
    nthreads: int, optional
    loop: tornado.ioloop.IOLoop
    local_directory: str, optional
        Directory where we place local resources
    name: str, optional
    memory_limit: int, float, string
        Number of bytes of memory that this worker should use.
        Set to zero for no limit.  Set to 'auto' to calculate
        as system.MEMORY_LIMIT * min(1, nthreads / total_cores)
        Use strings or numbers like 5GB or 5e9
    memory_target_fraction: float or False
        Fraction of memory to try to stay beneath
        (default: read from config key distributed.worker.memory.target)
    memory_spill_fraction: float or false
        Fraction of memory at which we start spilling to disk
        (default: read from config key distributed.worker.memory.spill)
    memory_pause_fraction: float or False
        Fraction of memory at which we stop running new tasks
        (default: read from config key distributed.worker.memory.pause)
    max_spill: int, string or False
        Limit of number of bytes to be spilled on disk.
        (default: read from config key distributed.worker.memory.max-spill)
    executor: concurrent.futures.Executor, dict[str, concurrent.futures.Executor], "offload"
        The executor(s) to use. Depending on the type, it has the following meanings:
            - Executor instance: The default executor.
            - Dict[str, Executor]: mapping names to Executor instances. If the
              "default" key isn't in the dict, a "default" executor will be created
              using ``ThreadPoolExecutor(nthreads)``.
            - Str: The string "offload", which refer to the same thread pool used for
              offloading communications. This results in the same thread being used
              for deserialization and computation.
    resources: dict
        Resources that this worker has like ``{'GPU': 2}``
    nanny: str
        Address on which to contact nanny, if it exists
    lifetime: str
        Amount of time like "1 hour" after which we gracefully shut down the worker.
        This defaults to None, meaning no explicit shutdown time.
    lifetime_stagger: str
        Amount of time like "5 minutes" to stagger the lifetime value
        The actual lifetime will be selected uniformly at random between
        lifetime +/- lifetime_stagger
    lifetime_restart: bool
        Whether or not to restart a worker after it has reached its lifetime
        Default False
    kwargs: optional
        Additional parameters to ServerNode constructor

    Examples
    --------

    Use the command line to start a worker::

        $ dask-scheduler
        Start scheduler at 127.0.0.1:8786

        $ dask-worker 127.0.0.1:8786
        Start worker at:               127.0.0.1:1234
        Registered with scheduler at:  127.0.0.1:8786

    See Also
    --------
    distributed.scheduler.Scheduler
    distributed.nanny.Nanny
    """

    _instances: ClassVar[weakref.WeakSet[Worker]] = weakref.WeakSet()
    _initialized_clients: ClassVar[weakref.WeakSet[Client]] = weakref.WeakSet()

    tasks: dict[str, TaskState]
    waiting_for_data_count: int
    has_what: defaultdict[str, set[str]]  # {worker address: {ts.key, ...}
    pending_data_per_worker: defaultdict[str, UniqueTaskHeap]
    nanny: Nanny | None
    _lock: threading.Lock
    data_needed: UniqueTaskHeap
    in_flight_workers: dict[str, set[str]]  # {worker address: {ts.key, ...}}
    total_out_connections: int
    total_in_connections: int
    comm_threshold_bytes: int
    comm_nbytes: int
    _missing_dep_flight: set[TaskState]
    threads: dict[str, int]  # {ts.key: thread ID}
    active_threads_lock: threading.Lock
    active_threads: dict[int, str]  # {thread ID: ts.key}
    active_keys: set[str]
    profile_keys: defaultdict[str, dict[str, Any]]
    profile_keys_history: deque[tuple[float, dict[str, dict[str, Any]]]]
    profile_recent: dict[str, Any]
    profile_history: deque[tuple[float, dict[str, Any]]]
    generation: int
    ready: list[tuple[tuple[int, ...], str]]  # heapq [(priority, key), ...]
    constrained: deque[str]
    _executing: set[TaskState]
    _in_flight_tasks: set[TaskState]
    executed_count: int
    long_running: set[str]
    log: deque[tuple]  # [(..., stimulus_id: str | None, timestamp: float), ...]
    incoming_transfer_log: deque[dict[str, Any]]
    outgoing_transfer_log: deque[dict[str, Any]]
    target_message_size: int
    validate: bool
    _transitions_table: dict[tuple[str, str], Callable]
    _transition_counter: int
    incoming_count: int
    outgoing_count: int
    outgoing_current_count: int
    repetitively_busy: int
    bandwidth: float
    latency: float
    profile_cycle_interval: float
    workspace: WorkSpace
    _workdir: WorkDir
    local_directory: str
    _client: Client | None
    bandwidth_workers: defaultdict[str, tuple[float, int]]
    bandwidth_types: defaultdict[type, tuple[float, int]]
    preloads: list[preloading.Preload]
    contact_address: str | None
    _start_port: int | None
    _start_host: str | None
    _interface: str | None
    _protocol: str
    _dashboard_address: str | None
    _dashboard: bool
    _http_prefix: str
    nthreads: int
    total_resources: dict[str, float]
    available_resources: dict[str, float]
    death_timeout: float | None
    lifetime: float | None
    lifetime_stagger: float | None
    lifetime_restart: bool
    extensions: dict
    security: Security
    connection_args: dict[str, Any]
    actors: dict[str, Actor | None]
    loop: IOLoop
    reconnect: bool
    executors: dict[str, Executor]
    batched_stream: BatchedSend
    name: Any
    scheduler_delay: float
    stream_comms: dict[str, BatchedSend]
    heartbeat_interval: float
    heartbeat_active: bool
    _ipython_kernel: Any | None = None
    services: dict[str, Any] = {}
    service_specs: dict[str, Any]
    metrics: dict[str, Callable[[Worker], Any]]
    startup_information: dict[str, Callable[[Worker], Any]]
    low_level_profiler: bool
    scheduler: Any
    execution_state: dict[str, Any]
    plugins: dict[str, WorkerPlugin]
    _pending_plugins: tuple[WorkerPlugin, ...]

    def __init__(
        self,
        scheduler_ip: str | None = None,
        scheduler_port: int | None = None,
        *,
        scheduler_file: str | None = None,
        nthreads: int | None = None,
        loop: IOLoop | None = None,
        local_dir: None = None,  # Deprecated, use local_directory instead
        local_directory: str | None = None,
        services: dict | None = None,
        name: Any | None = None,
        reconnect: bool = True,
        executor: Executor | dict[str, Executor] | Literal["offload"] | None = None,
        resources: dict[str, float] | None = None,
        silence_logs: int | None = None,
        death_timeout: Any | None = None,
        preload: list[str] | None = None,
        preload_argv: list[str] | list[list[str]] | None = None,
        security: Security | dict[str, Any] | None = None,
        contact_address: str | None = None,
        heartbeat_interval: Any = "1s",
        extensions: list[type] | None = None,
        metrics: Mapping[str, Callable[[Worker], Any]] = DEFAULT_METRICS,
        startup_information: Mapping[
            str, Callable[[Worker], Any]
        ] = DEFAULT_STARTUP_INFORMATION,
        interface: str | None = None,
        host: str | None = None,
        port: int | None = None,
        protocol: str | None = None,
        dashboard_address: str | None = None,
        dashboard: bool = False,
        http_prefix: str = "/",
        nanny: Nanny | None = None,
        plugins: tuple[WorkerPlugin, ...] = (),
        low_level_profiler: bool | None = None,
        validate: bool | None = None,
        profile_cycle_interval=None,
        lifetime: Any | None = None,
        lifetime_stagger: Any | None = None,
        lifetime_restart: bool | None = None,
        ###################################
        # Parameters to WorkerMemoryManager
        memory_limit: str | float = "auto",
        # Allow overriding the dict-like that stores the task outputs.
        # This is meant for power users only. See WorkerMemoryManager for details.
        data=None,
        # Deprecated parameters; please use dask config instead.
        memory_target_fraction: float | Literal[False] | None = None,
        memory_spill_fraction: float | Literal[False] | None = None,
        memory_pause_fraction: float | Literal[False] | None = None,
        ###################################
        # Parameters to Server
        **kwargs,
    ):
        self.tasks = {}
        self.waiting_for_data_count = 0
        self.has_what = defaultdict(set)
        self.pending_data_per_worker = defaultdict(UniqueTaskHeap)
        self.nanny = nanny
        self._lock = threading.Lock()

        self.data_needed = UniqueTaskHeap()

        self.in_flight_workers = {}
        self.total_out_connections = dask.config.get(
            "distributed.worker.connections.outgoing"
        )
        self.total_in_connections = dask.config.get(
            "distributed.worker.connections.incoming"
        )
        self.comm_threshold_bytes = int(10e6)
        self.comm_nbytes = 0
        self._missing_dep_flight = set()

        self.threads = {}

        self.active_threads_lock = threading.Lock()
        self.active_threads = {}
        self.active_keys = set()
        self.profile_keys = defaultdict(profile.create)
        self.profile_keys_history = deque(maxlen=3600)
        self.profile_recent = profile.create()
        self.profile_history = deque(maxlen=3600)

        self.generation = 0

        self.ready = []
        self.constrained = deque()
        self._executing = set()
        self._in_flight_tasks = set()
        self.executed_count = 0
        self.long_running = set()

        self.target_message_size = int(50e6)  # 50 MB

        self.log = deque(maxlen=100000)
        if validate is None:
            validate = dask.config.get("distributed.scheduler.validate")
        self.validate = validate
        self._transitions_table = {
            ("cancelled", "resumed"): self.transition_cancelled_resumed,
            ("cancelled", "fetch"): self.transition_cancelled_fetch,
            ("cancelled", "released"): self.transition_cancelled_released,
            ("cancelled", "waiting"): self.transition_cancelled_waiting,
            ("cancelled", "forgotten"): self.transition_cancelled_forgotten,
            ("cancelled", "memory"): self.transition_cancelled_memory,
            ("cancelled", "error"): self.transition_cancelled_error,
            ("resumed", "memory"): self.transition_generic_memory,
            ("resumed", "error"): self.transition_generic_error,
            ("resumed", "released"): self.transition_generic_released,
            ("resumed", "waiting"): self.transition_resumed_waiting,
            ("resumed", "fetch"): self.transition_resumed_fetch,
            ("resumed", "missing"): self.transition_resumed_missing,
            ("constrained", "executing"): self.transition_constrained_executing,
            ("constrained", "released"): self.transition_generic_released,
            ("error", "released"): self.transition_generic_released,
            ("executing", "error"): self.transition_executing_error,
            ("executing", "long-running"): self.transition_executing_long_running,
            ("executing", "memory"): self.transition_executing_memory,
            ("executing", "released"): self.transition_executing_released,
            ("executing", "rescheduled"): self.transition_executing_rescheduled,
            ("fetch", "flight"): self.transition_fetch_flight,
            ("fetch", "released"): self.transition_generic_released,
            ("flight", "error"): self.transition_flight_error,
            ("flight", "fetch"): self.transition_flight_fetch,
            ("flight", "memory"): self.transition_flight_memory,
            ("flight", "missing"): self.transition_flight_missing,
            ("flight", "released"): self.transition_flight_released,
            ("long-running", "error"): self.transition_generic_error,
            ("long-running", "memory"): self.transition_long_running_memory,
            ("long-running", "rescheduled"): self.transition_executing_rescheduled,
            ("long-running", "released"): self.transition_executing_released,
            ("memory", "released"): self.transition_memory_released,
            ("missing", "fetch"): self.transition_missing_fetch,
            ("missing", "released"): self.transition_missing_released,
            ("missing", "error"): self.transition_generic_error,
            ("ready", "error"): self.transition_generic_error,
            ("ready", "executing"): self.transition_ready_executing,
            ("ready", "released"): self.transition_generic_released,
            ("released", "error"): self.transition_generic_error,
            ("released", "fetch"): self.transition_released_fetch,
            ("released", "forgotten"): self.transition_released_forgotten,
            ("released", "memory"): self.transition_released_memory,
            ("released", "waiting"): self.transition_released_waiting,
            ("waiting", "constrained"): self.transition_waiting_constrained,
            ("waiting", "ready"): self.transition_waiting_ready,
            ("waiting", "released"): self.transition_generic_released,
        }

        self._transition_counter = 0
        self.incoming_transfer_log = deque(maxlen=100000)
        self.incoming_count = 0
        self.outgoing_transfer_log = deque(maxlen=100000)
        self.outgoing_count = 0
        self.outgoing_current_count = 0
        self.repetitively_busy = 0
        self.bandwidth = parse_bytes(dask.config.get("distributed.scheduler.bandwidth"))
        self.bandwidth_workers = defaultdict(
            lambda: (0, 0)
        )  # bw/count recent transfers
        self.bandwidth_types = defaultdict(lambda: (0, 0))  # bw/count recent transfers
        self.latency = 0.001
        self._client = None

        if profile_cycle_interval is None:
            profile_cycle_interval = dask.config.get("distributed.worker.profile.cycle")
        profile_cycle_interval = parse_timedelta(profile_cycle_interval, default="ms")
        assert profile_cycle_interval

        self._setup_logging(logger)

        if local_dir is not None:
            warnings.warn("The local_dir keyword has moved to local_directory")
            local_directory = local_dir

        if not local_directory:
            local_directory = dask.config.get("temporary-directory") or os.getcwd()

        os.makedirs(local_directory, exist_ok=True)
        local_directory = os.path.join(local_directory, "dask-worker-space")

        with warn_on_duration(
            "1s",
            "Creating scratch directories is taking a surprisingly long time. "
            "This is often due to running workers on a network file system. "
            "Consider specifying a local-directory to point workers to write "
            "scratch data to a local disk.",
        ):
            self._workspace = WorkSpace(os.path.abspath(local_directory))
            self._workdir = self._workspace.new_work_dir(prefix="worker-")
            self.local_directory = self._workdir.dir_path

        if not preload:
            preload = dask.config.get("distributed.worker.preload")
        if not preload_argv:
            preload_argv = dask.config.get("distributed.worker.preload-argv")
        assert preload is not None
        assert preload_argv is not None
        self.preloads = preloading.process_preloads(
            self, preload, preload_argv, file_dir=self.local_directory
        )

        if scheduler_file:
            cfg = json_load_robust(scheduler_file)
            scheduler_addr = cfg["address"]
        elif scheduler_ip is None and dask.config.get("scheduler-address", None):
            scheduler_addr = dask.config.get("scheduler-address")
        elif scheduler_port is None:
            scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
        self.contact_address = contact_address

        if protocol is None:
            protocol_address = scheduler_addr.split("://")
            if len(protocol_address) == 2:
                protocol = protocol_address[0]
            assert protocol

        self._start_port = port
        self._start_host = host
        if host:
            # Helpful error message if IPv6 specified incorrectly
            _, host_address = parse_address(host)
            if host_address.count(":") > 1 and not host_address.startswith("["):
                raise ValueError(
                    "Host address with IPv6 must be bracketed like '[::1]'; "
                    f"got {host_address}"
                )
        self._interface = interface
        self._protocol = protocol

        self.nthreads = nthreads or CPU_COUNT
        if resources is None:
            resources = dask.config.get("distributed.worker.resources", None)
            assert isinstance(resources, dict)

        self.total_resources = resources or {}
        self.available_resources = (resources or {}).copy()
        self.death_timeout = parse_timedelta(death_timeout)

        self.extensions = {}
        if silence_logs:
            silence_logging(level=silence_logs)

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("worker")

        self.actors = {}
        self.loop = loop or IOLoop.current()
        self.reconnect = reconnect

        # Common executors always available
        self.executors = {
            "offload": utils._offload_executor,
            "actor": ThreadPoolExecutor(1, thread_name_prefix="Dask-Actor-Threads"),
        }
        if nvml.device_get_count() > 0:
            self.executors["gpu"] = ThreadPoolExecutor(
                1, thread_name_prefix="Dask-GPU-Threads"
            )

        # Find the default executor
        if executor == "offload":
            self.executors["default"] = self.executors["offload"]
        elif isinstance(executor, dict):
            self.executors.update(executor)
        elif executor is not None:
            self.executors["default"] = executor
        if "default" not in self.executors:
            self.executors["default"] = ThreadPoolExecutor(
                self.nthreads, thread_name_prefix="Dask-Default-Threads"
            )

        self.batched_stream = BatchedSend(interval="2ms", loop=self.loop)
        self.name = name
        self.scheduler_delay = 0
        self.stream_comms = {}
        self.heartbeat_active = False
        self._ipython_kernel = None

        if self.local_directory not in sys.path:
            sys.path.insert(0, self.local_directory)

        self.services = {}
        self.service_specs = services or {}

        self._dashboard_address = dashboard_address
        self._dashboard = dashboard
        self._http_prefix = http_prefix

        self.metrics = dict(metrics) if metrics else {}
        self.startup_information = (
            dict(startup_information) if startup_information else {}
        )

        if low_level_profiler is None:
            low_level_profiler = dask.config.get("distributed.worker.profile.low-level")
        self.low_level_profiler = low_level_profiler

        handlers = {
            "gather": self.gather,
            "run": self.run,
            "run_coroutine": self.run_coroutine,
            "get_data": self.get_data,
            "update_data": self.update_data,
            "free_keys": self.handle_free_keys,
            "terminate": self.close,
            "ping": pingpong,
            "upload_file": self.upload_file,
            "start_ipython": self.start_ipython,
            "call_stack": self.get_call_stack,
            "profile": self.get_profile,
            "profile_metadata": self.get_profile_metadata,
            "get_logs": self.get_logs,
            "keys": self.keys,
            "versions": self.versions,
            "actor_execute": self.actor_execute,
            "actor_attribute": self.actor_attribute,
            "plugin-add": self.plugin_add,
            "plugin-remove": self.plugin_remove,
            "get_monitor_info": self.get_monitor_info,
        }

        stream_handlers = {
            "close": self.close,
            "cancel-compute": self.handle_cancel_compute,
            "acquire-replicas": self.handle_acquire_replicas,
            "compute-task": self.handle_compute_task,
            "free-keys": self.handle_free_keys,
            "remove-replicas": self.handle_remove_replicas,
            "steal-request": self.handle_steal_request,
            "worker-status-change": self.handle_worker_status_change,
        }

        super().__init__(
            handlers=handlers,
            stream_handlers=stream_handlers,
            io_loop=self.loop,
            connection_args=self.connection_args,
            **kwargs,
        )

        self.scheduler = self.rpc(scheduler_addr)
        self.execution_state = {
            "scheduler": self.scheduler.address,
            "ioloop": self.loop,
            "worker": self,
        }

        self.heartbeat_interval = parse_timedelta(heartbeat_interval, default="ms")
        pc = PeriodicCallback(self.heartbeat, self.heartbeat_interval * 1000)
        self.periodic_callbacks["heartbeat"] = pc

        pc = PeriodicCallback(
            lambda: self.batched_stream.send({"op": "keep-alive"}), 60000
        )
        self.periodic_callbacks["keep-alive"] = pc

        # FIXME annotations: https://github.com/tornadoweb/tornado/issues/3117
        pc = PeriodicCallback(self.find_missing, 1000)  # type: ignore
        self.periodic_callbacks["find-missing"] = pc

        self._address = contact_address

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        for ext in extensions:
            ext(self)

        self.memory_manager = WorkerMemoryManager(
            self,
            data=data,
            memory_limit=memory_limit,
            memory_target_fraction=memory_target_fraction,
            memory_spill_fraction=memory_spill_fraction,
            memory_pause_fraction=memory_pause_fraction,
        )

        setproctitle("dask-worker [not started]")

        profile_trigger_interval = parse_timedelta(
            dask.config.get("distributed.worker.profile.interval"), default="ms"
        )
        pc = PeriodicCallback(self.trigger_profile, profile_trigger_interval * 1000)
        self.periodic_callbacks["profile"] = pc

        pc = PeriodicCallback(self.cycle_profile, profile_cycle_interval * 1000)
        self.periodic_callbacks["profile-cycle"] = pc

        self.plugins = {}
        self._pending_plugins = plugins

        if lifetime is None:
            lifetime = dask.config.get("distributed.worker.lifetime.duration")
        self.lifetime = parse_timedelta(lifetime)

        if lifetime_stagger is None:
            lifetime_stagger = dask.config.get("distributed.worker.lifetime.stagger")
        lifetime_stagger = parse_timedelta(lifetime_stagger)

        if lifetime_restart is None:
            lifetime_restart = dask.config.get("distributed.worker.lifetime.restart")
        self.lifetime_restart = lifetime_restart

        if self.lifetime:
            self.lifetime += (random.random() * 2 - 1) * lifetime_stagger
            self.io_loop.call_later(self.lifetime, self.close_gracefully)

        Worker._instances.add(self)

    ################
    # Memory manager
    ################
    memory_manager: WorkerMemoryManager

    @property
    def data(self) -> MutableMapping[str, Any]:
        """{task key: task payload} of all completed tasks, whether they were computed on
        this Worker or computed somewhere else and then transferred here over the
        network.

        When using the default configuration, this is a zict buffer that automatically
        spills to disk whenever the target threshold is exceeded.
        If spilling is disabled, it is a plain dict instead.
        It could also be a user-defined arbitrary dict-like passed when initialising
        the Worker or the Nanny.
        Worker logic should treat this opaquely and stick to the MutableMapping API.
        """
        return self.memory_manager.data

    # Deprecated attributes moved to self.memory_manager.<name>
    memory_limit = DeprecatedMemoryManagerAttribute()
    memory_target_fraction = DeprecatedMemoryManagerAttribute()
    memory_spill_fraction = DeprecatedMemoryManagerAttribute()
    memory_pause_fraction = DeprecatedMemoryManagerAttribute()
    memory_monitor = DeprecatedMemoryMonitor()

    ##################
    # Administrative #
    ##################

    def __repr__(self):
        name = f", name: {self.name}" if self.name != self.address else ""
        return (
            f"<{self.__class__.__name__} {self.address!r}{name}, "
            f"status: {self.status.name}, "
            f"stored: {len(self.data)}, "
            f"running: {self.executing_count}/{self.nthreads}, "
            f"ready: {len(self.ready)}, "
            f"comm: {self.in_flight_tasks}, "
            f"waiting: {self.waiting_for_data_count}>"
        )

    @property
    def logs(self):
        return self._deque_handler.deque

    def log_event(self, topic, msg):
        self.loop.add_callback(
            self.batched_stream.send,
            {
                "op": "log-event",
                "topic": topic,
                "msg": msg,
            },
        )

    @property
    def executing_count(self) -> int:
        return len(self._executing)

    @property
    def in_flight_tasks(self) -> int:
        return len(self._in_flight_tasks)

    @property
    def worker_address(self):
        """For API compatibility with Nanny"""
        return self.address

    @property
    def executor(self):
        return self.executors["default"]

    @ServerNode.status.setter  # type: ignore
    def status(self, value):
        """Override Server.status to notify the Scheduler of status changes.
        Also handles unpausing.
        """
        prev_status = self.status
        ServerNode.status.__set__(self, value)
        self._send_worker_status_change()
        if prev_status == Status.paused and value == Status.running:
            self.ensure_computing()
            self.ensure_communicating()

    def _send_worker_status_change(self) -> None:
        if (
            self.batched_stream
            and self.batched_stream.comm
            and not self.batched_stream.comm.closed()
        ):
            self.batched_stream.send(
                {"op": "worker-status-change", "status": self._status.name}
            )
        elif self._status != Status.closed:
            self.loop.call_later(0.05, self._send_worker_status_change)

    async def get_metrics(self) -> dict:
        try:
            spilled_memory, spilled_disk = self.data.spilled_total  # type: ignore
        except AttributeError:
            # spilling is disabled
            spilled_memory, spilled_disk = 0, 0

        out = dict(
            executing=self.executing_count,
            in_memory=len(self.data),
            ready=len(self.ready),
            in_flight=self.in_flight_tasks,
            bandwidth={
                "total": self.bandwidth,
                "workers": dict(self.bandwidth_workers),
                "types": keymap(typename, self.bandwidth_types),
            },
            spilled_nbytes={
                "memory": spilled_memory,
                "disk": spilled_disk,
            },
        )
        out.update(self.monitor.recent())

        for k, metric in self.metrics.items():
            try:
                result = metric(self)
                if isawaitable(result):
                    result = await result
                # In case of collision, prefer core metrics
                out.setdefault(k, result)
            except Exception:  # TODO: log error once
                pass

        return out

    async def get_startup_information(self):
        result = {}
        for k, f in self.startup_information.items():
            try:
                v = f(self)
                if isawaitable(v):
                    v = await v
                result[k] = v
            except Exception:  # TODO: log error once
                pass

        return result

    def identity(self):
        return {
            "type": type(self).__name__,
            "id": self.id,
            "scheduler": self.scheduler.address,
            "nthreads": self.nthreads,
            "memory_limit": self.memory_manager.memory_limit,
        }

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Worker.identity
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = super()._to_dict(exclude=exclude)
        extra = {
            "status": self.status,
            "ready": self.ready,
            "constrained": self.constrained,
            "data_needed": list(self.data_needed),
            "pending_data_per_worker": {
                w: list(v) for w, v in self.pending_data_per_worker.items()
            },
            "long_running": self.long_running,
            "executing_count": self.executing_count,
            "in_flight_tasks": self.in_flight_tasks,
            "in_flight_workers": self.in_flight_workers,
            "log": self.log,
            "tasks": self.tasks,
            "logs": self.get_logs(),
            "config": dask.config.config,
            "incoming_transfer_log": self.incoming_transfer_log,
            "outgoing_transfer_log": self.outgoing_transfer_log,
        }
        info.update(extra)
        info.update(self.memory_manager._to_dict(exclude=exclude))
        info = {k: v for k, v in info.items() if k not in exclude}
        return recursive_to_dict(info, exclude=exclude)

    #####################
    # External Services #
    #####################

    async def _register_with_scheduler(self):
        self.periodic_callbacks["keep-alive"].stop()
        self.periodic_callbacks["heartbeat"].stop()
        start = time()
        if self.contact_address is None:
            self.contact_address = self.address
        logger.info("-" * 49)
        while True:
            try:
                _start = time()
                comm = await connect(self.scheduler.address, **self.connection_args)
                comm.name = "Worker->Scheduler"
                comm._server = weakref.ref(self)
                await comm.write(
                    dict(
                        op="register-worker",
                        reply=False,
                        address=self.contact_address,
                        status=self.status.name,
                        keys=list(self.data),
                        nthreads=self.nthreads,
                        name=self.name,
                        nbytes={
                            ts.key: ts.get_nbytes()
                            for ts in self.tasks.values()
                            # Only if the task is in memory this is a sensible
                            # result since otherwise it simply submits the
                            # default value
                            if ts.state == "memory"
                        },
                        types={k: typename(v) for k, v in self.data.items()},
                        now=time(),
                        resources=self.total_resources,
                        memory_limit=self.memory_manager.memory_limit,
                        local_directory=self.local_directory,
                        services=self.service_ports,
                        nanny=self.nanny,
                        pid=os.getpid(),
                        versions=get_versions(),
                        metrics=await self.get_metrics(),
                        extra=await self.get_startup_information(),
                    ),
                    serializers=["msgpack"],
                )
                future = comm.read(deserializers=["msgpack"])

                response = await future
                if response.get("warning"):
                    logger.warning(response["warning"])

                _end = time()
                middle = (_start + _end) / 2
                self._update_latency(_end - start)
                self.scheduler_delay = response["time"] - middle
                self.status = Status.running
                break
            except OSError:
                logger.info("Waiting to connect to: %26s", self.scheduler.address)
                await asyncio.sleep(0.1)
            except TimeoutError:  # pragma: no cover
                logger.info("Timed out when connecting to scheduler")
        if response["status"] != "OK":
            raise ValueError(f"Unexpected response from register: {response!r}")
        else:
            await asyncio.gather(
                *(
                    self.plugin_add(name=name, plugin=plugin)
                    for name, plugin in response["worker-plugins"].items()
                )
            )

            logger.info("        Registered to: %26s", self.scheduler.address)
            logger.info("-" * 49)

        self.batched_stream.start(comm)
        self.periodic_callbacks["keep-alive"].start()
        self.periodic_callbacks["heartbeat"].start()
        self.loop.add_callback(self.handle_scheduler, comm)

    def _update_latency(self, latency):
        self.latency = latency * 0.05 + self.latency * 0.95
        if self.digests is not None:
            self.digests["latency"].add(latency)

    async def heartbeat(self):
        if self.heartbeat_active:
            logger.debug("Heartbeat skipped: channel busy")
            return
        self.heartbeat_active = True
        logger.debug("Heartbeat: %s", self.address)
        try:
            start = time()
            response = await retry_operation(
                self.scheduler.heartbeat_worker,
                address=self.contact_address,
                now=start,
                metrics=await self.get_metrics(),
                executing={
                    key: start - self.tasks[key].start_time
                    for key in self.active_keys
                    if key in self.tasks
                },
            )
            end = time()
            middle = (start + end) / 2

            self._update_latency(end - start)

            if response["status"] == "missing":
                # If running, wait up to 0.5s and then re-register self.
                # Otherwise just exit.
                start = time()
                while self.status in Status.ANY_RUNNING and time() < start + 0.5:
                    await asyncio.sleep(0.01)
                if self.status in Status.ANY_RUNNING:
                    await self._register_with_scheduler()
                return

            self.scheduler_delay = response["time"] - middle
            self.periodic_callbacks["heartbeat"].callback_time = (
                response["heartbeat-interval"] * 1000
            )
            self.bandwidth_workers.clear()
            self.bandwidth_types.clear()
        except CommClosedError:
            logger.warning("Heartbeat to scheduler failed", exc_info=True)
            if not self.reconnect:
                await self.close(report=False)
        except OSError as e:
            # Scheduler is gone. Respect distributed.comm.timeouts.connect
            if "Timed out trying to connect" in str(e):
                await self.close(report=False)
            else:
                raise e
        finally:
            self.heartbeat_active = False

    async def handle_scheduler(self, comm):
        try:
            await self.handle_stream(
                comm, every_cycle=[self.ensure_communicating, self.ensure_computing]
            )
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            if self.reconnect and self.status in Status.ANY_RUNNING:
                logger.info("Connection to scheduler broken.  Reconnecting...")
                self.loop.add_callback(self.heartbeat)
            else:
                await self.close(report=False)

    def start_ipython(self, comm):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from distributed._ipython_utils import start_ipython

        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip, ns={"worker": self}, log=logger
            )
        return self._ipython_kernel.get_connection_info()

    async def upload_file(self, comm, filename=None, data=None, load=True):
        out_filename = os.path.join(self.local_directory, filename)

        def func(data):
            if isinstance(data, str):
                data = data.encode()
            with open(out_filename, "wb") as f:
                f.write(data)
                f.flush()
            return data

        if len(data) < 10000:
            data = func(data)
        else:
            data = await offload(func, data)

        if load:
            try:
                import_file(out_filename)
                cache_loads.data.clear()
            except Exception as e:
                logger.exception(e)
                raise e

        return {"status": "OK", "nbytes": len(data)}

    def keys(self) -> list[str]:
        return list(self.data)

    async def gather(self, who_has: dict[str, list[str]]) -> dict[str, Any]:
        who_has = {
            k: [coerce_to_address(addr) for addr in v]
            for k, v in who_has.items()
            if k not in self.data
        }
        result, missing_keys, missing_workers = await gather_from_workers(
            who_has, rpc=self.rpc, who=self.address
        )
        self.update_data(data=result, report=False)
        if missing_keys:
            logger.warning(
                "Could not find data: %s on workers: %s (who_has: %s)",
                missing_keys,
                missing_workers,
                who_has,
            )
            return {"status": "partial-fail", "keys": missing_keys}
        else:
            return {"status": "OK"}

    def get_monitor_info(
        self, recent: bool = False, start: float = 0
    ) -> dict[str, Any]:
        result = dict(
            range_query=(
                self.monitor.recent()
                if recent
                else self.monitor.range_query(start=start)
            ),
            count=self.monitor.count,
            last_time=self.monitor.last_time,
        )
        if nvml.device_get_count() > 0:
            result["gpu_name"] = self.monitor.gpu_name
            result["gpu_memory_total"] = self.monitor.gpu_memory_total
        return result

    #############
    # Lifecycle #
    #############

    async def start(self):
        if self.status and self.status in (
            Status.closed,
            Status.closing,
            Status.closing_gracefully,
        ):
            return
        assert self.status is Status.undefined, self.status

        await super().start()

        enable_gc_diagnosis()

        ports = parse_ports(self._start_port)
        for port in ports:
            start_address = address_from_user_args(
                host=self._start_host,
                port=port,
                interface=self._interface,
                protocol=self._protocol,
                security=self.security,
            )
            kwargs = self.security.get_listen_args("worker")
            if self._protocol in ("tcp", "tls"):
                kwargs = kwargs.copy()
                kwargs["default_host"] = get_ip(
                    get_address_host(self.scheduler.address)
                )
            try:
                await self.listen(start_address, **kwargs)
            except OSError as e:
                if len(ports) > 1 and e.errno == errno.EADDRINUSE:
                    continue
                else:
                    raise
            else:
                self._start_address = start_address
                break
        else:
            raise ValueError(
                f"Could not start Worker on host {self._start_host}"
                f"with port {self._start_port}"
            )

        # Start HTTP server associated with this Worker node
        routes = get_handlers(
            server=self,
            modules=dask.config.get("distributed.worker.http.routes"),
            prefix=self._http_prefix,
        )
        self.start_http_server(routes, self._dashboard_address)
        if self._dashboard:
            try:
                import distributed.dashboard.worker
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                distributed.dashboard.worker.connect(
                    self.http_application,
                    self.http_server,
                    self,
                    prefix=self._http_prefix,
                )
        self.ip = get_address_host(self.address)

        if self.name is None:
            self.name = self.address

        for preload in self.preloads:
            await preload.start()

        # Services listen on all addresses
        # Note Nanny is not a "real" service, just some metadata
        # passed in service_ports...
        self.start_services(self.ip)

        try:
            listening_address = "%s%s:%d" % (self.listener.prefix, self.ip, self.port)
        except Exception:
            listening_address = f"{self.listener.prefix}{self.ip}"

        logger.info("      Start worker at: %26s", self.address)
        logger.info("         Listening to: %26s", listening_address)
        for k, v in self.service_ports.items():
            logger.info("  {:>16} at: {:>26}".format(k, self.ip + ":" + str(v)))
        logger.info("Waiting to connect to: %26s", self.scheduler.address)
        logger.info("-" * 49)
        logger.info("              Threads: %26d", self.nthreads)
        if self.memory_manager.memory_limit:
            logger.info(
                "               Memory: %26s",
                format_bytes(self.memory_manager.memory_limit),
            )
        logger.info("      Local Directory: %26s", self.local_directory)

        setproctitle("dask-worker [%s]" % self.address)

        plugins_msgs = await asyncio.gather(
            *(
                self.plugin_add(plugin=plugin, catch_errors=False)
                for plugin in self._pending_plugins
            ),
            return_exceptions=True,
        )
        plugins_exceptions = [msg for msg in plugins_msgs if isinstance(msg, Exception)]
        if len(plugins_exceptions) >= 1:
            if len(plugins_exceptions) > 1:
                logger.error(
                    "Multiple plugin exceptions raised. All exceptions will be logged, the first is raised."
                )
                for exc in plugins_exceptions:
                    logger.error(repr(exc))
            raise plugins_exceptions[0]

        self._pending_plugins = ()

        await self._register_with_scheduler()

        self.start_periodic_callbacks()
        return self

    def _close(self, *args, **kwargs):
        warnings.warn("Worker._close has moved to Worker.close", stacklevel=2)
        return self.close(*args, **kwargs)

    async def close(
        self, report=True, timeout=30, nanny=True, executor_wait=True, safe=False
    ):
        with log_errors():
            if self.status in (Status.closed, Status.closing):
                await self.finished()
                return

            self.reconnect = False
            disable_gc_diagnosis()

            try:
                logger.info("Stopping worker at %s", self.address)
            except ValueError:  # address not available if already closed
                logger.info("Stopping worker")
            if self.status not in Status.ANY_RUNNING:
                logger.info("Closed worker has not yet started: %s", self.status)
            self.status = Status.closing

            for preload in self.preloads:
                await preload.teardown()

            if nanny and self.nanny:
                with self.rpc(self.nanny) as r:
                    await r.close_gracefully()

            setproctitle("dask-worker [closing]")

            teardowns = [
                plugin.teardown(self)
                for plugin in self.plugins.values()
                if hasattr(plugin, "teardown")
            ]

            await asyncio.gather(*(td for td in teardowns if isawaitable(td)))

            for pc in self.periodic_callbacks.values():
                pc.stop()

            if self._client:
                # If this worker is the last one alive, clean up the worker
                # initialized clients
                if not any(
                    w
                    for w in Worker._instances
                    if w != self and w.status in Status.ANY_RUNNING
                ):
                    for c in Worker._initialized_clients:
                        # Regardless of what the client was initialized with
                        # we'll require the result as a future. This is
                        # necessary since the heursitics of asynchronous are not
                        # reliable and we might deadlock here
                        c._asynchronous = True
                        if c.asynchronous:
                            await c.close()
                        else:
                            # There is still the chance that even with us
                            # telling the client to be async, itself will decide
                            # otherwise
                            c.close()

            with suppress(EnvironmentError, TimeoutError):
                if report and self.contact_address is not None:
                    await asyncio.wait_for(
                        self.scheduler.unregister(
                            address=self.contact_address, safe=safe
                        ),
                        timeout,
                    )
            await self.scheduler.close_rpc()
            self._workdir.release()

            self.stop_services()

            # Give some time for a UCX scheduler to complete closing endpoints
            # before closing self.batched_stream, otherwise the local endpoint
            # may be closed too early and errors be raised on the scheduler when
            # trying to send closing message.
            if self._protocol == "ucx":  # pragma: no cover
                await asyncio.sleep(0.2)

            if (
                self.batched_stream
                and self.batched_stream.comm
                and not self.batched_stream.comm.closed()
            ):
                self.batched_stream.send({"op": "close-stream"})

            if self.batched_stream:
                with suppress(TimeoutError):
                    await self.batched_stream.close(timedelta(seconds=timeout))

            for executor in self.executors.values():
                if executor is utils._offload_executor:
                    continue  # Never shutdown the offload executor
                if isinstance(executor, ThreadPoolExecutor):
                    executor._work_queue.queue.clear()
                    executor.shutdown(wait=executor_wait, timeout=timeout)
                else:
                    executor.shutdown(wait=executor_wait)

            self.stop()
            await self.rpc.close()

            self.status = Status.closed
            await super().close()

            setproctitle("dask-worker [closed]")
        return "OK"

    async def close_gracefully(self, restart=None):
        """Gracefully shut down a worker

        This first informs the scheduler that we're shutting down, and asks it
        to move our data elsewhere.  Afterwards, we close as normal
        """
        if self.status in (Status.closing, Status.closing_gracefully):
            await self.finished()

        if self.status == Status.closed:
            return

        if restart is None:
            restart = self.lifetime_restart

        logger.info("Closing worker gracefully: %s", self.address)
        # Wait for all tasks to leave the worker and don't accept any new ones.
        # Scheduler.retire_workers will set the status to closing_gracefully and push it
        # back to this worker.
        await self.scheduler.retire_workers(
            workers=[self.address], close_workers=False, remove=False
        )
        await self.close(safe=True, nanny=not restart)

    async def terminate(self, report: bool = True, **kwargs) -> str:
        await self.close(report=report, **kwargs)
        return "OK"

    async def wait_until_closed(self):
        warnings.warn("wait_until_closed has moved to finished()")
        await self.finished()
        assert self.status == Status.closed

    ################
    # Worker Peers #
    ################

    def send_to_worker(self, address, msg):
        if address not in self.stream_comms:
            bcomm = BatchedSend(interval="1ms", loop=self.loop)
            self.stream_comms[address] = bcomm

            async def batched_send_connect():
                comm = await connect(
                    address, **self.connection_args  # TODO, serialization
                )
                comm.name = "Worker->Worker"
                await comm.write({"op": "connection_stream"})

                bcomm.start(comm)

            self.loop.add_callback(batched_send_connect)

        self.stream_comms[address].send(msg)

    async def get_data(
        self, comm, keys=None, who=None, serializers=None, max_connections=None
    ):
        start = time()

        if max_connections is None:
            max_connections = self.total_in_connections

        # Allow same-host connections more liberally
        if (
            max_connections
            and comm
            and get_address_host(comm.peer_address) == get_address_host(self.address)
        ):
            max_connections = max_connections * 2

        if self.status == Status.paused:
            max_connections = 1
            throttle_msg = " Throttling outgoing connections because worker is paused."
        else:
            throttle_msg = ""

        if (
            max_connections is not False
            and self.outgoing_current_count >= max_connections
        ):
            logger.debug(
                "Worker %s has too many open connections to respond to data request "
                "from %s (%d/%d).%s",
                self.address,
                who,
                self.outgoing_current_count,
                max_connections,
                throttle_msg,
            )
            return {"status": "busy"}

        self.outgoing_current_count += 1
        data = {k: self.data[k] for k in keys if k in self.data}

        if len(data) < len(keys):
            for k in set(keys) - set(data):
                if k in self.actors:
                    from distributed.actor import Actor

                    data[k] = Actor(type(self.actors[k]), self.address, k, worker=self)

        msg = {"status": "OK", "data": {k: to_serialize(v) for k, v in data.items()}}
        nbytes = {k: self.tasks[k].nbytes for k in data if k in self.tasks}
        stop = time()
        if self.digests is not None:
            self.digests["get-data-load-duration"].add(stop - start)
        start = time()

        try:
            compressed = await comm.write(msg, serializers=serializers)
            response = await comm.read(deserializers=serializers)
            assert response == "OK", response
        except OSError:
            logger.exception(
                "failed during get data with %s -> %s", self.address, who, exc_info=True
            )
            comm.abort()
            raise
        finally:
            self.outgoing_current_count -= 1
        stop = time()
        if self.digests is not None:
            self.digests["get-data-send-duration"].add(stop - start)

        total_bytes = sum(filter(None, nbytes.values()))

        self.outgoing_count += 1
        duration = (stop - start) or 0.5  # windows
        self.outgoing_transfer_log.append(
            {
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "middle": (start + stop) / 2,
                "duration": duration,
                "who": who,
                "keys": nbytes,
                "total": total_bytes,
                "compressed": compressed,
                "bandwidth": total_bytes / duration,
            }
        )

        return Status.dont_reply

    ###################
    # Local Execution #
    ###################

    def update_data(
        self,
        data: dict[str, object],
        report: bool = True,
        stimulus_id: str = None,
    ) -> dict[str, Any]:
        if stimulus_id is None:
            stimulus_id = f"update-data-{time()}"
        recommendations: Recs = {}
        instructions: Instructions = []
        for key, value in data.items():
            try:
                ts = self.tasks[key]
                recommendations[ts] = ("memory", value)
            except KeyError:
                self.tasks[key] = ts = TaskState(key)

                try:
                    recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
                except Exception as e:
                    msg = error_message(e)
                    recommendations = {ts: tuple(msg.values())}
                else:
                    recommendations.update(recs)

            self.log.append((key, "receive-from-scatter", stimulus_id, time()))

        if report:
            instructions.append(AddKeysMsg(keys=list(data), stimulus_id=stimulus_id))

        self.transitions(recommendations, stimulus_id=stimulus_id)
        self._handle_instructions(instructions)
        return {"nbytes": {k: sizeof(v) for k, v in data.items()}, "status": "OK"}

    def handle_free_keys(self, keys: list[str], stimulus_id: str) -> None:
        """
        Handler to be called by the scheduler.

        The given keys are no longer referred to and required by the scheduler.
        The worker is now allowed to release the key, if applicable.

        This does not guarantee that the memory is released since the worker may
        still decide to hold on to the data and task since it is required by an
        upstream dependency.
        """
        self.log.append(("free-keys", keys, stimulus_id, time()))
        recommendations: Recs = {}
        for key in keys:
            ts = self.tasks.get(key)
            if ts:
                recommendations[ts] = "released"

        self.transitions(recommendations, stimulus_id=stimulus_id)

    def handle_remove_replicas(self, keys: list[str], stimulus_id: str) -> str:
        """Stream handler notifying the worker that it might be holding unreferenced,
        superfluous data.

        This should not actually happen during ordinary operations and is only intended
        to correct any erroneous state. An example where this is necessary is if a
        worker fetches data for a downstream task but that task is released before the
        data arrives. In this case, the scheduler will notify the worker that it may be
        holding this unnecessary data, if the worker hasn't released the data itself,
        already.

        This handler does not guarantee the task nor the data to be actually
        released but only asks the worker to release the data on a best effort
        guarantee. This protects from race conditions where the given keys may
        already have been rescheduled for compute in which case the compute
        would win and this handler is ignored.

        For stronger guarantees, see handler free_keys
        """
        self.log.append(("remove-replicas", keys, stimulus_id, time()))
        recommendations: Recs = {}

        rejected = []
        for key in keys:
            ts = self.tasks.get(key)
            if ts is None or ts.state != "memory":
                continue
            if not ts.is_protected():
                self.log.append(
                    (ts.key, "remove-replica-confirmed", stimulus_id, time())
                )
                recommendations[ts] = "released"
            else:
                rejected.append(key)

        if rejected:
            self.log.append(("remove-replica-rejected", rejected, stimulus_id, time()))
            smsg = AddKeysMsg(keys=rejected, stimulus_id=stimulus_id)
            self._handle_instructions([smsg])

        self.transitions(recommendations, stimulus_id=stimulus_id)

        return "OK"

    async def set_resources(self, **resources) -> None:
        for r, quantity in resources.items():
            if r in self.total_resources:
                self.available_resources[r] += quantity - self.total_resources[r]
            else:
                self.available_resources[r] = quantity
            self.total_resources[r] = quantity

        await retry_operation(
            self.scheduler.set_resources,
            resources=self.total_resources,
            worker=self.contact_address,
        )

    ###################
    # Task Management #
    ###################

    def handle_cancel_compute(self, key: str, stimulus_id: str) -> None:
        """
        Cancel a task on a best effort basis. This is only possible while a task
        is in state `waiting` or `ready`.
        Nothing will happen otherwise.
        """
        ts = self.tasks.get(key)
        if ts and ts.state in READY | {"waiting"}:
            self.log.append((key, "cancel-compute", stimulus_id, time()))
            # All possible dependents of TS should not be in state Processing on
            # scheduler side and therefore should not be assigned to a worker,
            # yet.
            assert not ts.dependents
            self.transition(ts, "released", stimulus_id=stimulus_id)

    def handle_acquire_replicas(
        self,
        *,
        keys: Collection[str],
        who_has: dict[str, Collection[str]],
        stimulus_id: str,
    ) -> None:
        recommendations: Recs = {}
        for key in keys:
            ts = self.ensure_task_exists(
                key=key,
                # Transfer this data after all dependency tasks of computations with
                # default or explicitly high (>0) user priority and before all
                # computations with low priority (<0). Note that the priority= parameter
                # of compute() is multiplied by -1 before it reaches TaskState.priority.
                priority=(1,),
                stimulus_id=stimulus_id,
            )
            if ts.state != "memory":
                recommendations[ts] = "fetch"

        self.update_who_has(who_has)
        self.transitions(recommendations, stimulus_id=stimulus_id)

    def ensure_task_exists(
        self, key: str, *, priority: tuple[int, ...], stimulus_id: str
    ) -> TaskState:
        try:
            ts = self.tasks[key]
            logger.debug("Data task %s already known (stimulus_id=%s)", ts, stimulus_id)
        except KeyError:
            self.tasks[key] = ts = TaskState(key)
        if not ts.priority:
            assert priority
            ts.priority = priority

        self.log.append((key, "ensure-task-exists", ts.state, stimulus_id, time()))
        return ts

    def handle_compute_task(
        self,
        *,
        key: str,
        who_has: dict[str, Collection[str]],
        priority: tuple[int, ...],
        duration: float,
        function=None,
        args=None,
        kwargs=None,
        task=no_value,  # distributed.scheduler.TaskState.run_spec
        nbytes: dict[str, int] | None = None,
        resource_restrictions: dict[str, float] | None = None,
        actor: bool = False,
        annotations: dict | None = None,
        stimulus_id: str,
    ) -> None:
        self.log.append((key, "compute-task", stimulus_id, time()))
        try:
            ts = self.tasks[key]
            logger.debug(
                "Asked to compute an already known task %s",
                {"task": ts, "stimulus_id": stimulus_id},
            )
        except KeyError:
            self.tasks[key] = ts = TaskState(key)

        ts.run_spec = SerializedTask(function, args, kwargs, task)

        assert isinstance(priority, tuple)
        priority = priority + (self.generation,)
        self.generation -= 1

        if actor:
            self.actors[ts.key] = None

        ts.exception = None
        ts.traceback = None
        ts.exception_text = ""
        ts.traceback_text = ""
        ts.priority = priority
        ts.duration = duration
        if resource_restrictions:
            ts.resource_restrictions = resource_restrictions
        ts.annotations = annotations

        recommendations: Recs = {}
        instructions: Instructions = []
        for dependency in who_has:
            dep_ts = self.ensure_task_exists(
                key=dependency,
                priority=priority,
                stimulus_id=stimulus_id,
            )

            # link up to child / parents
            ts.dependencies.add(dep_ts)
            dep_ts.dependents.add(ts)

        if ts.state in READY | {"executing", "waiting", "resumed"}:
            pass
        elif ts.state == "memory":
            recommendations[ts] = "memory"
            instructions.append(self._get_task_finished_msg(ts))
        elif ts.state in {
            "released",
            "fetch",
            "flight",
            "missing",
            "cancelled",
            "error",
        }:
            recommendations[ts] = "waiting"
        else:  # pragma: no cover
            raise RuntimeError(f"Unexpected task state encountered {ts} {stimulus_id}")

        self._handle_instructions(instructions)
        self.update_who_has(who_has)
        self.transitions(recommendations, stimulus_id=stimulus_id)

        if nbytes is not None:
            for key, value in nbytes.items():
                self.tasks[key].nbytes = value

    def transition_missing_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "missing"
            assert ts.priority is not None

        self._missing_dep_flight.discard(ts)
        ts.state = "fetch"
        ts.done = False
        self.data_needed.push(ts)
        return {}, []

    def transition_missing_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        self._missing_dep_flight.discard(ts)
        recs, instructions = self.transition_generic_released(
            ts, stimulus_id=stimulus_id
        )
        assert ts.key in self.tasks
        return recs, instructions

    def transition_flight_missing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        assert ts.done
        ts.state = "missing"
        self._missing_dep_flight.add(ts)
        ts.done = False
        return {}, []

    def transition_released_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "released"
            assert ts.priority is not None
        for w in ts.who_has:
            self.pending_data_per_worker[w].push(ts)
        ts.state = "fetch"
        ts.done = False
        self.data_needed.push(ts)
        return {}, []

    def transition_generic_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        self.release_key(ts.key, stimulus_id=stimulus_id)
        recs: Recs = {}
        for dependency in ts.dependencies:
            if (
                not dependency.waiters
                and dependency.state not in READY | PROCESSING | {"memory"}
            ):
                recs[dependency] = "released"

        if not ts.dependents:
            recs[ts] = "forgotten"

        return recs, []

    def transition_released_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "released"
            assert all(d.key in self.tasks for d in ts.dependencies)

        recommendations: Recs = {}
        ts.waiting_for_data.clear()
        for dep_ts in ts.dependencies:
            if dep_ts.state != "memory":
                ts.waiting_for_data.add(dep_ts)
                dep_ts.waiters.add(ts)
                if dep_ts.state not in {"fetch", "flight"}:
                    recommendations[dep_ts] = "fetch"

        if ts.waiting_for_data:
            self.waiting_for_data_count += 1
        elif ts.resource_restrictions:
            recommendations[ts] = "constrained"
        else:
            recommendations[ts] = "ready"

        ts.state = "waiting"
        return recommendations, []

    def transition_fetch_flight(
        self, ts: TaskState, worker, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "fetch"
            assert ts.who_has

        ts.done = False
        ts.state = "flight"
        ts.coming_from = worker
        self._in_flight_tasks.add(ts)
        return {}, []

    def transition_memory_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        recs, instructions = self.transition_generic_released(
            ts, stimulus_id=stimulus_id
        )
        instructions.append(ReleaseWorkerDataMsg(ts.key))
        return recs, instructions

    def transition_waiting_constrained(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "waiting"
            assert not ts.waiting_for_data
            assert all(
                dep.key in self.data or dep.key in self.actors
                for dep in ts.dependencies
            )
            assert all(dep.state == "memory" for dep in ts.dependencies)
            assert ts.key not in self.ready
        ts.state = "constrained"
        self.constrained.append(ts.key)
        return {}, []

    def transition_long_running_rescheduled(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        recs: Recs = {ts: "released"}
        smsg = RescheduleMsg(key=ts.key, worker=self.address)
        return recs, [smsg]

    def transition_executing_rescheduled(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        for resource, quantity in ts.resource_restrictions.items():
            self.available_resources[resource] += quantity
        self._executing.discard(ts)

        recs: Recs = {ts: "released"}
        smsg = RescheduleMsg(key=ts.key, worker=self.address)
        return recs, [smsg]

    def transition_waiting_ready(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "waiting"
            assert ts.key not in self.ready
            assert not ts.waiting_for_data
            for dep in ts.dependencies:
                assert dep.key in self.data or dep.key in self.actors
                assert dep.state == "memory"

        ts.state = "ready"
        assert ts.priority is not None
        heapq.heappush(self.ready, (ts.priority, ts.key))

        return {}, []

    def transition_cancelled_error(
        self,
        ts: TaskState,
        exception,
        traceback,
        exception_text,
        traceback_text,
        *,
        stimulus_id: str,
    ) -> tuple[Recs, Instructions]:
        recs: Recs = {}
        instructions: Instructions = []
        if ts._previous == "executing":
            recs, instructions = self.transition_executing_error(
                ts,
                exception,
                traceback,
                exception_text,
                traceback_text,
                stimulus_id=stimulus_id,
            )
        elif ts._previous == "flight":
            recs, instructions = self.transition_flight_error(
                ts,
                exception,
                traceback,
                exception_text,
                traceback_text,
                stimulus_id=stimulus_id,
            )
        if ts._next:
            recs[ts] = ts._next
        return recs, instructions

    def transition_generic_error(
        self,
        ts: TaskState,
        exception: Exception,
        traceback: object,
        exception_text: str,
        traceback_text: str,
        *,
        stimulus_id: str,
    ) -> tuple[Recs, Instructions]:
        ts.exception = exception
        ts.traceback = traceback
        ts.exception_text = exception_text
        ts.traceback_text = traceback_text
        ts.state = "error"
        smsg = TaskErredMsg(
            key=ts.key,
            exception=ts.exception,
            traceback=ts.traceback,
            exception_text=ts.exception_text,
            traceback_text=ts.traceback_text,
            thread=self.threads.get(ts.key),
            startstops=ts.startstops,
        )

        return {}, [smsg]

    def transition_executing_error(
        self,
        ts: TaskState,
        exception,
        traceback,
        exception_text,
        traceback_text,
        *,
        stimulus_id: str,
    ) -> tuple[Recs, Instructions]:
        for resource, quantity in ts.resource_restrictions.items():
            self.available_resources[resource] += quantity
        self._executing.discard(ts)
        return self.transition_generic_error(
            ts,
            exception,
            traceback,
            exception_text,
            traceback_text,
            stimulus_id=stimulus_id,
        )

    def _transition_from_resumed(
        self, ts: TaskState, finish: TaskStateState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        """`resumed` is an intermediate degenerate state which splits further up
        into two states depending on what the last signal / next state is
        intended to be. There are only two viable choices depending on whether
        the task is required to be fetched from another worker `resumed(fetch)`
        or the task shall be computed on this worker `resumed(waiting)`.

        The only viable state transitions ending up here are

        flight -> cancelled -> resumed(waiting)

        or

        executing -> cancelled -> resumed(fetch)

        depending on the origin. Equally, only `fetch`, `waiting` or `released`
        are allowed output states.

        See also `transition_resumed_waiting`
        """
        recs: Recs = {}
        instructions: Instructions = []
        if ts.done:
            next_state = ts._next
            # if the next state is already intended to be waiting or if the
            # coro/thread is still running (ts.done==False), this is a noop
            if ts._next != finish:
                recs, instructions = self.transition_generic_released(
                    ts, stimulus_id=stimulus_id
                )
            assert next_state
            recs[ts] = next_state
        else:
            ts._next = finish
        return recs, instructions

    def transition_resumed_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        """
        See Worker._transition_from_resumed
        """
        return self._transition_from_resumed(ts, "fetch", stimulus_id=stimulus_id)

    def transition_resumed_missing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        """
        See Worker._transition_from_resumed
        """
        return self._transition_from_resumed(ts, "missing", stimulus_id=stimulus_id)

    def transition_resumed_waiting(self, ts: TaskState, *, stimulus_id: str):
        """
        See Worker._transition_from_resumed
        """
        return self._transition_from_resumed(ts, "waiting", stimulus_id=stimulus_id)

    def transition_cancelled_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if ts.done:
            return {ts: "released"}, []
        elif ts._previous == "flight":
            ts.state = ts._previous
            return {}, []
        else:
            assert ts._previous == "executing"
            return {ts: ("resumed", "fetch")}, []

    def transition_cancelled_resumed(
        self, ts: TaskState, next: TaskStateState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        ts._next = next
        ts.state = "resumed"
        return {}, []

    def transition_cancelled_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if ts.done:
            return {ts: "released"}, []
        elif ts._previous == "executing":
            ts.state = ts._previous
            return {}, []
        else:
            assert ts._previous == "flight"
            return {ts: ("resumed", "waiting")}, []

    def transition_cancelled_forgotten(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        ts._next = "forgotten"
        if not ts.done:
            return {}, []
        return {ts: "released"}, []

    def transition_cancelled_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if not ts.done:
            ts._next = "released"
            return {}, []
        next_state = ts._next
        assert next_state
        self._executing.discard(ts)
        self._in_flight_tasks.discard(ts)

        for resource, quantity in ts.resource_restrictions.items():
            self.available_resources[resource] += quantity
        recs, instructions = self.transition_generic_released(
            ts, stimulus_id=stimulus_id
        )
        if next_state != "released":
            recs[ts] = next_state
        return recs, instructions

    def transition_executing_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        ts._previous = ts.state
        ts._next = "released"
        # See https://github.com/dask/distributed/pull/5046#discussion_r685093940
        ts.state = "cancelled"
        ts.done = False
        return {}, []

    def transition_long_running_memory(
        self, ts: TaskState, value=no_value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        self.executed_count += 1
        return self.transition_generic_memory(ts, value=value, stimulus_id=stimulus_id)

    def transition_generic_memory(
        self, ts: TaskState, value=no_value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if value is no_value and ts.key not in self.data:
            raise RuntimeError(
                f"Tried to transition task {ts} to `memory` without data available"
            )

        if ts.resource_restrictions is not None:
            for resource, quantity in ts.resource_restrictions.items():
                self.available_resources[resource] += quantity

        self._executing.discard(ts)
        self._in_flight_tasks.discard(ts)
        ts.coming_from = None
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
            return recs, []
        if self.validate:
            assert ts.key in self.data or ts.key in self.actors
        smsg = self._get_task_finished_msg(ts)
        return recs, [smsg]

    def transition_executing_memory(
        self, ts: TaskState, value=no_value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert ts.state == "executing" or ts.key in self.long_running
            assert not ts.waiting_for_data
            assert ts.key not in self.ready

        self._executing.discard(ts)
        self.executed_count += 1
        return self.transition_generic_memory(ts, value=value, stimulus_id=stimulus_id)

    def transition_constrained_executing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert not ts.waiting_for_data
            assert ts.key not in self.data
            assert ts.state in READY
            assert ts.key not in self.ready
            for dep in ts.dependencies:
                assert dep.key in self.data or dep.key in self.actors

        for resource, quantity in ts.resource_restrictions.items():
            self.available_resources[resource] -= quantity
        ts.state = "executing"
        self._executing.add(ts)
        self.loop.add_callback(self.execute, ts.key, stimulus_id=stimulus_id)
        return {}, []

    def transition_ready_executing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if self.validate:
            assert not ts.waiting_for_data
            assert ts.key not in self.data
            assert ts.state in READY
            assert ts.key not in self.ready
            assert all(
                dep.key in self.data or dep.key in self.actors
                for dep in ts.dependencies
            )

        ts.state = "executing"
        self._executing.add(ts)
        self.loop.add_callback(self.execute, ts.key, stimulus_id=stimulus_id)
        return {}, []

    def transition_flight_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        # If this transition is called after the flight coroutine has finished,
        # we can reset the task and transition to fetch again. If it is not yet
        # finished, this should be a no-op
        if not ts.done:
            return {}, []

        recommendations: Recs = {}
        ts.state = "fetch"
        ts.coming_from = None
        ts.done = False
        if not ts.who_has:
            recommendations[ts] = "missing"
        else:
            self.data_needed.push(ts)
            for w in ts.who_has:
                self.pending_data_per_worker[w].push(ts)
        return recommendations, []

    def transition_flight_error(
        self,
        ts: TaskState,
        exception,
        traceback,
        exception_text,
        traceback_text,
        *,
        stimulus_id: str,
    ) -> tuple[Recs, Instructions]:
        self._in_flight_tasks.discard(ts)
        ts.coming_from = None
        return self.transition_generic_error(
            ts,
            exception,
            traceback,
            exception_text,
            traceback_text,
            stimulus_id=stimulus_id,
        )

    def transition_flight_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        if ts.done:
            # FIXME: Is this even possible? Would an assert instead be more
            # sensible?
            return self.transition_generic_released(ts, stimulus_id=stimulus_id)
        else:
            ts._previous = "flight"
            ts._next = "released"
            # See https://github.com/dask/distributed/pull/5046#discussion_r685093940
            ts.state = "cancelled"
            return {}, []

    def transition_cancelled_memory(
        self, ts: TaskState, value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        assert ts._next
        return {ts: ts._next}, []

    def transition_executing_long_running(
        self, ts: TaskState, compute_duration: float, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        ts.state = "long-running"
        self._executing.discard(ts)
        self.long_running.add(ts.key)
        smsg = LongRunningMsg(key=ts.key, compute_duration=compute_duration)
        self.io_loop.add_callback(self.ensure_computing)
        return {}, [smsg]

    def transition_released_memory(
        self, ts: TaskState, value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
            return recs, []
        smsg = AddKeysMsg(keys=[ts.key], stimulus_id=stimulus_id)
        return recs, [smsg]

    def transition_flight_memory(
        self, ts: TaskState, value, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        self._in_flight_tasks.discard(ts)
        ts.coming_from = None
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
            return recs, []
        smsg = AddKeysMsg(keys=[ts.key], stimulus_id=stimulus_id)
        return recs, [smsg]

    def transition_released_forgotten(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Recs, Instructions]:
        recommendations: Recs = {}
        # Dependents _should_ be released by the scheduler before this
        if self.validate:
            assert not any(d.state != "forgotten" for d in ts.dependents)
        for dep in ts.dependencies:
            dep.dependents.discard(ts)
            if dep.state == "released" and not dep.dependents:
                recommendations[dep] = "forgotten"

        # Mark state as forgotten in case it is still referenced
        ts.state = "forgotten"
        self.tasks.pop(ts.key, None)
        return recommendations, []

    def _transition(
        self, ts: TaskState, finish: str | tuple, *args, stimulus_id: str, **kwargs
    ) -> tuple[Recs, Instructions]:
        if isinstance(finish, tuple):
            # the concatenated transition path might need to access the tuple
            assert not args
            finish, *args = finish  # type: ignore

        if ts is None or ts.state == finish:
            return {}, []

        start = ts.state
        func = self._transitions_table.get((start, cast(str, finish)))

        if func is not None:
            self._transition_counter += 1
            recs, instructions = func(ts, *args, stimulus_id=stimulus_id, **kwargs)
            self._notify_plugins("transition", ts.key, start, finish, **kwargs)

        elif "released" not in (start, finish):
            # start -> "released" -> finish
            try:
                recs, instructions = self._transition(
                    ts, "released", stimulus_id=stimulus_id
                )
                v = recs.get(ts, (finish, *args))
                v_state: str
                v_args: list | tuple
                if isinstance(v, tuple):
                    v_state, *v_args = v
                else:
                    v_state, v_args = v, ()
                b_recs, b_instructions = self._transition(
                    ts, v_state, *v_args, stimulus_id=stimulus_id
                )
                recs.update(b_recs)
                instructions += b_instructions
            except InvalidTransition:
                raise InvalidTransition(
                    f"Impossible transition from {start} to {finish} for {ts.key}"
                ) from None

        else:
            raise InvalidTransition(
                f"Impossible transition from {start} to {finish} for {ts.key}"
            )

        self.log.append(
            (
                # key
                ts.key,
                # initial
                start,
                # recommended
                finish,
                # final
                ts.state,
                # new recommendations
                {ts.key: new for ts, new in recs.items()},
                stimulus_id,
                time(),
            )
        )
        return recs, instructions

    def transition(
        self, ts: TaskState, finish: str, *, stimulus_id: str, **kwargs
    ) -> None:
        """Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'processing'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        recs, instructions = self._transition(
            ts, finish, stimulus_id=stimulus_id, **kwargs
        )
        self._handle_instructions(instructions)
        self.transitions(recs, stimulus_id=stimulus_id)

    def transitions(self, recommendations: Recs, *, stimulus_id: str) -> None:
        """Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        instructions = []

        remaining_recs = recommendations.copy()
        tasks = set()
        while remaining_recs:
            ts, finish = remaining_recs.popitem()
            tasks.add(ts)
            a_recs, a_instructions = self._transition(
                ts, finish, stimulus_id=stimulus_id
            )

            remaining_recs.update(a_recs)
            instructions += a_instructions

        if self.validate:
            # Full state validation is very expensive
            for ts in tasks:
                self.validate_task(ts)

        if self.batched_stream.closed():
            logger.debug(
                "BatchedSend closed while transitioning tasks. %d tasks not sent.",
                len(instructions),
            )
        else:
            self._handle_instructions(instructions)

    def _handle_instructions(self, instructions: list[Instruction]) -> None:
        # TODO this method is temporary.
        #      See final design: https://github.com/dask/distributed/issues/5894
        for inst in instructions:
            if isinstance(inst, SendMessageToScheduler):
                self.batched_stream.send(inst.to_dict())
            else:
                raise TypeError(inst)  # pragma: nocover

    def maybe_transition_long_running(
        self, ts: TaskState, *, compute_duration: float, stimulus_id: str
    ):
        if ts.state == "executing":
            self.transition(
                ts,
                "long-running",
                compute_duration=compute_duration,
                stimulus_id=stimulus_id,
            )
            assert ts.state == "long-running"

    def stateof(self, key: str) -> dict[str, Any]:
        ts = self.tasks[key]
        return {
            "executing": ts.state == "executing",
            "waiting_for_data": bool(ts.waiting_for_data),
            "heap": key in pluck(1, self.ready),
            "data": key in self.data,
        }

    def story(self, *keys_or_tasks: str | TaskState) -> list[tuple]:
        keys = [e.key if isinstance(e, TaskState) else e for e in keys_or_tasks]
        return [
            msg
            for msg in self.log
            if any(key in msg for key in keys)
            or any(
                key in c
                for key in keys
                for c in msg
                if isinstance(c, (tuple, list, set))
            )
        ]

    def ensure_communicating(self) -> None:
        stimulus_id = f"ensure-communicating-{time()}"
        skipped_worker_in_flight = []

        while self.data_needed and (
            len(self.in_flight_workers) < self.total_out_connections
            or self.comm_nbytes < self.comm_threshold_bytes
        ):
            logger.debug(
                "Ensure communicating. Pending: %d. Connections: %d/%d",
                len(self.data_needed),
                len(self.in_flight_workers),
                self.total_out_connections,
            )

            ts = self.data_needed.pop()

            if ts.state != "fetch":
                continue

            workers = [w for w in ts.who_has if w not in self.in_flight_workers]
            if not workers:
                assert ts.priority is not None
                skipped_worker_in_flight.append(ts)
                continue

            host = get_address_host(self.address)
            local = [w for w in workers if get_address_host(w) == host]
            if local:
                worker = random.choice(local)
            else:
                worker = random.choice(list(workers))
            assert worker != self.address

            to_gather, total_nbytes = self.select_keys_for_gather(worker, ts.key)

            self.log.append(
                ("gather-dependencies", worker, to_gather, stimulus_id, time())
            )

            self.comm_nbytes += total_nbytes
            self.in_flight_workers[worker] = to_gather
            recommendations: Recs = {
                self.tasks[d]: ("flight", worker) for d in to_gather
            }
            self.transitions(recommendations, stimulus_id=stimulus_id)

            self.loop.add_callback(
                self.gather_dep,
                worker=worker,
                to_gather=to_gather,
                total_nbytes=total_nbytes,
                stimulus_id=stimulus_id,
            )

        for el in skipped_worker_in_flight:
            self.data_needed.push(el)

    def _get_task_finished_msg(self, ts: TaskState) -> TaskFinishedMsg:
        if ts.key not in self.data and ts.key not in self.actors:
            raise RuntimeError(f"Task {ts} not ready")
        typ = ts.type
        if ts.nbytes is None or typ is None:
            try:
                value = self.data[ts.key]
            except KeyError:
                value = self.actors[ts.key]
            ts.nbytes = sizeof(value)
            typ = ts.type = type(value)
            del value
        try:
            typ_serialized = dumps_function(typ)
        except PicklingError:
            # Some types fail pickling (example: _thread.lock objects),
            # send their name as a best effort.
            typ_serialized = pickle.dumps(typ.__name__, protocol=4)
        return TaskFinishedMsg(
            key=ts.key,
            nbytes=ts.nbytes,
            type=typ_serialized,
            typename=typename(typ),
            metadata=ts.metadata,
            thread=self.threads.get(ts.key),
            startstops=ts.startstops,
        )

    def _put_key_in_memory(self, ts: TaskState, value, *, stimulus_id: str) -> Recs:
        """
        Put a key into memory and set data related task state attributes.
        On success, generate recommendations for dependents.

        This method does not generate any scheduler messages since this method
        cannot distinguish whether it has to be an `add-task` or a
        `task-finished` signal. The caller is required to generate this message
        on success.

        Raises
        ------
        Exception:
            In case the data is put into the in memory buffer and a serialization error
            occurs during spilling, this raises that error. This has to be handled by
            the caller since most callers generate scheduler messages on success (see
            comment above) but we need to signal that this was not successful.

            Can only trigger if distributed.worker.memory.target is enabled, the value
            is individually larger than target * memory_limit, and the task is not an
            actor.
        """
        if ts.key in self.data:
            ts.state = "memory"
            return {}

        recommendations: Recs = {}
        if ts.key in self.actors:
            self.actors[ts.key] = value
        else:
            start = time()
            self.data[ts.key] = value
            stop = time()
            if stop - start > 0.020:
                ts.startstops.append(
                    {"action": "disk-write", "start": start, "stop": stop}
                )

        ts.state = "memory"
        if ts.nbytes is None:
            ts.nbytes = sizeof(value)

        ts.type = type(value)

        for dep in ts.dependents:
            dep.waiting_for_data.discard(ts)
            if not dep.waiting_for_data and dep.state == "waiting":
                self.waiting_for_data_count -= 1
                recommendations[dep] = "ready"

        self.log.append((ts.key, "put-in-memory", stimulus_id, time()))
        return recommendations

    def select_keys_for_gather(self, worker, dep):
        assert isinstance(dep, str)
        deps = {dep}

        total_bytes = self.tasks[dep].get_nbytes()
        L = self.pending_data_per_worker[worker]

        while L:
            ts = L.pop()
            if ts.state != "fetch":
                continue
            if total_bytes + ts.get_nbytes() > self.target_message_size:
                break
            deps.add(ts.key)
            total_bytes += ts.get_nbytes()

        return deps, total_bytes

    @property
    def total_comm_bytes(self):
        warnings.warn(
            "The attribute `Worker.total_comm_bytes` has been renamed to `comm_threshold_bytes`. "
            "Future versions will only support the new name.",
            FutureWarning,
        )
        return self.comm_threshold_bytes

    def _filter_deps_for_fetch(
        self, to_gather_keys: Iterable[str]
    ) -> tuple[set[str], set[str], TaskState | None]:
        """Filter a list of keys before scheduling coroutines to fetch data from workers.

        Returns
        -------
        in_flight_keys:
            The subset of keys in to_gather_keys in state `flight` or `resumed`
        cancelled_keys:
            The subset of tasks in to_gather_keys in state `cancelled` or `memory`
        cause:
            The task to attach startstops of this transfer to
        """
        in_flight_tasks: set[TaskState] = set()
        cancelled_keys: set[str] = set()
        for key in to_gather_keys:
            ts = self.tasks.get(key)
            if ts is None:
                continue

            # At this point, a task has been transitioned fetch->flight
            # flight is only allowed to be transitioned into
            # {memory, resumed, cancelled}
            # resumed and cancelled will block any further transition until this
            # coro has been finished

            if ts.state in ("flight", "resumed"):
                in_flight_tasks.add(ts)
            # If the key is already in memory, the fetch should not happen which
            # is signalled by the cancelled_keys
            elif ts.state in {"cancelled", "memory"}:
                cancelled_keys.add(key)
            else:
                raise RuntimeError(
                    f"Task {ts.key} found in illegal state {ts.state}. "
                    "Only states `flight`, `resumed` and `cancelled` possible."
                )

        # For diagnostics we want to attach the transfer to a single task. this
        # task is typically the next to be executed but since we're fetching
        # tasks for potentially many dependents, an exact match is not possible.
        # If there are no dependents, this is a pure replica fetch
        cause = None
        for ts in in_flight_tasks:
            if ts.dependents:
                cause = next(iter(ts.dependents))
                break
            else:
                cause = ts
        in_flight_keys = {ts.key for ts in in_flight_tasks}
        return in_flight_keys, cancelled_keys, cause

    def _update_metrics_received_data(
        self, start: float, stop: float, data: dict, cause: TaskState, worker: str
    ) -> None:

        total_bytes = sum(self.tasks[key].get_nbytes() for key in data)

        cause.startstops.append(
            {
                "action": "transfer",
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "source": worker,
            }
        )
        duration = (stop - start) or 0.010
        bandwidth = total_bytes / duration
        self.incoming_transfer_log.append(
            {
                "start": start + self.scheduler_delay,
                "stop": stop + self.scheduler_delay,
                "middle": (start + stop) / 2.0 + self.scheduler_delay,
                "duration": duration,
                "keys": {key: self.tasks[key].nbytes for key in data},
                "total": total_bytes,
                "bandwidth": bandwidth,
                "who": worker,
            }
        )
        if total_bytes > 1_000_000:
            self.bandwidth = self.bandwidth * 0.95 + bandwidth * 0.05
            bw, cnt = self.bandwidth_workers[worker]
            self.bandwidth_workers[worker] = (bw + bandwidth, cnt + 1)

            types = set(map(type, data.values()))
            if len(types) == 1:
                [typ] = types
                bw, cnt = self.bandwidth_types[typ]
                self.bandwidth_types[typ] = (bw + bandwidth, cnt + 1)

        if self.digests is not None:
            self.digests["transfer-bandwidth"].add(total_bytes / duration)
            self.digests["transfer-duration"].add(duration)
        self.counters["transfer-count"].add(len(data))
        self.incoming_count += 1

    async def gather_dep(
        self,
        worker: str,
        to_gather: Iterable[str],
        total_nbytes: int,
        *,
        stimulus_id: str,
    ) -> None:
        """Gather dependencies for a task from a worker who has them

        Parameters
        ----------
        worker : str
            Address of worker to gather dependencies from
        to_gather : list
            Keys of dependencies to gather from worker -- this is not
            necessarily equivalent to the full list of dependencies of ``dep``
            as some dependencies may already be present on this worker.
        total_nbytes : int
            Total number of bytes for all the dependencies in to_gather combined
        """
        if self.status not in Status.ANY_RUNNING:  # type: ignore
            return

        recommendations: Recs = {}
        with log_errors():
            response = {}
            to_gather_keys: set[str] = set()
            cancelled_keys: set[str] = set()
            try:
                to_gather_keys, cancelled_keys, cause = self._filter_deps_for_fetch(
                    to_gather
                )

                if not to_gather_keys:
                    self.log.append(
                        ("nothing-to-gather", worker, to_gather, stimulus_id, time())
                    )
                    return

                assert cause
                # Keep namespace clean since this func is long and has many
                # dep*, *ts* variables
                del to_gather

                self.log.append(
                    ("request-dep", worker, to_gather_keys, stimulus_id, time())
                )
                logger.debug(
                    "Request %d keys for task %s from %s",
                    len(to_gather_keys),
                    cause,
                    worker,
                )

                start = time()
                response = await get_data_from_worker(
                    self.rpc, to_gather_keys, worker, who=self.address
                )
                stop = time()
                if response["status"] == "busy":
                    return

                self._update_metrics_received_data(
                    start=start,
                    stop=stop,
                    data=response["data"],
                    cause=cause,
                    worker=worker,
                )
                self.log.append(
                    ("receive-dep", worker, set(response["data"]), stimulus_id, time())
                )

            except OSError:
                logger.exception("Worker stream died during communication: %s", worker)
                has_what = self.has_what.pop(worker)
                self.pending_data_per_worker.pop(worker)
                self.log.append(
                    ("receive-dep-failed", worker, has_what, stimulus_id, time())
                )
                for d in has_what:
                    ts = self.tasks[d]
                    ts.who_has.remove(worker)

            except Exception as e:
                logger.exception(e)
                if self.batched_stream and LOG_PDB:
                    import pdb

                    pdb.set_trace()
                msg = error_message(e)
                for k in self.in_flight_workers[worker]:
                    ts = self.tasks[k]
                    recommendations[ts] = tuple(msg.values())
                raise
            finally:
                self.comm_nbytes -= total_nbytes
                busy = response.get("status", "") == "busy"
                data = response.get("data", {})

                if busy:
                    self.log.append(
                        ("busy-gather", worker, to_gather_keys, stimulus_id, time())
                    )

                for d in self.in_flight_workers.pop(worker):
                    ts = self.tasks[d]
                    ts.done = True
                    if d in cancelled_keys:
                        if ts.state == "cancelled":
                            recommendations[ts] = "released"
                        else:
                            recommendations[ts] = "fetch"
                    elif d in data:
                        recommendations[ts] = ("memory", data[d])
                    elif busy:
                        recommendations[ts] = "fetch"
                    elif ts not in recommendations:
                        ts.who_has.discard(worker)
                        self.has_what[worker].discard(ts.key)
                        self.log.append((d, "missing-dep", stimulus_id, time()))
                        self.batched_stream.send(
                            {"op": "missing-data", "errant_worker": worker, "key": d}
                        )
                        recommendations[ts] = "fetch" if ts.who_has else "missing"
                del data, response
                self.transitions(recommendations, stimulus_id=stimulus_id)
                self.ensure_computing()

                if not busy:
                    self.repetitively_busy = 0
                else:
                    # Exponential backoff to avoid hammering scheduler/worker
                    self.repetitively_busy += 1
                    await asyncio.sleep(0.100 * 1.5**self.repetitively_busy)

                    await self.query_who_has(*to_gather_keys)

                self.ensure_communicating()

    async def find_missing(self) -> None:
        with log_errors():
            if not self._missing_dep_flight:
                return
            try:
                if self.validate:
                    for ts in self._missing_dep_flight:
                        assert not ts.who_has

                stimulus_id = f"find-missing-{time()}"
                who_has = await retry_operation(
                    self.scheduler.who_has,
                    keys=[ts.key for ts in self._missing_dep_flight],
                )
                who_has = {k: v for k, v in who_has.items() if v}
                self.update_who_has(who_has)
                recommendations: Recs = {}
                for ts in self._missing_dep_flight:
                    if ts.who_has:
                        recommendations[ts] = "fetch"
                self.transitions(recommendations, stimulus_id=stimulus_id)

            finally:
                # This is quite arbitrary but the heartbeat has scaling implemented
                self.periodic_callbacks[
                    "find-missing"
                ].callback_time = self.periodic_callbacks["heartbeat"].callback_time
                self.ensure_communicating()
                self.ensure_computing()

    async def query_who_has(self, *deps: str) -> dict[str, Collection[str]]:
        with log_errors():
            who_has = await retry_operation(self.scheduler.who_has, keys=deps)
            self.update_who_has(who_has)
            return who_has

    def update_who_has(self, who_has: dict[str, Collection[str]]) -> None:
        try:
            for dep, workers in who_has.items():
                if not workers:
                    continue

                if dep in self.tasks:
                    dep_ts = self.tasks[dep]
                    if self.address in workers and self.tasks[dep].state != "memory":
                        logger.debug(
                            "Scheduler claims worker %s holds data for task %s which is not true.",
                            self.name,
                            dep,
                        )
                        # Do not mutate the input dict. That's rude
                        workers = set(workers) - {self.address}
                    dep_ts.who_has.update(workers)

                    for worker in workers:
                        self.has_what[worker].add(dep)
                        self.pending_data_per_worker[worker].push(dep_ts)
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    def handle_steal_request(self, key: str, stimulus_id: str) -> None:
        # There may be a race condition between stealing and releasing a task.
        # In this case the self.tasks is already cleared. The `None` will be
        # registered as `already-computing` on the other end
        ts = self.tasks.get(key)
        state = ts.state if ts is not None else None

        response = {
            "op": "steal-response",
            "key": key,
            "state": state,
            "stimulus_id": stimulus_id,
        }
        self.batched_stream.send(response)

        if state in READY | {"waiting"}:
            assert ts
            # If task is marked as "constrained" we haven't yet assigned it an
            # `available_resources` to run on, that happens in
            # `transition_constrained_executing`
            self.transition(ts, "released", stimulus_id=stimulus_id)

    def handle_worker_status_change(self, status: str) -> None:
        new_status = Status.lookup[status]  # type: ignore

        if (
            new_status == Status.closing_gracefully
            and self._status not in Status.ANY_RUNNING  # type: ignore
        ):
            logger.error(
                "Invalid Worker.status transition: %s -> %s", self._status, new_status
            )
            # Reiterate the current status to the scheduler to restore sync
            self._send_worker_status_change()
        else:
            # Update status and send confirmation to the Scheduler (see status.setter)
            self.status = new_status

    def release_key(
        self,
        key: str,
        cause: TaskState | None = None,
        report: bool = True,
        *,
        stimulus_id: str,
    ) -> None:
        try:
            if self.validate:
                assert not isinstance(key, TaskState)
            ts = self.tasks[key]
            # needed for legacy notification support
            state_before = ts.state
            ts.state = "released"

            logger.debug(
                "Release key %s",
                {"key": key, "cause": cause, "stimulus_id": stimulus_id},
            )
            if cause:
                self.log.append(
                    (key, "release-key", {"cause": cause}, stimulus_id, time())
                )
            else:
                self.log.append((key, "release-key", stimulus_id, time()))
            if key in self.data:
                try:
                    del self.data[key]
                except FileNotFoundError:
                    logger.error("Tried to delete %s but no file found", exc_info=True)
            if key in self.actors:
                del self.actors[key]

            for worker in ts.who_has:
                self.has_what[worker].discard(ts.key)
            ts.who_has.clear()

            if key in self.threads:
                del self.threads[key]

            if ts.resource_restrictions is not None:
                if ts.state == "executing":
                    for resource, quantity in ts.resource_restrictions.items():
                        self.available_resources[resource] += quantity

            for d in ts.dependencies:
                ts.waiting_for_data.discard(d)
                d.waiters.discard(ts)

            ts.waiting_for_data.clear()
            ts.nbytes = None
            ts._previous = None
            ts._next = None
            ts.done = False

            self._executing.discard(ts)
            self._in_flight_tasks.discard(ts)

            self._notify_plugins(
                "release_key", key, state_before, cause, stimulus_id, report
            )
        except CommClosedError:
            # Batched stream send might raise if it was already closed
            pass
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    ################
    # Execute Task #
    ################

    def run(self, comm, function, args=(), wait=True, kwargs=None):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    def run_coroutine(self, comm, function, args=(), kwargs=None, wait=True):
        return run(self, comm, function=function, args=args, kwargs=kwargs, wait=wait)

    async def plugin_add(
        self,
        plugin: WorkerPlugin | bytes,
        name: str | None = None,
        catch_errors: bool = True,
    ) -> dict[str, Any]:
        with log_errors(pdb=False):
            if isinstance(plugin, bytes):
                # Note: historically we have accepted duck-typed classes that don't
                # inherit from WorkerPlugin. Don't do `assert isinstance`.
                plugin = cast("WorkerPlugin", pickle.loads(plugin))

            if name is None:
                name = _get_plugin_name(plugin)

            assert name

            if name in self.plugins:
                await self.plugin_remove(name=name)

            self.plugins[name] = plugin

            logger.info("Starting Worker plugin %s" % name)
            if hasattr(plugin, "setup"):
                try:
                    result = plugin.setup(worker=self)
                    if isawaitable(result):
                        result = await result
                except Exception as e:
                    if not catch_errors:
                        raise
                    msg = error_message(e)
                    return msg

            return {"status": "OK"}

    async def plugin_remove(self, name: str) -> dict[str, Any]:
        with log_errors(pdb=False):
            logger.info(f"Removing Worker plugin {name}")
            try:
                plugin = self.plugins.pop(name)
                if hasattr(plugin, "teardown"):
                    result = plugin.teardown(worker=self)
                    if isawaitable(result):
                        result = await result
            except Exception as e:
                msg = error_message(e)
                return msg

            return {"status": "OK"}

    async def actor_execute(
        self,
        actor=None,
        function=None,
        args=(),
        kwargs: dict | None = None,
    ) -> dict[str, Any]:
        kwargs = kwargs or {}
        separate_thread = kwargs.pop("separate_thread", True)
        key = actor
        actor = self.actors[key]
        func = getattr(actor, function)
        name = key_split(key) + "." + function

        try:
            if iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            elif separate_thread:
                result = await self.loop.run_in_executor(
                    self.executors["actor"],
                    apply_function_actor,
                    func,
                    args,
                    kwargs,
                    self.execution_state,
                    name,
                    self.active_threads,
                    self.active_threads_lock,
                )
            else:
                result = func(*args, **kwargs)
            return {"status": "OK", "result": to_serialize(result)}
        except Exception as ex:
            return {"status": "error", "exception": to_serialize(ex)}

    def actor_attribute(self, actor=None, attribute=None) -> dict[str, Any]:
        try:
            value = getattr(self.actors[actor], attribute)
            return {"status": "OK", "result": to_serialize(value)}
        except Exception as ex:
            return {"status": "error", "exception": to_serialize(ex)}

    def meets_resource_constraints(self, key: str) -> bool:
        ts = self.tasks[key]
        if not ts.resource_restrictions:
            return True
        for resource, needed in ts.resource_restrictions.items():
            if self.available_resources[resource] < needed:
                return False

        return True

    async def _maybe_deserialize_task(
        self, ts: TaskState, *, stimulus_id: str
    ) -> tuple[Callable, tuple, dict[str, Any]] | None:
        if ts.run_spec is None:
            return None
        try:
            start = time()
            # Offload deserializing large tasks
            if sizeof(ts.run_spec) > OFFLOAD_THRESHOLD:
                function, args, kwargs = await offload(_deserialize, *ts.run_spec)
            else:
                function, args, kwargs = _deserialize(*ts.run_spec)
            stop = time()

            if stop - start > 0.010:
                ts.startstops.append(
                    {"action": "deserialize", "start": start, "stop": stop}
                )
            return function, args, kwargs
        except Exception as e:
            logger.error("Could not deserialize task", exc_info=True)
            self.log.append((ts.key, "deserialize-error", stimulus_id, time()))
            emsg = error_message(e)
            emsg.pop("status")
            self.transition(
                ts,
                "error",
                **emsg,
                stimulus_id=stimulus_id,
            )
            raise

    def ensure_computing(self) -> None:
        if self.status in (Status.paused, Status.closing_gracefully):
            return
        try:
            stimulus_id = f"ensure-computing-{time()}"
            while self.constrained and self.executing_count < self.nthreads:
                key = self.constrained[0]
                ts = self.tasks.get(key, None)
                if ts is None or ts.state != "constrained":
                    self.constrained.popleft()
                    continue
                if self.meets_resource_constraints(key):
                    self.constrained.popleft()
                    self.transition(ts, "executing", stimulus_id=stimulus_id)
                else:
                    break
            while self.ready and self.executing_count < self.nthreads:
                priority, key = heapq.heappop(self.ready)
                ts = self.tasks.get(key)
                if ts is None:
                    # It is possible for tasks to be released while still remaining on
                    # `ready` The scheduler might have re-routed to a new worker and
                    # told this worker to release.  If the task has "disappeared" just
                    # continue through the heap
                    continue
                elif ts.key in self.data:
                    self.transition(ts, "memory", stimulus_id=stimulus_id)
                elif ts.state in READY:
                    self.transition(ts, "executing", stimulus_id=stimulus_id)
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    async def execute(self, key: str, *, stimulus_id: str) -> None:
        if self.status in {Status.closing, Status.closed, Status.closing_gracefully}:
            return
        if key not in self.tasks:
            return
        ts = self.tasks[key]

        try:
            if ts.state == "cancelled":
                # This might happen if keys are canceled
                logger.debug(
                    "Trying to execute task %s which is not in executing state anymore",
                    ts,
                )
                ts.done = True
                self.transition(ts, "released", stimulus_id=stimulus_id)
                return

            if self.validate:
                assert not ts.waiting_for_data
                assert ts.state == "executing"
                assert ts.run_spec is not None

            function, args, kwargs = await self._maybe_deserialize_task(  # type: ignore
                ts, stimulus_id=stimulus_id
            )

            args2, kwargs2 = self._prepare_args_for_execution(ts, args, kwargs)

            if ts.annotations is not None and "executor" in ts.annotations:
                executor = ts.annotations["executor"]
            else:
                executor = "default"
            assert executor in self.executors
            assert key == ts.key
            self.active_keys.add(ts.key)

            result: dict
            try:
                e = self.executors[executor]
                ts.start_time = time()
                if iscoroutinefunction(function):
                    result = await apply_function_async(
                        function,
                        args2,
                        kwargs2,
                        self.scheduler_delay,
                    )
                elif "ThreadPoolExecutor" in str(type(e)):
                    result = await self.loop.run_in_executor(
                        e,
                        apply_function,
                        function,
                        args2,
                        kwargs2,
                        self.execution_state,
                        ts.key,
                        self.active_threads,
                        self.active_threads_lock,
                        self.scheduler_delay,
                    )
                else:
                    result = await self.loop.run_in_executor(
                        e,
                        apply_function_simple,
                        function,
                        args2,
                        kwargs2,
                        self.scheduler_delay,
                    )
            finally:
                self.active_keys.discard(ts.key)

            key = ts.key
            # key *must* be still in tasks. Releasing it directly is forbidden
            # without going through cancelled
            ts = self.tasks.get(key)  # type: ignore
            assert ts, self.story(key)
            ts.done = True
            result["key"] = ts.key
            value = result.pop("result", None)
            ts.startstops.append(
                {"action": "compute", "start": result["start"], "stop": result["stop"]}
            )
            self.threads[ts.key] = result["thread"]
            recommendations: Recs = {}
            if result["op"] == "task-finished":
                ts.nbytes = result["nbytes"]
                ts.type = result["type"]
                recommendations[ts] = ("memory", value)
                if self.digests is not None:
                    self.digests["task-duration"].add(result["stop"] - result["start"])
            elif isinstance(result.pop("actual-exception"), Reschedule):
                recommendations[ts] = "rescheduled"
            else:
                logger.warning(
                    "Compute Failed\n"
                    "Key:       %s\n"
                    "Function:  %s\n"
                    "args:      %s\n"
                    "kwargs:    %s\n"
                    "Exception: %r\n",
                    ts.key,
                    str(funcname(function))[:1000],
                    convert_args_to_str(args2, max_len=1000),
                    convert_kwargs_to_str(kwargs2, max_len=1000),
                    result["exception_text"],
                )
                recommendations[ts] = (
                    "error",
                    result["exception"],
                    result["traceback"],
                    result["exception_text"],
                    result["traceback_text"],
                )

            self.transitions(recommendations, stimulus_id=stimulus_id)

            logger.debug("Send compute response to scheduler: %s, %s", ts.key, result)

            if self.validate:
                assert ts.state != "executing"
                assert not ts.waiting_for_data

        except Exception as exc:
            assert ts
            logger.error(
                "Exception during execution of task %s.", ts.key, exc_info=True
            )
            emsg = error_message(exc)
            emsg.pop("status")
            self.transition(
                ts,
                "error",
                **emsg,
                stimulus_id=stimulus_id,
            )
        finally:
            self.ensure_computing()
            self.ensure_communicating()

    def _prepare_args_for_execution(
        self, ts: TaskState, args: tuple, kwargs: dict[str, Any]
    ) -> tuple[tuple, dict[str, Any]]:
        start = time()
        data = {}
        for dep in ts.dependencies:
            k = dep.key
            try:
                data[k] = self.data[k]
            except KeyError:
                from distributed.actor import Actor  # TODO: create local actor

                data[k] = Actor(type(self.actors[k]), self.address, k, self)
        args2 = pack_data(args, data, key_types=(bytes, str))
        kwargs2 = pack_data(kwargs, data, key_types=(bytes, str))
        stop = time()
        if stop - start > 0.005:
            ts.startstops.append({"action": "disk-read", "start": start, "stop": stop})
            if self.digests is not None:
                self.digests["disk-load-duration"].add(stop - start)
        return args2, kwargs2

    ##################
    # Administrative #
    ##################
    def cycle_profile(self) -> None:
        now = time() + self.scheduler_delay
        prof, self.profile_recent = self.profile_recent, profile.create()
        self.profile_history.append((now, prof))

        self.profile_keys_history.append((now, dict(self.profile_keys)))
        self.profile_keys.clear()

    def trigger_profile(self) -> None:
        """
        Get a frame from all actively computing threads

        Merge these frames into existing profile counts
        """
        if not self.active_threads:  # hope that this is thread-atomic?
            return
        start = time()
        with self.active_threads_lock:
            active_threads = self.active_threads.copy()
        frames = sys._current_frames()
        frames = {ident: frames[ident] for ident in active_threads}
        llframes = {}
        if self.low_level_profiler:
            llframes = {ident: profile.ll_get_stack(ident) for ident in active_threads}
        for ident, frame in frames.items():
            if frame is not None:
                key = key_split(active_threads[ident])
                llframe = llframes.get(ident)

                state = profile.process(
                    frame, True, self.profile_recent, stop="distributed/worker.py"
                )
                profile.llprocess(llframe, None, state)
                profile.process(
                    frame, True, self.profile_keys[key], stop="distributed/worker.py"
                )

        stop = time()
        if self.digests is not None:
            self.digests["profile-duration"].add(stop - start)

    async def get_profile(
        self,
        start=None,
        stop=None,
        key=None,
        server: bool = False,
    ):
        now = time() + self.scheduler_delay
        if server:
            history = self.io_loop.profile
        elif key is None:
            history = self.profile_history
        else:
            history = [(t, d[key]) for t, d in self.profile_keys_history if key in d]

        if start is None:
            istart = 0
        else:
            istart = bisect.bisect_left(history, (start,))

        if stop is None:
            istop = None
        else:
            istop = bisect.bisect_right(history, (stop,)) + 1
            if istop >= len(history):
                istop = None  # include end

        if istart == 0 and istop is None:
            history = list(history)
        else:
            iistop = len(history) if istop is None else istop
            history = [history[i] for i in range(istart, iistop)]

        prof = profile.merge(*pluck(1, history))

        if not history:
            return profile.create()

        if istop is None and (start is None or start < now):
            if key is None:
                recent = self.profile_recent
            else:
                recent = self.profile_keys[key]
            prof = profile.merge(prof, recent)

        return prof

    async def get_profile_metadata(
        self, start: float = 0, stop: float | None = None
    ) -> dict[str, Any]:
        add_recent = stop is None
        now = time() + self.scheduler_delay
        stop = stop or now
        result = {
            "counts": [
                (t, d["count"]) for t, d in self.profile_history if start < t < stop
            ],
            "keys": [
                (t, {k: d["count"] for k, d in v.items()})
                for t, v in self.profile_keys_history
                if start < t < stop
            ],
        }
        if add_recent:
            result["counts"].append((now, self.profile_recent["count"]))
            result["keys"].append(
                (now, {k: v["count"] for k, v in self.profile_keys.items()})
            )
        return result

    def get_call_stack(self, keys: Collection[str] | None = None) -> dict[str, Any]:
        with self.active_threads_lock:
            sys_frames = sys._current_frames()
            frames = {key: sys_frames[tid] for tid, key in self.active_threads.items()}
        if keys is not None:
            frames = {key: frames[key] for key in keys if key in frames}

        return {key: profile.call_stack(frame) for key, frame in frames.items()}

    def _notify_plugins(self, method_name, *args, **kwargs):
        for name, plugin in self.plugins.items():
            if hasattr(plugin, method_name):
                if method_name == "release_key":
                    warnings.warn(
                        "The `WorkerPlugin.release_key` hook is deprecated and will be "
                        "removed in a future version. A similar event can now be "
                        "caught by filtering for a `finish=='released'` event in the "
                        "`WorkerPlugin.transition` hook.",
                        FutureWarning,
                    )

                try:
                    getattr(plugin, method_name)(*args, **kwargs)
                except Exception:
                    logger.info(
                        "Plugin '%s' failed with exception", name, exc_info=True
                    )

    ##############
    # Validation #
    ##############

    def validate_task_memory(self, ts):
        assert ts.key in self.data or ts.key in self.actors
        assert isinstance(ts.nbytes, int)
        assert not ts.waiting_for_data
        assert ts.key not in self.ready
        assert ts.state == "memory"

    def validate_task_executing(self, ts):
        assert ts.state == "executing"
        assert ts.run_spec is not None
        assert ts.key not in self.data
        assert not ts.waiting_for_data
        for dep in ts.dependencies:
            assert dep.state == "memory", self.story(dep)
            assert dep.key in self.data or dep.key in self.actors

    def validate_task_ready(self, ts):
        assert ts.key in pluck(1, self.ready)
        assert ts.key not in self.data
        assert ts.state != "executing"
        assert not ts.done
        assert not ts.waiting_for_data
        assert all(
            dep.key in self.data or dep.key in self.actors for dep in ts.dependencies
        )

    def validate_task_waiting(self, ts):
        assert ts.key not in self.data
        assert ts.state == "waiting"
        assert not ts.done
        if ts.dependencies and ts.run_spec:
            assert not all(dep.key in self.data for dep in ts.dependencies)

    def validate_task_flight(self, ts):
        assert ts.key not in self.data
        assert ts in self._in_flight_tasks
        assert not any(dep.key in self.ready for dep in ts.dependents)
        assert ts.coming_from
        assert ts.coming_from in self.in_flight_workers
        assert ts.key in self.in_flight_workers[ts.coming_from]

    def validate_task_fetch(self, ts):
        assert ts.key not in self.data
        assert self.address not in ts.who_has
        assert not ts.done
        assert ts in self.data_needed
        assert ts.who_has

        for w in ts.who_has:
            assert ts.key in self.has_what[w]
            assert ts in self.pending_data_per_worker[w]

    def validate_task_missing(self, ts):
        assert ts.key not in self.data
        assert not ts.who_has
        assert not ts.done
        assert not any(ts.key in has_what for has_what in self.has_what.values())
        assert ts in self._missing_dep_flight

    def validate_task_cancelled(self, ts):
        assert ts.key not in self.data
        assert ts._previous
        assert ts._next

    def validate_task_resumed(self, ts):
        assert ts.key not in self.data
        assert ts._next
        assert ts._previous

    def validate_task_released(self, ts):
        assert ts.key not in self.data
        assert not ts._next
        assert not ts._previous
        assert ts not in self._executing
        assert ts not in self._in_flight_tasks
        assert ts not in self._missing_dep_flight
        assert ts not in self._missing_dep_flight
        assert not any(ts.key in has_what for has_what in self.has_what.values())
        assert not ts.waiting_for_data
        assert not ts.done
        assert not ts.exception
        assert not ts.traceback

    def validate_task(self, ts):
        try:
            if ts.key in self.tasks:
                assert self.tasks[ts.key] == ts
            if ts.state == "memory":
                self.validate_task_memory(ts)
            elif ts.state == "waiting":
                self.validate_task_waiting(ts)
            elif ts.state == "missing":
                self.validate_task_missing(ts)
            elif ts.state == "cancelled":
                self.validate_task_cancelled(ts)
            elif ts.state == "resumed":
                self.validate_task_resumed(ts)
            elif ts.state == "ready":
                self.validate_task_ready(ts)
            elif ts.state == "executing":
                self.validate_task_executing(ts)
            elif ts.state == "flight":
                self.validate_task_flight(ts)
            elif ts.state == "fetch":
                self.validate_task_fetch(ts)
            elif ts.state == "released":
                self.validate_task_released(ts)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()

            raise AssertionError(
                f"Invalid TaskState encountered for {ts!r}.\nStory:\n{self.story(ts)}\n"
            ) from e

    def validate_state(self):
        if self.status not in Status.ANY_RUNNING:
            return
        try:
            assert self.executing_count >= 0
            waiting_for_data_count = 0
            for ts in self.tasks.values():
                assert ts.state is not None
                # check that worker has task
                for worker in ts.who_has:
                    assert ts.key in self.has_what[worker]
                # check that deps have a set state and that dependency<->dependent links
                # are there
                for dep in ts.dependencies:
                    # self.tasks was just a dict of tasks
                    # and this check was originally that the key was in `task_state`
                    # so we may have popped the key out of `self.tasks` but the
                    # dependency can still be in `memory` before GC grabs it...?
                    # Might need better bookkeeping
                    assert dep.state is not None
                    assert ts in dep.dependents, ts
                if ts.waiting_for_data:
                    waiting_for_data_count += 1
                for ts_wait in ts.waiting_for_data:
                    assert ts_wait.key in self.tasks
                    assert (
                        ts_wait.state
                        in READY | {"executing", "flight", "fetch", "missing"}
                        or ts_wait in self._missing_dep_flight
                        or ts_wait.who_has.issubset(self.in_flight_workers)
                    ), (ts, ts_wait, self.story(ts), self.story(ts_wait))
            assert self.waiting_for_data_count == waiting_for_data_count
            for worker, keys in self.has_what.items():
                for k in keys:
                    assert worker in self.tasks[k].who_has

            for ts in self.tasks.values():
                self.validate_task(ts)

        except Exception as e:
            self.loop.add_callback(self.close)
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise

    #######################################
    # Worker Clients (advanced workloads) #
    #######################################

    @property
    def client(self) -> Client:
        with self._lock:
            if self._client:
                return self._client
            else:
                return self._get_client()

    def _get_client(self, timeout: float | None = None) -> Client:
        """Get local client attached to this worker

        If no such client exists, create one

        See Also
        --------
        get_client
        """

        if timeout is None:
            timeout = dask.config.get("distributed.comm.timeouts.connect")

        timeout = parse_timedelta(timeout, "s")

        try:
            from distributed.client import default_client

            client = default_client()
        except ValueError:  # no clients found, need to make a new one
            pass
        else:
            # must be lazy import otherwise cyclic import
            from distributed.deploy.cluster import Cluster

            if (
                client.scheduler
                and client.scheduler.address == self.scheduler.address
                # The below conditions should only happen in case a second
                # cluster is alive, e.g. if a submitted task spawned its onwn
                # LocalCluster, see gh4565
                or (
                    isinstance(client._start_arg, str)
                    and client._start_arg == self.scheduler.address
                    or isinstance(client._start_arg, Cluster)
                    and client._start_arg.scheduler_address == self.scheduler.address
                )
            ):
                self._client = client

        if not self._client:
            from distributed.client import Client

            asynchronous = in_async_call(self.loop)
            self._client = Client(
                self.scheduler,
                loop=self.loop,
                security=self.security,
                set_as_default=True,
                asynchronous=asynchronous,
                direct_to_workers=True,
                name="worker",
                timeout=timeout,
            )
            Worker._initialized_clients.add(self._client)
            if not asynchronous:
                assert self._client.status == "running"

        return self._client

    def get_current_task(self) -> str:
        """Get the key of the task we are currently running

        This only makes sense to run within a task

        Examples
        --------
        >>> from dask.distributed import get_worker
        >>> def f():
        ...     return get_worker().get_current_task()

        >>> future = client.submit(f)  # doctest: +SKIP
        >>> future.result()  # doctest: +SKIP
        'f-1234'

        See Also
        --------
        get_worker
        """
        return self.active_threads[threading.get_ident()]


def get_worker() -> Worker:
    """Get the worker currently running this task

    Examples
    --------
    >>> def f():
    ...     worker = get_worker()  # The worker on which this task is running
    ...     return worker.address

    >>> future = client.submit(f)  # doctest: +SKIP
    >>> future.result()  # doctest: +SKIP
    'tcp://127.0.0.1:47373'

    See Also
    --------
    get_client
    worker_client
    """
    try:
        return thread_state.execution_state["worker"]
    except AttributeError:
        try:
            return first(
                w
                for w in Worker._instances
                if w.status in Status.ANY_RUNNING  # type: ignore
            )
        except StopIteration:
            raise ValueError("No workers found")


def get_client(address=None, timeout=None, resolve_address=True) -> Client:
    """Get a client while within a task.

    This client connects to the same scheduler to which the worker is connected

    Parameters
    ----------
    address : str, optional
        The address of the scheduler to connect to. Defaults to the scheduler
        the worker is connected to.
    timeout : int or str
        Timeout (in seconds) for getting the Client. Defaults to the
        ``distributed.comm.timeouts.connect`` configuration value.
    resolve_address : bool, default True
        Whether to resolve `address` to its canonical form.

    Returns
    -------
    Client

    Examples
    --------
    >>> def f():
    ...     client = get_client(timeout="10s")
    ...     futures = client.map(lambda x: x + 1, range(10))  # spawn many tasks
    ...     results = client.gather(futures)
    ...     return sum(results)

    >>> future = client.submit(f)  # doctest: +SKIP
    >>> future.result()  # doctest: +SKIP
    55

    See Also
    --------
    get_worker
    worker_client
    secede
    """

    if timeout is None:
        timeout = dask.config.get("distributed.comm.timeouts.connect")

    timeout = parse_timedelta(timeout, "s")

    if address and resolve_address:
        address = comm.resolve_address(address)
    try:
        worker = get_worker()
    except ValueError:  # could not find worker
        pass
    else:
        if not address or worker.scheduler.address == address:
            return worker._get_client(timeout=timeout)

    from distributed.client import Client

    try:
        client = Client.current()  # TODO: assumes the same scheduler
    except ValueError:
        client = None
    if client and (not address or client.scheduler.address == address):
        return client
    elif address:
        return Client(address, timeout=timeout)
    else:
        raise ValueError("No global client found and no address provided")


def secede():
    """
    Have this task secede from the worker's thread pool

    This opens up a new scheduling slot and a new thread for a new task. This
    enables the client to schedule tasks on this node, which is
    especially useful while waiting for other jobs to finish (e.g., with
    ``client.gather``).

    Examples
    --------
    >>> def mytask(x):
    ...     # do some work
    ...     client = get_client()
    ...     futures = client.map(...)  # do some remote work
    ...     secede()  # while that work happens, remove ourself from the pool
    ...     return client.gather(futures)  # return gathered results

    See Also
    --------
    get_client
    get_worker
    """
    worker = get_worker()
    tpe_secede()  # have this thread secede from the thread pool
    duration = time() - thread_state.start_time
    worker.loop.add_callback(
        worker.maybe_transition_long_running,
        worker.tasks[thread_state.key],
        compute_duration=duration,
        stimulus_id=f"secede-{thread_state.key}-{time()}",
    )


class Reschedule(Exception):
    """Reschedule this task

    Raising this exception will stop the current execution of the task and ask
    the scheduler to reschedule this task, possibly on a different machine.

    This does not guarantee that the task will move onto a different machine.
    The scheduler will proceed through its normal heuristics to determine the
    optimal machine to accept this task.  The machine will likely change if the
    load across the cluster has significantly changed since first scheduling
    the task.
    """


async def get_data_from_worker(
    rpc,
    keys,
    worker,
    who=None,
    max_connections=None,
    serializers=None,
    deserializers=None,
):
    """Get keys from worker

    The worker has a two step handshake to acknowledge when data has been fully
    delivered.  This function implements that handshake.

    See Also
    --------
    Worker.get_data
    Worker.gather_dep
    utils_comm.gather_data_from_workers
    """
    if serializers is None:
        serializers = rpc.serializers
    if deserializers is None:
        deserializers = rpc.deserializers

    async def _get_data():
        comm = await rpc.connect(worker)
        comm.name = "Ephemeral Worker->Worker for gather"
        try:
            response = await send_recv(
                comm,
                serializers=serializers,
                deserializers=deserializers,
                op="get_data",
                keys=keys,
                who=who,
                max_connections=max_connections,
            )
            try:
                status = response["status"]
            except KeyError:  # pragma: no cover
                raise ValueError("Unexpected response", response)
            else:
                if status == "OK":
                    await comm.write("OK")
            return response
        finally:
            rpc.reuse(worker, comm)

    return await retry_operation(_get_data, operation="get_data_from_worker")


job_counter = [0]


cache_loads = LRU(maxsize=100)


def loads_function(bytes_object):
    """Load a function from bytes, cache bytes"""
    if len(bytes_object) < 100000:
        try:
            result = cache_loads[bytes_object]
        except KeyError:
            result = pickle.loads(bytes_object)
            cache_loads[bytes_object] = result
        return result
    return pickle.loads(bytes_object)


def _deserialize(function=None, args=None, kwargs=None, task=no_value):
    """Deserialize task inputs and regularize to func, args, kwargs"""
    if function is not None:
        function = loads_function(function)
    if args and isinstance(args, bytes):
        args = pickle.loads(args)
    if kwargs and isinstance(kwargs, bytes):
        kwargs = pickle.loads(kwargs)

    if task is not no_value:
        assert not function and not args and not kwargs
        function = execute_task
        args = (task,)

    return function, args or (), kwargs or {}


def execute_task(task):
    """Evaluate a nested task

    >>> inc = lambda x: x + 1
    >>> execute_task((inc, 1))
    2
    >>> execute_task((sum, [1, 2, (inc, 3)]))
    7
    """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    elif isinstance(task, list):
        return list(map(execute_task, task))
    else:
        return task


cache_dumps = LRU(maxsize=100)

_cache_lock = threading.Lock()


def dumps_function(func) -> bytes:
    """Dump a function to bytes, cache functions"""
    try:
        with _cache_lock:
            result = cache_dumps[func]
    except KeyError:
        result = pickle.dumps(func, protocol=4)
        if len(result) < 100000:
            with _cache_lock:
                cache_dumps[func] = result
    except TypeError:  # Unhashable function
        result = pickle.dumps(func, protocol=4)
    return result


def dumps_task(task):
    """Serialize a dask task

    Returns a dict of bytestrings that can each be loaded with ``loads``

    Examples
    --------
    Either returns a task as a function, args, kwargs dict

    >>> from operator import add
    >>> dumps_task((add, 1))  # doctest: +SKIP
    {'function': b'\x80\x04\x95\x00\x8c\t_operator\x94\x8c\x03add\x94\x93\x94.'
     'args': b'\x80\x04\x95\x07\x00\x00\x00K\x01K\x02\x86\x94.'}

    Or as a single task blob if it can't easily decompose the result.  This
    happens either if the task is highly nested, or if it isn't a task at all

    >>> dumps_task(1)  # doctest: +SKIP
    {'task': b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'}
    """
    if istask(task):
        if task[0] is apply and not any(map(_maybe_complex, task[2:])):
            d = {"function": dumps_function(task[1]), "args": warn_dumps(task[2])}
            if len(task) == 4:
                d["kwargs"] = warn_dumps(task[3])
            return d
        elif not any(map(_maybe_complex, task[1:])):
            return {"function": dumps_function(task[0]), "args": warn_dumps(task[1:])}
    return to_serialize(task)


_warn_dumps_warned = [False]


def warn_dumps(obj, dumps=pickle.dumps, limit=1e6):
    """Dump an object to bytes, warn if those bytes are large"""
    b = dumps(obj, protocol=4)
    if not _warn_dumps_warned[0] and len(b) > limit:
        _warn_dumps_warned[0] = True
        s = str(obj)
        if len(s) > 70:
            s = s[:50] + " ... " + s[-15:]
        warnings.warn(
            "Large object of size %s detected in task graph: \n"
            "  %s\n"
            "Consider scattering large objects ahead of time\n"
            "with client.scatter to reduce scheduler burden and \n"
            "keep data on workers\n\n"
            "    future = client.submit(func, big_data)    # bad\n\n"
            "    big_future = client.scatter(big_data)     # good\n"
            "    future = client.submit(func, big_future)  # good"
            % (format_bytes(len(b)), s)
        )
    return b


def apply_function(
    function,
    args,
    kwargs,
    execution_state,
    key,
    active_threads,
    active_threads_lock,
    time_delay,
):
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()
    with active_threads_lock:
        active_threads[ident] = key
    thread_state.start_time = time()
    thread_state.execution_state = execution_state
    thread_state.key = key

    msg = apply_function_simple(function, args, kwargs, time_delay)

    with active_threads_lock:
        del active_threads[ident]
    return msg


def apply_function_simple(
    function,
    args,
    kwargs,
    time_delay,
):
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()
    start = time()
    try:
        result = function(*args, **kwargs)
    except Exception as e:
        msg = error_message(e)
        msg["op"] = "task-erred"
        msg["actual-exception"] = e
    else:
        msg = {
            "op": "task-finished",
            "status": "OK",
            "result": result,
            "nbytes": sizeof(result),
            "type": type(result) if result is not None else None,
        }
    finally:
        end = time()
    msg["start"] = start + time_delay
    msg["stop"] = end + time_delay
    msg["thread"] = ident
    return msg


async def apply_function_async(
    function,
    args,
    kwargs,
    time_delay,
):
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()
    start = time()
    try:
        result = await function(*args, **kwargs)
    except Exception as e:
        msg = error_message(e)
        msg["op"] = "task-erred"
        msg["actual-exception"] = e
    else:
        msg = {
            "op": "task-finished",
            "status": "OK",
            "result": result,
            "nbytes": sizeof(result),
            "type": type(result) if result is not None else None,
        }
    finally:
        end = time()
    msg["start"] = start + time_delay
    msg["stop"] = end + time_delay
    msg["thread"] = ident
    return msg


def apply_function_actor(
    function, args, kwargs, execution_state, key, active_threads, active_threads_lock
):
    """Run a function, collect information

    Returns
    -------
    msg: dictionary with status, result/error, timings, etc..
    """
    ident = threading.get_ident()

    with active_threads_lock:
        active_threads[ident] = key

    thread_state.execution_state = execution_state
    thread_state.key = key
    thread_state.actor = True

    result = function(*args, **kwargs)

    with active_threads_lock:
        del active_threads[ident]

    return result


def get_msg_safe_str(msg):
    """Make a worker msg, which contains args and kwargs, safe to cast to str:
    allowing for some arguments to raise exceptions during conversion and
    ignoring them.
    """

    class Repr:
        def __init__(self, f, val):
            self._f = f
            self._val = val

        def __repr__(self):
            return self._f(self._val)

    msg = msg.copy()
    if "args" in msg:
        msg["args"] = Repr(convert_args_to_str, msg["args"])
    if "kwargs" in msg:
        msg["kwargs"] = Repr(convert_kwargs_to_str, msg["kwargs"])
    return msg


def convert_args_to_str(args, max_len: int | None = None) -> str:
    """Convert args to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(args))]
    for i, arg in enumerate(args):
        try:
            sarg = repr(arg)
        except Exception:
            sarg = "< could not convert arg to str >"
        strs[i] = sarg
        length += len(sarg) + 2
        if max_len is not None and length > max_len:
            return "({}".format(", ".join(strs[: i + 1]))[:max_len]
    else:
        return "({})".format(", ".join(strs))


def convert_kwargs_to_str(kwargs: dict, max_len: int | None = None) -> str:
    """Convert kwargs to a string, allowing for some arguments to raise
    exceptions during conversion and ignoring them.
    """
    length = 0
    strs = ["" for i in range(len(kwargs))]
    for i, (argname, arg) in enumerate(kwargs.items()):
        try:
            sarg = repr(arg)
        except Exception:
            sarg = "< could not convert arg to str >"
        skwarg = repr(argname) + ": " + sarg
        strs[i] = skwarg
        length += len(skwarg) + 2
        if max_len is not None and length > max_len:
            return "{{{}".format(", ".join(strs[: i + 1]))[:max_len]
    else:
        return "{{{}}}".format(", ".join(strs))


async def run(server, comm, function, args=(), kwargs=None, is_coro=None, wait=True):
    kwargs = kwargs or {}
    function = pickle.loads(function)
    if is_coro is None:
        is_coro = iscoroutinefunction(function)
    else:
        warnings.warn(
            "The is_coro= parameter is deprecated. "
            "We now automatically detect coroutines/async functions"
        )
    assert wait or is_coro, "Combination not supported"
    if args:
        args = pickle.loads(args)
    if kwargs:
        kwargs = pickle.loads(kwargs)
    if has_arg(function, "dask_worker"):
        kwargs["dask_worker"] = server
    if has_arg(function, "dask_scheduler"):
        kwargs["dask_scheduler"] = server
    logger.info("Run out-of-band function %r", funcname(function))
    try:
        if not is_coro:
            result = function(*args, **kwargs)
        else:
            if wait:
                result = await function(*args, **kwargs)
            else:
                server.loop.add_callback(function, *args, **kwargs)
                result = None

    except Exception as e:
        logger.warning(
            "Run Failed\nFunction: %s\nargs:     %s\nkwargs:   %s\n",
            str(funcname(function))[:1000],
            convert_args_to_str(args, max_len=1000),
            convert_kwargs_to_str(kwargs, max_len=1000),
            exc_info=True,
        )

        response = error_message(e)
    else:
        response = {"status": "OK", "result": to_serialize(result)}
    return response


_global_workers = Worker._instances

try:
    if nvml.device_get_count() < 1:
        raise RuntimeError
except (Exception, RuntimeError):
    pass
else:

    async def gpu_metric(worker):
        result = await offload(nvml.real_time)
        return result

    DEFAULT_METRICS["gpu"] = gpu_metric

    def gpu_startup(worker):
        return nvml.one_time()

    DEFAULT_STARTUP_INFORMATION["gpu"] = gpu_startup


def print(*args, **kwargs):
    """Dask print function
    This prints both wherever this function is run, and also in the user's
    client session
    """
    try:
        worker = get_worker()
    except ValueError:
        pass
    else:
        msg = {
            "args": tuple(stringify(arg) for arg in args),
            "kwargs": {k: stringify(v) for k, v in kwargs.items()},
        }
        worker.log_event("print", msg)

    builtins.print(*args, **kwargs)


def warn(*args, **kwargs):
    """Dask warn function
    This raises a warning both wherever this function is run, and also
    in the user's client session
    """
    try:
        worker = get_worker()
    except ValueError:  # pragma: no cover
        pass
    else:
        worker.log_event("warn", {"args": args, "kwargs": kwargs})

    warnings.warn(*args, **kwargs)
