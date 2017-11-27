from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque, OrderedDict, Mapping, Set
from datetime import timedelta
from functools import partial
import itertools
import json
import logging
import operator
import os
import pickle
import random
import six

from sortedcontainers import SortedSet, SortedDict
try:
    from cytoolz import frequencies, merge, pluck, merge_sorted, first
except ImportError:
    from toolz import frequencies, merge, pluck, merge_sorted, first
from toolz import memoize, valmap, first, second, concat, compose
from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop

from dask.core import reverse_dict
from dask.order import order

from .batched import BatchedSend
from .comm import (normalize_address, resolve_address,
                   get_address_host, unparse_host_port)
from .compatibility import finalize
from .config import config, log_format
from .core import (rpc, connect, Server, send_recv,
                   error_message, clean_exception, CommClosedError)
from . import profile
from .metrics import time
from .node import ServerNode
from .security import Security
from .utils import (All, ignoring, get_ip, get_fileno_limit, log_errors,
                    key_split, validate_key, no_default, DequeHandler)
from .utils_comm import (scatter_to_workers, gather_from_workers)
from .versions import get_versions

from .publish import PublishExtension
from .queues import QueueExtension
from .recreate_exceptions import ReplayExceptionScheduler
from .lock import LockExtension
from .stealing import WorkStealing
from .variable import VariableExtension


logger = logging.getLogger(__name__)


BANDWIDTH = config.get('bandwidth', 100e6)
ALLOWED_FAILURES = config.get('allowed-failures', 3)

LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)
DEFAULT_DATA_SIZE = config.get('default-data-size', 1000)

DEFAULT_EXTENSIONS = [
    LockExtension,
    PublishExtension,
    ReplayExceptionScheduler,
    QueueExtension,
    VariableExtension,
]

if config.get('work-stealing', True):
    DEFAULT_EXTENSIONS.append(WorkStealing)


class ClientState(object):
    __slots__ = (
        'client_key',
        'wants_what',
        )

    def __init__(self, client):
        self.client_key = client
        self.wants_what = set()

    def __repr__(self):
        return "<Client %r>" % (self.client_key,)

    def __str__(self):
        return self.client_key


class WorkerState(object):
    __slots__ = (
        'worker_key',
        'ncores',
        'resources',
        'used_resources',
        'nbytes',
        'has_what',
        'processing',
        'occupancy',
        )

    def __init__(self, worker, ncores):
        self.worker_key = worker
        self.ncores = ncores
        self.resources = {}
        self.used_resources = {}
        self.nbytes = 0
        self.occupancy = 0
        self.processing = {}
        self.has_what = set()

    def __repr__(self):
        return "<Worker %r>" % (self.worker_key,)

    def __str__(self):
        return self.worker_key


class TaskState(object):
    __slots__ = (
        # General description
        'key',
        'run_spec',
        'dependencies',
        'dependents',
        'priority',
        'host_restrictions',
        'worker_restrictions',
        'resource_restrictions',
        'loose_restrictions',
        'who_wants',
        # Task state
        'state',
        'waiting_on',
        'waiters',
        'processing_on',
        'who_has',
        'exception',
        'traceback',
        'exception_blame',
        'suspicious',
        'retries',
        'nbytes',
        )

    # XXX released, unrunnable

    def __init__(self, key, run_spec):
        assert isinstance(key, (str, bytes))
        self.key = key
        self.run_spec = run_spec
        self.state = None
        self.exception = self.traceback = self.exception_blame = None
        self.suspicious = self.retries = 0
        self.nbytes = None
        self.priority = None
        self.who_wants = set()
        self.dependencies = set()
        self.dependents = set()
        self.who_has = set()
        self.processing_on = None

    def get_nbytes(self):
        nbytes = self.nbytes
        return nbytes if nbytes is not None else DEFAULT_DATA_SIZE

    def __repr__(self):
        return "<Task %r %s>" % (self.key, self.state)

    def validate(self):
        for cs in self.who_wants:
            assert isinstance(cs, ClientState), (repr(cs), self.who_wants)
        for ws in self.who_has:
            assert isinstance(ws, WorkerState), (repr(ws), self.who_has)
        for ts in self.dependencies:
            assert isinstance(ts, TaskState), (repr(ts), self.dependencies)
        for ts in self.dependents:
            assert isinstance(ts, TaskState), (repr(ts), self.dependents)


class _StateLegacyMapping(Mapping):
    """
    A mapping interface mimicking the former Scheduler state dictionaries.
    """

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return iter(self._states)

    def __len__(self):
        return len(self._states)

    def __getitem__(self, key):
        return self._accessor(self._states[key])

    def __repr__(self):
        return "%s(%s)" % (self.__class__, dict(self))


class _OptionalStateLegacyMapping(_StateLegacyMapping):
    """
    Similar to _StateLegacyMapping, but a false-y value is interpreted
    as a missing key.
    """
    # For tasks etc.

    def __iter__(self):
        accessor = self._accessor
        for k, v in self._states.items():
            if accessor(v):
                yield k

    def __len__(self):
        accessor = self._accessor
        return sum(bool(accessor(v)) for v in self._states.values())

    def __getitem__(self, key):
        v = self._accessor(self._states[key])
        if v:
            return v
        else:
            raise KeyError


class _StateLegacySet(Set):
    """
    Similar to _StateLegacyMapping, but exposes a set containing
    all values with a true value.
    """
    # For loose_restrictions

    def __init__(self, states, accessor):
        self._states = states
        self._accessor = accessor

    def __iter__(self):
        return filter(None, map(self._accessor, self._states))

    def __len__(self):
        return sum(map(bool, map(self._accessor, self._states)))

    def __contains__(self, k):
        return bool(self._accessor(self._states[k]))

    def __repr__(self):
        return "%s(%s)" % (self.__class__, set(self))


def _legacy_task_key_set(tasks):
    """
    Transform a set of task states into a set of task keys.
    """
    return {ts.key for ts in tasks}


def _legacy_client_key_set(clients):
    """
    Transform a set of client states into a set of client keys.
    """
    return {cs.client_key for cs in clients}


def _legacy_worker_key_set(workers):
    """
    Transform a set of worker states into a set of worker keys.
    """
    return {ws.worker_key for ws in workers}


def _legacy_task_key_dict(task_dict):
    """
    Transform a dict of {task state: value} into a dict of {task key: value}.
    """
    return {ts.key: value for ts, value in task_dict.items()}


class Scheduler(ServerNode):
    """ Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world through Comm objects.
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask-scheduler``
    executable::

         $ dask-scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{key: task}``:
        Dictionary mapping key to a serialized task like the following:
        ``{'function': b'...', 'args': b'...'}`` or ``{'task': b'...'}``
    * **dependencies:** ``{key: {keys}}``:
        Dictionary showing which keys depend on which others
    * **dependents:** ``{key: {keys}}``:
        Dictionary showing which keys are dependent on which others
    * **task_state:** ``{key: string}``:
        Dictionary listing the current state of every task among the following:
        released, waiting, queue, no-worker, processing, memory, erred
    * **priority:** ``{key: tuple}``:
        A score per key that determines its priority
    * **waiting:** ``{key: {key}}``:
        Dictionary like dependencies but excludes keys already computed
    * **waiting_data:** ``{key: {key}}``:
        Dictionary like dependents but excludes keys already computed
    * **ready:** ``deque(key)``
        Keys that are ready to run, but not yet assigned to a worker
    * **processing:** ``{worker: {key: cost}}``:
        Set of keys currently in execution on each worker and their expected
        duration
    * **rprocessing:** ``{key: worker}``:
        The worker currently executing a particular task
    * **who_has:** ``{key: {worker}}``:
        Where each key lives.  The current state of distributed memory.
    * **has_what:** ``{worker: {key}}``:
        What worker has what keys.  The transpose of who_has.
    * **released:** ``{keys}``
        Set of keys that are known, but released from memory
    * **unrunnable:** ``{key}``
        Keys that we are unable to run
    * **host_restrictions:** ``{key: {hostnames}}``:
        A set of hostnames per key of where that key can be run.  Usually this
        is empty unless a key has been specifically restricted to only run on
        certain hosts.
    * **worker_restrictions:** ``{key: {workers}}``:
        Like host_restrictions except that these include specific host:port
        worker names
    * **loose_restrictions:** ``{key}``:
        Set of keys for which we are allow to violate restrictions (see above)
        if not valid workers are present.
    * **resource_restrictions:** ``{key: {str: Number}}``:
        Resources required by a task, such as ``{'GPU': 1}`` or
        ``{'memory': 1e9}``.  These names must match resources specified when
        creating workers.
    * **worker_resources:** ``{worker: {str: Number}}``:
        The available resources on each worker like ``{'gpu': 2, 'mem': 1e9}``.
        These are abstract quantities that constrain certain tasks from running
        at the same time.
    * **used_resources:** ``{worker: {str: Number}}``:
        The sum of each resource used by all tasks allocated to a particular
        worker.
    * **exceptions:** ``{key: Exception}``:
        A dict mapping keys to remote exceptions
    * **tracebacks:** ``{key: list}``:
        A dict mapping keys to remote tracebacks stored as a list of strings
    * **exceptions_blame:** ``{key: key}``:
        A dict mapping a key to another key on which it depends that has failed
    * **suspicious_tasks:** ``{key: int}``
        Number of times a task has been involved in a worker failure
    * **retries:** ``{key: int}``
        Number of times a task may be automatically retried after failing
    * **deleted_keys:** ``{key: {workers}}``
        Locations of workers that have keys that should be deleted
    * **wants_what:** ``{client: {key}}``:
        What keys are wanted by each client..  The transpose of who_wants.
    * **who_wants:** ``{key: {client}}``:
        Which clients want each key.  The active targets of computation.
    * **nbytes:** ``{key: int}``:
        Number of bytes for a key as reported by workers holding that key.
    * **ncores:** ``{worker: int}``:
        Number of cores owned by each worker
    * **idle:** ``{worker}``:
        Set of workers that are not fully utilized
    * **worker_info:** ``{worker: {str: data}}``:
        Information about each worker
    * **host_info:** ``{hostname: dict}``:
        Information about each worker host
    * **worker_bytes:** ``{worker: int}``:
        Number of bytes in memory on each worker
    * **occupancy:** ``{worker: time}``
        Expected runtime for all tasks currently processing on a worker

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like Bokeh
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **comms:** ``[Comm]``:
        A list of Comms from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    * **coroutines:** ``[Futures]``:
        A list of active futures that control operation
    """
    default_port = 8786

    def __init__(
            self,
            center=None,
            loop=None,
            delete_interval=500,
            synchronize_worker_interval=60000,
            services=None,
            allowed_failures=ALLOWED_FAILURES,
            extensions=None,
            validate=False,
            scheduler_file=None,
            security=None,
            **kwargs):

        self._setup_logging()

        # Attributes
        self.allowed_failures = allowed_failures
        self.validate = validate
        self.status = None
        self.delete_interval = delete_interval
        self.synchronize_worker_interval = synchronize_worker_interval
        self.digests = None
        self.service_specs = services or {}
        self.services = {}
        self.scheduler_file = scheduler_file

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args('scheduler')
        self.listen_args = self.security.get_listen_args('scheduler')

        # Communication state
        self.loop = loop or IOLoop.current()
        self.worker_comms = dict()
        self.comms = dict()
        self.coroutines = []
        self._worker_coroutines = []
        self._ipython_kernel = None

        # Task state
        self.task_states = dict()
        for old_attr, new_attr, wrap in [
                ('task_state', 'state', None),
                ('priority', 'priority', None),
                ('dependencies', 'dependencies', _legacy_task_key_set),
                ('dependents', 'dependents', _legacy_task_key_set),
                ('nbytes', 'nbytes', None),
                ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr,
                    _StateLegacyMapping(self.task_states, func))

        for old_attr, new_attr, wrap in [
                ('tasks', 'run_spec', None),
                ('who_wants', 'who_wants', _legacy_client_key_set),
                ('who_has', 'who_has', _legacy_worker_key_set),
                ('rprocessing', 'processing_on', None),
                ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr,
                    _OptionalStateLegacyMapping(self.task_states, func))

        self.generation = 0
        self.released = set()
        self.task_duration = {prefix: 0.00001 for prefix in fast_tasks}
        self.unknown_durations = defaultdict(set)
        self.host_restrictions = dict()
        self.worker_restrictions = dict()
        self.resource_restrictions = dict()
        self.loose_restrictions = set()
        self.retries = dict()
        self.suspicious_tasks = defaultdict(lambda: 0)
        self.waiting = dict()
        self.waiting_data = dict()
        self.ready = deque()
        self.unrunnable = set()
        self.exceptions = dict()
        self.tracebacks = dict()
        self.exceptions_blame = dict()
        self.datasets = dict()
        self.n_tasks = 0
        self.task_metadata = dict()

        # Client state
        # XXX rename to self.clients?
        self.client_states = dict()
        for old_attr, new_attr, wrap in [
                ('wants_what', 'wants_what', _legacy_task_key_set),
                ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr,
                    _StateLegacyMapping(self.client_states, func))
        self.client_states['fire-and-forget'] = ClientState('fire-and-forget')

        # Worker state
        self.workers = SortedDict()
        for old_attr, new_attr, wrap in [
                ('ncores', 'ncores', None),
                ('worker_bytes', 'nbytes', None),
                ('worker_resources', 'resources', None),
                ('used_resources', 'used_resources', None),
                ('occupancy', 'occupancy', None),
                ('processing', 'processing', _legacy_task_key_dict),
                ('has_what', 'has_what', _legacy_task_key_set),
                ]:
            func = operator.attrgetter(new_attr)
            if wrap is not None:
                func = compose(wrap, func)
            setattr(self, old_attr,
                    _StateLegacyMapping(self.workers, func))

        self.idle = SortedSet(key=operator.attrgetter('worker_key'))
        self.saturated = set()

        self.total_ncores = 0
        self.total_occupancy = 0
        self.worker_info = dict()
        self.host_info = defaultdict(dict)
        self.resources = defaultdict(dict)
        self.aliases = dict()

        self._task_collections = [self.waiting, self.waiting_data,
                                  self.released,
                                  self.host_restrictions, self.worker_restrictions,
                                  self.loose_restrictions, self.ready,
                                  self.unknown_durations,
                                  self.resource_restrictions, self.retries]

        self._worker_collections = [self.workers,
                                    self.worker_info, self.host_info,
                                    self.worker_restrictions, self.host_restrictions,
                                    self.resource_restrictions,
                                    self.resources, self.aliases]

        self.extensions = {}
        self.plugins = []
        self.transition_log = deque(maxlen=config.get('transition-log-length',
                                                      100000))
        self.log = deque(maxlen=config.get('transition-log-length', 100000))

        self.worker_handlers = {'task-finished': self.handle_task_finished,
                                'task-erred': self.handle_task_erred,
                                'release': self.handle_release_data,
                                'release-worker-data': self.release_worker_data,
                                'add-keys': self.add_keys,
                                'missing-data': self.handle_missing_data,
                                'long-running': self.handle_long_running,
                                'reschedule': self.reschedule}

        self.client_handlers = {'update-graph': self.update_graph,
                                'client-desires-keys': self.client_desires_keys,
                                'update-data': self.update_data,
                                'report-key': self.report_on_key,
                                'client-releases-keys': self.client_releases_keys,
                                'restart': self.restart}

        self.handlers = {'register-client': self.add_client,
                         'scatter': self.scatter,
                         'register': self.add_worker,
                         'unregister': self.remove_worker,
                         'gather': self.gather,
                         'cancel': self.stimulus_cancel,
                         'feed': self.feed,
                         'terminate': self.close,
                         'broadcast': self.broadcast,
                         'ncores': self.get_ncores,
                         'has_what': self.get_has_what,
                         'who_has': self.get_who_has,
                         'processing': self.get_processing,
                         'call_stack': self.get_call_stack,
                         'profile': self.get_profile,
                         'logs': self.get_logs,
                         'worker_logs': self.get_worker_logs,
                         'nbytes': self.get_nbytes,
                         'versions': self.get_versions,
                         'add_keys': self.add_keys,
                         'rebalance': self.rebalance,
                         'replicate': self.replicate,
                         'start_ipython': self.start_ipython,
                         'run_function': self.run_function,
                         'update_data': self.update_data,
                         'set_resources': self.add_resources,
                         'retire_workers': self.retire_workers,
                         'get_metadata': self.get_metadata,
                         'set_metadata': self.set_metadata,
                         'get_task_status': self.get_task_status}

        self._transitions = {
            ('released', 'waiting'): self.transition_released_waiting,
            ('waiting', 'released'): self.transition_waiting_released,
            ('waiting', 'processing'): self.transition_waiting_processing,
            ('waiting', 'memory'): self.transition_waiting_memory,
            ('processing', 'released'): self.transition_processing_released,
            ('processing', 'memory'): self.transition_processing_memory,
            ('processing', 'erred'): self.transition_processing_erred,
            ('no-worker', 'released'): self.transition_no_worker_released,
            ('no-worker', 'waiting'): self.transition_no_worker_waiting,
            ('released', 'forgotten'): self.transition_released_forgotten,
            ('memory', 'forgotten'): self.transition_memory_forgotten,
            ('erred', 'forgotten'): self.transition_released_forgotten,
            ('memory', 'released'): self.transition_memory_released,
            ('released', 'erred'): self.transition_released_erred
        }

        connection_limit = get_fileno_limit() / 2

        super(Scheduler, self).__init__(
            handlers=self.handlers, io_loop=self.loop,
            connection_limit=connection_limit, deserialize=False,
            connection_args=self.connection_args,
            **kwargs)

        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        for ext in extensions:
            ext(self)

    ##################
    # Administration #
    ##################

    def __repr__(self):
        return '<Scheduler: "%s" processes: %d cores: %d>' % (
            self.address, len(self.workers), self.total_ncores)

    def identity(self, comm=None):
        """ Basic information about ourselves and our cluster """
        d = {'type': type(self).__name__,
             'id': str(self.id),
             'address': self.address,
             'services': {key: v.port for (key, v) in self.services.items()},
             'workers': dict(self.worker_info)}
        return d

    def get_worker_service_addr(self, worker, service_name):
        """
        Get the (host, port) address of the named service on the *worker*.
        Returns None if the service doesn't exist.
        """
        info = self.worker_info[worker]
        port = info['services'].get(service_name)
        if port is None:
            return None
        else:
            return info['host'], port

    def get_versions(self, comm):
        """ Basic information about ourselves and our cluster """
        return get_versions()

    def start_services(self, listen_ip):
        for k, v in self.service_specs.items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            if listen_ip == '0.0.0.0':
                listen_ip = ''  # for IPv6

            try:
                service = v(self, io_loop=self.loop)
                service.listen((listen_ip, port))
                self.services[k] = service
            except Exception as e:
                logger.info("Could not launch service: %r", (k, port),
                            exc_info=True)

    def stop_services(self):
        for service in self.services.values():
            service.stop()

    def start(self, addr_or_port=8786, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        # XXX what about nested state such as ClientState.wants_what
        # (see also fire-and-forget...)
        for collection in self._task_collections:
            collection.clear()

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        for cor in self.coroutines:
            if cor.done():
                exc = cor.exception()
                if exc:
                    raise exc

        if self.status != 'running':
            if isinstance(addr_or_port, int):
                # Listen on all interfaces.  `get_ip()` is not suitable
                # as it would prevent connecting via 127.0.0.1.
                self.listen(('', addr_or_port), listen_args=self.listen_args)
                self.ip = get_ip()
                listen_ip = ''
            else:
                self.listen(addr_or_port, listen_args=self.listen_args)
                self.ip = get_address_host(self.listen_address)
                listen_ip = self.ip

            if listen_ip == '0.0.0.0':
                listen_ip = ''

            if isinstance(addr_or_port, str) and addr_or_port.startswith('inproc://'):
                listen_ip = 'localhost'

            # Services listen on all addresses
            self.start_services(listen_ip)

            self.status = 'running'
            logger.info("  Scheduler at: %25s", self.address)
            for k, v in self.services.items():
                logger.info("%11s at: %25s", k, '%s:%d' % (listen_ip, v.port))

        if self.scheduler_file:
            with open(self.scheduler_file, 'w') as f:
                json.dump(self.identity(), f, indent=2)

            fn = self.scheduler_file  # remove file when we close the process

            def del_scheduler_file():
                if os.path.exists(fn):
                    os.remove(fn)

            finalize(self, del_scheduler_file)

        self.loop.add_callback(self.reevaluate_occupancy)
        self.start_periodic_callbacks()

        return self.finished()

    @gen.coroutine
    def finished(self):
        """ Wait until all coroutines have ceased """
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    def close_comms(self):
        """ Close all active Comms."""
        for comm in self.comms.values():
            comm.abort()
        self.rpc.close()

    @gen.coroutine
    def close(self, comm=None, fast=False):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status == 'closed':
            return
        logger.info("Scheduler closing...")
        self.stop_services()
        for ext in self.extensions:
            with ignoring(AttributeError):
                ext.teardown()
        logger.info("Scheduler closing all comms")
        yield self.cleanup()
        if not fast:
            yield self.finished()
        self.close_comms()
        self.status = 'closed'
        self.stop()
        yield super(Scheduler, self).close()

    @gen.coroutine
    def close_worker(self, stream=None, worker=None):
        """ Remove a worker from the cluster

        This both removes the worker from our local state and also sends a
        signal to the worker to shut down.  This works regardless of whether or
        not the worker has a nanny process restarting it
        """
        logger.info("Closing worker %s", worker)
        with log_errors():
            self.log_event(worker, {'action': 'close-worker'})
            nanny_addr = self.get_worker_service_addr(worker, 'nanny')
            address = nanny_addr or worker

            self.remove_worker(address=worker)

            with rpc(address, connection_args=self.connection_args) as r:
                try:
                    yield r.terminate(report=False)
                except EnvironmentError as e:
                    logger.info("Exception from worker while closing: %s", e)

            self.remove_worker(address=worker)

    @gen.coroutine
    def cleanup(self):
        """ Clean up queues and coroutines, prepare to stop """
        if self.status == 'closing':
            raise gen.Return()

        self.status = 'closing'
        logger.debug("Cleaning up coroutines")

        futures = []
        for w, comm in list(self.worker_comms.items()):
            with ignoring(AttributeError):
                futures.append(comm.close())

        for future in futures:
            yield future

    def _setup_logging(self):
        self._deque_handler = DequeHandler(n=config.get('log-length', 10000))
        self._deque_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(self._deque_handler)
        finalize(self, logger.removeHandler, self._deque_handler)

    ###########
    # Stimuli #
    ###########

    def add_worker(self, comm=None, address=None, keys=(), ncores=None,
                   name=None, resolve_address=True, nbytes=None, now=None,
                   resources=None, host_info=None, **info):
        """ Add a new worker to the cluster """
        with log_errors():
            local_now = time()
            now = now or time()
            info = info or {}
            host_info = host_info or {}

            address = self.coerce_address(address, resolve_address)
            host = get_address_host(address)
            self.host_info[host]['last-seen'] = local_now

            address = normalize_address(address)

            ws = self.workers.get(address)
            if ws is None:
                ws = self.workers[address] = WorkerState(address, ncores)
                existing = False
            else:
                existing = True

            if address not in self.worker_info:
                self.worker_info[address] = dict()

            if info:
                self.worker_info[address].update(info)

            if host_info:
                self.host_info[host].update(host_info)

            self.worker_info[address]['ncores'] = ncores

            delay = time() - now
            self.worker_info[address]['time-delay'] = delay
            self.worker_info[address]['last-seen'] = time()
            if resources:
                self.add_resources(worker=address, resources=resources)
                self.worker_info[address]['resources'] = resources

            if existing:
                self.log_event(address, merge({'action': 'heartbeat'}, info))
                return {'status': 'OK', 'time': time()}

            name = name or address
            if name in self.aliases:
                return {'status': 'error',
                        'message': 'name taken, %s' % name,
                        'time': time()}

            if 'addresses' not in self.host_info[host]:
                self.host_info[host].update({'addresses': set(), 'cores': 0})

            self.host_info[host]['addresses'].add(address)
            self.host_info[host]['cores'] += ncores

            self.total_ncores += ncores
            self.aliases[name] = address
            self.worker_info[address]['name'] = name
            self.worker_info[address]['host'] = host

            # Do not need to adjust self.total_occupancy as self.occupancy[ws] cannot exist before this.
            self.check_idle_saturated(ws)

            # for key in keys:  # TODO
            #     self.mark_key_in_memory(key, [address])

            self.worker_comms[address] = BatchedSend(interval=5, loop=self.loop)
            self._worker_coroutines.append(self.handle_worker(address))

            if ws.ncores > len(ws.processing):
                self.idle.add(ws)

            for plugin in self.plugins[:]:
                try:
                    plugin.add_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if nbytes:
                for key in nbytes:
                    state = self.task_state.get(key)
                    if state in ('processing', 'waiting'):
                        recommendations = self.transition(key, 'memory',
                                                          worker=address, nbytes=nbytes[key])
                        self.transitions(recommendations)

            recommendations = {}
            for key in list(self.unrunnable):
                valid = self.valid_workers(key)
                if valid is True or ws in valid:
                    recommendations[key] = 'waiting'

            if recommendations:
                self.transitions(recommendations)

            self.log_event(address, {'action': 'add-worker'})
            self.log_event('all', {'action': 'add-worker',
                                   'worker': address})
            logger.info("Register %s", str(address))
            return {'status': 'OK', 'time': time()}

    def update_graph(self, client=None, tasks=None, keys=None,
                     dependencies=None, restrictions=None, priority=None,
                     loose_restrictions=None, resources=None,
                     submitting_task=None, retries=None):
        """
        Add new computations to the internal dask graph

        This happens whenever the Client calls submit, map, get, or compute.
        """
        start = time()
        keys = set(keys)
        if len(tasks) > 1:
            self.log_event(['all', client], {'action': 'update_graph',
                                             'count': len(tasks)})

        # Remove aliases
        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]

        dependencies = dependencies or {}

        n = 0
        while len(tasks) != n:  # walk thorough new tasks, cancel any bad deps
            n = len(tasks)
            for k, deps in list(dependencies.items()):
                if any(dep not in self.task_states and dep not in tasks
                       for dep in deps):  # bad key
                    logger.info('User asked for computation on lost data, %s', k)
                    del tasks[k]
                    del dependencies[k]
                    if k in keys:
                        keys.remove(k)
                    self.report({'op': 'cancelled-key', 'key': k}, client=client)
                    self.client_releases_keys(keys=[k], client=client)

        # Remove any self-dependencies (happens on test_publish_bag()
        # and others)
        for k in dependencies:
            deps = set(dependencies[k])
            if k in deps:
                deps.remove(k)
            dependencies[k] = deps

        # XXX use task states everywhere

        stack = list(keys)
        touched = set()
        while stack:
            k = stack.pop()
            if k in touched:
                continue
            # XXX Have a method get_task_state(self, k) ?
            ts = self.task_states.get(k)
            if ts is None:
                ts = self.task_states[k] = TaskState(k, tasks.get(k))
                ts.state = 'released'
                self.released.add(k)
            elif not ts.run_spec:
                ts.run_spec = tasks.get(k)

            touched.add(k)
            stack.extend(dependencies.get(k, ()))

        self.client_desires_keys(keys=keys, client=client)

        for key, deps in dependencies.items():
            ts = self.task_states.get(key)
            if ts is None or ts.dependencies:
                continue
            for dep in deps:
                dts = self.task_states[dep]
                ts.dependencies.add(dts)
                dts.dependents.add(ts)

        recommendations = OrderedDict()

        new_priority = priority or order(tasks)  # TODO: define order wrt old graph
        if submitting_task:  # sub-tasks get better priority than parent tasks
            ts = self.task_states.get(submitting_task)
            if ts is not None:
                generation = ts.priority[0] - 0.01
            else:  # super-task already cleaned up
                generation = self.generation
        else:
            self.generation += 1  # older graph generations take precedence
            generation = self.generation
        for key in set(new_priority) & touched:
            ts = self.task_states[key]
            if ts.priority is None:
                ts.priority = (generation, new_priority[key])  # prefer old

        runnables = [key for key in keys | touched
                     if self.task_states[key].run_spec]

        for key in runnables:
            ts = self.task_states[key]
            if ts.priority is None and ts.run_spec:
                ts.priority = (self.generation, 0)

        if restrictions:
            # *restrictions* is a dict keying task ids to lists of
            # restriction specifications (either worker names or addresses)
            worker_restrictions = defaultdict(set)
            host_restrictions = defaultdict(set)
            for k, v in restrictions.items():
                if v is None:
                    continue
                for w in v:
                    try:
                        w = self.coerce_address(w)
                    except ValueError:
                        # Not a valid address, but perhaps it's a hostname
                        host_restrictions[k].add(w)
                    else:
                        worker_restrictions[k].add(w)

            self.worker_restrictions.update(worker_restrictions)
            self.host_restrictions.update(host_restrictions)

            if loose_restrictions:
                self.loose_restrictions |= set(loose_restrictions)

        if resources:
            self.resource_restrictions.update(resources)

        if retries:
            self.retries.update(retries)

        for ts in sorted([self.task_states[key] for key in runnables],
                          key=operator.attrgetter('priority')):
            if ts.state == 'released' and ts.run_spec:
                recommendations[ts.key] = 'waiting'

        for key in touched | keys:
            ts = self.task_states[key]
            for dts in ts.dependencies:
                if dts.key in self.exceptions_blame:
                    self.exceptions_blame[key] = self.exceptions_blame[dts.key]
                    recommendations[key] = 'erred'
                    break

        self.transitions(recommendations)

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(self, client=client, tasks=tasks,
                                    keys=keys, restrictions=restrictions or {},
                                    dependencies=dependencies,
                                    loose_restrictions=loose_restrictions)
            except Exception as e:
                logger.exception(e)

        for key in keys:
            ts = self.task_states[key]
            if ts.state in ('memory', 'erred'):
                self.report_on_key(key, client=client)

        end = time()
        if self.digests is not None:
            self.digests['update-graph-duration'].add(end - start)

        # TODO: balance workers

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Stimulus task finished %s, %s", key, worker)

        ts = self.task_states.get(key)
        if ts is None:
            return {}
        ws = self.workers[worker]

        if ts.state == 'processing':
            recommendations = self.transition(key, 'memory', worker=worker,
                                              **kwargs)

            if ts.state == 'memory':
                if ts not in ws.has_what:
                    ws.has_what.add(ts)
                    ws.nbytes += ts.get_nbytes()
                    ts.who_has.add(ws)
        else:
            logger.debug("Received already computed task, worker: %s, state: %s"
                         ", key: %s, who_has: %s",
                         worker, ts.state, key, ts.who_has)
            if ws not in ts.who_has:
                self.worker_send(worker, {'op': 'release-task', 'key': key})
            recommendations = {}

        return recommendations

    def stimulus_task_erred(self, key=None, worker=None,
                            exception=None, traceback=None, **kwargs):
        """ Mark that a task has erred on a particular worker """
        logger.debug("Stimulus task erred %s, %s", key, worker)

        ts = self.task_states.get(key)
        if ts is None:
            return {}

        if ts.state == 'processing':
            retries = self.retries.get(key, 0)
            if retries > 0:
                self.retries[key] = retries - 1
                recommendations = self.transition(key, 'waiting')
            else:
                recommendations = self.transition(key, 'erred',
                                                  cause=key,
                                                  exception=exception,
                                                  traceback=traceback,
                                                  worker=worker,
                                                  **kwargs)
        else:
            recommendations = {}

        return recommendations

    def stimulus_missing_data(self, cause=None, key=None, worker=None,
                              ensure=True, **kwargs):
        """ Mark that certain keys have gone missing.  Recover. """
        with log_errors():
            logger.debug("Stimulus missing data %s, %s", key, worker)

            ts = self.task_states.get(key)
            if ts is None or ts.state == 'memory':
                return {}
            cts = self.task_states.get(cause)

            recommendations = OrderedDict()

            if cts is not None and cts.state == 'memory':  # couldn't find this
                for ws in cts.who_has:  # TODO: this behavior is extreme
                    ws.has_what.remove(cts)
                    cts.who_has.remove(ws)
                    ws.nbytes -= cts.get_nbytes()
                recommendations[cause] = 'released'

            if key:
                recommendations[key] = 'released'

            self.transitions(recommendations)

            if self.validate:
                assert cause not in self.who_has

            return {}

    def remove_worker(self, comm=None, address=None, safe=False, close=True):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors():
            if self.status == 'closed':
                return
            if address not in self.workers:
                return 'already-removed'

            address = self.coerce_address(address)
            host = get_address_host(address)

            ws = self.workers[address]

            self.log_event(['all', address], {'action': 'remove-worker',
                                              'worker': address,
                                              'processing-tasks': ws.processing})
            logger.info("Remove worker %s", address)
            if close:
                with ignoring(AttributeError, CommClosedError):
                    self.worker_comms[address].send({'op': 'close'})

            self.remove_resources(address)

            self.host_info[host]['cores'] -= ws.ncores
            self.host_info[host]['addresses'].remove(address)
            self.total_ncores -= ws.ncores

            if not self.host_info[host]['addresses']:
                del self.host_info[host]

            del self.worker_comms[address]
            del self.aliases[self.worker_info[address]['name']]
            del self.worker_info[address]
            self.idle.discard(ws)
            self.saturated.discard(ws)
            del self.workers[address]

            recommendations = OrderedDict()

            in_flight = set(ws.processing)
            for ts in list(in_flight):
                k = ts.key
                if not safe:
                    self.suspicious_tasks[k] += 1
                if not safe and self.suspicious_tasks[k] > self.allowed_failures:
                    e = pickle.dumps(KilledWorker(k, address))
                    r = self.transition(k, 'erred', exception=e, cause=k)
                    recommendations.update(r)
                    in_flight.remove(k)
                else:
                    recommendations[k] = 'released'

            self.total_occupancy -= ws.occupancy

            for ts in ws.has_what:
                ts.who_has.remove(ws)
                if not ts.who_has:
                    if ts.run_spec:
                        recommendations[ts.key] = 'released'
                    else:  # pure data
                        recommendations[ts.key] = 'forgotten'

            self.transitions(recommendations)

            for plugin in self.plugins[:]:
                try:
                    plugin.remove_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if not self.workers:
                logger.info("Lost all workers")

            logger.debug("Removed worker %s", address)
        return 'OK'

    def stimulus_cancel(self, comm, keys=None, client=None, force=False):
        """ Stop execution on a list of keys """
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        if client:
            self.log_event(client, {'action': 'cancel', 'count': len(keys),
                                    'force': force})
        for key in keys:
            self.cancel_key(key, client, force=force)

    def cancel_key(self, key, client, retries=5, force=False):
        """ Cancel a particular key and all dependents """
        # TODO: this should be converted to use the transition mechanism
        ts = self.task_states.get(key)
        cs = self.client_states[client]
        if ts is None or not ts.who_wants:  # no key yet, lets try again in a moment
            if retries:
                self.loop.add_future(gen.sleep(0.2),
                                     lambda _: self.cancel_key(key, client, retries - 1))
            return
        if force or ts.who_wants == {cs}:  # no one else wants this key
            for dts in list(ts.dependents):
                self.cancel_key(dts.key, client, force=force)
        logger.info("Scheduler cancels key %s.  Force=%s", key, force)
        self.report({'op': 'cancelled-key', 'key': key})
        clients = list(ts.who_wants) if force else [cs]
        for c in clients:
            self.client_releases_keys(keys=[key], client=c.client_key)

    def client_desires_keys(self, keys=None, client=None):
        cs = self.client_states.get(client)
        if cs is None:
            # For publish, queues etc.
            cs = self.client_states[client] = ClientState(client)
        for k in keys:
            ts = self.task_states.get(k)
            if ts is None:
                # For publish, queues etc.
                ts = self.task_states[k] = TaskState(k, None)
                ts.state = 'released'
                self.released.add(k)
            ts.who_wants.add(cs)
            cs.wants_what.add(ts)

            if ts.state in ('memory', 'erred'):
                self.report_on_key(k, client=client)

    def client_releases_keys(self, keys=None, client=None):
        """ Remove keys from client desired list """
        cs = self.client_states[client]
        tasks2 = set()
        for key in list(keys):
            ts = self.task_states.get(key)
            if ts is not None and ts in cs.wants_what:
                cs.wants_what.remove(ts)
                s = ts.who_wants
                s.remove(cs)
                if not s:
                    tasks2.add(ts)

        for ts in tasks2:
            key = ts.key
            if key in self.waiting_data and not self.waiting_data[key]:
                r = self.transition(key, 'released')
                self.transitions(r)
            if not ts.dependents:
                r = self.transition(key, 'forgotten')
                self.transitions(r)

    ######################################
    # Task Validation (currently unused) #
    ######################################

    def validate_released(self, key):
        ts = self.task_states[key]
        assert ts.state == 'released'
        assert key not in self.waiting_data
        assert not ts.who_has
        assert not ts.processing_on
        # assert key not in self.ready
        assert key not in self.waiting
        assert not any(key in self.waiting_data.get(dts.key, ())
                       for dts in ts.dependencies)
        assert key in self.released

    def validate_waiting(self, key):
        ts = self.task_states[key]
        assert key in self.waiting
        assert key in self.waiting_data
        assert not ts.who_has
        assert not ts.processing_on
        assert key not in self.released
        for dts in ts.dependencies:
            assert bool(dts.who_has) + (dts.key in self.waiting[key]) == 1
            #assert dep in self.waiting[key] == key in self.waiting_data[dep]
            assert key in self.waiting_data[dts.key]

    def validate_processing(self, key):
        ts = self.task_states[key]
        assert key not in self.waiting
        assert key in self.waiting_data
        ws = ts.processing_on
        assert ws
        assert ts in ws.processing
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has
            assert key in self.waiting_data[dts.key]

    def validate_memory(self, key):
        ts = self.task_states[key]
        assert ts.who_has
        assert not ts.processing_on
        assert key not in self.waiting
        assert key not in self.released
        for dts in ts.dependents:
            assert bool(dts.who_has) + (dts.key in self.waiting_data[key]) == 1

    def validate_no_worker(self, key):
        ts = self.task_states[key]
        assert key in self.unrunnable
        assert key not in self.waiting
        assert key not in self.released
        assert not ts.processing_on
        assert not ts.who_has
        for dts in ts.dependencies:
            assert dts.who_has

    def validate_erred(self, key):
        ts = self.task_states[key]
        assert key in self.exceptions_blame
        assert not ts.who_has

    def validate_key(self, key):
        try:
            ts = self.task_states.get(key)
            if ts is None:
                logger.debug("Key lost: %s", key)
            else:
                ts.validate()
                try:
                    func = getattr(self, 'validate_' + ts.state.replace('-', '_'))
                except AttributeError:
                    logger.error("self.validate_%s not found",
                                 ts.state.replace('-', '_'))
                else:
                    func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def validate_state(self, allow_overlap=False):
        # XXX rewrite this for state objects
        validate_state(self.dependencies, self.dependents, self.waiting,
                       self.waiting_data, self.ready, self.who_has,
                       self.processing, None, self.released, self.who_wants,
                       self.wants_what, tasks=self.tasks, erred=self.exceptions_blame,
                       allow_overlap=allow_overlap)

        if not (set(self.workers) ==
                set(self.worker_info) ==
                set(self.worker_comms)):
            raise ValueError("Workers not the same in all collections")

        for w, ws in self.workers.items():
            assert isinstance(w, str), (type(w), w)
            assert isinstance(ws, WorkerState), (type(ws), ws)
            assert ws.worker_key == w

        for k, ts in self.task_states.items():
            assert isinstance(k, str), (type(k), k)
            assert isinstance(ts, TaskState), (type(ts), ts)
            assert ts.key == k

        for c, cs in self.client_states.items():
            # client=None is often used in tests...
            assert c is None or isinstance(c, str), (type(c), c)
            assert isinstance(cs, ClientState), (type(cs), cs)
            assert cs.client_key == c

        a = {w: ws.nbytes for w, ws in self.workers.items()}
        b = {w: sum(ts.nbytes for ts in ws.has_what)
             for w, ws in self.workers.items()}
        assert a == b, (a, b)

        for key, workers in self.who_has.items():
            ts = self.task_states[key]
            for worker in workers:
                assert ts in self.workers[worker].has_what

        for ws in self.workers.values():
            for ts in ws.has_what:
                assert ws.worker_key in self.who_has[ts.key]

        assert all(self.who_has.values())

        actual_total_occupancy = 0
        for worker, ws in self.workers.items():
        #     for d in self.extensions['stealing'].in_flight.values():
        #         if worker in (d['thief'], d['victim']):
        #             continue
            assert abs(sum(ws.processing.values()) - ws.occupancy) < 1e-8
            actual_total_occupancy += ws.occupancy

        assert abs(actual_total_occupancy - self.total_occupancy) < 1e-8

    ###################
    # Manage Messages #
    ###################

    def report(self, msg, ts=None, client=None):
        """
        Publish updates to all listening Queues and Comms

        If the message contains a key then we only send the message to those
        comms that care about the key.
        """
        if client is not None:
            try:
                comm = self.comms[client]
                comm.send(msg)
            except CommClosedError:
                if self.status == 'running':
                    logger.critical("Tried writing to closed comm: %s", msg)
            except KeyError:
                pass

        if ts is None and 'key' in msg:
            ts = self.task_states.get(msg['key'])
        if ts is None:
            # Notify all clients
            comms = self.comms.values()
        else:
            # Notify clients interested in key
            comms = [self.comms[c.client_key]
                     for c in ts.who_wants
                     if c.client_key in self.comms]
        for c in comms:
            try:
                c.send(msg)
                # logger.debug("Scheduler sends message to client %s", msg)
            except CommClosedError:
                if self.status == 'running':
                    logger.critical("Tried writing to closed comm: %s", msg)

    @gen.coroutine
    def add_client(self, comm, client=None):
        """ Add client to network

        We listen to all future messages from this Comm.
        """
        assert client is not None
        logger.info("Receive client connection: %s", client)
        self.log_event(['all', client], {'action': 'add-client',
                                         'client': client})
        self.client_states[client] = ClientState(client)
        try:
            yield self.handle_client(comm, client=client)
        finally:
            if not comm.closed():
                self.comms[client].send({'op': 'stream-closed'})
            try:
                yield self.comms[client].close()
                del self.comms[client]
                logger.info("Close client connection: %s", client)
            except TypeError:  # comm becomes None during GC
                pass

    def remove_client(self, client=None):
        """ Remove client from network """
        logger.info("Remove client %s", client)
        self.log_event(['all', client], {'action': 'remove-client',
                                         'client': client})
        try:
            cs = self.client_states[client]
        except KeyError:
            # XXX is this a legitimate condition?
            pass
        else:
            self.client_releases_keys(keys=[ts.key for ts in cs.wants_what],
                                      client=cs.client_key)
            del self.client_states[client]

    @gen.coroutine
    def handle_client(self, comm, client=None):
        """
        Listen and respond to messages from clients

        This runs once per Client Comm or Queue.

        See Also
        --------
        Scheduler.worker_stream: The equivalent function for workers
        """
        bcomm = BatchedSend(interval=2, loop=self.loop)
        bcomm.start(comm)
        self.comms[client] = bcomm

        try:
            bcomm.send({'op': 'stream-start'})

            breakout = False

            while True:
                try:
                    msgs = yield comm.read()
                except (CommClosedError, AssertionError, GeneratorExit):
                    logger.info("Connection to client %s broken", str(client))
                    break
                except Exception as e:
                    logger.exception(e)
                    bcomm.send(error_message(e, status='scheduler-error'))
                    continue

                if self.status == 'closed':
                    return

                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    # logger.debug("scheduler receives message %s", msg)
                    try:
                        op = msg.pop('op')
                    except Exception as e:
                        logger.exception(e)
                        bcomm.end(error_message(e, status='scheduler-error'))

                    if op == 'close-stream':
                        breakout = True
                        break
                    elif op == 'close':
                        breakout = True
                        self.close()
                        break
                    elif op in self.client_handlers:
                        try:
                            handler = self.client_handlers[op]
                            if 'client' not in msg:
                                msg['client'] = client
                            result = handler(**msg)
                            if isinstance(result, gen.Future):
                                yield result
                        except Exception as e:
                            logger.exception(e)
                            raise
                    else:
                        logger.warning("Bad message: op=%s, %s", op, msg, exc_info=True)

                    if op == 'close':
                        breakout = True
                        break
                if breakout:
                    break

            self.remove_client(client=client)
            logger.debug('Finished handle_client coroutine')
        except Exception:
            try:
                logger.error("Exception in handle_client", exc_info=True)
            except TypeError:
                pass

    def send_task_to_worker(self, worker, key):
        """ Send a single computational task to a worker """
        try:
            ts = self.task_states[key]

            msg = {'op': 'compute-task',
                   'key': key,
                   'priority': ts.priority,
                   'duration': self.get_task_duration(key)}
            if key in self.resource_restrictions:
                msg['resource_restrictions'] = self.resource_restrictions[key]

            deps = ts.dependencies
            if deps:
                #msg['who_has'] = {dep.key: list(self.who_has[dep.key]) for dep in deps}
                msg['who_has'] = {dep.key: [ws.worker_key for ws in dep.who_has]
                                  for dep in deps}
                msg['nbytes'] = {dep.key: dep.nbytes for dep in deps}

            if self.validate and deps:
                assert all(msg['who_has'].values())

            task = ts.run_spec
            if type(task) is dict:
                msg.update(task)
            else:
                msg['task'] = task

            self.worker_send(worker, msg)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def handle_uncaught_error(self, **msg):
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(self, key=None, **msg):
        validate_key(key)
        r = self.stimulus_task_finished(key=key, **msg)
        self.transitions(r)

    def handle_task_erred(self, key=None, **msg):
        r = self.stimulus_task_erred(key=key, **msg)
        self.transitions(r)

    def handle_release_data(self, key=None, worker=None, client=None, **msg):
        ts = self.task_states[key]
        ws = self.workers[worker]
        if ts.processing_on is not ws:
            return
        r = self.stimulus_missing_data(key=key, ensure=False, **msg)
        self.transitions(r)

    def handle_missing_data(self, key=None, errant_worker=None, **kwargs):
        logger.debug("handle missing data key=%s worker=%s", key, errant_worker)
        self.log.append(('missing', key, errant_worker))

        ts = self.task_states.get(key)
        if ts is None or not ts.who_has:
            return
        ws = self.workers[errant_worker]
        if ws in ts.who_has:
            ts.who_has.remove(ws)
            ws.has_what.remove(ts)
            ws.nbytes -= ts.get_nbytes()
        if not ts.who_has:
            if ts.run_spec:
                self.transitions({key: 'released'})
            else:
                self.transitions({key: 'forgotten'})

    def release_worker_data(self, stream=None, keys=None, worker=None):
        ws = self.workers[worker]
        tasks = {self.task_states[k] for k in keys}
        removed_tasks = tasks & ws.has_what
        ws.has_what -= removed_tasks

        recommendations = {}
        for ts in removed_tasks:
            wh = ts.who_has
            wh.remove(ws)
            if not wh:
                recommendations[key] = 'released'
        if recommendations:
            self.transitions(recommendations)

    def handle_long_running(self, key=None, worker=None, compute_duration=None):
        """ A task has seceded from the thread pool

        We stop the task from being stolen in the future, and change task
        duration accounting as if the task has stopped.
        """
        if 'stealing' in self.extensions:
            self.extensions['stealing'].remove_key_from_stealable(key)

        ts = self.task_states[key]
        ws = ts.processing_on
        if ws is None:
            logger.debug("Received long-running signal from duplicate task. "
                         "Ignoring.")
            return

        if compute_duration:
            ks = key_split(key)
            old_duration = self.task_duration.get(ks, 0)
            new_duration = compute_duration
            if not old_duration:
                avg_duration = new_duration
            else:
                avg_duration = (0.5 * old_duration
                                + 0.5 * new_duration)

            self.task_duration[ks] = avg_duration

        ws.occupancy -= ws.processing[ts]
        self.total_occupancy -= ws.processing[ts]
        ws.processing[ts] = 0

    @gen.coroutine
    def handle_worker(self, worker):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        try:
            comm = yield connect(worker, connection_args=self.connection_args)
        except Exception as e:
            logger.error("Failed to connect to worker %r: %s",
                         worker, e)
            self.remove_worker(address=worker)
            return
        yield comm.write({'op': 'compute-stream', 'reply': False})
        worker_comm = self.worker_comms[worker]
        worker_comm.start(comm)
        logger.info("Starting worker compute stream, %s", worker)

        io_error = None
        try:
            while True:
                msgs = yield comm.read()
                start = time()

                if not isinstance(msgs, list):
                    msgs = [msgs]

                if worker in self.worker_info and not comm.closed():
                    self.counters['worker-message-length'].add(len(msgs))
                    for msg in msgs:
                        if msg == 'OK':  # from close
                            break
                        if 'status' in msg and 'error' in msg['status']:
                            try:
                                logger.error("error from worker %s: %s",
                                         worker, clean_exception(**msg)[1])
                            except Exception:
                                logger.error("error from worker %s", worker)
                        op = msg.pop('op')
                        if op:
                            handler = self.worker_handlers[op]
                            handler(worker=worker, **msg)

                end = time()
                if self.digests is not None:
                    self.digests['handle-worker-duration'].add(end - start)

        except (CommClosedError, EnvironmentError) as e:
            io_error = e
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise
        finally:
            if worker in self.worker_comms:
                # Worker didn't send us a close message
                if io_error:
                    logger.info("Worker %r failed from closed comm: %s",
                                worker, io_error)
                worker_comm.abort()
                self.remove_worker(address=worker)
            else:
                assert comm.closed()
                worker_comm.abort()

    def add_plugin(self, plugin):
        """
        Add external plugin to scheduler

        See https://distributed.readthedocs.io/en/latest/plugins.html
        """
        self.plugins.append(plugin)

    def remove_plugin(self, plugin):
        """ Remove external plugin from scheduler """
        self.plugins.remove(plugin)

    def worker_send(self, worker, msg):
        """ Send message to worker

        This also handles connection failures by adding a callback to remove
        the worker on the next cycle.
        """
        try:
            self.worker_comms[worker].send(msg)
        except (CommClosedError, AttributeError):
            self.loop.add_callback(self.remove_worker, address=worker)

    ############################
    # Less common interactions #
    ############################

    @gen.coroutine
    def scatter(self, comm=None, data=None, workers=None, client=None,
                broadcast=False, timeout=2):
        """ Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        start = time()
        while not self.workers:
            yield gen.sleep(0.2)
            if time() > start + timeout:
                raise gen.TimeoutError("No workers found")

        if workers is None:
            ncores = {w: ws.ncores for w, ws in self.workers.items()}
        else:
            workers = [self.coerce_address(w) for w in workers]
            ncores = {w: self.workers[w].ncores for w in workers}

        assert isinstance(data, dict)

        keys, who_has, nbytes = yield scatter_to_workers(ncores, data,
                                                         rpc=self.rpc,
                                                         report=False)

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

        if broadcast:
            if broadcast == True:  # flake8: noqa
                n = len(ncores)
            else:
                n = broadcast
            yield self.replicate(keys=keys, workers=workers, n=n)

        self.log_event([client, 'all'], {'action': 'scatter',
                                         'client': client,
                                         'count': len(data)})
        raise gen.Return(keys)

    @gen.coroutine
    def gather(self, comm=None, keys=None):
        """ Collect data in from workers """
        keys = list(keys)
        who_has = {key: [ws.worker_key for ws in self.task_states[key].who_has]
                   for key in keys}

        data, missing_keys, missing_workers = yield gather_from_workers(
            who_has, rpc=self.rpc, close=False)
        if not missing_keys:
            result = {'status': 'OK', 'data': data}
        else:
            missing_states = [(self.task_states[key].state
                               if key in self.task_states else None)
                              for key in missing_keys]
            logger.debug("Couldn't gather keys %s state: %s workers: %s",
                         missing_keys, missing_states, missing_workers)
            result = {'status': 'error', 'keys': missing_keys}
            with log_errors():
                for worker in missing_workers:
                    self.remove_worker(address=worker)  # this is extreme
                for key, workers in missing_keys.items():
                    if not workers:
                        continue
                    ts = self.task_states[key]
                    logger.exception("Workers don't have promised key. "
                                     "This should never occur: %s, %s",
                                     str(workers), str(key))
                    for worker in workers:
                        ws = self.workers.get(worker)
                        if ws is not None and ts in ws.has_what:
                            ws.has_what.remove(ts)
                            ts.who_has.remove(ws)
                            ws.nbytes -= ts.get_nbytes()
                            self.transitions({key: 'released'})

        self.log_event('all', {'action': 'gather',
                               'count': len(keys)})
        raise gen.Return(result)

    def clear_task_state(self):
        logger.info("Clear task state")
        for collection in self._task_collections:
            collection.clear()
        for collection in self._worker_collections:
            collection.clear()

    @gen.coroutine
    def restart(self, client=None, timeout=3):
        """ Restart all workers.  Reset local state. """
        with log_errors():

            n_workers = len(self.workers)

            logger.info("Send lost future signal to clients")
            for cs in self.client_states.values():
                self.client_releases_keys(keys=[ts.key for ts in cs.wants_what],
                                          client=cs.client_key)

            nannies = {addr: self.get_worker_service_addr(addr, 'nanny')
                       for addr in self.workers}

            for addr in list(self.workers):
                try:
                    # Ask the worker to close if it doesn't have a nanny,
                    # otherwise the nanny will kill it anyway
                    self.remove_worker(address=addr, close=addr not in nannies)
                except Exception as e:
                    logger.info("Exception while restarting.  This is normal",
                                exc_info=True)

            self.clear_task_state()

            for plugin in self.plugins[:]:
                try:
                    plugin.restart(self)
                except Exception as e:
                    logger.exception(e)

            logger.debug("Send kill signal to nannies: %s", nannies)

            nannies = [rpc(nanny_address, connection_args=self.connection_args)
                       for nanny_address in nannies.values()
                       if nanny_address is not None]

            try:
                resps = All([nanny.restart(close=True, timeout=timeout * 0.8)
                             for nanny in nannies])
                resps = yield gen.with_timeout(timedelta(seconds=timeout), resps)
                assert all(resp == 'OK' for resp in resps)
            except gen.TimeoutError:
                logger.error("Nannies didn't report back restarted within "
                             "timeout.  Continuuing with restart process")
            finally:
                for nanny in nannies:
                    nanny.close_rpc()

            self.start()

            self.log_event([client, 'all'], {'action': 'restart',
                                             'client': client})
            start = time()
            while time() < start + 10 and len(self.workers) < n_workers:
                yield gen.sleep(0.01)

            self.report({'op': 'restart'})

    @gen.coroutine
    def broadcast(self, comm=None, msg=None, workers=None, hosts=None,
                  nanny=False):
        """ Broadcast message to workers, return all results """
        if workers is None:
            if hosts is None:
                workers = list(self.workers)
            else:
                workers = []
        if hosts is not None:
            for host in hosts:
                if host in self.host_info:
                    workers.extend(self.host_info[host]['addresses'])
        # TODO replace with worker_list

        if nanny:
            addresses = [self.get_worker_service_addr(w, 'nanny')
                         for w in workers]
        else:
            addresses = workers

        @gen.coroutine
        def send_message(addr):
            comm = yield connect(addr, deserialize=self.deserialize,
                                 connection_args=self.connection_args)
            resp = yield send_recv(comm, close=True, **msg)
            raise gen.Return(resp)

        results = yield All([send_message(self.coerce_address(address))
                             for address in addresses
                             if address is not None])

        raise Return(dict(zip(workers, results)))

    @gen.coroutine
    def rebalance(self, comm=None, keys=None, workers=None):
        """ Rebalance keys so that each worker stores roughly equal bytes

        **Policy**

        This orders the workers by what fraction of bytes of the existing keys
        they have.  It walks down this list from most-to-least.  At each worker
        it sends the largest results it can find and sends them to the least
        occupied worker until either the sender or the recipient are at the
        average expected load.
        """
        with log_errors():
            if keys:
                tasks = {self.task_states[k] for k in keys}
                missing_data = [ts.key for ts in tasks if not ts.who_has]
                if missing_data:
                    raise Return({'status': 'missing-data',
                                  'keys': missing_data})
            else:
                tasks = set(self.task_states.values())

            if workers:
                workers = {self.workers[w] for w in workers}
                workers_by_task = {ts: ts.who_has & workers for ts in tasks}
            else:
                workers = set(self.workers.values())
                workers_by_task = {ts: ts.who_has for ts in tasks}

            tasks_by_worker = {ws: set() for ws in workers}

            for k, v in workers_by_task.items():
                for vv in v:
                    tasks_by_worker[vv].add(k)

            worker_bytes = {ws: sum(ts.get_nbytes() for ts in v)
                            for ws, v in tasks_by_worker.items()}

            avg = sum(worker_bytes.values()) / len(worker_bytes)

            sorted_workers = list(map(first, sorted(worker_bytes.items(),
                                                    key=second, reverse=True)))

            recipients = iter(reversed(sorted_workers))
            recipient = next(recipients)
            msgs = []  # (sender, recipient, key)
            for sender in sorted_workers[:len(workers) // 2]:
                sender_keys = {ts: ts.get_nbytes()
                               for ts in tasks_by_worker[sender]}
                sender_keys = iter(sorted(sender_keys.items(),
                                          key=second, reverse=True))

                try:
                    while worker_bytes[sender] > avg:
                        while (worker_bytes[recipient] < avg and
                               worker_bytes[sender] > avg):
                            ts, nb = next(sender_keys)
                            if ts not in tasks_by_worker[recipient]:
                                tasks_by_worker[recipient].add(ts)
                                # tasks_by_worker[sender].remove(ts)
                                msgs.append((sender, recipient, ts))
                                worker_bytes[sender] -= nb
                                worker_bytes[recipient] += nb
                        if worker_bytes[sender] > avg:
                            recipient = next(recipients)
                except StopIteration:
                    break

            to_recipients = defaultdict(lambda: defaultdict(list))
            to_senders = defaultdict(list)
            for sender, recipient, ts in msgs:
                to_recipients[recipient.worker_key][ts.key].append(sender.worker_key)
                to_senders[sender.worker_key].append(ts.key)

            result = yield {r: self.rpc(addr=r).gather(who_has=v)
                            for r, v in to_recipients.items()}
            for r, v in to_recipients.items():
                self.log_event(r, {'action': 'rebalance',
                                   'who_has': v})

            self.log_event('all', {'action': 'rebalance',
                                   'total-keys': len(tasks),
                                   'senders': valmap(len, to_senders),
                                   'recipients': valmap(len, to_recipients),
                                   'moved_keys': len(msgs)})

            if not all(r['status'] == 'OK' for r in result.values()):
                raise Return({'status': 'missing-data',
                              'keys': sum([r['keys'] for r in result
                                           if 'keys' in r], [])})

            for sender, recipient, ts in msgs:
                ts.who_has.add(recipient)
                recipient.has_what.add(ts)
                recipient.nbytes += ts.get_nbytes()
                self.log.append(('rebalance', ts.key, time(),
                                 sender.worker_key, recipient.worker_key))

            result = yield {r: self.rpc(addr=r).delete_data(keys=v, report=False)
                            for r, v in to_senders.items()}

            for sender, recipient, ts in msgs:
                ts.who_has.remove(sender)
                sender.has_what.remove(ts)
                sender.nbytes -= ts.get_nbytes()

            raise Return({'status': 'OK'})

    @gen.coroutine
    def replicate(self, comm=None, keys=None, n=None, workers=None,
                  branching_factor=2, delete=True):
        """ Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation.
            The larger the branching factor, the more data we copy in
            a single step, but the more a given worker risks being
            swamped by data requests.

        See also
        --------
        Scheduler.rebalance
        """
        assert branching_factor > 0

        workers = {self.workers[w] for w in self.workers_list(workers)}
        if n is None:
            n = len(workers)
        else:
            n = min(n, len(workers))
        if n == 0:
            raise ValueError("Can not use replicate to delete data")

        tasks = {self.task_states[k] for k in keys}
        missing_data = [ts.key for ts in tasks if not ts.who_has]
        if missing_data:
            raise Return({'status': 'missing-data',
                          'keys': missing_data})

        # Delete extraneous data
        if delete:
            del_worker_tasks = defaultdict(set)
            for ts in tasks:
                del_candidates = ts.who_has & workers
                if len(del_candidates) > n:
                    for ws in random.sample(del_candidates,
                                            len(del_candidates) - n):
                        del_worker_tasks[ws].add(ts)

            yield [self.rpc(addr=ws.worker_key)
                       .delete_data(keys=[ts.key for ts in tasks], report=False)
                   for ws, tasks in del_worker_tasks.items()]

            for ws, tasks in del_worker_tasks.items():
                ws.has_what -= tasks
                for ts in tasks:
                    ts.who_has.remove(ws)
                    ws.nbytes -= ts.get_nbytes()
                self.log_event(ws.worker_key,
                               {'action': 'replicate-remove',
                                'keys': [ts.key for ts in tasks]})

        # Copy not-yet-filled data
        while tasks:
            gathers = defaultdict(dict)
            for ts in list(tasks):
                n_missing = n - len(ts.who_has & workers)
                if n_missing <= 0:
                    # Already replicated enough
                    tasks.remove(ts)
                    continue

                count = min(n_missing,
                            branching_factor * len(ts.who_has))
                assert count > 0

                for ws in random.sample(workers - ts.who_has, count):
                    gathers[ws.worker_key][ts.key] = [wws.worker_key
                                                      for wws in ts.who_has]

            results = yield {w: self.rpc(addr=w).gather(who_has=who_has)
                             for w, who_has in gathers.items()}
            for w, v in results.items():
                if v['status'] == 'OK':
                    self.add_keys(worker=w, keys=list(gathers[w]))
                else:
                    logger.warning("Communication failed during replication: %s",
                                   v)

                self.log_event(w, {'action': 'replicate-add',
                                   'keys': gathers[w]})

        self.log_event('all', {'action': 'replicate',
                               'workers': list(workers),
                               'key-count': len(keys),
                               'branching-factor': branching_factor})

    def workers_to_close(self, memory_ratio=2):
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are idle (not running anything) and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_factor: Number
            Amount of extra space we want to have for our stored data.
            Defaults two 2, or that we want to have twice as much memory as we
            currently have data.

        Returns
        -------
        to_close: list of workers that are OK to close
        """
        with log_errors():
            # XXX processing isn't used is the heuristics below
            if all(ws.processing for ws in self.workers.values()):
                return []

            limit_bytes = {w: self.worker_info[w]['memory_limit']
                           for w in self.worker_info}

            limit = sum(limit_bytes.values())
            total = sum(ws.nbytes for ws in self.workers.values())
            idle = sorted(self.idle, key=operator.attrgetter('nbytes'), reverse=True)

            to_close = []

            while idle:
                w = idle.pop().worker_key
                limit -= limit_bytes[w]
                if limit >= memory_ratio * total:  # still plenty of space
                    to_close.append(w)
                else:
                    break

            return to_close

    @gen.coroutine
    def retire_workers(self, comm=None, workers=None, remove=True, close=False,
                       close_workers=False):
        if close:
            logger.warning("The keyword close= has been deprecated. "
                           "Use close_workers= instead")
        close_workers = close_workers or close
        with log_errors():
            if workers is None:
                while True:
                    try:
                        workers = self.workers_to_close()
                        if workers:
                            yield self.retire_workers(workers=workers,
                                                      remove=remove, close_workers=close_workers)
                        raise gen.Return(list(workers))
                    except KeyError:  # keys left during replicate
                        pass

            workers = {self.workers[w] for w in workers}
            if len(workers) > 0:
                # Keys orphaned by retiring those workers
                keys = set.union(*[w.has_what for w in workers])
                keys = {ts.key for ts in keys if ts.who_has.issubset(workers)}
            else:
                keys = set()

            other_workers = set(self.workers.values()) - workers
            if keys:
                if other_workers:
                    yield self.replicate(keys=keys,
                                         workers=[ws.worker_key for ws in other_workers],
                                         n=1, delete=False)
                else:
                    raise gen.Return([])

            worker_keys = [ws.worker_key for ws in workers]
            if close_workers and worker_keys:
                yield [self.close_worker(worker=w)
                       for w in worker_keys]
            if remove:
                for w in worker_keys:
                    self.remove_worker(address=w, safe=True)

            self.log_event('all', {'action': 'retire-workers',
                                   'workers': worker_keys,
                                   'moved-keys': len(keys)})
            self.log_event(worker_keys, {'action': 'retired'})

            raise gen.Return(worker_keys)

    def add_keys(self, comm=None, worker=None, keys=()):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.
        """
        if worker not in self.worker_info:
            return 'not found'
        ws = self.workers[worker]
        for key in keys:
            ts = self.task_states.get(key)
            if ts is not None:
                if ts not in ws.has_what:
                    ws.nbytes += ts.get_nbytes()
                    ws.has_what.add(ts)
                    ts.who_has.add(ws)
            else:
                self.worker_send(worker, {'op': 'delete-data',
                                          'keys': [key],
                                          'report': False})
        return 'OK'

    def update_data(self, comm=None, who_has=None, nbytes=None, client=None):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        with log_errors():
            who_has = {k: [self.coerce_address(vv) for vv in v]
                       for k, v in who_has.items()}
            logger.debug("Update data %s", who_has)

            # for key, workers in who_has.items():  # TODO
            #     self.mark_key_in_memory(key, workers)

            for key, workers in who_has.items():
                ts = self.task_states.get(key)
                if ts is None:
                    ts = self.task_states[key] = TaskState(key, None)
                ts.state = 'memory'
                if key in nbytes:
                    ts.nbytes = nbytes[key]
                for w in workers:
                    ws = self.workers[w]
                    if ts not in ws.has_what:
                        ws.nbytes += ts.get_nbytes()
                        ws.has_what.add(ts)
                        ts.who_has.add(ws)
                if key not in self.waiting_data:
                    self.waiting_data[key] = set()
                self.report({'op': 'key-in-memory',
                             'key': key,
                             'workers': list(workers)})

            if client:
                self.client_desires_keys(keys=list(who_has), client=client)

    def report_on_key(self, key=None, ts=None, client=None):
        assert (key is None) + (ts is None) == 1, (key, ts)
        if ts is None:
            ts = self.task_states[key]
        else:
            key = ts.key
        if ts.state == 'forgotten':
            self.report({'op': 'cancelled-key',
                         'key': key}, ts=ts, client=client)
        elif ts.state == 'memory':
            self.report({'op': 'key-in-memory',
                         'key': key}, ts=ts, client=client)
        elif ts.state == 'erred':
            failing_key = self.exceptions_blame[key]
            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key, None)},
                        ts=ts, client=client)

    @gen.coroutine
    def feed(self, comm, function=None, setup=None, teardown=None, interval=1, **kwargs):
        """
        Provides a data Comm to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
        import pickle
        with log_errors():
            if function:
                function = pickle.loads(function)
            if setup:
                setup = pickle.loads(setup)
            if teardown:
                teardown = pickle.loads(teardown)
            state = setup(self) if setup else None
            if isinstance(state, gen.Future):
                state = yield state
            try:
                while self.status == 'running':
                    if state is None:
                        response = function(self)
                    else:
                        response = function(self, state)
                    yield comm.write(response)
                    yield gen.sleep(interval)
            except (EnvironmentError, CommClosedError):
                pass
            finally:
                if teardown:
                    teardown(self, state)

    def get_processing(self, comm=None, workers=None):
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: [ts.key for ts in self.workers[w].processing]
                    for w in workers}
        else:
            return {w: [ts.key for ts in ws.processing]
                    for w, ws in self.workers.items()}

    def get_who_has(self, comm=None, keys=None):
        if keys is not None:
            return {k: [ws.worker_key for ws in self.task_states[k].who_has]
                    for k in keys if k in self.task_states}
        else:
            return {key: [ws.worker_key for ws in ts.who_has]
                    for key, ts in self.task_states.items()}

    def get_has_what(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: [ts.key for ts in self.workers[w].has_what]
                    for w in workers if w in self.workers}
        else:
            return {w: [ts.key for ts in ws.has_what]
                    for w, ws in self.workers.items()}

    def get_ncores(self, comm=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: self.workers[w].ncores
                    for w in workers if w in self.workers}
        else:
            return {w: ws.ncores for w, ws in self.workers.items()}

    @gen.coroutine
    def get_call_stack(self, comm=None, keys=None):
        if keys is not None:
            stack = list(keys)
            processing = set()
            while stack:
                key = stack.pop()
                ts = self.task_states[key]
                if ts.state == 'waiting':
                    stack.extend(dts.key for dts in ts.dependencies)
                elif ts.state == 'processing':
                    processing.add(ts)

            workers = defaultdict(list)
            for ts in processing:
                if ts.processing_on:
                    workers[ts.processing_on.worker_key].append(ts.key)
        else:
            workers = {w: None for w in self.workers}

        if not workers:
            raise gen.Return({})

        else:
            response = yield {w: self.rpc(w).call_stack(keys=v)
                              for w, v in workers.items()}
            response = {k: v for k, v in response.items() if v}
            raise gen.Return(response)

    def get_nbytes(self, comm=None, keys=None, summary=True):
        with log_errors():
            if keys is not None:
                result = {k: self.task_states[k].nbytes for k in keys}
            else:
                result = dict(self.nbytes)

            if summary:
                out = defaultdict(lambda: 0)
                for k, v in result.items():
                    out[key_split(k)] += v
                result = out

            return result

    def get_comm_cost(self, ts, ws):
        """
        Get the estimated communication cost (in s.) to compute the task
        on the given worker.
        """
        return (sum(dts.nbytes
                    for dts in ts.dependencies - ws.has_what)
                / BANDWIDTH)

    def get_task_duration(self, key, default=0.5):
        """
        Get the estimated computation cost of the given key
        (not including any communication cost).
        """
        ks = key_split(key)
        try:
            return self.task_duration[ks]
        except KeyError:
            self.unknown_durations[ks].add(key)
            return default

    def run_function(self, stream, function, args=(), kwargs={}):
        """ Run a function within this process

        See Also
        --------
        Client.run_on_scheduler:
        """
        from .worker import run
        self.log_event('all', {'action': 'run-function', 'function': function})
        return run(self, stream, function=function, args=args, kwargs=kwargs)

    def set_metadata(self, stream=None, keys=None, value=None):
        try:
            metadata = self.task_metadata
            for key in keys[:-1]:
                if key not in metadata or not isinstance(metadata[key], (dict, list)):
                    metadata[key] = dict()
                metadata = metadata[key]
            metadata[keys[-1]] = value
        except Exception as e:
            import pdb; pdb.set_trace()

    def get_metadata(self, stream=None, keys=None, default=no_default):
        metadata = self.task_metadata
        for key in keys[:-1]:
            metadata = metadata[key]
        try:
            return metadata[keys[-1]]
        except KeyError:
            if default != no_default:
                return default
            else:
                raise

    def get_task_status(self, stream=None, keys=None):
        return {key: (self.task_states[key].state
                      if key in self.task_states else None)
                for key in keys}

    #####################
    # State Transitions #
    #####################

    def transition_released_waiting(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert ts.run_spec
                assert key not in self.waiting
                assert not ts.who_has
                assert not ts.processing_on
                # assert all(dep in self.task_state
                #            for dep in self.dependencies[key])

            if any(dts.state == 'forgotten' for dts in ts.dependencies):
                return {key: 'forgotten'}

            recommendations = OrderedDict()

            for dts in ts.dependencies:
                if dts.key in self.exceptions_blame:
                    self.exceptions_blame[key] = self.exceptions_blame[dts.key]
                    recommendations[key] = 'erred'
                    return recommendations

            self.waiting[key] = set()

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    self.waiting[key].add(dep)
                if dep in self.released:
                    recommendations[dep] = 'waiting'
                else:
                    self.waiting_data[dep].add(key)

            self.waiting_data[key] = {dts.key for dts in ts.dependents
                                      if not dts.who_has
                                      and dts.key not in self.released
                                      and dts.key not in self.exceptions_blame}

            if not self.waiting[key]:
                if self.workers:
                    ts.state = 'waiting'
                    recommendations[key] = 'processing'
                else:
                    self.unrunnable.add(key)
                    del self.waiting[key]
                    ts.state = 'no-worker'
            else:
                ts.state = 'waiting'

            if self.validate:
                if ts.state == 'waiting':
                    assert key in self.waiting

            self.released.remove(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_no_worker_waiting(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert key in self.unrunnable
                assert key not in self.waiting
                assert not ts.who_has
                assert not ts.processing_on

            self.unrunnable.remove(key)

            #if not all(dep in self.task_states
                       #for dep in ts.dependencies):
            if any(dts.state == 'forgotten' for dts in ts.dependencies):
                return {key: 'forgotten'}

            recommendations = OrderedDict()

            self.waiting[key] = set()

            for dts in ts.dependencies:
                dep = dts.key
                if not dts.who_has:
                    self.waiting[key].add(dep)
                if dep in self.released:
                    recommendations[dep] = 'waiting'
                else:
                    self.waiting_data[dep].add(key)

            ts.state = 'waiting'

            if not self.waiting[key]:
                if self.workers:
                    recommendations[key] = 'processing'
                else:
                    ts.state = 'no-worker'

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def decide_worker(self, key):
        """
        Decide on a worker for processing *key*.  Return a WorkerState.
        """
        ts = self.task_states[key]
        valid_workers = self.valid_workers(key)

        if not valid_workers and key not in self.loose_restrictions and self.workers:
            self.unrunnable.add(key)
            ts.state = 'no-worker'
            return None

        if ts.dependencies or valid_workers is not True:
            worker = decide_worker(ts, self.workers.values(),
                                   valid_workers, self.loose_restrictions,
                                   partial(self.worker_objective, ts))
        elif self.idle:
            if len(self.idle) < 20:  # smart but linear in small case
                worker = min(self.idle,
                             key=operator.attrgetter('occupancy'))
            else:  # dumb but fast in large case
                worker = self.idle[self.n_tasks % len(self.idle)]
        else:
            if len(self.workers) < 20:  # smart but linear in small case
                worker = min(self.workers.values(),
                             key=operator.attrgetter('occupancy'))
            else:  # dumb but fast in large case
                worker = self.workers[
                    self.workers.iloc[self.n_tasks % len(self.workers)]
                    ]

        assert worker is None or isinstance(worker, WorkerState), (type(worker), worker)
        return worker

    def transition_waiting_processing(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert key in self.waiting
                assert not self.waiting[key]
                assert not ts.who_has
                assert key not in self.exceptions_blame
                assert not ts.processing_on
                # assert key not in self.readyset
                assert key not in self.unrunnable
                assert all(dts.who_has
                           for dts in ts.dependencies)

            if any(not dts.who_has for dts in ts.dependencies):
                # XXX can happen? see validation above
                return {}

            del self.waiting[key]

            ws = self.decide_worker(key)
            if ws is None:
                return {}
            worker = ws.worker_key

            duration = self.get_task_duration(key)
            comm = self.get_comm_cost(ts, ws)

            ws.processing[ts] = duration + comm
            ts.processing_on = ws
            ws.occupancy += duration + comm
            self.total_occupancy += duration + comm
            ts.state = 'processing'
            self.consume_resources(key, worker)
            self.check_idle_saturated(ws)
            self.n_tasks += 1

            # logger.debug("Send job to worker: %s, %s", worker, key)

            self.send_task_to_worker(worker, key)

            if self.validate:
                assert key not in self.waiting

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_waiting_memory(self, key, nbytes=None, worker=None, **kwargs):
        try:
            ws = self.workers[worker]
            ts = self.task_states[key]

            if self.validate:
                assert not ts.processing_on
                assert key in self.waiting
                assert ts.state == 'waiting'

            del self.waiting[key]

            if nbytes is not None:
                ts.nbytes = nbytes

            ts.who_has.add(ws)
            ws.has_what.add(ts)
            ws.nbytes += ts.get_nbytes()

            self.check_idle_saturated(ws)

            recommendations = OrderedDict()

            deps = ts.dependents
            if len(deps) > 1:
                deps = sorted(deps, key=operator.attrgetter('priority'),
                              reverse=True)

            for dts in deps:
                dep = dts.key
                if dep in self.waiting:
                    s = self.waiting[dep]
                    s.remove(key)
                    if not s:  # new task ready to run
                        recommendations[dep] = 'processing'

            for dts in ts.dependencies:
                dep = dts.key
                if dep in self.waiting_data:
                    s = self.waiting_data[dep]
                    s.remove(key)
                    if not s and not dts.who_wants:
                        recommendations[dep] = 'released'

            if (not self.waiting_data.get(key) and
                    not ts.who_wants):
                recommendations[key] = 'released'
            else:
                msg = {'op': 'key-in-memory',
                       'key': key}
                self.report(msg)

            ts.state = 'memory'

            if self.validate:
                assert not ts.processing_on
                assert key not in self.waiting
                assert ts.who_has

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_processing_memory(self, key, nbytes=None, type=None,
                                     worker=None, startstops=None, **kwargs):
        try:
            ts = self.task_states[key]
            assert worker
            assert isinstance(worker, str)

            if self.validate:
                assert ts.processing_on
                ws = ts.processing_on
                assert ts in ws.processing
                assert key not in self.waiting
                assert not ts.who_has
                assert key not in self.exceptions_blame
                # assert all(dep in self.waiting_data[key ] for dep in
                #         self.dependents[key] if self.task_state[dep] in
                #         ['waiting', 'queue', 'stacks'])

                assert ts.state == 'processing'

            ws = self.workers.get(worker)
            if ws is None:
                return {key: 'released'}

            if ws is not ts.processing_on:  # someone else has this task
                logger.warning("Unexpected worker completed task, likely due to"
                               " work stealing.  Expected: %s, Got: %s, Key: %s",
                               ts.processing_on, ws, key)
                return {}

            if startstops:
                L = [(b, c) for a, b, c in startstops if a == 'compute']
                if L:
                    compute_start, compute_stop = L[0]
                else:  # This is very rare
                    compute_start = compute_stop = None
            else:
                compute_start = compute_stop = None

            #############################
            # Update Timing Information #
            #############################
            if compute_start and ws.processing.get(ts, True):
                # Update average task duration for worker
                info = self.worker_info[worker]
                ks = key_split(key)
                old_duration = self.task_duration.get(ks, 0)
                new_duration = compute_stop - compute_start
                if not old_duration:
                    avg_duration = new_duration
                else:
                    avg_duration = (0.5 * old_duration
                                    + 0.5 * new_duration)

                self.task_duration[ks] = avg_duration

                if ks in self.unknown_durations:
                    for k in self.unknown_durations.pop(ks):
                        tts = self.task_states[k]
                        if tts.processing_on:
                            wws = tts.processing_on
                            old = wws.processing[tts]
                            comm = self.get_comm_cost(tts, wws)
                            wws.processing[tts] = avg_duration + comm
                            wws.occupancy += avg_duration + comm - old
                            self.total_occupancy += avg_duration + comm - old

                info['last-task'] = compute_stop

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                ts.nbytes = nbytes

            self.release_resources(key, worker)

            ts.who_has.add(ws)
            ws.has_what.add(ts)
            ws.nbytes += ts.get_nbytes()

            ts.processing_on = None
            duration = ws.processing.pop(ts)
            if not ws.processing:
                self.total_occupancy -= ws.occupancy
                ws.occupancy = 0
            else:
                self.total_occupancy -= duration
                ws.occupancy -= duration
            self.check_idle_saturated(ws)

            recommendations = OrderedDict()

            deps = ts.dependents
            if len(deps) > 1:
                deps = sorted(deps, key=operator.attrgetter('priority'),
                              reverse=True)

            for dts in deps:
                dep = dts.key
                if dep in self.waiting:
                    s = self.waiting[dep]
                    s.remove(key)
                    if not s:  # new task ready to run
                        recommendations[dep] = 'processing'

            for dts in ts.dependencies:
                dep = dts.key
                if dep in self.waiting_data:
                    s = self.waiting_data[dep]
                    s.remove(key)
                    if not s and not dts.who_wants:
                        recommendations[dep] = 'released'

            if (not self.waiting_data.get(key) and
                    not ts.who_wants):
                recommendations[key] = 'released'
            else:
                msg = {'op': 'key-in-memory',
                       'key': key}
                if type is not None:
                    msg['type'] = type
                self.report(msg)

            ts.state = 'memory'

            cs = self.client_states['fire-and-forget']
            if ts in cs.wants_what:
                self.client_releases_keys(client='fire-and-forget', keys=[key])

            if self.validate:
                assert not ts.processing_on
                assert key not in self.waiting

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_memory_released(self, key, safe=False):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert key not in self.released
                # assert key not in self.readyset
                assert key not in self.waiting
                assert not ts.processing_on
                if safe:
                    assert not self.waiting_data.get(key)
                # assert key not in self.who_wants

            recommendations = OrderedDict()

            for dep in self.waiting_data.get(key, ()):  # lost dependency
                dts = self.task_states[dep]
                if dts.state in ('no-worker', 'processing'):
                    recommendations[dep] = 'waiting'
                elif dts.state == 'waiting':
                    self.waiting[dep].add(key)

            # XXX factor this out?
            for ws in ts.who_has:
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
                self.worker_send(ws.worker_key, {'op': 'delete-data',
                                                 'keys': [key],
                                                 'report': False})
            ts.who_has.clear()

            self.released.add(key)

            ts.state = 'released'
            self.report({'op': 'lost-data', 'key': key})

            if not ts.run_spec:  # pure data
                recommendations[key] = 'forgotten'
            elif any(dts.state == 'forgotten' for dts in ts.dependencies):
                recommendations[key] = 'forgotten'
            elif ts.who_wants or self.waiting_data.get(key):
                recommendations[key] = 'waiting'

            if key in self.waiting_data:
                del self.waiting_data[key]

            if self.validate:
                assert key not in self.waiting

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_released_erred(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert key in self.exceptions_blame
                    assert not ts.who_has
                    assert key not in self.waiting
                    assert key not in self.waiting_data

            recommendations = {}

            failing_key = self.exceptions_blame[key]

            for dts in ts.dependents:
                dep = dts.key
                self.exceptions_blame[dep] = failing_key
                if not dts.who_has:
                    recommendations[dep] = 'erred'

            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key, None)})

            ts.state = 'erred'
            self.released.remove(key)

            # TODO: waiting data?
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_waiting_released(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert key in self.waiting
                assert key in self.waiting_data
                assert not ts.who_has
                assert not ts.processing_on

            recommendations = {}

            del self.waiting[key]

            for dts in ts.dependencies:
                dep = dts.key
                if dep in self.waiting_data:
                    if key in self.waiting_data[dep]:
                        self.waiting_data[dep].remove(key)
                    if not self.waiting_data[dep] and not dts.who_wants:
                        recommendations[dep] = 'released'
                    assert dts.state != 'erred'

            ts.state = 'released'
            self.released.add(key)

            if self.validate:
                assert not any(key in self.waiting_data.get(dts.key, ())
                               for dts in ts.dependencies)

            #if any(dep not in self.task_states
                   #for dep in ts.dependencies):
            if any(dts.state == 'forgotten' for dts in ts.dependencies):
                recommendations[key] = 'forgotten'

            elif (key not in self.exceptions_blame and
                  (ts.who_wants or self.waiting_data.get(key))):
                recommendations[key] = 'waiting'

            del self.waiting_data[key]

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_processing_released(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert ts.processing_on
                assert not ts.who_has
                assert key not in self.waiting
                assert self.task_states[key].state == 'processing'

            # XXX factor this out?
            ws = ts.processing_on
            ts.processing_on = None
            w = ws.worker_key
            if w in self.workers:
                duration = ws.processing.pop(ts)
                if not ws.processing:
                    self.total_occupancy -= ws.occupancy
                    ws.occupancy = 0
                else:
                    self.total_occupancy -= duration
                    ws.occupancy -= duration
                self.check_idle_saturated(ws)
                # XXX release_resources() and friends should take (ts, ws)?
                self.release_resources(key, w)
                self.worker_send(w, {'op': 'release-task', 'key': key})

            self.released.add(key)
            ts.state = 'released'

            recommendations = OrderedDict()

            if any(dts.state == 'forgotten' for dts in ts.dependencies):
                recommendations[key] = 'forgotten'
            elif self.waiting_data[key] or ts.who_wants:
                recommendations[key] = 'waiting'
            else:
                for dts in ts.dependencies:
                    dep = dts.key
                    if dep not in self.released:
                        assert key in self.waiting_data[dep]
                        self.waiting_data[dep].remove(key)
                        if not self.waiting_data[dep] and not dts.who_wants:
                            recommendations[dep] = 'released'
                del self.waiting_data[key]

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_processing_erred(self, key, cause=None, exception=None,
                                    traceback=None, **kwargs):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert cause or key in self.exceptions_blame
                assert ts.processing_on
                assert not ts.who_has
                assert key not in self.waiting

            if exception:
                self.exceptions[key] = exception
            if traceback:
                self.tracebacks[key] = traceback
            if cause:
                self.exceptions_blame[key] = cause

            failing_key = self.exceptions_blame[key]

            recommendations = {}

            for dts in ts.dependents:
                dep = dts.key
                self.exceptions_blame[dep] = key
                recommendations[dep] = 'erred'

            for dts in ts.dependencies:
                dep = dts.key
                if dep in self.waiting_data:
                    s = self.waiting_data[dep]
                    if key in s:
                        s.remove(key)
                    if not s and not dts.who_wants:
                        recommendations[dep] = 'released'

            ws = ts.processing_on
            ts.processing_on = None
            w = ws.worker_key
            if w in self.workers:  # worker may have been removed
                duration = ws.processing.pop(ts)
                if not ws.processing:
                    self.total_occupancy -= ws.occupancy
                    ws.occupancy = 0
                else:
                    self.total_occupancy -= duration
                    ws.occupancy -= duration
                self.check_idle_saturated(ws)
                self.release_resources(key, w)

            del self.waiting_data[key]  # do anything with this?

            ts.state = 'erred'

            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key)})

            cs = self.client_states['fire-and-forget']
            if ts in cs.wants_what:
                self.client_releases_keys(client='fire-and-forget', keys=[key])

            if self.validate:
                assert not ts.processing_on

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def remove_key(self, key):
        ts = self.task_states.pop(key)
        ts.state = 'forgotten'
        if key in self.worker_restrictions:
            del self.worker_restrictions[key]
        if key in self.host_restrictions:
            del self.host_restrictions[key]
        if key in self.loose_restrictions:
            self.loose_restrictions.remove(key)
        if key in self.exceptions:
            del self.exceptions[key]
        if key in self.exceptions_blame:
            del self.exceptions_blame[key]
        if key in self.tracebacks:
            del self.tracebacks[key]
        if key in self.released:
            self.released.remove(key)
        if key in self.waiting_data:
            del self.waiting_data[key]
        if key in self.suspicious_tasks:
            del self.suspicious_tasks[key]
        if key in self.retries:
            del self.retries[key]
        if key in self.resource_restrictions:
            del self.resource_restrictions[key]
        if key in self.task_metadata:
            del self.task_metadata[key]

    def _propagate_forgotten(self, ts, recommendations):
        key = ts.key
        for dts in ts.dependents:
            dep = dts.key
            if dts.state not in ('memory', 'error'):
            #if dts.state == 'released':
                recommendations[dep] = 'forgotten'

        for dts in ts.dependencies:
            dep = dts.key
            try:
                s = dts.dependents
                s.remove(ts)
                if not s and not dts.who_wants:
                    assert dep is not key
                    recommendations[dep] = 'forgotten'
            except KeyError:
                pass
            try:
                self.waiting_data[dep].discard(key)
            except KeyError:
                pass

        # XXX redundant with dependents handling above?
        if key in self.waiting_data:
            for dep in self.waiting_data[key]:
                recommendations[dep] = 'forgotten'

        for ws in ts.who_has:
            if ws is not None:  # in case worker has died
                ws.has_what.remove(ts)
                ws.nbytes -= ts.get_nbytes()
                self.worker_send(ws.worker_key, {'op': 'delete-data',
                                                 'keys': [key],
                                                 'report': False})
        ts.who_has.clear()

        if self.validate:
            assert all(key not in dts.dependents
                       for dts in ts.dependencies
                       if dts.state != 'forgotten')
            assert all(key not in self.waiting_data.get(dts.key, ())
                       for dts in ts.dependencies
                       if dts.state != 'forgotten')

    def transition_memory_forgotten(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert self.task_states[key].state == 'memory'
                assert key in self.waiting_data
                assert not ts.processing_on
                # assert key not in self.ready
                assert key not in self.waiting

            recommendations = {}
            self._propagate_forgotten(ts, recommendations)

            self.remove_key(key)

            self.report_on_key(ts=ts)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_no_worker_released(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert self.task_states[key].state == 'no-worker'
                assert not ts.who_has
                assert key not in self.waiting

            self.unrunnable.remove(key)
            self.released.add(key)
            ts.state = 'released'

            for dts in ts.dependencies:
                try:
                    self.waiting_data[dts.key].remove(key)
                except KeyError:  # dep may also be released
                    pass

            del self.waiting_data[key]

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition_released_forgotten(self, key):
        try:
            ts = self.task_states[key]

            if self.validate:
                assert self.task_states[key].state in ('released', 'erred')
                # assert not self.waiting_data[key]
                if ts.run_spec and all(dts.state != 'forgotten'
                                       for dts in ts.dependencies):
                    assert not ts.who_wants
                    assert not ts.dependents
                    assert not any(key in self.waiting_data.get(dep, ())
                                   for dep in ts.dependencies)
                assert not ts.who_has
                assert not ts.processing_on
                # assert key not in self.ready
                assert key not in self.waiting

            recommendations = {}
            self._propagate_forgotten(ts, recommendations)

            self.remove_key(key)

            self.report_on_key(ts=ts)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transition(self, key, finish, *args, **kwargs):
        """ Transition a key from its current state to the finish state

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
        try:
            try:
                ts = self.task_states[key]
            except KeyError:
                return {}
            start = ts.state
            if start == finish:
                return {}

            if (start, finish) in self._transitions:
                func = self._transitions[start, finish]
                recommendations = func(key, *args, **kwargs)
            else:
                func = self._transitions['released', finish]
                assert not args and not kwargs
                a = self.transition(key, 'released')
                if key in a:
                    func = self._transitions['released', a[key]]
                b = func(key)
                a = a.copy()
                a.update(b)
                recommendations = a
                start = 'released'

            finish2 = ts.state if key in self.task_states else 'forgotten'
            self.transition_log.append((key, start, finish2, recommendations,
                                        time()))
            if self.validate:
                logger.debug("Transition %s->%s: %s New: %s",
                             start, finish2, key, recommendations)
            for plugin in self.plugins:
                try:
                    plugin.transition(key, start, finish2, *args, **kwargs)
                except Exception:
                    logger.info("Plugin failed with exception", exc_info=True)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def transitions(self, recommendations):
        """ Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        keys = set()
        recommendations = recommendations.copy()
        while recommendations:
            key, finish = recommendations.popitem()
            keys.add(key)
            new = self.transition(key, finish)
            recommendations.update(new)

        if self.validate:
            for key in keys:
                self.validate_key(key)

    def story(self, *keys):
        """ Get all transitions that touch one of the input keys """
        keys = set(keys)
        return [t for t in self.transition_log
                if t[0] in keys or keys.intersection(t[3])]

    transition_story = story

    def reschedule(self, key=None, worker=None):
        """ Reschedule a task

        Things may have shifted and this task may now be better suited to run
        elsewhere
        """
        ts = self.task_states[key]
        if ts.state != 'processing':
            return
        if worker and ts.processing_on.worker_key != worker:
            return
        self.transitions({key: 'released'})

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def check_idle_saturated(self, ws, occ=None):
        if self.total_ncores == 0:
            return
        if occ is None:
            occ = ws.occupancy
        nc = ws.ncores
        p = len(ws.processing)

        avg = self.total_occupancy / self.total_ncores

        if p < nc or occ / nc < avg / 2:
            self.idle.add(ws)
            self.saturated.discard(ws)
        else:
            self.idle.discard(ws)

            pending = occ * (p - nc) / p / nc
            if p > nc and pending > 0.4 and pending > 1.9 * avg:
                self.saturated.add(ws)
            else:
                self.saturated.discard(ws)

    def valid_workers(self, key):
        """ Return set of currently valid workers for key

        If all workers are valid then this returns ``True``.
        This checks tracks the following state:

        *  worker_restrictions
        *  host_restrictions
        *  resource_restrictions
        """
        s = True

        if key in self.worker_restrictions:
            s = {w for w in self.worker_restrictions[key]
                 if w in self.worker_info}

        if key in self.host_restrictions:
            # Resolve the alias here rather than early, for the worker
            # may not be connected when host_restrictions is populated
            hr = [self.coerce_hostname(h) for h in self.host_restrictions[key]]
            # XXX need HostState?
            ss = [self.host_info[h]['addresses']
                  for h in hr if h in self.host_info]
            ss = set.union(*ss) if ss else set()
            if s is True:
                s = ss
            else:
                s |= ss

        if self.resource_restrictions.get(key):
            w = {resource: {w for w, supplied in self.resources[resource].items()
                            if supplied >= required}
                 for resource, required in self.resource_restrictions[key].items()}

            ww = set.intersection(*w.values())

            if s is True:
                s = ww
            else:
                s &= ww

        if s is True:
            return s
        else:
            return {self.workers[w] for w in s}

    def consume_resources(self, key, worker):
        ws = self.workers[worker]
        if key in self.resource_restrictions:
            for r, required in self.resource_restrictions[key].items():
                ws.used_resources[r] += required

    def release_resources(self, key, worker):
        ws = self.workers[worker]
        if key in self.resource_restrictions:
            for r, required in self.resource_restrictions[key].items():
                ws.used_resources[r] -= required

    #####################
    # Utility functions #
    #####################

    def add_resources(self, stream=None, worker=None, resources=None):
        ws = self.workers[worker]
        ws.resources = resources or {}
        ws.used_resources = {}
        for resource, quantity in ws.resources.items():
            ws.used_resources[resource] = 0
            self.resources[resource][worker] = quantity
        return 'OK'

    def remove_resources(self, worker):
        ws = self.workers[worker]
        for resource, quantity in ws.resources.items():
            del self.resources[resource][worker]

    def coerce_address(self, addr, resolve=True):
        """
        Coerce possible input addresses to canonical form.
        *resolve* can be disabled for testing with fake hostnames.

        Handles strings, tuples, or aliases.
        """
        # XXX how many address-parsing routines do we have?
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, tuple):
            addr = unparse_host_port(*addr)
        if not isinstance(addr, six.string_types):
            raise TypeError("addresses should be strings or tuples, got %r"
                            % (addr,))

        if resolve:
            addr = resolve_address(addr)
        else:
            addr = normalize_address(addr)

        return addr

    def coerce_hostname(self, host):
        """
        Coerce the hostname of a worker.
        """
        if host in self.aliases:
            return self.worker_info[self.aliases[host]]['host']
        else:
            return host

    def workers_list(self, workers):
        """
        List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.workers)

        out = set()
        for w in workers:
            if ':' in w:
                out.add(w)
            else:
                out.update({ww for ww in self.workers if w in ww})  # TODO: quadratic
        return list(out)

    def start_ipython(self, comm=None):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython
        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip,
                ns={'scheduler': self},
                log=logger,
            )
        return self._ipython_kernel.get_connection_info()

    def worker_objective(self, ts, ws):
        """
        Objective function to determine which worker should get the task

        Minimize expected start time.  If a tie then break with data storage.
        """
        comm_bytes = sum([dts.get_nbytes()
                          for dts in ts.dependencies
                          if ws not in dts.who_has])
        stack_time = ws.occupancy / ws.ncores
        start_time = comm_bytes / BANDWIDTH + stack_time
        return (start_time, ws.nbytes)

    @gen.coroutine
    def get_profile(self, comm=None, workers=None, merge_workers=True,
                    start=None, stop=None, key=None):
        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        result = yield {w: self.rpc(w).profile(start=start, stop=stop, key=key)
                        for w in workers}
        if merge_workers:
            result = profile.merge(*result.values())
        raise gen.Return(result)

    @gen.coroutine
    def get_profile_metadata(self, comm=None, workers=None, merge_workers=True,
                             start=None, stop=None, profile_cycle_interval=None):
        dt = profile_cycle_interval or config.get('profile-cycle-interval', 1000) / 1000
        if workers is None:
            workers = self.workers
        else:
            workers = set(self.workers) & set(workers)
        result = yield {w: self.rpc(w).profile_metadata(start=start, stop=stop)
                        for w in workers}

        counts = [v['counts'] for v in result.values()]
        counts = itertools.groupby(merge_sorted(*counts), lambda t: t[0] // dt * dt)
        counts = [(time, sum(pluck(1, group))) for time, group in counts]

        keys = set()
        for v in result.values():
            for t, d in v['keys']:
                for k in d:
                    keys.add(k)
        keys = {k: [] for k in keys}

        groups1 = [v['keys'] for v in result.values()]
        groups2 = list(merge_sorted(*groups1, key=first))

        last = 0
        for t, d in groups2:
            tt = t // dt * dt
            if tt > last:
                last = tt
                for k, v in keys.items():
                    v.append([tt, 0])
            for k, v in d.items():
                keys[k][-1][1] += v

        raise gen.Return({'counts': counts, 'keys': keys})

    def get_logs(self, comm=None, n=None):
        deque_handler = self._deque_handler
        if n is None:
            L = list(deque_handler.deque)
        else:
            L = deque_handler.deque
            L = [L[-i] for i in range(min(n, len(L)))]
        return [(msg.levelname, deque_handler.format(msg)) for msg in L]


    @gen.coroutine
    def get_worker_logs(self, comm=None, n=None, workers=None):
        results = yield self.broadcast(msg={'op': 'get_logs', 'n': n},
                                       workers=workers)
        raise gen.Return(results)

    ###########
    # Cleanup #
    ###########

    @gen.coroutine
    def reevaluate_occupancy(self):
        """ Periodically reassess task duration time

        The expected duration of a task can change over time.  Unfortunately we
        don't have a good constant-time way to propagate the effects of these
        changes out to the summaries that they affect, like the total expected
        runtime of each of the workers, or what tasks are stealable.

        In this coroutine we walk through all of the workers and re-align their
        estimates with the current state of tasks.  We do this periodically
        rather than at every transition, and we only do it if the scheduler
        process isn't under load (using psutil.Process.cpu_percent()).  This
        lets us avoid this fringe optimization when we have better things to
        think about.
        """
        DELAY = 0.1
        try:
            import psutil
            proc = psutil.Process()
            last = time()

            while self.status != 'closed':
                yield gen.sleep(DELAY)
                last = time()

                for w in list(self.workers):
                    while proc.cpu_percent() > 50:
                        yield gen.sleep(DELAY)
                        last = time()

                    if self.status == 'closed':
                        return

                    ws = self.workers.get(w)
                    try:
                        if ws is None or not ws.processing:
                            continue
                        self._reevaluate_occupancy_worker(ws)
                    finally:
                        del ws  # lose ref

                    duration = time() - last
                    if duration > 0.005:  # 5ms since last release
                        yield gen.sleep(duration * 5)  # 25ms gap
                        last = time()
        except Exception:
            logger.error("Error in reevaluate occupancy", exc_info=True)
            raise

    def _reevaluate_occupancy_worker(self, ws):
        """ See reevaluate_occupancy """
        old = ws.occupancy

        new = 0
        nbytes = 0
        for ts in ws.processing:
            duration = self.get_task_duration(ts.key)
            comm = self.get_comm_cost(ts, ws)
            ws.processing[ts] = duration + comm
            new += duration + comm

        ws.occupancy = new
        self.total_occupancy += new - old
        self.check_idle_saturated(ws)

        # significant increase in duration
        if (new > old * 1.3) and ('stealing' in self.extensions):
            steal = self.extensions['stealing']
            for ts in ws.processing:
                steal.remove_key_from_stealable(ts.key)
                steal.put_key_in_stealable(ts.key)


def decide_worker(ts, all_workers, valid_workers,
                  loose_restrictions, objective):
    """
    # XXX docstring is obsolete

    Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> occupancy = {'alice:8000': 0, 'bob:8000': 0}
    >>> who_has = {'a': {'alice:8000'}}
    >>> nbytes = {'a': 100}
    >>> ncores = {'alice:8000': 1, 'bob:8000': 1}
    >>> valid_workers = True
    >>> loose_restrictions = set()

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, occupancy, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'alice:8000'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice:8000', 'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'a'}}
    >>> decide_worker(dependencies, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'bob:8000'

    Optionally provide valid workers of where jobs are allowed to occur

    >>> valid_workers = {'alice:8000', 'charlie:8000'}
    >>> decide_worker(dependencies, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'alice:8000'

    If the task requires data communication, then we choose to minimize the
    number of bytes sent between workers. This takes precedence over worker
    occupancy.

    >>> dependencies = {'c': {'a', 'b'}}
    >>> who_has = {'a': {'alice:8000'}, 'b': {'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'b'}}
    >>> nbytes = {'a': 1, 'b': 1000}

    >>> decide_worker(dependencies, who_has, has_what,
    ...               {}, set(), nbytes, ncores, 'c')
    'bob:8000'
    """
    deps = ts.dependencies
    assert all(dts.who_has for dts in deps)
    candidates = frequencies([ws for dts in deps
                              for ws in dts.who_has])
    if valid_workers is True:
        if not candidates:
            candidates = all_workers
    else:
        candidates = valid_workers & set(candidates)
        if not candidates:
            candidates = valid_workers
            if not candidates:
                if ts.key in loose_restrictions:
                    return decide_worker(ts, all_workers, True, set(), objective)

                else:
                    return None
    if not candidates:
        return None

    if len(candidates) == 1:
        return first(candidates)

    return min(candidates, key=objective)


def validate_state(dependencies, dependents, waiting, waiting_data, ready,
                   who_has, processing, finished_results, released,
                   who_wants, wants_what, tasks=None, allow_overlap=False,
                   erred=None, **kwargs):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.
    """
    # XXX update this for state objects?
    in_processing = {k for v in processing.values() for k in v}
    keys = {key for key in dependents if not dependents[key]}
    ready_set = set(ready)

    assert set(waiting).issubset(dependencies), "waiting not subset of deps"
    assert set(waiting_data).issubset(dependents), "waiting_data not subset"
    if tasks is not None:
        assert ready_set.issubset(tasks), "All ready tasks are tasks"
        assert set(dependents).issubset(set(tasks) | set(who_has)), "all dependents tasks"
        assert set(dependencies).issubset(set(tasks) | set(who_has)), "all dependencies tasks"

    for k, v in waiting.items():
        assert v, "waiting on empty set"
        assert v.issubset(dependencies[k]), "waiting set not dependencies"
        for vv in v:
            assert vv not in who_has, ("waiting dependency in memory", k, vv)
            assert vv not in released, ("dependency released", k, vv)
        for dep in dependencies[k]:
            assert dep in v or who_has.get(dep), ("dep missing", k, dep)

    for k, v in waiting_data.items():
        for vv in v:
            if vv in released:
                raise ValueError('dependent not in play', k, vv)
            if not (vv in waiting or
                    vv in in_processing):
                raise ValueError('dependent not in play2', k, vv)

    for v in concat(processing.values()):
        assert v in dependencies, ("all processing keys in dependencies", sorted(processing.values()), sorted(dependencies))

    for key in who_has:
        assert key in waiting_data or key in who_wants

    @memoize
    def check_key(key):
        """ Validate a single key, recurse downwards """
        vals = ([key in waiting,
                 key in ready,
                 key in in_processing,
                 not not who_has.get(key),
                 key in released,
                 key in erred])
        if ((allow_overlap and sum(vals) < 1) or
                (not allow_overlap and sum(vals) != 1)):
            raise ValueError("Key exists in wrong number of places", key, vals)

        for dep in dependencies[key]:
            if dep in dependents:
                check_key(dep)  # Recursive case

        if who_has.get(key):
            assert not any(key in waiting.get(dep, ())
                           for dep in dependents.get(key, ()))
            assert not waiting.get(key)

        if key in in_processing:
            if not all(who_has.get(dep) for dep in dependencies[key]):
                raise ValueError("Key in processing without all deps",
                                 key)
            assert not waiting.get(key)
            assert key not in ready

        if finished_results is not None:
            if key in finished_results:
                assert who_has.get(key)
                assert key in keys

            if key in keys and who_has.get(key):
                assert key in finished_results

        for key, s in who_wants.items():
            assert s, "empty who_wants"
            for client in s:
                assert key in wants_what[client]

        if key in waiting:
            assert waiting[key], 'waiting empty'

        if key in ready:
            assert key not in waiting

        return True

    assert all(map(check_key, keys))


_round_robin = [0]


fast_tasks = {'rechunk-split', 'shuffle-split'}


class KilledWorker(Exception):
    pass
