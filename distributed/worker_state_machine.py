from __future__ import annotations

import abc
import asyncio
import heapq
import logging
import operator
import random
import sys
import warnings
import weakref
from collections import defaultdict, deque
from collections.abc import (
    Callable,
    Collection,
    Container,
    Iterator,
    Mapping,
    MutableMapping,
)
from copy import copy
from dataclasses import dataclass, field
from functools import lru_cache, partial, singledispatchmethod
from itertools import chain
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, TypedDict, cast

from tlz import peekn, pluck

import dask
from dask.utils import parse_bytes, typename

from distributed._stories import worker_story
from distributed.collections import HeapSet
from distributed.comm import get_address_host
from distributed.core import ErrorMessage, error_message
from distributed.metrics import time
from distributed.protocol import pickle
from distributed.protocol.serialize import Serialize
from distributed.sizeof import safe_sizeof as sizeof
from distributed.utils import recursive_to_dict

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

    # Circular imports
    from distributed.diagnostics.plugin import WorkerPlugin
    from distributed.worker import Worker

    # TODO move out of TYPE_CHECKING (requires Python >=3.10)
    TaskStateState: TypeAlias = Literal[
        "cancelled",
        "constrained",
        "error",
        "executing",
        "fetch",
        "flight",
        "forgotten",
        "long-running",
        "memory",
        "missing",
        "ready",
        "released",
        "rescheduled",
        "resumed",
        "waiting",
    ]
else:
    TaskStateState = str

# TaskState.state subsets
PROCESSING: set[TaskStateState] = {
    "waiting",
    "ready",
    "constrained",
    "executing",
    "long-running",
    "cancelled",
    "resumed",
}
READY: set[TaskStateState] = {"ready", "constrained"}
# Valid states for a task that is found in TaskState.waiting_for_data
WAITING_FOR_DATA: set[TaskStateState] = {
    "constrained",
    "executing",
    "fetch",
    "flight",
    "long-running",
    "missing",
    "ready",
    "resumed",
    "waiting",
}

NO_VALUE = "--no-value-sentinel--"


class SerializedTask(NamedTuple):
    """Info from distributed.scheduler.TaskState.run_spec
    Input to distributed.worker._deserialize

    (function, args kwargs) and task are mutually exclusive
    """

    function: bytes | None = None
    args: bytes | tuple | list | None = None
    kwargs: bytes | dict[str, Any] | None = None
    task: object = NO_VALUE


class StartStop(TypedDict, total=False):
    action: str
    start: float
    stop: float
    source: str  # optional


class InvalidTransition(Exception):
    def __init__(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        story: list[tuple],
    ):
        self.key = key
        self.start = start
        self.finish = finish
        self.story = story

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}: {self.key} :: {self.start}->{self.finish}"
            + "\n"
            + "  Story:\n    "
            + "\n    ".join(map(str, self.story))
        )

    __str__ = __repr__

    def to_event(self) -> tuple[str, dict[str, Any]]:
        return (
            "invalid-worker-transition",
            {
                "key": self.key,
                "start": self.start,
                "finish": self.finish,
                "story": self.story,
            },
        )


class TransitionCounterMaxExceeded(InvalidTransition):
    def to_event(self) -> tuple[str, dict[str, Any]]:
        topic, msg = super().to_event()
        return "transition-counter-max-exceeded", msg


class InvalidTaskState(Exception):
    def __init__(
        self,
        key: str,
        state: TaskStateState,
        story: list[tuple],
    ):
        self.key = key
        self.state = state
        self.story = story

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}: {self.key} :: {self.state}"
            + "\n"
            + "  Story:\n    "
            + "\n    ".join(map(str, self.story))
        )

    __str__ = __repr__

    def to_event(self) -> tuple[str, dict[str, Any]]:
        return (
            "invalid-worker-task-state",
            {
                "key": self.key,
                "state": self.state,
                "story": self.story,
            },
        )


class RecommendationsConflict(Exception):
    """Two or more recommendations for the same task suggested different finish states"""


@lru_cache
def _default_data_size() -> int:
    return parse_bytes(dask.config.get("distributed.scheduler.default-data-size"))


# Note: can't specify __slots__ manually to enable slots in Python <3.10 in a @dataclass
# that defines any default values
dc_slots = {"slots": True} if sys.version_info >= (3, 10) else {}


@dataclass(repr=False, eq=False, **dc_slots)
class TaskState:
    """Holds volatile state relating to an individual Dask task.

    Not to be confused with :class:`distributed.scheduler.TaskState`, which holds
    similar information on the scheduler side.
    """

    #: Task key. Mandatory.
    key: str
    #: A named tuple containing the ``function``, ``args``, ``kwargs`` and ``task``
    #: associated with this `TaskState` instance. This defaults to ``None`` and can
    #: remain empty if it is a dependency that this worker will receive from another
    #: worker.
    run_spec: SerializedTask | None = None

    #: The data needed by this key to run
    dependencies: set[TaskState] = field(default_factory=set)
    #: The keys that use this dependency
    dependents: set[TaskState] = field(default_factory=set)
    #: Subset of dependencies that are not in memory
    waiting_for_data: set[TaskState] = field(default_factory=set)
    #: Subset of dependents that are not in memory
    waiters: set[TaskState] = field(default_factory=set)

    #: The current state of the task
    state: TaskStateState = "released"
    #: The previous state of the task. This is a state machine implementation detail.
    _previous: TaskStateState | None = None
    #: The next state of the task. This is a state machine implementation detail.
    _next: TaskStateState | None = None

    #: Expected duration of the task
    duration: float | None = None
    #: The priority this task given by the scheduler. Determines run order.
    priority: tuple[int, ...] | None = None
    #: Addresses of workers that we believe have this data
    who_has: set[str] = field(default_factory=set)
    #: The worker that current task data is coming from if task is in flight
    coming_from: str | None = None
    #: Abstract resources required to run a task
    resource_restrictions: dict[str, float] = field(default_factory=dict)
    #: The exception caused by running a task if it erred (serialized)
    exception: Serialize | None = None
    #: The traceback caused by running a task if it erred (serialized)
    traceback: Serialize | None = None
    #: string representation of exception
    exception_text: str = ""
    #: string representation of traceback
    traceback_text: str = ""
    #: The type of a particular piece of data
    type: type | None = None
    #: The number of times a dependency has not been where we expected it
    suspicious_count: int = 0
    #: Log of transfer, load, and compute times for a task
    startstops: list[StartStop] = field(default_factory=list)
    #: Time at which task begins running
    start_time: float | None = None
    #: Time at which task finishes running
    stop_time: float | None = None
    #: Metadata related to the task.
    #: Stored metadata should be msgpack serializable (e.g. int, string, list, dict).
    metadata: dict = field(default_factory=dict)
    #: The size of the value of the task, if in memory
    nbytes: int | None = None
    #: Arbitrary task annotations
    annotations: dict | None = None
    #: True if the task is in memory or erred; False otherwise
    done: bool = False

    _instances: ClassVar[weakref.WeakSet[TaskState]] = weakref.WeakSet()

    # Support for weakrefs to a class with __slots__
    __weakref__: Any = field(init=False)

    def __post_init__(self) -> None:
        TaskState._instances.add(self)

    def __repr__(self) -> str:
        return f"<TaskState {self.key!r} {self.state}>"

    def __hash__(self) -> int:
        """Override dataclass __hash__, reverting to the default behaviour
        hash(o) == id(o).

        Note that we also defined @dataclass(eq=False), which reverts to the default
        behaviour (a == b) == (a is b).

        On first thought, it would make sense to use TaskState.key for equality and
        hashing. However, a task may be forgotten and a new TaskState object with the
        same key may be created in its place later on. In the Worker state, you should
        never have multiple TaskState objects with the same key; see
        WorkerState.validate_state for relevant checks. We can't assert the same thing
        in __eq__ though, as multiple objects with the same key may appear in
        TaskState._instances for a brief period of time.
        """
        return id(self)

    def get_nbytes(self) -> int:
        nbytes = self.nbytes
        return nbytes if nbytes is not None else _default_data_size()

    def _to_dict_no_nest(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict

        Notes
        -----
        This class uses ``_to_dict_no_nest`` instead of ``_to_dict``.
        When a task references another task, just print the task repr. All tasks
        should neatly appear under Worker.tasks. This also prevents a RecursionError
        during particularly heavy loads, which have been observed to happen whenever
        there's an acyclic dependency chain of ~200+ tasks.
        """
        out = recursive_to_dict(self, exclude=exclude, members=True)
        # Remove all Nones and empty containers
        return {k: v for k, v in out.items() if v}

    def is_protected(self) -> bool:
        return self.state in PROCESSING or any(
            dep_ts.state in PROCESSING for dep_ts in self.dependents
        )


@dataclass
class Instruction:
    """Command from the worker state machine to the Worker, in response to an event"""

    __slots__ = ("stimulus_id",)
    stimulus_id: str


@dataclass
class GatherDep(Instruction):
    __slots__ = ("worker", "to_gather", "total_nbytes")
    worker: str
    to_gather: set[str]
    total_nbytes: int


@dataclass
class Execute(Instruction):
    __slots__ = ("key",)
    key: str


@dataclass
class RetryBusyWorkerLater(Instruction):
    __slots__ = ("worker",)
    worker: str


class SendMessageToScheduler(Instruction):
    #: Matches a key in Scheduler.stream_handlers
    op: ClassVar[str]
    __slots__ = ()

    def to_dict(self) -> dict[str, Any]:
        """Convert object to dict so that it can be serialized with msgpack"""
        d = {k: getattr(self, k) for k in self.__annotations__}
        d["op"] = self.op
        d["stimulus_id"] = self.stimulus_id
        return d


@dataclass
class TaskFinishedMsg(SendMessageToScheduler):
    op = "task-finished"

    key: str
    nbytes: int | None
    type: bytes  # serialized class
    typename: str
    metadata: dict
    thread: int | None
    startstops: list[StartStop]
    __slots__ = tuple(__annotations__)

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d["status"] = "OK"
        return d


@dataclass
class TaskErredMsg(SendMessageToScheduler):
    op = "task-erred"

    key: str
    exception: Serialize
    traceback: Serialize | None
    exception_text: str
    traceback_text: str
    thread: int | None
    startstops: list[StartStop]
    __slots__ = tuple(__annotations__)

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d["status"] = "error"
        return d

    @staticmethod
    def from_task(
        ts: TaskState, stimulus_id: str, thread: int | None = None
    ) -> TaskErredMsg:
        assert ts.exception
        return TaskErredMsg(
            key=ts.key,
            exception=ts.exception,
            traceback=ts.traceback,
            exception_text=ts.exception_text,
            traceback_text=ts.traceback_text,
            thread=thread,
            startstops=ts.startstops,
            stimulus_id=stimulus_id,
        )


@dataclass
class ReleaseWorkerDataMsg(SendMessageToScheduler):
    op = "release-worker-data"

    __slots__ = ("key",)
    key: str


# Not to be confused with RescheduleEvent below or the distributed.Reschedule Exception
@dataclass
class RescheduleMsg(SendMessageToScheduler):
    op = "reschedule"

    __slots__ = ("key",)
    key: str


@dataclass
class LongRunningMsg(SendMessageToScheduler):
    op = "long-running"

    __slots__ = ("key", "compute_duration")
    key: str
    compute_duration: float


@dataclass
class AddKeysMsg(SendMessageToScheduler):
    op = "add-keys"

    __slots__ = ("keys",)
    keys: Collection[str]


@dataclass
class RequestRefreshWhoHasMsg(SendMessageToScheduler):
    """Worker -> Scheduler asynchronous request for updated who_has information.
    Not to be confused with the scheduler.who_has synchronous RPC call, which is used
    by the Client.

    See also
    --------
    RefreshWhoHasEvent
    distributed.scheduler.Scheduler.request_refresh_who_has
    distributed.client.Client.who_has
    distributed.scheduler.Scheduler.get_who_has
    """

    op = "request-refresh-who-has"

    __slots__ = ("keys",)
    keys: Collection[str]


@dataclass
class StealResponseMsg(SendMessageToScheduler):
    """Worker->Scheduler response to ``{op: steal-request}``

    See also
    --------
    StealRequestEvent
    """

    op = "steal-response"

    __slots__ = ("key", "state")
    key: str
    state: TaskStateState | None


@dataclass
class StateMachineEvent:
    __slots__ = ("stimulus_id", "handled")
    stimulus_id: str
    #: timestamp of when the event was handled by the worker
    # TODO Switch to @dataclass(slots=True), uncomment the line below, and remove the
    #      __new__ method (requires Python >=3.10)
    # handled: float | None = field(init=False, default=None)
    _classes: ClassVar[dict[str, type[StateMachineEvent]]] = {}

    def __new__(cls, *args: Any, **kwargs: Any) -> StateMachineEvent:
        self = object.__new__(cls)
        self.handled = None
        return self

    def __init_subclass__(cls) -> None:
        StateMachineEvent._classes[cls.__name__] = cls

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        """Produce a variant version of self that is small enough to be stored in memory
        in the medium term and contains meaningful information for debugging
        """
        self.handled: float | None = handled
        return self

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.

        See also
        --------
        distributed.utils.recursive_to_dict
        """
        info = {
            "cls": type(self).__name__,
            "stimulus_id": self.stimulus_id,
            "handled": self.handled,
        }
        info.update(
            {
                k: getattr(self, k)
                for k in self.__annotations__
                # Necessary for subclasses that don't define their own annotations
                if k != "_classes"
            }
        )
        info = {k: v for k, v in info.items() if k not in exclude}
        return recursive_to_dict(info, exclude=exclude)

    @staticmethod
    def from_dict(d: dict) -> StateMachineEvent:
        """Convert the output of ``recursive_to_dict`` back into the original object.
        The output object is meaningful for the purpose of rebuilding the state machine,
        but not necessarily identical to the original.
        """
        kwargs = d.copy()
        cls = StateMachineEvent._classes[kwargs.pop("cls")]
        handled = kwargs.pop("handled")
        inst = cls(**kwargs)
        inst.handled = handled
        inst._after_from_dict()
        return inst

    def _after_from_dict(self) -> None:
        """Optional post-processing after an instance is created by ``from_dict``"""


@dataclass
class PauseEvent(StateMachineEvent):
    __slots__ = ()


@dataclass
class UnpauseEvent(StateMachineEvent):
    __slots__ = ()


@dataclass
class RetryBusyWorkerEvent(StateMachineEvent):
    __slots__ = ("worker",)
    worker: str


@dataclass
class GatherDepDoneEvent(StateMachineEvent):
    """:class:`GatherDep` instruction terminated (abstract base class)"""

    __slots__ = ("worker", "total_nbytes")
    worker: str
    total_nbytes: int  # Must be the same as in GatherDep instruction


@dataclass
class GatherDepSuccessEvent(GatherDepDoneEvent):
    """:class:`GatherDep` instruction terminated:
    remote worker fetched successfully
    """

    __slots__ = ("data",)

    data: dict[str, object]  # There may be less keys than in GatherDep

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = copy(self)
        out.handled = handled
        out.data = {k: None for k in self.data}
        return out

    def _after_from_dict(self) -> None:
        self.data = {k: None for k in self.data}


@dataclass
class GatherDepBusyEvent(GatherDepDoneEvent):
    """:class:`GatherDep` instruction terminated:
    remote worker is busy
    """

    __slots__ = ()


@dataclass
class GatherDepNetworkFailureEvent(GatherDepDoneEvent):
    """:class:`GatherDep` instruction terminated:
    network failure while trying to communicate with remote worker
    """

    __slots__ = ()


@dataclass
class GatherDepFailureEvent(GatherDepDoneEvent):
    """class:`GatherDep` instruction terminated:
    generic error raised (not a network failure); e.g. data failed to deserialize.
    """

    exception: Serialize
    traceback: Serialize | None
    exception_text: str
    traceback_text: str
    __slots__ = tuple(__annotations__)

    def _after_from_dict(self) -> None:
        self.exception = Serialize(Exception())
        self.traceback = None

    @classmethod
    def from_exception(
        cls,
        err: BaseException,
        *,
        worker: str,
        total_nbytes: int,
        stimulus_id: str,
    ) -> GatherDepFailureEvent:
        msg = error_message(err)
        return cls(
            worker=worker,
            total_nbytes=total_nbytes,
            exception=msg["exception"],
            traceback=msg["traceback"],
            exception_text=msg["exception_text"],
            traceback_text=msg["traceback_text"],
            stimulus_id=stimulus_id,
        )


@dataclass
class ComputeTaskEvent(StateMachineEvent):
    key: str
    who_has: dict[str, Collection[str]]
    nbytes: dict[str, int]
    priority: tuple[int, ...]
    duration: float
    run_spec: SerializedTask | None
    function: bytes | None
    args: bytes | tuple | list | None | None
    kwargs: bytes | dict[str, Any] | None
    resource_restrictions: dict[str, float]
    actor: bool
    annotations: dict
    __slots__ = tuple(__annotations__)

    def __post_init__(self) -> None:
        # Fixes after msgpack decode
        if isinstance(self.priority, list):  # type: ignore[unreachable]
            self.priority = tuple(self.priority)  # type: ignore[unreachable]

        if self.function is not None:
            assert self.run_spec is None
            self.run_spec = SerializedTask(
                function=self.function, args=self.args, kwargs=self.kwargs
            )
        elif not isinstance(self.run_spec, SerializedTask):
            self.run_spec = SerializedTask(task=self.run_spec)

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        return StateMachineEvent._to_dict(self._clean(), exclude=exclude)

    def _clean(self) -> StateMachineEvent:
        out = copy(self)
        out.function = None
        out.kwargs = None
        out.args = None
        out.run_spec = SerializedTask(task=None, function=None, args=None, kwargs=None)
        return out

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = self._clean()
        out.handled = handled
        return out

    def _after_from_dict(self) -> None:
        self.run_spec = SerializedTask(task=None, function=None, args=None, kwargs=None)

    @staticmethod
    def dummy(
        key: str,
        *,
        who_has: dict[str, Collection[str]] | None = None,
        nbytes: dict[str, int] | None = None,
        priority: tuple[int, ...] = (0,),
        duration: float = 1.0,
        resource_restrictions: dict[str, float] | None = None,
        actor: bool = False,
        annotations: dict | None = None,
        stimulus_id: str,
    ) -> ComputeTaskEvent:
        """Build a dummy event, with most attributes set to a reasonable default.
        This is a convenience method to be used in unit testing only.
        """
        return ComputeTaskEvent(
            key=key,
            who_has=who_has or {},
            nbytes=nbytes or {k: 1 for k in who_has or ()},
            priority=priority,
            duration=duration,
            run_spec=None,
            function=None,
            args=None,
            kwargs=None,
            resource_restrictions=resource_restrictions or {},
            actor=actor,
            annotations=annotations or {},
            stimulus_id=stimulus_id,
        )


@dataclass
class ExecuteSuccessEvent(StateMachineEvent):
    key: str
    value: object
    start: float
    stop: float
    nbytes: int
    type: type | None
    __slots__ = tuple(__annotations__)

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = copy(self)
        out.handled = handled
        out.value = None
        return out

    def _after_from_dict(self) -> None:
        self.value = None
        self.type = None

    @staticmethod
    def dummy(
        key: str,
        value: object = None,
        *,
        nbytes: int = 1,
        stimulus_id: str,
    ) -> ExecuteSuccessEvent:
        """Build a dummy event, with most attributes set to a reasonable default.
        This is a convenience method to be used in unit testing only.
        """
        return ExecuteSuccessEvent(
            key=key,
            value=value,
            start=0.0,
            stop=1.0,
            nbytes=nbytes,
            type=None,
            stimulus_id=stimulus_id,
        )


@dataclass
class ExecuteFailureEvent(StateMachineEvent):
    key: str
    start: float | None
    stop: float | None
    exception: Serialize
    traceback: Serialize | None
    exception_text: str
    traceback_text: str
    __slots__ = tuple(__annotations__)

    def _after_from_dict(self) -> None:
        self.exception = Serialize(Exception())
        self.traceback = None

    @classmethod
    def from_exception(
        cls,
        err_or_msg: BaseException | ErrorMessage,
        *,
        key: str,
        start: float | None = None,
        stop: float | None = None,
        stimulus_id: str,
    ) -> ExecuteFailureEvent:
        if isinstance(err_or_msg, dict):
            msg = err_or_msg
        else:
            msg = error_message(err_or_msg)

        return cls(
            key=key,
            start=start,
            stop=stop,
            exception=msg["exception"],
            traceback=msg["traceback"],
            exception_text=msg["exception_text"],
            traceback_text=msg["traceback_text"],
            stimulus_id=stimulus_id,
        )

    @staticmethod
    def dummy(
        key: str,
        *,
        stimulus_id: str,
    ) -> ExecuteFailureEvent:
        """Build a dummy event, with most attributes set to a reasonable default.
        This is a convenience method to be used in unit testing only.
        """
        return ExecuteFailureEvent(
            key=key,
            start=None,
            stop=None,
            exception=Serialize(None),
            traceback=None,
            exception_text="",
            traceback_text="",
            stimulus_id=stimulus_id,
        )


@dataclass
class CancelComputeEvent(StateMachineEvent):
    __slots__ = ("key",)
    key: str


@dataclass
class AlreadyCancelledEvent(StateMachineEvent):
    __slots__ = ("key",)
    key: str


# Not to be confused with RescheduleMsg above or the distributed.Reschedule Exception
@dataclass
class RescheduleEvent(StateMachineEvent):
    __slots__ = ("key",)
    key: str


@dataclass
class FindMissingEvent(StateMachineEvent):
    __slots__ = ()


@dataclass
class RefreshWhoHasEvent(StateMachineEvent):
    """Scheduler -> Worker message containing updated who_has information.

    See also
    --------
    RequestRefreshWhoHasMsg
    """

    __slots__ = ("who_has",)
    # {key: [worker address, ...]}
    who_has: dict[str, Collection[str]]


@dataclass
class AcquireReplicasEvent(StateMachineEvent):
    __slots__ = ("who_has", "nbytes")
    who_has: dict[str, Collection[str]]
    nbytes: dict[str, int]


@dataclass
class RemoveReplicasEvent(StateMachineEvent):
    __slots__ = ("keys",)
    keys: Collection[str]


@dataclass
class FreeKeysEvent(StateMachineEvent):
    __slots__ = ("keys",)
    keys: Collection[str]


@dataclass
class StealRequestEvent(StateMachineEvent):
    """Event that requests a worker to release a key because it's now being computed
    somewhere else.

    See also
    --------
    StealResponseMsg
    """

    __slots__ = ("key",)
    key: str


@dataclass
class UpdateDataEvent(StateMachineEvent):
    __slots__ = ("data", "report")
    data: dict[str, object]
    report: bool

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = copy(self)
        out.handled = handled
        out.data = dict.fromkeys(self.data)
        return out


@dataclass
class SecedeEvent(StateMachineEvent):
    __slots__ = ("key", "compute_duration")
    key: str
    compute_duration: float


if TYPE_CHECKING:
    # TODO remove quotes (requires Python >=3.9)
    # TODO get out of TYPE_CHECKING (requires Python >=3.10)
    # {TaskState -> finish: TaskStateState | (finish: TaskStateState, transition *args)}
    Recs: TypeAlias = "dict[TaskState, TaskStateState | tuple]"
    Instructions: TypeAlias = "list[Instruction]"
    RecsInstrs: TypeAlias = "tuple[Recs, Instructions]"
else:
    Recs = dict
    Instructions = list
    RecsInstrs = tuple


def merge_recs_instructions(*args: RecsInstrs) -> RecsInstrs:
    """Merge multiple (recommendations, instructions) tuples.
    Collisions in recommendations are only allowed if identical.
    """
    recs: Recs = {}
    instr: Instructions = []
    for recs_i, instr_i in args:
        for ts, finish in recs_i.items():
            if ts in recs and recs[ts] != finish:
                raise RecommendationsConflict(
                    f"Mismatched recommendations for {ts.key}: {recs[ts]} vs. {finish}"
                )
            recs[ts] = finish
        instr += instr_i
    return recs, instr


class WorkerState:
    """State machine encapsulating the lifetime of all tasks on a worker.

    Not to be confused with :class:`distributed.scheduler.WorkerState`.

    .. note::
       The data attributes of this class are implementation details and may be
       changed without a deprecation cycle.

    .. warning::
       The attributes of this class are all heavily correlated with each other.
       *Do not* modify them directly, *ever*, as it is extremely easy to obtain a broken
       state this way, which in turn will likely result in cluster-wide deadlocks.

       The state should be exclusively mutated through :meth:`handle_stimulus`.
    """

    #: Worker <IP address>:<port>. This is used in decision-making by the state machine,
    #: e.g. to determine if a peer worker is running on the same host or not.
    #: This attribute may not be known when the WorkerState is initialised. It *must* be
    #: set before the first call to :meth:`handle_stimulus`.
    address: str

    #: ``{key: TaskState}``. The tasks currently executing on this worker (and any
    #: dependencies of those tasks)
    tasks: dict[str, TaskState]

    #: ``{ts.key: thread ID}``. This collection is shared by reference between
    #: :class:`~distributed.worker.Worker` and this class. While the WorkerState is
    #: thread-agnostic, it still needs access to this information in some cases.
    #: This collection is populated by :meth:`distributed.worker.Worker.execute`.
    #: It does not *need* to be populated for the WorkerState to work.
    threads: dict[str, int]

    #: In-memory tasks data. This collection is shared by reference between
    #: :class:`~distributed.worker.Worker`,
    #: :class:`~distributed.worker_memory.WorkerMemoryManager`, and this class.
    data: MutableMapping[str, object]

    #: ``{name: worker plugin}``. This collection is shared by reference between
    #: :class:`~distributed.worker.Worker` and this class. The Worker managed adding and
    #: removing plugins, while the WorkerState invokes the ``WorkerPlugin.transition``
    #: method, is available.
    plugins: dict[str, WorkerPlugin]

    # heapq ``[(priority, key), ...]``. Keys that are ready to run.
    ready: list[tuple[tuple[int, ...], str]]

    #: Keys for which we have the data to run, but are waiting on abstract resources
    #: like GPUs. Stored in a FIFO deque.
    #: See :attr:`available_resources` and :doc:`resources`.
    constrained: deque[str]

    #: Number of tasks that can be executing in parallel.
    #: At any given time, :meth:`executing_count` <= nthreads.
    nthreads: int

    #: True if the state machine should start executing more tasks and fetch
    #: dependencies whenever a slot is available. This property must be kept aligned
    #: with the Worker: ``WorkerState.running == (Worker.status is Status.running)``.
    running: bool

    #: A count of how many tasks are currently waiting for data
    waiting_for_data_count: int

    #: ``{worker address: {ts.key, ...}``.
    #: The data that we care about that we think a worker has
    has_what: defaultdict[str, set[str]]

    #: The tasks which still require data in order to execute and are in memory on at
    #: least another worker, prioritized as per-worker heaps. All and only tasks with
    #: ``TaskState.state == 'fetch'`` are in this collection. A :class:`TaskState` with
    #: multiple entries in :attr:`~TaskState.who_has` will appear multiple times here.
    data_needed: defaultdict[str, HeapSet[TaskState]]

    #: Number of bytes to fetch from the same worker in a single call to
    #: :meth:`BaseWorker.gather_dep`. Multiple small tasks that can be fetched from the
    #: same worker will be clustered in a single instruction as long as their combined
    #: size doesn't exceed this value.
    target_message_size: int

    #: All and only tasks with ``TaskState.state == 'missing'``.
    missing_dep_flight: set[TaskState]

    #: Which tasks that are coming to us in current peer-to-peer connections.
    #: All and only tasks with TaskState.state == 'flight'.
    #: See also :meth:`in_flight_tasks_count`.
    in_flight_tasks: set[TaskState]

    #: ``{worker address: {ts.key, ...}}``
    #: The workers from which we are currently gathering data and the dependencies we
    #: expect from those connections. Workers in this dict won't be asked for additional
    #: dependencies until the current query returns.
    in_flight_workers: dict[str, set[str]]

    #: The total number of bytes in flight
    comm_nbytes: int

    #: The maximum number of concurrent incoming requests for data.
    #: See also :attr:`distributed.worker.Worker.total_in_connections`.
    total_out_connections: int

    #: Ignore :attr:`total_out_connections` as long as :attr:`comm_nbytes` is
    #: less than this value.
    comm_threshold_bytes: int

    #: Peer workers that recently returned a busy status. Workers in this set won't be
    #: asked for additional dependencies for some time.
    busy_workers: set[str]

    #: Counter that decreases every time the compute-task handler is invoked by the
    #: Scheduler. It is appended to :attr:`TaskState.priority` and acts as a
    #: tie-breaker between tasks that have the same priority on the Scheduler,
    #: determining a last-in-first-out order between them.
    generation: int

    #: ``{resource name: amount}``. Total resources available for task execution.
    #: See :doc: `resources`.
    total_resources: dict[str, float]

    #: ``{resource name: amount}``. Current resources that aren't being currently
    #: consumed by task execution. Always less or equal to :attr:`total_resources`.
    #: See :doc:`resources`.
    available_resources: dict[str, float]

    #: Set of tasks that are currently running.
    #: See also :meth:`executing_count` and :attr:`long_runing`.
    executing: set[TaskState]

    #: Set of tasks that are currently running and have called
    #: :func:`~distributed.secede`.
    #: These tasks do not appear in the :attr:`executing` set.
    long_running: set[TaskState]

    #: A number of tasks that this worker has run in its lifetime.
    #: See also :meth:`executing_count`.
    executed_count: int

    #: Actor tasks. See :doc:`actors`.
    actors: dict[str, object]

    #: Transition log: ``[(..., stimulus_id: str | None, timestamp: float), ...]``
    #: The number of stimuli logged is capped.
    #: See also :meth:`story` and :attr:`stimulus_log`.
    log: deque[tuple]

    #: Log of all stimuli received by :meth:`handle_stimulus`.
    #: The number of events logged is capped.
    #: See also :attr:`log` and :meth:`stimulus_story`.
    stimulus_log: deque[StateMachineEvent]

    #: If True, enable expensive internal consistency check.
    #: Typically disabled in production.
    validate: bool

    #: Total number of state transitions so far.
    #: See also :attr:`log` and :attr:`transition_counter_max`.
    transition_counter: int

    #: Raise an error if the :attr:`transition_counter` ever reaches this value.
    #: This is meant for debugging only, to catch infinite recursion loops.
    #: In production, it should always be set to False.
    transition_counter_max: int | Literal[False]

    #: Statically-seeded random state, used to guarantee determinism whenever a
    #: pseudo-random choice is required
    rng: random.Random

    __slots__ = tuple(__annotations__)

    def __init__(
        self,
        *,
        nthreads: int = 1,
        address: str | None = None,
        data: MutableMapping[str, object] | None = None,
        threads: dict[str, int] | None = None,
        plugins: dict[str, WorkerPlugin] | None = None,
        resources: Mapping[str, float] | None = None,
        total_out_connections: int = 9999,
        validate: bool = True,
        transition_counter_max: int | Literal[False] = False,
    ):
        self.nthreads = nthreads

        # address may not be known yet when the State Machine is initialised.
        # Raise AttributeError if a method tries reading it before it's been set.
        if address:
            self.address = address

        # These collections are normally passed by reference by the Worker.
        # For the sake of convenience, create independent ones during unit tests.
        self.data = data if data is not None else {}
        self.threads = threads if threads is not None else {}
        self.plugins = plugins if plugins is not None else {}
        self.total_resources = dict(resources) if resources is not None else {}
        self.available_resources = self.total_resources.copy()

        self.validate = validate
        self.tasks = {}
        self.running = True
        self.waiting_for_data_count = 0
        self.has_what = defaultdict(set)
        self.data_needed = defaultdict(
            partial(HeapSet[TaskState], key=operator.attrgetter("priority"))
        )
        self.in_flight_workers = {}
        self.busy_workers = set()
        self.total_out_connections = total_out_connections
        self.comm_threshold_bytes = int(10e6)
        self.comm_nbytes = 0
        self.missing_dep_flight = set()
        self.generation = 0
        self.ready = []
        self.constrained = deque()
        self.executing = set()
        self.in_flight_tasks = set()
        self.executed_count = 0
        self.long_running = set()
        self.target_message_size = int(50e6)  # 50 MB
        self.log = deque(maxlen=100_000)
        self.stimulus_log = deque(maxlen=10_000)
        self.transition_counter = 0
        self.transition_counter_max = transition_counter_max
        self.actors = {}
        self.rng = random.Random(0)

    def handle_stimulus(self, *stims: StateMachineEvent) -> Instructions:
        """Process one or more external events, transition relevant tasks to new states,
        and return a list of instructions to be executed as a consequence.

        See also
        --------
        BaseWorker.handle_stimulus
        """
        instructions = []
        handled = time()
        for stim in stims:
            if not isinstance(stim, FindMissingEvent):
                self.stimulus_log.append(stim.to_loggable(handled=handled))
            recs, instr = self._handle_event(stim)
            instructions += instr
            instructions += self._transitions(recs, stimulus_id=stim.stimulus_id)
        return instructions

    #############
    # Accessors #
    #############

    @property
    def executing_count(self) -> int:
        """Count of tasks currently executing on this worker.
        Does not include long running (a.k.a. seceded) and cancelled tasks.

        See also
        --------
        WorkerState.executing
        WorkerState.executed_count
        WorkerState.nthreads
        """
        return len(self.executing)

    @property
    def all_running_tasks(self) -> set[TaskState]:
        """All tasks that are currently occupying a thread.
        These are:

        - ``ts.status in ("executing", "long-running", "cancelled")``
        - ``ts.status == "resumed" and ts._previous in ("executing", "long-running")``
        """
        # Note: cancelled and resumed tasks are still in either of these sets
        return self.executing | self.long_running

    @property
    def in_flight_tasks_count(self) -> int:
        """Count of tasks currently being replicated from other workers to this one.

        See also
        --------
        WorkerState.in_flight_tasks
        """
        return len(self.in_flight_tasks)

    #########################
    # Shared helper methods #
    #########################

    def _ensure_task_exists(
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

    def _update_who_has(self, who_has: Mapping[str, Collection[str]]) -> None:
        for key, workers in who_has.items():
            ts = self.tasks.get(key)
            if not ts:
                # The worker sent a refresh-who-has request to the scheduler but, by the
                # time the answer comes back, some of the keys have been forgotten.
                continue
            workers = set(workers)

            if self.address in workers:
                workers.remove(self.address)
                # This can only happen if rebalance() recently asked to release a key,
                # but the RPC call hasn't returned yet. rebalance() is flagged as not
                # being safe to run while the cluster is not at rest and has already
                # been penned in to be redesigned on top of the AMM.
                # It is not necessary to send a message back to the
                # scheduler here, because it is guaranteed that there's already a
                # release-worker-data message in transit to it.
                if ts.state != "memory":
                    logger.debug(  # pragma: nocover
                        "Scheduler claims worker %s holds data for task %s, "
                        "which is not true.",
                        self.address,
                        ts,
                    )

            if ts.who_has == workers:
                continue

            for worker in ts.who_has - workers:
                self.has_what[worker].remove(key)
                if ts.state == "fetch":
                    self.data_needed[worker].remove(ts)

            for worker in workers - ts.who_has:
                self.has_what[worker].add(key)
                if ts.state == "fetch":
                    self.data_needed[worker].add(ts)

            ts.who_has = workers

    def _purge_state(self, ts: TaskState) -> None:
        """Ensure that TaskState attributes are reset to a neutral default and
        Worker-level state associated to the provided key is cleared (e.g.
        who_has)
        This is idempotent
        """
        logger.debug("Purge task: %s", ts)
        key = ts.key
        self.data.pop(key, None)
        self.actors.pop(key, None)

        for worker in ts.who_has:
            self.has_what[worker].discard(ts.key)
            self.data_needed[worker].discard(ts)
        ts.who_has.clear()

        self.threads.pop(key, None)

        for d in ts.dependencies:
            ts.waiting_for_data.discard(d)
            d.waiters.discard(ts)

        ts.waiting_for_data.clear()
        ts.nbytes = None
        ts._previous = None
        ts._next = None
        ts.done = False

        self.executing.discard(ts)
        self.long_running.discard(ts)
        self.in_flight_tasks.discard(ts)

    def _ensure_communicating(self, *, stimulus_id: str) -> RecsInstrs:
        """Transition tasks from fetch to flight, until there are no more tasks in fetch
        state or a threshold has been reached.
        """
        if not self.running or not self.data_needed:
            return {}, []
        if (
            len(self.in_flight_workers) >= self.total_out_connections
            and self.comm_nbytes >= self.comm_threshold_bytes
        ):
            return {}, []

        recommendations: Recs = {}
        instructions: Instructions = []

        for worker, available_tasks in self._select_workers_for_gather():
            assert worker != self.address
            to_gather_tasks, total_nbytes = self._select_keys_for_gather(
                available_tasks
            )
            assert to_gather_tasks
            to_gather_keys = {ts.key for ts in to_gather_tasks}

            logger.debug(
                "Gathering %d tasks from %s; %d more remain. "
                "Pending workers: %d; connections: %d/%d; busy: %d",
                len(to_gather_tasks),
                worker,
                len(available_tasks),
                len(self.data_needed),
                len(self.in_flight_workers),
                self.total_out_connections,
                len(self.busy_workers),
            )
            self.log.append(
                ("gather-dependencies", worker, to_gather_keys, stimulus_id, time())
            )

            for ts in to_gather_tasks:
                if self.validate:
                    assert ts.state == "fetch"
                    assert worker in ts.who_has
                    assert ts not in recommendations
                recommendations[ts] = ("flight", worker)

            # A single invocation of _ensure_communicating may generate up to one
            # GatherDep instruction per worker. Multiple tasks from the same worker may
            # be clustered in the same instruction by _select_keys_for_gather. But once
            # a worker has been selected for a GatherDep and added to in_flight_workers,
            # it won't be selected again until the gather completes.
            instructions.append(
                GatherDep(
                    worker=worker,
                    to_gather=to_gather_keys,
                    total_nbytes=total_nbytes,
                    stimulus_id=stimulus_id,
                )
            )

            self.in_flight_workers[worker] = to_gather_keys
            self.comm_nbytes += total_nbytes
            if (
                len(self.in_flight_workers) >= self.total_out_connections
                and self.comm_nbytes >= self.comm_threshold_bytes
            ):
                break

        return recommendations, instructions

    def _select_workers_for_gather(self) -> Iterator[tuple[str, HeapSet[TaskState]]]:
        """Helper of _ensure_communicating.

        Yield the peer workers and tasks in data_needed, sorted by:

        1. By highest-priority task available across all workers
        2. If tied, first by local peer workers, then remote. Note that, if a task is
           replicated across multiple host, it may go in a tie with itself.
        3. If still tied, by number of tasks available to be fetched from the host
           (see note below)
        4. If still tied, by a random element. This is statically seeded to guarantee
           reproducibility.

           FIXME https://github.com/dask/distributed/issues/6620
                 You won't get determinism when a single task is replicated on multiple
                 workers, because TaskState.who_has changes order at every interpreter
                 restart.

        Omit workers that are either busy or in flight.
        Remove peer workers with no tasks from data_needed.

        Note
        ----
        Instead of number of tasks, we could've measured total nbytes and/or number of
        tasks that only exist on the worker. Raw number of tasks is cruder but simpler.
        """
        host = get_address_host(self.address)
        heap = []

        for worker, tasks in list(self.data_needed.items()):
            if not tasks:
                del self.data_needed[worker]
                continue
            if worker in self.in_flight_workers or worker in self.busy_workers:
                continue
            heap.append(
                (
                    tasks.peek().priority,
                    get_address_host(worker) != host,  # False < True
                    -len(tasks),
                    self.rng.random(),
                    worker,
                    tasks,
                )
            )

        heapq.heapify(heap)
        while heap:
            _, is_remote, ntasks_neg, rnd, worker, tasks = heapq.heappop(heap)
            # The number of tasks and possibly the top priority task may have changed
            # since the last sort, since _select_keys_for_gather may have removed tasks
            # that are also replicated on a higher-priority worker.
            if not tasks:
                del self.data_needed[worker]
            elif -ntasks_neg != len(tasks):
                heapq.heappush(
                    heap,
                    (tasks.peek().priority, is_remote, -len(tasks), rnd, worker, tasks),
                )
            else:
                yield worker, tasks
                if not tasks:  # _select_keys_for_gather just emptied it
                    del self.data_needed[worker]

    def _select_keys_for_gather(
        self, available: HeapSet[TaskState]
    ) -> tuple[list[TaskState], int]:
        """Helper of _ensure_communicating.

        Fetch all tasks that are replicated on the target worker within a single
        message, up to target_message_size.
        """
        to_gather: list[TaskState] = []
        total_nbytes = 0

        while available:
            ts = available.peek()
            # The top-priority task is fetched regardless of its size
            if to_gather and total_nbytes + ts.get_nbytes() > self.target_message_size:
                break
            for worker in ts.who_has:
                # This also effectively pops from available
                self.data_needed[worker].remove(ts)
            to_gather.append(ts)
            total_nbytes += ts.get_nbytes()

        return to_gather, total_nbytes

    def _ensure_computing(self) -> RecsInstrs:
        if not self.running:
            return {}, []

        recs: Recs = {}
        while self.constrained and len(self.executing) < self.nthreads:
            key = self.constrained[0]
            ts = self.tasks.get(key, None)
            if ts is None or ts.state != "constrained":
                self.constrained.popleft()
                continue

            # There may be duplicates in the self.constrained and self.ready queues.
            # This happens if a task:
            # 1. is assigned to a Worker and transitioned to ready (heappush)
            # 2. is stolen (no way to pop from heap, the task stays there)
            # 3. is assigned to the worker again (heappush again)
            if ts in recs:
                continue

            if not self._resource_restrictions_satisfied(ts):
                break

            self.constrained.popleft()
            self._acquire_resources(ts)

            recs[ts] = "executing"
            self.executing.add(ts)

        while self.ready and len(self.executing) < self.nthreads:
            _, key = heapq.heappop(self.ready)
            ts = self.tasks.get(key)
            if ts is None:
                # It is possible for tasks to be released while still remaining on
                # `ready`. The scheduler might have re-routed to a new worker and
                # told this worker to release. If the task has "disappeared", just
                # continue through the heap.
                continue

            if key in self.data:
                # See comment above about duplicates
                if self.validate:
                    assert ts not in recs or recs[ts] == "memory"
                recs[ts] = "memory"
            elif ts.state in READY:
                # See comment above about duplicates
                if self.validate:
                    assert ts not in recs or recs[ts] == "executing"
                recs[ts] = "executing"
                self.executing.add(ts)

        return recs, []

    def _get_task_finished_msg(
        self, ts: TaskState, stimulus_id: str
    ) -> TaskFinishedMsg:
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
            typ_serialized = pickle.dumps(typ, protocol=4)
        except Exception:
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
            stimulus_id=stimulus_id,
        )

    def _put_key_in_memory(
        self, ts: TaskState, value: object, *, stimulus_id: str
    ) -> Recs:
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

    ###############
    # Transitions #
    ###############

    def _transition_generic_fetch(self, ts: TaskState, stimulus_id: str) -> RecsInstrs:
        if not ts.who_has:
            return {ts: "missing"}, []

        ts.state = "fetch"
        ts.done = False
        assert ts.priority
        for w in ts.who_has:
            self.data_needed[w].add(ts)
        return {}, []

    def _transition_missing_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        self.missing_dep_flight.discard(ts)
        self._purge_state(ts)
        return self._transition_released_waiting(ts, stimulus_id=stimulus_id)

    def _transition_missing_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert ts.state == "missing"

        if not ts.who_has:
            return {}, []

        self.missing_dep_flight.discard(ts)
        return self._transition_generic_fetch(ts, stimulus_id=stimulus_id)

    def _transition_missing_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        self.missing_dep_flight.discard(ts)
        recs, instructions = self._transition_generic_released(
            ts, stimulus_id=stimulus_id
        )
        assert ts.key in self.tasks
        return recs, instructions

    def _transition_flight_missing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        assert ts.done
        return self._transition_generic_missing(ts, stimulus_id=stimulus_id)

    def _transition_generic_missing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert not ts.who_has

        ts.state = "missing"
        self.missing_dep_flight.add(ts)
        ts.done = False
        return {}, []

    def _transition_released_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert ts.state == "released"
        return self._transition_generic_fetch(ts, stimulus_id=stimulus_id)

    def _transition_generic_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        self._purge_state(ts)
        recs: Recs = {}
        for dependency in ts.dependencies:
            if (
                not dependency.waiters
                and dependency.state not in READY | PROCESSING | {"memory"}
            ):
                recs[dependency] = "released"

        ts.state = "released"
        if not ts.dependents:
            recs[ts] = "forgotten"

        return merge_recs_instructions(
            (recs, []),
            self._ensure_computing(),
        )

    def _transition_released_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert all(d.key in self.tasks for d in ts.dependencies)

        recommendations: Recs = {}
        ts.waiting_for_data.clear()
        for dep_ts in ts.dependencies:
            if dep_ts.state != "memory":
                ts.waiting_for_data.add(dep_ts)
                dep_ts.waiters.add(ts)
                recommendations[dep_ts] = "fetch"

        if ts.waiting_for_data:
            self.waiting_for_data_count += 1
        elif ts.resource_restrictions:
            recommendations[ts] = "constrained"
        else:
            recommendations[ts] = "ready"

        ts.state = "waiting"
        return recommendations, []

    def _transition_fetch_flight(
        self, ts: TaskState, worker: str, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert ts.state == "fetch"
            assert ts.who_has
            # The task has already been removed by _ensure_communicating
            for w in ts.who_has:
                assert ts not in self.data_needed[w]

        ts.done = False
        ts.state = "flight"
        ts.coming_from = worker
        self.in_flight_tasks.add(ts)
        return {}, []

    def _transition_memory_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        recs, instructions = self._transition_generic_released(
            ts, stimulus_id=stimulus_id
        )
        instructions.append(ReleaseWorkerDataMsg(key=ts.key, stimulus_id=stimulus_id))
        return recs, instructions

    def _transition_waiting_constrained(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
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
        return self._ensure_computing()

    def _transition_executing_rescheduled(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        """Note: this transition is triggered exclusively by a task raising the
        Reschedule() Exception; it is not involved in work stealing.
        The task is always done.
        """
        if self.validate:
            # Notably, we're missing the third state in which a task can raise
            # Reschedule(), which is "cancelled"
            assert ts.state in ("executing", "long-running"), ts

        self._release_resources(ts)
        self.executing.discard(ts)
        self.long_running.discard(ts)

        return merge_recs_instructions(
            ({}, [RescheduleMsg(key=ts.key, stimulus_id=stimulus_id)]),
            # Note: this is not the same as recommending {ts: "released"} on the
            # previous line, as it would instead transition the task to cancelled - but
            # a task that raised the Reschedule() exception is finished!
            self._transition_generic_released(ts, stimulus_id=stimulus_id),
            self._ensure_computing(),
        )

    def _transition_waiting_ready(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
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

        return self._ensure_computing()

    def _transition_cancelled_error(
        self,
        ts: TaskState,
        exception: Serialize,
        traceback: Serialize | None,
        exception_text: str,
        traceback_text: str,
        *,
        stimulus_id: str,
    ) -> RecsInstrs:
        assert ts._previous in ("executing", "long-running")
        recs, instructions = self._transition_executing_error(
            ts,
            exception,
            traceback,
            exception_text,
            traceback_text,
            stimulus_id=stimulus_id,
        )
        # We'll ignore instructions, i.e. we choose to not submit the failure
        # message to the scheduler since from the schedulers POV it already
        # released this task
        if self.validate:
            assert len(instructions) == 1
            assert isinstance(instructions[0], TaskErredMsg)
            assert instructions[0].key == ts.key
        instructions.clear()
        # Workers should never "retry" tasks. A transition to error should, by
        # default, be the end. Since cancelled indicates that the scheduler lost
        # interest, we can transition straight to released
        assert ts not in recs
        recs[ts] = "released"
        return recs, instructions

    def _transition_generic_error(
        self,
        ts: TaskState,
        exception: Serialize,
        traceback: Serialize | None,
        exception_text: str,
        traceback_text: str,
        *,
        stimulus_id: str,
    ) -> RecsInstrs:
        ts.exception = exception
        ts.traceback = traceback
        ts.exception_text = exception_text
        ts.traceback_text = traceback_text
        ts.state = "error"
        smsg = TaskErredMsg.from_task(
            ts,
            stimulus_id=stimulus_id,
            thread=self.threads.get(ts.key),
        )

        return {}, [smsg]

    def _transition_executing_error(
        self,
        ts: TaskState,
        exception: Serialize,
        traceback: Serialize | None,
        exception_text: str,
        traceback_text: str,
        *,
        stimulus_id: str,
    ) -> RecsInstrs:
        self._release_resources(ts)
        self.executing.discard(ts)
        self.long_running.discard(ts)

        return merge_recs_instructions(
            self._transition_generic_error(
                ts,
                exception,
                traceback,
                exception_text,
                traceback_text,
                stimulus_id=stimulus_id,
            ),
            self._ensure_computing(),
        )

    def _transition_from_resumed(
        self, ts: TaskState, finish: TaskStateState, stimulus_id: str
    ) -> RecsInstrs:
        """`resumed` is an intermediate degenerate state which splits further up
        into two states depending on what the last signal / next state is
        intended to be. There are only two viable choices depending on whether
        the task is required to be fetched from another worker `resumed(fetch)`
        or the task shall be computed on this worker `resumed(waiting)`.

        The only viable state transitions ending up here are

        flight -> cancelled -> resumed(waiting)

        or

        executing -> cancelled -> resumed(fetch)

        depending on the origin. Equally, only `fetch`, `waiting`, or `released`
        are allowed output states.

        See also `_transition_resumed_waiting`
        """
        recs: Recs = {}
        instructions: Instructions = []

        if ts._previous == finish:
            # We're back where we started. We should forget about the entire
            # cancellation attempt
            ts.state = finish
            ts._next = None
            ts._previous = None
        elif not ts.done:
            # If we're not done, yet, just remember where we want to be next
            ts._next = finish
        else:
            # Flight/executing finished unsuccessfully, i.e. not in memory
            assert finish != "memory"
            next_state = ts._next
            assert next_state in {"waiting", "fetch"}, next_state
            assert ts._previous in {"executing", "long-running", "flight"}, ts._previous

            if next_state != finish:
                recs, instructions = self._transition_generic_released(
                    ts, stimulus_id=stimulus_id
                )
            recs[ts] = next_state

        return recs, instructions

    def _transition_resumed_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        """See Worker._transition_from_resumed"""
        recs, instructions = self._transition_from_resumed(
            ts, "fetch", stimulus_id=stimulus_id
        )
        if self.validate:
            # This would only be possible in a fetch->cancelled->resumed->fetch loop,
            # but there are no transitions from fetch which set the state to cancelled.
            # If this assertion failed, we' need to call _ensure_communicating like in
            # the other transitions that set ts.status = "fetch".
            assert ts.state != "fetch"
        return recs, instructions

    def _transition_resumed_missing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        """See Worker._transition_from_resumed"""
        return self._transition_from_resumed(ts, "missing", stimulus_id=stimulus_id)

    def _transition_resumed_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if not ts.done:
            ts.state = "cancelled"
            ts._next = None
            return {}, []
        else:
            return self._transition_generic_released(ts, stimulus_id=stimulus_id)

    def _transition_resumed_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        """See Worker._transition_from_resumed"""
        return self._transition_from_resumed(ts, "waiting", stimulus_id=stimulus_id)

    def _transition_cancelled_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if ts.done:
            return {ts: "released"}, []
        elif ts._previous == "flight":
            ts.state = ts._previous
            return {}, []
        else:
            assert ts._previous in ("executing", "long-running")
            ts.state = "resumed"
            ts._next = "fetch"
            return {}, []

    def _transition_cancelled_waiting(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if ts.done:
            return {ts: "released"}, []
        elif ts._previous in ("executing", "long-running"):
            ts.state = ts._previous
            return {}, []
        else:
            assert ts._previous == "flight"
            ts.state = "resumed"
            ts._next = "waiting"
            return {}, []

    def _transition_cancelled_forgotten(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        ts._next = "forgotten"
        if not ts.done:
            return {}, []
        return {ts: "released"}, []

    def _transition_cancelled_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if not ts.done:
            return {}, []
        self.executing.discard(ts)
        self.long_running.discard(ts)
        self.in_flight_tasks.discard(ts)

        self._release_resources(ts)
        return self._transition_generic_released(ts, stimulus_id=stimulus_id)

    def _transition_executing_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        ts._previous = ts.state
        ts._next = None
        # See https://github.com/dask/distributed/pull/5046#discussion_r685093940
        ts.state = "cancelled"
        ts.done = False
        return {}, []

    def _transition_generic_memory(
        self, ts: TaskState, value: object = NO_VALUE, *, stimulus_id: str
    ) -> RecsInstrs:
        if value is NO_VALUE and ts.key not in self.data:
            raise RuntimeError(
                f"Tried to transition task {ts} to `memory` without data available"
            )

        self._release_resources(ts)
        self.executing.discard(ts)
        self.long_running.discard(ts)
        self.in_flight_tasks.discard(ts)
        ts.coming_from = None

        instructions: Instructions = []
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
        else:
            if self.validate:
                assert ts.key in self.data or ts.key in self.actors
            instructions.append(
                self._get_task_finished_msg(ts, stimulus_id=stimulus_id)
            )

        return recs, instructions

    def _transition_executing_memory(
        self, ts: TaskState, value: object = NO_VALUE, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert ts.state in ("executing", "long-running")
            assert not ts.waiting_for_data
            assert ts.key not in self.ready

        self.executing.discard(ts)
        self.long_running.discard(ts)
        self.executed_count += 1
        return merge_recs_instructions(
            self._transition_generic_memory(ts, value=value, stimulus_id=stimulus_id),
            self._ensure_computing(),
        )

    def _transition_constrained_executing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if self.validate:
            assert not ts.waiting_for_data
            assert ts.key not in self.data
            assert ts.state in READY
            assert ts.key not in self.ready
            for dep in ts.dependencies:
                assert dep.key in self.data or dep.key in self.actors

        ts.state = "executing"
        instr = Execute(key=ts.key, stimulus_id=stimulus_id)
        return {}, [instr]

    def _transition_ready_executing(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
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
        instr = Execute(key=ts.key, stimulus_id=stimulus_id)
        return {}, [instr]

    def _transition_flight_fetch(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        # If this transition is called after the flight coroutine has finished,
        # we can reset the task and transition to fetch again. If it is not yet
        # finished, this should be a no-op
        if not ts.done:
            return {}, []

        ts.coming_from = None
        return self._transition_generic_fetch(ts, stimulus_id=stimulus_id)

    def _transition_flight_error(
        self,
        ts: TaskState,
        exception: Serialize,
        traceback: Serialize | None,
        exception_text: str,
        traceback_text: str,
        *,
        stimulus_id: str,
    ) -> RecsInstrs:
        self.in_flight_tasks.discard(ts)
        ts.coming_from = None
        return self._transition_generic_error(
            ts,
            exception,
            traceback,
            exception_text,
            traceback_text,
            stimulus_id=stimulus_id,
        )

    def _transition_flight_released(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        if ts.done:
            # FIXME: Is this even possible? Would an assert instead be more
            # sensible?
            return self._transition_generic_released(ts, stimulus_id=stimulus_id)
        else:
            ts._previous = "flight"
            ts._next = None
            # See https://github.com/dask/distributed/pull/5046#discussion_r685093940
            ts.state = "cancelled"
            return {}, []

    def _transition_cancelled_memory(
        self, ts: TaskState, value: object, *, stimulus_id: str
    ) -> RecsInstrs:
        # We only need this because the to-memory signatures require a value but
        # we do not want to store a cancelled result and want to release
        # immediately
        assert ts.done
        return self._transition_cancelled_released(ts, stimulus_id=stimulus_id)

    def _transition_executing_long_running(
        self, ts: TaskState, compute_duration: float, *, stimulus_id: str
    ) -> RecsInstrs:
        ts.state = "long-running"
        self.executing.discard(ts)
        self.long_running.add(ts)

        smsg = LongRunningMsg(
            key=ts.key, compute_duration=compute_duration, stimulus_id=stimulus_id
        )
        return merge_recs_instructions(
            ({}, [smsg]),
            self._ensure_computing(),
        )

    def _transition_released_memory(
        self, ts: TaskState, value: object, *, stimulus_id: str
    ) -> RecsInstrs:
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
            return recs, []
        smsg = AddKeysMsg(keys=[ts.key], stimulus_id=stimulus_id)
        return recs, [smsg]

    def _transition_flight_memory(
        self, ts: TaskState, value: object, *, stimulus_id: str
    ) -> RecsInstrs:
        self.in_flight_tasks.discard(ts)
        ts.coming_from = None
        try:
            recs = self._put_key_in_memory(ts, value, stimulus_id=stimulus_id)
        except Exception as e:
            msg = error_message(e)
            recs = {ts: tuple(msg.values())}
            return recs, []
        smsg = AddKeysMsg(keys=[ts.key], stimulus_id=stimulus_id)
        return recs, [smsg]

    def _transition_released_forgotten(
        self, ts: TaskState, *, stimulus_id: str
    ) -> RecsInstrs:
        recommendations: Recs = {}
        # Dependents _should_ be released by the scheduler before this
        if self.validate:
            assert not any(d.state != "forgotten" for d in ts.dependents)
        for dep in ts.dependencies:
            dep.dependents.discard(ts)
            if dep.state == "released" and not dep.dependents:
                recommendations[dep] = "forgotten"
        self._purge_state(ts)
        # Mark state as forgotten in case it is still referenced
        ts.state = "forgotten"
        self.tasks.pop(ts.key, None)
        return recommendations, []

    # {
    #     (start, finish):
    #     transition_<start>_<finish>(
    #         self, ts: TaskState, *args, stimulus_id: str
    #     ) -> (recommendations, instructions)
    # }
    _TRANSITIONS_TABLE: ClassVar[
        Mapping[tuple[TaskStateState, TaskStateState], Callable[..., RecsInstrs]]
    ] = {
        ("cancelled", "fetch"): _transition_cancelled_fetch,
        ("cancelled", "released"): _transition_cancelled_released,
        ("cancelled", "missing"): _transition_cancelled_released,
        ("cancelled", "waiting"): _transition_cancelled_waiting,
        ("cancelled", "forgotten"): _transition_cancelled_forgotten,
        ("cancelled", "rescheduled"): _transition_cancelled_released,
        ("cancelled", "memory"): _transition_cancelled_memory,
        ("cancelled", "error"): _transition_cancelled_error,
        ("resumed", "memory"): _transition_generic_memory,
        ("resumed", "error"): _transition_generic_error,
        ("resumed", "released"): _transition_resumed_released,
        ("resumed", "waiting"): _transition_resumed_waiting,
        ("resumed", "fetch"): _transition_resumed_fetch,
        ("resumed", "missing"): _transition_resumed_missing,
        ("constrained", "executing"): _transition_constrained_executing,
        ("constrained", "released"): _transition_generic_released,
        ("error", "released"): _transition_generic_released,
        ("executing", "error"): _transition_executing_error,
        ("executing", "long-running"): _transition_executing_long_running,
        ("executing", "memory"): _transition_executing_memory,
        ("executing", "released"): _transition_executing_released,
        ("executing", "rescheduled"): _transition_executing_rescheduled,
        ("fetch", "flight"): _transition_fetch_flight,
        ("fetch", "missing"): _transition_generic_missing,
        ("fetch", "released"): _transition_generic_released,
        ("flight", "error"): _transition_flight_error,
        ("flight", "fetch"): _transition_flight_fetch,
        ("flight", "memory"): _transition_flight_memory,
        ("flight", "missing"): _transition_flight_missing,
        ("flight", "released"): _transition_flight_released,
        ("long-running", "error"): _transition_executing_error,
        ("long-running", "memory"): _transition_executing_memory,
        ("long-running", "rescheduled"): _transition_executing_rescheduled,
        ("long-running", "released"): _transition_executing_released,
        ("memory", "released"): _transition_memory_released,
        ("missing", "fetch"): _transition_missing_fetch,
        ("missing", "released"): _transition_missing_released,
        ("missing", "error"): _transition_generic_error,
        ("missing", "waiting"): _transition_missing_waiting,
        ("ready", "error"): _transition_generic_error,
        ("ready", "executing"): _transition_ready_executing,
        ("ready", "released"): _transition_generic_released,
        ("released", "error"): _transition_generic_error,
        ("released", "fetch"): _transition_released_fetch,
        ("released", "missing"): _transition_generic_missing,
        ("released", "forgotten"): _transition_released_forgotten,
        ("released", "memory"): _transition_released_memory,
        ("released", "waiting"): _transition_released_waiting,
        ("waiting", "constrained"): _transition_waiting_constrained,
        ("waiting", "ready"): _transition_waiting_ready,
        ("waiting", "released"): _transition_generic_released,
    }

    def _notify_plugins(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        for name, plugin in self.plugins.items():
            if hasattr(plugin, method_name):
                try:
                    getattr(plugin, method_name)(*args, **kwargs)
                except Exception:
                    logger.info(
                        "Plugin '%s' failed with exception", name, exc_info=True
                    )

    def _transition(
        self,
        ts: TaskState,
        finish: TaskStateState | tuple,
        *args: Any,
        stimulus_id: str,
    ) -> RecsInstrs:
        """Transition a key from its current state to the finish state

        See Also
        --------
        Worker.transitions: wrapper around this method
        """
        if isinstance(finish, tuple):
            # the concatenated transition path might need to access the tuple
            assert not args
            args = finish[1:]
            finish = cast(TaskStateState, finish[0])

        if ts.state == finish:
            return {}, []

        start = ts.state
        func = self._TRANSITIONS_TABLE.get((start, finish))

        # Notes:
        # - in case of transition through released, this counter is incremented by 2
        # - this increase happens before the actual transitions, so that it can
        #   catch potential infinite recursions
        self.transition_counter += 1
        if (
            self.transition_counter_max
            and self.transition_counter >= self.transition_counter_max
        ):
            raise TransitionCounterMaxExceeded(ts.key, start, finish, self.story(ts))

        if func is not None:
            recs, instructions = func(self, ts, *args, stimulus_id=stimulus_id)
            self._notify_plugins("transition", ts.key, start, finish)

        elif "released" not in (start, finish):
            # start -> "released" -> finish
            try:
                recs, instructions = self._transition(
                    ts, "released", stimulus_id=stimulus_id
                )
                v_state: TaskStateState
                v_args: list | tuple
                while v := recs.pop(ts, None):
                    if isinstance(v, tuple):
                        v_state, *v_args = v
                    else:
                        v_state, v_args = v, ()
                    if v_state == "forgotten":
                        # We do not want to forget. The purpose of this
                        # transition path is to get to `finish`
                        continue
                    recs, instructions = merge_recs_instructions(
                        (recs, instructions),
                        self._transition(ts, v_state, *v_args, stimulus_id=stimulus_id),
                    )
                recs, instructions = merge_recs_instructions(
                    (recs, instructions),
                    self._transition(ts, finish, *args, stimulus_id=stimulus_id),
                )
            except (InvalidTransition, RecommendationsConflict) as e:
                raise InvalidTransition(ts.key, start, finish, self.story(ts)) from e

        else:
            raise InvalidTransition(ts.key, start, finish, self.story(ts))

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

    def _resource_restrictions_satisfied(self, ts: TaskState) -> bool:
        return all(
            self.available_resources[resource] >= needed
            for resource, needed in ts.resource_restrictions.items()
        )

    def _acquire_resources(self, ts: TaskState) -> None:
        for resource, needed in ts.resource_restrictions.items():
            self.available_resources[resource] -= needed

    def _release_resources(self, ts: TaskState) -> None:
        for resource, needed in ts.resource_restrictions.items():
            self.available_resources[resource] += needed

    def _transitions(self, recommendations: Recs, *, stimulus_id: str) -> Instructions:
        """Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        instructions = []
        tasks = set()

        def process_recs(recs: Recs) -> None:
            while recs:
                ts, finish = recs.popitem()
                tasks.add(ts)
                a_recs, a_instructions = self._transition(
                    ts, finish, stimulus_id=stimulus_id
                )
                recs.update(a_recs)
                instructions.extend(a_instructions)

        process_recs(recommendations.copy())

        # We could call _ensure_communicating after we change something that could
        # trigger a new call to gather_dep (e.g. on transitions to fetch,
        # GatherDepDoneEvent, or RetryBusyWorkerEvent). However, doing so we'd
        # potentially call it too early, before all tasks have transitioned to fetch.
        # This in turn would hurt aggregation of multiple tasks into a single GatherDep
        # instruction.
        # Read: https://github.com/dask/distributed/issues/6497
        a_recs, a_instructions = self._ensure_communicating(stimulus_id=stimulus_id)
        instructions += a_instructions
        process_recs(a_recs)

        if self.validate:
            # Full state validation is very expensive
            for ts in tasks:
                self.validate_task(ts)

        return instructions

    ##########
    # Events #
    ##########

    @singledispatchmethod
    def _handle_event(self, ev: StateMachineEvent) -> RecsInstrs:
        raise TypeError(ev)  # pragma: nocover

    @_handle_event.register
    def _handle_update_data(self, ev: UpdateDataEvent) -> RecsInstrs:
        recommendations: Recs = {}
        instructions: Instructions = []
        for key, value in ev.data.items():
            try:
                ts = self.tasks[key]
                recommendations[ts] = ("memory", value)
            except KeyError:
                self.tasks[key] = ts = TaskState(key)

                try:
                    recs = self._put_key_in_memory(
                        ts, value, stimulus_id=ev.stimulus_id
                    )
                except Exception as e:
                    msg = error_message(e)
                    recommendations = {ts: tuple(msg.values())}
                else:
                    recommendations.update(recs)

            self.log.append((key, "receive-from-scatter", ev.stimulus_id, time()))

        if ev.report:
            instructions.append(
                AddKeysMsg(keys=list(ev.data), stimulus_id=ev.stimulus_id)
            )

        return recommendations, instructions

    @_handle_event.register
    def _handle_free_keys(self, ev: FreeKeysEvent) -> RecsInstrs:
        """Handler to be called by the scheduler.

        The given keys are no longer referred to and required by the scheduler.
        The worker is now allowed to release the key, if applicable.

        This does not guarantee that the memory is released since the worker may
        still decide to hold on to the data and task since it is required by an
        upstream dependency.
        """
        self.log.append(("free-keys", ev.keys, ev.stimulus_id, time()))
        recommendations: Recs = {}
        for key in ev.keys:
            ts = self.tasks.get(key)
            if ts:
                recommendations[ts] = "released"
        return recommendations, []

    @_handle_event.register
    def _handle_remove_replicas(self, ev: RemoveReplicasEvent) -> RecsInstrs:
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
        recommendations: Recs = {}
        instructions: Instructions = []

        rejected = []
        for key in ev.keys:
            ts = self.tasks.get(key)
            if ts is None or ts.state != "memory":
                continue
            if not ts.is_protected():
                self.log.append(
                    (ts.key, "remove-replica-confirmed", ev.stimulus_id, time())
                )
                recommendations[ts] = "released"
            else:
                rejected.append(key)

        if rejected:
            self.log.append(
                ("remove-replica-rejected", rejected, ev.stimulus_id, time())
            )
            instructions.append(AddKeysMsg(keys=rejected, stimulus_id=ev.stimulus_id))

        return recommendations, instructions

    @_handle_event.register
    def _handle_acquire_replicas(self, ev: AcquireReplicasEvent) -> RecsInstrs:
        if self.validate:
            assert ev.who_has.keys() == ev.nbytes.keys()
            assert all(ev.who_has.values())

        recommendations: Recs = {}
        for key, nbytes in ev.nbytes.items():
            ts = self._ensure_task_exists(
                key=key,
                # Transfer this data after all dependency tasks of computations with
                # default or explicitly high (>0) user priority and before all
                # computations with low priority (<0). Note that the priority= parameter
                # of compute() is multiplied by -1 before it reaches TaskState.priority.
                priority=(1,),
                stimulus_id=ev.stimulus_id,
            )
            if ts.state != "memory":
                ts.nbytes = nbytes
                recommendations[ts] = "fetch"

        self._update_who_has(ev.who_has)
        return recommendations, []

    @_handle_event.register
    def _handle_compute_task(self, ev: ComputeTaskEvent) -> RecsInstrs:
        try:
            ts = self.tasks[ev.key]
            logger.debug(
                "Asked to compute an already known task %s",
                {"task": ts, "stimulus_id": ev.stimulus_id},
            )
        except KeyError:
            self.tasks[ev.key] = ts = TaskState(ev.key)
        self.log.append((ev.key, "compute-task", ts.state, ev.stimulus_id, time()))

        recommendations: Recs = {}
        instructions: Instructions = []

        if ts.state in READY | {
            "executing",
            "long-running",
            "waiting",
        }:
            pass
        elif ts.state == "memory":
            instructions.append(
                self._get_task_finished_msg(ts, stimulus_id=ev.stimulus_id)
            )
        elif ts.state == "error":
            instructions.append(TaskErredMsg.from_task(ts, stimulus_id=ev.stimulus_id))
        elif ts.state in {
            "released",
            "fetch",
            "flight",
            "missing",
            "cancelled",
            "resumed",
        }:
            recommendations[ts] = "waiting"

            ts.run_spec = ev.run_spec

            priority = ev.priority + (self.generation,)
            self.generation -= 1

            if ev.actor:
                self.actors[ts.key] = None

            ts.exception = None
            ts.traceback = None
            ts.exception_text = ""
            ts.traceback_text = ""
            ts.priority = priority
            ts.duration = ev.duration
            ts.resource_restrictions = ev.resource_restrictions
            ts.annotations = ev.annotations

            if self.validate:
                assert ev.who_has.keys() == ev.nbytes.keys()
                for dep_workers in ev.who_has.values():
                    assert dep_workers
                    assert len(dep_workers) == len(set(dep_workers))

            for dep_key, nbytes in ev.nbytes.items():
                dep_ts = self._ensure_task_exists(
                    key=dep_key,
                    priority=priority,
                    stimulus_id=ev.stimulus_id,
                )
                self.tasks[dep_key].nbytes = nbytes

                # link up to child / parents
                ts.dependencies.add(dep_ts)
                dep_ts.dependents.add(ts)

            self._update_who_has(ev.who_has)
        else:
            raise RuntimeError(  # pragma: nocover
                f"Unexpected task state encountered for {ts}; "
                f"stimulus_id={ev.stimulus_id}; story={self.story(ts)}"
            )

        return recommendations, instructions

    def _gather_dep_done_common(self, ev: GatherDepDoneEvent) -> Iterator[TaskState]:
        """Common code for the handlers of all subclasses of GatherDepDoneEvent.

        Yields the tasks that need to transition out of flight.
        """
        self.comm_nbytes -= ev.total_nbytes
        keys = self.in_flight_workers.pop(ev.worker)
        for key in keys:
            ts = self.tasks[key]
            ts.done = True
            yield ts

    @_handle_event.register
    def _handle_gather_dep_success(self, ev: GatherDepSuccessEvent) -> RecsInstrs:
        """gather_dep terminated successfully.
        The response may contain less keys than the request.
        """
        recommendations: Recs = {}
        for ts in self._gather_dep_done_common(ev):
            if ts.key in ev.data:
                recommendations[ts] = ("memory", ev.data[ts.key])
            else:
                self.log.append((ts.key, "missing-dep", ev.stimulus_id, time()))
                if self.validate:
                    assert ts.state != "fetch"
                    assert ts not in self.data_needed[ev.worker]
                ts.who_has.discard(ev.worker)
                self.has_what[ev.worker].discard(ts.key)
                recommendations[ts] = "fetch"

        return recommendations, []

    @_handle_event.register
    def _handle_gather_dep_busy(self, ev: GatherDepBusyEvent) -> RecsInstrs:
        """gather_dep terminated: remote worker is busy"""
        # Avoid hammering the worker. If there are multiple replicas
        # available, immediately try fetching from a different worker.
        self.busy_workers.add(ev.worker)

        recommendations: Recs = {}
        refresh_who_has = []
        for ts in self._gather_dep_done_common(ev):
            recommendations[ts] = "fetch"
            if not ts.who_has - self.busy_workers:
                refresh_who_has.append(ts.key)

        instructions: Instructions = [
            RetryBusyWorkerLater(worker=ev.worker, stimulus_id=ev.stimulus_id),
        ]

        if refresh_who_has:
            # All workers that hold known replicas of our tasks are busy.
            # Try querying the scheduler for unknown ones.
            instructions.append(
                RequestRefreshWhoHasMsg(
                    keys=refresh_who_has, stimulus_id=ev.stimulus_id
                )
            )

        return recommendations, instructions

    @_handle_event.register
    def _handle_gather_dep_network_failure(
        self, ev: GatherDepNetworkFailureEvent
    ) -> RecsInstrs:
        """gather_dep terminated: network failure while trying to
        communicate with remote worker

        Though the network failure could be transient, we assume it is not, and
        preemptively act as though the other worker has died (including removing all
        keys from it, even ones we did not fetch).

        This optimization leads to faster completion of the fetch, since we immediately
        either retry a different worker, or ask the scheduler to inform us of a new
        worker if no other worker is available.
        """
        recommendations: Recs = {}

        for ts in self._gather_dep_done_common(ev):
            self.log.append((ts.key, "missing-dep", ev.stimulus_id, time()))
            recommendations[ts] = "fetch"

        for ts in self.data_needed.pop(ev.worker, ()):
            if self.validate:
                assert ts.state == "fetch"
                assert ev.worker in ts.who_has
            if ts.who_has == {ev.worker}:
                # This can override a recommendation from the previous for loop
                recommendations[ts] = "missing"

        for key in self.has_what.pop(ev.worker):
            ts = self.tasks[key]
            ts.who_has.remove(ev.worker)

        return recommendations, []

    @_handle_event.register
    def _handle_gather_dep_failure(self, ev: GatherDepFailureEvent) -> RecsInstrs:
        """gather_dep terminated: generic error raised (not a network failure);
        e.g. data failed to deserialize.
        """
        recommendations: Recs = {
            ts: (
                "error",
                ev.exception,
                ev.traceback,
                ev.exception_text,
                ev.traceback_text,
            )
            for ts in self._gather_dep_done_common(ev)
        }

        return recommendations, []

    @_handle_event.register
    def _handle_secede(self, ev: SecedeEvent) -> RecsInstrs:
        ts = self.tasks.get(ev.key)
        if ts and ts.state == "executing":
            return {ts: ("long-running", ev.compute_duration)}, []
        else:
            return {}, []

    @_handle_event.register
    def _handle_steal_request(self, ev: StealRequestEvent) -> RecsInstrs:
        # There may be a race condition between stealing and releasing a task.
        # In this case the self.tasks is already cleared. The `None` will be
        # registered as `already-computing` on the other end
        ts = self.tasks.get(ev.key)
        state = ts.state if ts is not None else None
        smsg = StealResponseMsg(key=ev.key, state=state, stimulus_id=ev.stimulus_id)

        if state in READY | {"waiting"}:
            # If task is marked as "constrained" we haven't yet assigned it an
            # `available_resources` to run on, that happens in
            # `_transition_constrained_executing`
            assert ts
            return {ts: "released"}, [smsg]
        else:
            return {}, [smsg]

    @_handle_event.register
    def _handle_pause(self, ev: PauseEvent) -> RecsInstrs:
        """Prevent any further tasks to be executed or gathered. Tasks that are
        currently executing or in flight will continue to progress.
        """
        self.running = False
        return {}, []

    @_handle_event.register
    def _handle_unpause(self, ev: UnpauseEvent) -> RecsInstrs:
        """Emerge from paused status"""
        self.running = True
        return self._ensure_computing()

    @_handle_event.register
    def _handle_retry_busy_worker(self, ev: RetryBusyWorkerEvent) -> RecsInstrs:
        self.busy_workers.discard(ev.worker)
        return {}, []

    @_handle_event.register
    def _handle_cancel_compute(self, ev: CancelComputeEvent) -> RecsInstrs:
        """Cancel a task on a best-effort basis. This is only possible while a task
        is in state `waiting` or `ready`; nothing will happen otherwise.
        """
        ts = self.tasks.get(ev.key)
        if not ts or ts.state not in READY | {"waiting"}:
            return {}, []

        self.log.append((ev.key, "cancel-compute", ev.stimulus_id, time()))
        # All possible dependents of ts should not be in state Processing on
        # scheduler side and therefore should not be assigned to a worker, yet.
        assert not ts.dependents
        return {ts: "released"}, []

    @_handle_event.register
    def _handle_already_cancelled(self, ev: AlreadyCancelledEvent) -> RecsInstrs:
        """Task is already cancelled by the time execute() runs"""
        # key *must* be still in tasks. Releasing it directly is forbidden
        # without going through cancelled
        ts = self.tasks.get(ev.key)
        assert ts, self.story(ev.key)
        ts.done = True
        return {ts: "released"}, []

    @_handle_event.register
    def _handle_execute_success(self, ev: ExecuteSuccessEvent) -> RecsInstrs:
        """Task completed successfully"""
        # key *must* be still in tasks. Releasing it directly is forbidden
        # without going through cancelled
        ts = self.tasks.get(ev.key)
        assert ts, self.story(ev.key)

        ts.done = True
        ts.startstops.append({"action": "compute", "start": ev.start, "stop": ev.stop})
        ts.nbytes = ev.nbytes
        ts.type = ev.type
        return {ts: ("memory", ev.value)}, []

    @_handle_event.register
    def _handle_execute_failure(self, ev: ExecuteFailureEvent) -> RecsInstrs:
        """Task execution failed"""
        # key *must* be still in tasks. Releasing it directly is forbidden
        # without going through cancelled
        ts = self.tasks.get(ev.key)
        assert ts, self.story(ev.key)

        ts.done = True
        if ev.start is not None and ev.stop is not None:
            ts.startstops.append(
                {"action": "compute", "start": ev.start, "stop": ev.stop}
            )

        return {
            ts: (
                "error",
                ev.exception,
                ev.traceback,
                ev.exception_text,
                ev.traceback_text,
            )
        }, []

    @_handle_event.register
    def _handle_reschedule(self, ev: RescheduleEvent) -> RecsInstrs:
        """Task raised Reschedule() exception while it was running.

        Note: this has nothing to do with work stealing, which instead causes a
        FreeKeysEvent.
        """
        # key *must* be still in tasks. Releasing it directly is forbidden
        # without going through cancelled
        ts = self.tasks.get(ev.key)
        assert ts, self.story(ev.key)

        ts.done = True
        return {ts: "rescheduled"}, []

    @_handle_event.register
    def _handle_find_missing(self, ev: FindMissingEvent) -> RecsInstrs:
        if not self.missing_dep_flight:
            return {}, []

        if self.validate:
            for ts in self.missing_dep_flight:
                assert not ts.who_has, self.story(ts)

        smsg = RequestRefreshWhoHasMsg(
            keys=[ts.key for ts in self.missing_dep_flight],
            stimulus_id=ev.stimulus_id,
        )
        return {}, [smsg]

    @_handle_event.register
    def _handle_refresh_who_has(self, ev: RefreshWhoHasEvent) -> RecsInstrs:
        self._update_who_has(ev.who_has)
        recommendations: Recs = {}
        instructions: Instructions = []

        for key in ev.who_has:
            ts = self.tasks.get(key)
            if not ts:
                continue

            if ts.who_has and ts.state == "missing":
                recommendations[ts] = "fetch"
            elif not ts.who_has and ts.state == "fetch":
                recommendations[ts] = "missing"
            # Note: if ts.who_has and ts.state == "fetch", we may have just acquired new
            # replicas whereas all previously known workers are in flight or busy. We
            # rely on _transitions to call _ensure_communicating every time, even in
            # absence of recommendations, to potentially kick off a new call to
            # gather_dep.

        return recommendations, instructions

    ###############
    # Diagnostics #
    ###############

    def story(self, *keys_or_tasks_or_stimuli: str | TaskState) -> list[tuple]:
        """Return all records from the transitions log involving one or more tasks or
        stimulus_id's
        """
        keys_or_stimuli = {
            e.key if isinstance(e, TaskState) else e for e in keys_or_tasks_or_stimuli
        }
        return worker_story(keys_or_stimuli, self.log)

    def stimulus_story(
        self, *keys_or_tasks: str | TaskState
    ) -> list[StateMachineEvent]:
        """Return all state machine events involving one or more tasks"""
        keys = {e.key if isinstance(e, TaskState) else e for e in keys_or_tasks}
        return [ev for ev in self.stimulus_log if getattr(ev, "key", None) in keys]

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        """Dictionary representation for debugging purposes.
        Not type stable and not intended for roundtrips.

        See also
        --------
        Client.dump_cluster_state
        distributed.utils.recursive_to_dict
        """
        info = {
            "address": self.address,
            "nthreads": self.nthreads,
            "running": self.running,
            "ready": self.ready,
            "constrained": self.constrained,
            "data": dict.fromkeys(self.data),
            "data_needed": {
                w: [ts.key for ts in tss.sorted()]
                for w, tss in self.data_needed.items()
            },
            "executing": {ts.key for ts in self.executing},
            "long_running": {ts.key for ts in self.long_running},
            "in_flight_tasks": {ts.key for ts in self.in_flight_tasks},
            "in_flight_workers": self.in_flight_workers,
            "busy_workers": self.busy_workers,
            "log": self.log,
            "stimulus_log": self.stimulus_log,
            "transition_counter": self.transition_counter,
            "tasks": self.tasks,
        }
        info = {k: v for k, v in info.items() if k not in exclude}
        return recursive_to_dict(info, exclude=exclude)

    ##############
    # Validation #
    ##############

    def _validate_task_memory(self, ts: TaskState) -> None:
        assert ts.key in self.data or ts.key in self.actors
        assert isinstance(ts.nbytes, int)
        assert not ts.waiting_for_data
        assert ts.key not in self.ready
        assert ts.state == "memory"

    def _validate_task_executing(self, ts: TaskState) -> None:
        if ts.state == "executing":
            assert ts in self.executing
            assert ts not in self.long_running
        else:
            assert ts.state == "long-running"
            assert ts not in self.executing
            assert ts in self.long_running

        assert ts.run_spec is not None
        assert ts.key not in self.data
        assert not ts.waiting_for_data
        for dep in ts.dependencies:
            assert dep.state == "memory", self.story(dep)
            assert dep.key in self.data or dep.key in self.actors

    def _validate_task_ready(self, ts: TaskState) -> None:
        assert ts.key in pluck(1, self.ready)
        assert ts.key not in self.data
        assert ts.state != "executing"
        assert not ts.done
        assert not ts.waiting_for_data
        assert all(
            dep.key in self.data or dep.key in self.actors for dep in ts.dependencies
        )

    def _validate_task_waiting(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert ts.state == "waiting"
        assert not ts.done
        if ts.dependencies and ts.run_spec:
            assert not all(dep.key in self.data for dep in ts.dependencies)

    def _validate_task_flight(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert ts in self.in_flight_tasks
        assert not any(dep.key in self.ready for dep in ts.dependents)
        assert ts.coming_from
        assert ts.coming_from in self.in_flight_workers
        assert ts.key in self.in_flight_workers[ts.coming_from]

    def _validate_task_fetch(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert self.address not in ts.who_has
        assert not ts.done
        assert ts.who_has
        for w in ts.who_has:
            assert ts.key in self.has_what[w]
            assert ts in self.data_needed[w]

    def _validate_task_missing(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert not ts.who_has
        assert not ts.done
        assert not any(ts.key in has_what for has_what in self.has_what.values())
        assert ts in self.missing_dep_flight

    def _validate_task_cancelled(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert ts._previous in {"long-running", "executing", "flight"}
        # We'll always transition to released after it is done
        assert ts._next is None, (ts.key, ts._next, self.story(ts))

    def _validate_task_resumed(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert ts._next in {"fetch", "waiting"}
        assert ts._previous in {"long-running", "executing", "flight"}

    def _validate_task_released(self, ts: TaskState) -> None:
        assert ts.key not in self.data
        assert not ts._next
        assert not ts._previous
        for tss in self.data_needed.values():
            assert ts not in tss
        assert ts not in self.executing
        assert ts not in self.in_flight_tasks
        assert ts not in self.missing_dep_flight

        # The below assert statement is true most of the time. If a task performs the
        # transition flight->cancel->waiting, its dependencies are normally in released
        # state. However, the compute-task call for their previous dependent provided
        # them with who_has, such that this assert is no longer true.
        #
        # assert not any(ts.key in has_what for has_what in self.has_what.values())

        assert not ts.waiting_for_data
        assert not ts.done
        assert not ts.exception
        assert not ts.traceback

    def validate_task(self, ts: TaskState) -> None:
        try:
            if ts.key in self.tasks:
                assert self.tasks[ts.key] is ts
            if ts.state == "memory":
                self._validate_task_memory(ts)
            elif ts.state == "waiting":
                self._validate_task_waiting(ts)
            elif ts.state == "missing":
                self._validate_task_missing(ts)
            elif ts.state == "cancelled":
                self._validate_task_cancelled(ts)
            elif ts.state == "resumed":
                self._validate_task_resumed(ts)
            elif ts.state == "ready":
                self._validate_task_ready(ts)
            elif ts.state in ("executing", "long-running"):
                self._validate_task_executing(ts)
            elif ts.state == "flight":
                self._validate_task_flight(ts)
            elif ts.state == "fetch":
                self._validate_task_fetch(ts)
            elif ts.state == "released":
                self._validate_task_released(ts)
        except Exception as e:
            logger.exception(e)
            raise InvalidTaskState(
                key=ts.key, state=ts.state, story=self.story(ts)
            ) from e

    def validate_state(self) -> None:
        for ts in self.tasks.values():
            # check that worker has task
            for worker in ts.who_has:
                assert worker != self.address
                assert ts.key in self.has_what[worker]
            # check that deps have a set state and that dependency<->dependent links
            # are there
            for dep in ts.dependencies:
                # self.tasks was just a dict of tasks
                # and this check was originally that the key was in `task_state`
                # so we may have popped the key out of `self.tasks` but the
                # dependency can still be in `memory` before GC grabs it...?
                # Might need better bookkeeping
                assert self.tasks[dep.key] is dep
                assert ts in dep.dependents, ts

            for ts_wait in ts.waiting_for_data:
                assert self.tasks[ts_wait.key] is ts_wait
                assert ts_wait.state in WAITING_FOR_DATA, ts_wait

        # FIXME https://github.com/dask/distributed/issues/6319
        # assert self.waiting_for_data_count == sum(
        #     bool(ts.waiting_for_data) for ts in self.tasks.values()
        # )

        for worker, keys in self.has_what.items():
            assert worker != self.address
            for k in keys:
                assert k in self.tasks, self.story(k)
                assert worker in self.tasks[k].who_has

        for worker, tss in self.data_needed.items():
            for ts in tss:
                assert ts.state == "fetch"
                assert worker in ts.who_has

        # FIXME https://github.com/dask/distributed/issues/6689
        # for ts in self.executing:
        #     assert ts.state == "executing" or (
        #         ts.state in ("cancelled", "resumed") and ts._previous == "executing"
        #     ), self.story(ts)
        # for ts in self.long_running:
        #     assert ts.state == "long-running" or (
        #         ts.state in ("cancelled", "resumed") and ts._previous == "long-running"
        #     ), self.story(ts)

        # Test that there aren't multiple TaskState objects with the same key in any
        # Set[TaskState]. See note in TaskState.__hash__.
        for ts in chain(
            *self.data_needed.values(),
            self.missing_dep_flight,
            self.in_flight_tasks,
            self.executing,
            self.long_running,
        ):
            assert self.tasks[ts.key] is ts

        for ts in self.tasks.values():
            self.validate_task(ts)

        if self.transition_counter_max:
            assert self.transition_counter < self.transition_counter_max

        # Test that there aren't multiple TaskState objects with the same key in data_needed
        for tss in self.data_needed.values():
            assert len({ts.key for ts in tss}) == len(tss)


class BaseWorker(abc.ABC):
    """Wrapper around the :class:`WorkerState` that implements instructions handling.
    This is an abstract class with several ``@abc.abstractmethod`` methods, to be
    subclassed by :class:`~distributed.worker.Worker` and by unit test mock-ups.
    """

    state: WorkerState
    _async_instructions: set[asyncio.Task]

    def __init__(self, state: WorkerState):
        self.state = state
        self._async_instructions = set()

    def _handle_stimulus_from_task(
        self, task: asyncio.Task[StateMachineEvent | None]
    ) -> None:
        """An asynchronous instruction just completed; process the returned stimulus."""
        self._async_instructions.remove(task)
        try:
            # This *should* never raise any other exceptions
            stim = task.result()
        except asyncio.CancelledError:
            return
        if stim:
            self.handle_stimulus(stim)

    def handle_stimulus(self, *stims: StateMachineEvent) -> None:
        """Forward one or more external stimuli to :meth:`WorkerState.handle_stimulus`
        and process the returned instructions, invoking the relevant Worker callbacks
        (``@abc.abstractmethod`` methods below).

        Spawn asyncio tasks for all asynchronous instructions and start tracking them.

        See also
        --------
        WorkerState.handle_stimulus
        """
        instructions = self.state.handle_stimulus(*stims)

        for inst in instructions:
            task: asyncio.Task | None = None

            if isinstance(inst, SendMessageToScheduler):
                self.batched_send(inst.to_dict())

            elif isinstance(inst, GatherDep):
                assert inst.to_gather
                keys_str = ", ".join(peekn(27, inst.to_gather)[0])
                if len(keys_str) > 80:
                    keys_str = keys_str[:77] + "..."
                task = asyncio.create_task(
                    self.gather_dep(
                        inst.worker,
                        inst.to_gather,
                        total_nbytes=inst.total_nbytes,
                        stimulus_id=inst.stimulus_id,
                    ),
                    name=f"gather_dep({inst.worker}, {{{keys_str}}})",
                )

            elif isinstance(inst, Execute):
                task = asyncio.create_task(
                    self.execute(inst.key, stimulus_id=inst.stimulus_id),
                    name=f"execute({inst.key})",
                )

            elif isinstance(inst, RetryBusyWorkerLater):
                task = asyncio.create_task(
                    self.retry_busy_worker_later(inst.worker),
                    name=f"retry_busy_worker_later({inst.worker})",
                )

            else:
                raise TypeError(inst)  # pragma: nocover

            if task is not None:
                self._async_instructions.add(task)
                task.add_done_callback(self._handle_stimulus_from_task)

    async def close(self, timeout: float = 30) -> None:
        """Cancel all asynchronous instructions"""
        if not self._async_instructions:
            return
        for task in self._async_instructions:
            task.cancel()
        # async tasks can handle cancellation and could take an arbitrary amount
        # of time to terminate
        _, pending = await asyncio.wait(self._async_instructions, timeout=timeout)
        for task in pending:
            logger.error(
                f"Failed to cancel asyncio task after {timeout} seconds: {task}"
            )

    @abc.abstractmethod
    def batched_send(self, msg: dict[str, Any]) -> None:
        """Send a fire-and-forget message to the scheduler through bulk comms.

        Parameters
        ----------
        msg: dict
            msgpack-serializable message to send to the scheduler.
            Must have a 'op' key which is registered in Scheduler.stream_handlers.
        """
        ...

    @abc.abstractmethod
    async def gather_dep(
        self,
        worker: str,
        to_gather: Collection[str],
        total_nbytes: int,
        *,
        stimulus_id: str,
    ) -> StateMachineEvent | None:
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
        ...

    @abc.abstractmethod
    async def execute(self, key: str, *, stimulus_id: str) -> StateMachineEvent | None:
        """Execute a task"""
        ...

    @abc.abstractmethod
    async def retry_busy_worker_later(self, worker: str) -> StateMachineEvent | None:
        """Wait some time, then take a peer worker out of busy state"""
        ...


class DeprecatedWorkerStateAttribute:
    name: str
    target: str | None

    def __init__(self, target: str | None = None):
        self.target = target

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def _warn_deprecated(self) -> None:
        warnings.warn(
            f"The `Worker.{self.name}` attribute has been moved to "
            f"`Worker.state.{self.target or self.name}`",
            FutureWarning,
        )

    def __get__(self, instance: Worker | None, owner: type[Worker]) -> Any:
        if instance is None:
            # This is triggered by Sphinx
            return None  # pragma: nocover
        self._warn_deprecated()
        return getattr(instance.state, self.target or self.name)

    def __set__(self, instance: Worker, value: Any) -> None:
        self._warn_deprecated()
        setattr(instance.state, self.target or self.name, value)
