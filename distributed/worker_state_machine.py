from __future__ import annotations

import sys
from collections.abc import Collection, Container
from copy import copy
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, TypedDict

import dask
from dask.utils import parse_bytes

from distributed.core import ErrorMessage, error_message
from distributed.protocol.serialize import Serialize
from distributed.utils import recursive_to_dict

if TYPE_CHECKING:
    # TODO move to typing and get out of TYPE_CHECKING (requires Python >=3.10)
    from typing_extensions import TypeAlias

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

    def __repr__(self):
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

    def __repr__(self):
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

    # Support for weakrefs to a class with __slots__
    __weakref__: Any = field(init=False)

    def __repr__(self) -> str:
        return f"<TaskState {self.key!r} {self.state}>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskState) or other.key != self.key:
            return False
        # When a task transitions to forgotten and exits Worker.tasks, it should be
        # immediately dereferenced. If the same task is recreated later on on the
        # worker, we should not have to deal with its previous incarnation lingering.
        assert other is self
        return True

    def __hash__(self) -> int:
        return hash(self.key)

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


@dataclass
class EnsureCommunicatingAfterTransitions(Instruction):
    __slots__ = ()


@dataclass
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
    __slots__ = tuple(__annotations__)  # type: ignore

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
    __slots__ = tuple(__annotations__)  # type: ignore

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

    def __new__(cls, *args, **kwargs):
        self = object.__new__(cls)
        self.handled = None
        return self

    def __init_subclass__(cls):
        StateMachineEvent._classes[cls.__name__] = cls

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        """Produce a variant version of self that is small enough to be stored in memory
        in the medium term and contains meaningful information for debugging
        """
        self.handled = handled
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
        info.update({k: getattr(self, k) for k in self.__annotations__})
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
    __slots__ = tuple(__annotations__)  # type: ignore

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
    run_spec: SerializedTask
    resource_restrictions: dict[str, float]
    actor: bool
    annotations: dict
    __slots__ = tuple(__annotations__)  # type: ignore

    def __post_init__(self):
        # Fixes after msgpack decode
        if isinstance(self.priority, list):
            self.priority = tuple(self.priority)

        if isinstance(self.run_spec, dict):
            self.run_spec = SerializedTask(**self.run_spec)
        elif not isinstance(self.run_spec, SerializedTask):
            self.run_spec = SerializedTask(task=self.run_spec)

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = copy(self)
        out.handled = handled
        out.run_spec = SerializedTask(task=None)
        return out

    def _after_from_dict(self) -> None:
        self.run_spec = SerializedTask(task=None)


@dataclass
class ExecuteSuccessEvent(StateMachineEvent):
    key: str
    value: object
    start: float
    stop: float
    nbytes: int
    type: type | None
    __slots__ = tuple(__annotations__)  # type: ignore

    def to_loggable(self, *, handled: float) -> StateMachineEvent:
        out = copy(self)
        out.handled = handled
        out.value = None
        return out

    def _after_from_dict(self) -> None:
        self.value = None
        self.type = None


@dataclass
class ExecuteFailureEvent(StateMachineEvent):
    key: str
    start: float | None
    stop: float | None
    exception: Serialize
    traceback: Serialize | None
    exception_text: str
    traceback_text: str
    __slots__ = tuple(__annotations__)  # type: ignore

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
    __slots__ = ("who_has",)
    who_has: dict[str, Collection[str]]


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
