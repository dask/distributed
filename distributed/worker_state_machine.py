from __future__ import annotations

import heapq
import sys
from collections.abc import Callable, Container, Iterator
from copy import copy
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Collection  # TODO move to collections.abc (requires Python >=3.9)
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


class SerializedTask(NamedTuple):
    function: Callable
    args: tuple
    kwargs: dict[str, Any]
    task: object  # distributed.scheduler.TaskState.run_spec


class StartStop(TypedDict, total=False):
    action: str
    start: float
    stop: float
    source: str  # optional


class InvalidTransition(Exception):
    def __init__(self, key, start, finish, story):
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


class TransitionCounterMaxExceeded(InvalidTransition):
    pass


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


class UniqueTaskHeap(Collection[TaskState]):
    """A heap of TaskState objects ordered by TaskState.priority.
    Ties are broken by string comparison of the key. Keys are guaranteed to be
    unique. Iterating over this object returns the elements in priority order.
    """

    __slots__ = ("_known", "_heap")
    _known: set[str]
    _heap: list[tuple[tuple[int, ...], str, TaskState]]

    def __init__(self):
        self._known = set()
        self._heap = []

    def push(self, ts: TaskState) -> None:
        """Add a new TaskState instance to the heap. If the key is already
        known, no object is added.

        Note: This does not update the priority / heap order in case priority
        changes.
        """
        assert isinstance(ts, TaskState)
        if ts.key not in self._known:
            assert ts.priority
            heapq.heappush(self._heap, (ts.priority, ts.key, ts))
            self._known.add(ts.key)

    def pop(self) -> TaskState:
        """Pop the task with highest priority from the heap."""
        _, key, ts = heapq.heappop(self._heap)
        self._known.remove(key)
        return ts

    def peek(self) -> TaskState:
        """Get the highest priority TaskState without removing it from the heap"""
        return self._heap[0][2]

    def __contains__(self, x: object) -> bool:
        if isinstance(x, TaskState):
            x = x.key
        return x in self._known

    def __iter__(self) -> Iterator[TaskState]:
        return (ts for _, _, ts in sorted(self._heap))

    def __len__(self) -> int:
        return len(self._known)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items>"


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


@dataclass
class ReleaseWorkerDataMsg(SendMessageToScheduler):
    op = "release-worker-data"

    __slots__ = ("key",)
    key: str


@dataclass
class MissingDataMsg(SendMessageToScheduler):
    op = "missing-data"

    __slots__ = ("key", "errant_worker")
    key: str
    errant_worker: str


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
    keys: list[str]


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
class GatherDepDoneEvent(StateMachineEvent):
    """Temporary hack - to be removed"""

    __slots__ = ()


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
        for k, v in recs_i.items():
            if k in recs and recs[k] != v:
                raise ValueError(
                    f"Mismatched recommendations for {k}: {recs[k]} vs. {v}"
                )
            recs[k] = v
        instr += instr_i
    return recs, instr
