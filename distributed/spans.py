from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import dask.config

from distributed.metrics import time

if TYPE_CHECKING:
    from distributed import Scheduler
    from distributed.scheduler import TaskGroup, TaskState, TaskStateState


@contextmanager
def span(*tags: str) -> Iterator[None]:
    """Tag group of tasks to be part of a certain group, called a span.

    This context manager can be nested, thus creating sub-spans.
    Every cluster defines a global "default" span when no span has been defined by the client.

    Examples
    --------
    >>> import dask.array as da
    >>> import distributed
    >>> client = distributed.Client()
    >>> with span("my_workflow"):
    ...     with span("phase 1"):
    ...         a = da.random.random(10)
    ...         b = a + 1
    ...     with span("phase 2"):
    ...         c = b * 2
    ... d = c.sum()
    >>> d.compute()

    In the above example,
    - Tasks of collections a and b will be annotated on the scheduler and workers with
      ``{'span': ('my_workflow', 'phase 1')}``
    - Tasks of collection c (that aren't already part of a or b) will be annotated with
      ``{'span': ('my_workflow', 'phase 2')}``
    - Tasks of collection d (that aren't already part of a, b, or c) will *not* be
      annotated but will nonetheless be attached to span ``('default', )``

    You may also set more than one tag at once; e.g.
    >>> with span("workflow1", "version1"):
    ...     ...


    Note
    ----
    Spans are based on annotations, and just like annotations they can be lost during
    optimization. Set config ``optimizatione.fuse.active: false`` to prevent this issue.
    """
    prev_id = dask.config.get("annotations.span", ())
    with dask.config.set({"annotations.span": prev_id + tags}):
        yield


class Span:
    #: (<tag>, <tag>, ...)
    #: Matches ``TaskState.annotations["span"]``, both on the scheduler and the worker,
    #: as well as ``TaskGroup.span``.
    #: Tasks with no 'span' annotation will be attached to Span ``("default", )``.
    id: tuple[str, ...]

    #: Direct children of this span tree
    #: Note: you can get the parent through
    #: ``distributed.extensions["spans"].spans[self.id[:-1]]``
    children: set[Span]

    #: Task groups *directly* belonging to this span.
    #:
    #: See also
    #: --------
    #  traverse_groups
    #:
    #: Notes
    #: -----
    #: TaskGroups are forgotten when the last task is forgotten. If a user calls
    #: compute() twice on the same collection, you'll have more than one group with the
    #: same tg.key in this set! For the same reason, while the same TaskGroup object is
    #: guaranteed to be attached to exactly one Span, you may have different TaskGroups
    #: with the same key attached to different Spans.
    groups: set[TaskGroup]

    #: Time when the span first appeared on the scheduler.
    #: The same property on parent spans is always less than or equal to this.
    #:
    #: See also
    #: --------
    #: start
    #: stop
    enqueued: float

    __slots__ = tuple(__annotations__)

    def __init__(self, span_id: tuple[str, ...], enqueued: float):
        self.id = span_id
        self.enqueued = enqueued
        self.children = set()
        self.groups = set()

    def __repr__(self) -> str:
        return f"Span{self.id}"

    def traverse_spans(self) -> Iterator[Span]:
        """Top-down recursion of all spans belonging to this span tree, including self"""
        yield self
        for child in self.children:
            yield from child.traverse_spans()

    def traverse_groups(self) -> Iterator[TaskGroup]:
        """All TaskGroups belonging to this span tree"""
        for span in self.traverse_spans():
            yield from span.groups

    @property
    def start(self) -> float:
        """Earliest time when a task belonging to this span tree started computing;
        0 if no task has *finished* computing yet.

        Note
        ----
        This is not updated until at least one task has *finished* computing.
        It could move backwards as tasks complete.

        See also
        --------
        enqueued
        stop
        distributed.scheduler.TaskGroup.start
        """
        return min(
            (tg.start for tg in self.traverse_groups() if tg.start != 0.0),
            default=0.0,
        )

    @property
    def stop(self) -> float:
        """Latest time when a task belonging to this span tree finished computing;
        0 if no task has finished computing yet.

        See also
        --------
        enqueued
        start
        distributed.scheduler.TaskGroup.stop
        """
        return max(tg.stop for tg in self.traverse_groups())

    @property
    def states(self) -> defaultdict[TaskStateState, int]:
        """The number of tasks currently in each state in this span tree;
        e.g. ``{"memory": 10, "processing": 3, "released": 4, ...}``.

        See also
        --------
        distributed.scheduler.TaskGroup.states
        """
        out: defaultdict[TaskStateState, int] = defaultdict(int)
        for tg in self.traverse_groups():
            for state, cnt in tg.states.items():
                out[state] += cnt
        return out

    @property
    def all_durations(self) -> defaultdict[str, float]:
        """Cumulative duration of all completed actions in this span tree, by action

        See also
        --------
        duration
        distributed.scheduler.TaskGroup.all_durations
        """
        out: defaultdict[str, float] = defaultdict(float)
        for tg in self.traverse_groups():
            for action, nsec in tg.all_durations.items():
                out[action] += nsec
        return out

    @property
    def duration(self) -> float:
        """The total amount of time spent on all tasks in this span tree

        See also
        --------
        all_durations
        distributed.scheduler.TaskGroup.duration
        """
        return sum(tg.duration for tg in self.traverse_groups())

    @property
    def nbytes_total(self) -> int:
        """The total number of bytes that this span tree has produced

        See also
        --------
        distributed.scheduler.TaskGroup.nbytes_total
        """
        return sum(tg.nbytes_total for tg in self.traverse_groups())


class SpansExtension:
    """Scheduler extension for spans support"""

    #: All Span objects by span_id
    spans: dict[tuple[str, ...], Span]

    #: Only the spans that don't have any parents {client_id: Span}.
    #: This is a convenience helper structure to speed up searches.
    root_spans: dict[str, Span]

    #: All spans, keyed by the individual tags that make up their span_id.
    #: This is a convenience helper structure to speed up searches.
    spans_search_by_tag: defaultdict[str, set[Span]]

    def __init__(self, scheduler: Scheduler):
        self.spans = {}
        self.root_spans = {}
        self.spans_search_by_tag = defaultdict(set)

    def new_tasks(self, tss: Iterable[TaskState]) -> dict[str, tuple[str, ...]]:
        """Acknowledge the creation of new tasks on the scheduler.
        Attach tasks to either the desired span or to ("default", ).
        Update TaskState.annotations["span"] and TaskGroup.span.

        Returns
        -------
        {task key: span id}, only for tasks that explicitly define a span
        """
        out = {}
        for ts in tss:
            # You may have different tasks belonging to the same TaskGroup but to
            # different spans. If that happens, arbitrarily force everything onto the
            # span of the earliest encountered TaskGroup.
            tg = ts.group
            if tg.span:
                span_id = tg.span
            else:
                span_id = ts.annotations.get("span", ("default",))
                assert isinstance(span_id, tuple)
                tg.span = span_id
                span = self._ensure_span(span_id)
                span.groups.add(tg)

            # Override ts.annotations["span"] with span_id from task group
            if span_id == ("default",):
                ts.annotations.pop("span", None)
            else:
                ts.annotations["span"] = out[ts.key] = span_id

        return out

    def _ensure_span(self, span_id: tuple[str, ...], enqueued: float = 0.0) -> Span:
        """Create Span if it doesn't exist and return it"""
        try:
            return self.spans[span_id]
        except KeyError:
            pass

        # When recursively creating parent spans, make sure that parents are not newer
        # than the children
        enqueued = enqueued or time()

        span = self.spans[span_id] = Span(span_id, enqueued)
        for tag in span_id:
            self.spans_search_by_tag[tag].add(span)
        if len(span_id) > 1:
            parent = self._ensure_span(span_id[:-1], enqueued)
            parent.children.add(span)
        else:
            self.root_spans[span_id[0]] = span

        return span
