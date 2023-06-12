from __future__ import annotations

import uuid
import weakref
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import dask.config

from distributed.metrics import time

if TYPE_CHECKING:
    from distributed import Scheduler, Worker
    from distributed.scheduler import TaskGroup, TaskState, TaskStateState, WorkerState


@contextmanager
def span(*tags: str) -> Iterator[str]:
    """Tag group of tasks to be part of a certain group, called a span.

    This context manager can be nested, thus creating sub-spans. If you close and
    re-open a span context manager with the same tag, you'll end up with two separate
    spans.

    Every cluster defines a global "default" span when no span has been defined by the
    client; the default span is automatically closed and reopened when all tasks
    associated to it have been completed; in other words the cluster is idle save for
    tasks that are explicitly annotated by a span. Note that, in some edge cases, you
    may end up with overlapping default spans, e.g. if a worker crashes and all unique
    tasks that were in memory on it need to be recomputed.

    Examples
    --------
    >>> import dask.array as da
    >>> import distributed
    >>> client = distributed.Client()
    >>> with span("my workflow"):
    ...     with span("phase 1"):
    ...         a = da.random.random(10)
    ...         b = a + 1
    ...     with span("phase 2"):
    ...         c = b * 2
    ... d = c.sum()
    >>> d.compute()

    In the above example,
    - Tasks of collections a and b are annotated to belong to span
      ``('my workflow', 'phase 1')``, 'ids': (<id0>, <id1>)}``;
    - Tasks of collection c (that aren't already part of a or b) are annotated to belong
      to span ``('my workflow', 'phase 2')``;
    - Tasks of collection d (that aren't already part of a, b, or c) are *not*
      annotated but will nonetheless be attached to span ``('default', )``.

    You may also set more than one tag at once; e.g.
    >>> with span("workflow1", "version1"):
    ...     ...

    Finally, you may capture the ID of a span on the client to match it with the
    :class:`Span` objects the scheduler:
    >>> cluster = distributed.LocalCluster()
    >>> client = distributed.Client(cluster)
    >>> with span("my workflow") as span_id:
    ...     client.submit(lambda: "Hello world!").result()
    >>> span = client.cluster.scheduler.extensions["spans"].spans[span_id]

    Notes
    -----
    Spans are based on annotations, and just like annotations they can be lost during
    optimization. Set config ``optimization.fuse.active: false`` to prevent this issue.
    """
    if not tags:
        raise ValueError("Must specify at least one span tag")

    prev_tags = dask.config.get("annotations.span.name", ())
    # You must specify the full history of IDs, not just the parent, because
    # otherwise you would not be able to uniquely identify grandparents when
    # they have no tasks of their own.
    prev_ids = dask.config.get("annotations.span.ids", ())
    ids = tuple(str(uuid.uuid4()) for _ in tags)
    with dask.annotate(span={"name": prev_tags + tags, "ids": prev_ids + ids}):
        yield ids[-1]


class Span:
    #: (<tag>, <tag>, ...)
    #: Matches ``TaskState.annotations["span"]["name"]``, both on the scheduler and the
    #: worker.
    name: tuple[str, ...]

    #: <uuid>
    #: Taken from ``TaskState.annotations["span"]["id"][-1]``.
    #: Matches ``distributed.scheduler.TaskState.group.span_id``
    #: and ``distributed.worker_state_machine.TaskState.span_id``.
    id: str

    _parent: weakref.ref[Span] | None

    #: Direct children of this span, sorted by creation time
    children: list[Span]

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
    #: same tg.name in this set! For the same reason, while the same TaskGroup object is
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

    _cumulative_worker_metrics: defaultdict[tuple[str, ...], float]

    # Support for weakrefs to a class with __slots__
    __weakref__: Any

    __slots__ = tuple(__annotations__)

    def __init__(self, name: tuple[str, ...], id_: str, parent: Span | None):
        self.name = name
        self.id = id_
        self._parent = weakref.ref(parent) if parent is not None else None
        self.enqueued = time()
        self.children = []
        self.groups = set()
        self._cumulative_worker_metrics = defaultdict(float)

    def __repr__(self) -> str:
        return f"Span<name={self.name}, id={self.id}>"

    @property
    def parent(self) -> Span | None:
        if self._parent:
            out = self._parent()
            assert out
            return out
        return None

    def traverse_spans(self) -> Iterator[Span]:
        """Top-down recursion of all spans belonging to this branch off span tree,
        including self
        """
        yield self
        for child in self.children:
            yield from child.traverse_spans()

    def traverse_groups(self) -> Iterator[TaskGroup]:
        """All TaskGroups belonging to this branch of span tree"""
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
            for state, count in tg.states.items():
                out[state] += count
        return out

    @property
    def done(self) -> bool:
        """Return True if all tasks in this span tree are completed; False otherwise.

        Notes
        -----
        This property may transition from True to False, e.g. when a new sub-span is
        added or when a worker that contained the only replica of a task in memory
        crashes and the task need to be recomputed.

        See also
        --------
        distributed.scheduler.TaskGroup.done
        """
        return all(tg.done for tg in self.traverse_groups())

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

    @property
    def cumulative_worker_metrics(self) -> defaultdict[tuple[str, ...], float]:
        """Replica of Worker.digests_total and Scheduler.cumulative_worker_metrics, but
        only for the metrics that can be attributed to the current span tree.
        The span_id has been removed from the key.

        At the moment of writing, all keys are
        ``("execute", <task prefix>, <activity>, <unit>)``
        but more may be added in the future with a different format; please test for
        ``k[0] == "execute"``.
        """
        out: defaultdict[tuple[str, ...], float] = defaultdict(float)
        for child in self.traverse_spans():
            for k, v in child._cumulative_worker_metrics.items():
                out[k] += v
        return out


class SpansSchedulerExtension:
    """Scheduler extension for spans support"""

    #: All Span objects by id
    spans: dict[str, Span]

    #: Only the spans that don't have any parents, sorted by creation time.
    #: This is a convenience helper structure to speed up searches.
    root_spans: list[Span]

    #: All spans, keyed by their full name and sorted by creation time.
    #: This is a convenience helper structure to speed up searches.
    spans_search_by_name: defaultdict[tuple[str, ...], list[Span]]

    #: All spans, keyed by the individual tags that make up their name and sorted by
    #: creation time.
    #: This is a convenience helper structure to speed up searches.
    spans_search_by_tag: defaultdict[str, list[Span]]

    def __init__(self, scheduler: Scheduler):
        self.spans = {}
        self.root_spans = []
        self.spans_search_by_name = defaultdict(list)
        self.spans_search_by_tag = defaultdict(list)

    def observe_tasks(self, tss: Iterable[TaskState]) -> None:
        """Acknowledge the existence of runnable tasks on the scheduler. These may
        either be new tasks, tasks that were previously unrunnable, or tasks that were
        already fed into this method already.

        Attach newly observed tasks to either the desired span or to ("default", ).
        Update TaskGroup.span_id and wipe TaskState.annotations["span"].
        """
        default_span = None

        for ts in tss:
            # You may have different tasks belonging to the same TaskGroup but to
            # different spans. If that happens, arbitrarily force everything onto the
            # span of the earliest encountered TaskGroup.
            tg = ts.group
            if not tg.span_id:
                ann = ts.annotations.get("span")
                if ann:
                    span = self._ensure_span(ann["name"], ann["ids"])
                else:
                    if not default_span:
                        default_span = self._ensure_default_span()
                    span = default_span

                tg.span_id = span.id
                span.groups.add(tg)

            # The span may be completely different from the one referenced by the
            # annotation, due to the TaskGroup collision issue explained above.
            # Remove the annotation to avoid confusion, and instead rely on
            # distributed.scheduler.TaskState.group.span_id and
            # distributed.worker_state_machine.TaskState.span_id.
            ts.annotations.pop("span", None)

    def _ensure_default_span(self) -> Span:
        """Return the currently active default span, or create one if the previous one
        terminated. In other words, do not reuse the previous default span if all tasks
        that were not explicitly annotated with :func:`spans` on the client side are
        finished.
        """
        defaults = self.spans_search_by_name["default",]
        if defaults and not defaults[-1].done:
            return defaults[-1]
        return self._ensure_span(("default",), (str(uuid.uuid4()),))

    def _ensure_span(self, name: tuple[str, ...], ids: tuple[str, ...]) -> Span:
        """Create Span if it doesn't exist and return it"""
        try:
            return self.spans[ids[-1]]
        except KeyError:
            pass

        assert len(name) == len(ids)
        assert len(name) > 0

        parent = None
        for i in range(1, len(name)):
            parent = self._ensure_span(name[:i], ids[:i])

        span = Span(name=name, id_=ids[-1], parent=parent)
        self.spans[span.id] = span
        self.spans_search_by_name[name].append(span)
        for tag in name:
            self.spans_search_by_tag[tag].append(span)
        if parent:
            parent.children.append(span)
        else:
            self.root_spans.append(span)

        return span

    def heartbeat(
        self, ws: WorkerState, data: dict[str, dict[tuple[str, ...], float]]
    ) -> None:
        """Triggered by SpansWorkerExtension.heartbeat().

        Populate :meth:`Span.cumulative_worker_metrics` with data from the worker.

        See also
        --------
        SpansWorkerExtension.heartbeat
        Span.cumulative_worker_metrics
        """
        for span_id, metrics in data.items():
            span = self.spans[span_id]
            for k, v in metrics.items():
                span._cumulative_worker_metrics[k] += v


class SpansWorkerExtension:
    """Worker extension for spans support"""

    worker: Worker

    def __init__(self, worker: Worker):
        self.worker = worker

    def heartbeat(self) -> dict[str, dict[tuple[str, ...], float]]:
        """Apportion the metrics that do have a span to the Spans on the scheduler

        Returns
        -------
        ``{span_id: {("execute", prefix, activity, unit): value}}``

        See also
        --------
        SpansSchedulerExtension.heartbeat
        Span.cumulative_worker_metrics
        """
        out: defaultdict[str, dict[tuple[str, ...], float]] = defaultdict(dict)
        for k, v in self.worker.digests_total_since_heartbeat.items():
            if isinstance(k, tuple) and k[0] == "execute":
                _, span_id, prefix, activity, unit = k
                assert span_id is not None
                out[span_id]["execute", prefix, activity, unit] = v
        return dict(out)
