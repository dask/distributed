from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import dask.config

if TYPE_CHECKING:
    from distributed import Scheduler
    from distributed.scheduler import TaskState


@contextmanager
def span(*tags: str) -> Iterator[None]:
    """Tag group of tasks to be part of a certain group, called a span.

    This context manager can be nested, thus creating sub-spans.
    Every cluster defines a "default" span when no span has been defined by the client.

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

    __slots__ = tuple(__annotations__)

    def __init__(self, span_id: tuple[str, ...]):
        self.id = span_id
        self.children = set()

    def __repr__(self) -> str:
        return f"Span{self.id}"


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
        Update TaskState.annotations["span"].

        Returns
        -------
        {task key: span id}, only for tasks that explicitly define a span
        """
        out = {}
        for ts in tss:
            span_id = ts.annotations.get("span", ())
            assert isinstance(span_id, tuple)
            if span_id:
                ts.annotations["span"] = out[ts.key] = span_id
            else:
                span_id = ("default",)
            self._ensure_span(span_id)

        return out

    def _ensure_span(self, span_id: tuple[str, ...]) -> Span:
        """Create Span if it doesn't exist and return it"""
        try:
            return self.spans[span_id]
        except KeyError:
            pass

        span = self.spans[span_id] = Span(span_id)
        for tag in span_id:
            self.spans_search_by_tag[tag].add(span)
        if len(span_id) > 1:
            parent = self._ensure_span(span_id[:-1])
            parent.children.add(span)
        else:
            self.root_spans[span_id[0]] = span

        return span
