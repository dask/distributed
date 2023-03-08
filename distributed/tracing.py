from __future__ import annotations

import contextvars
import time
from collections import Counter
from contextlib import contextmanager
from functools import cached_property
from typing import Callable, Hashable, Iterator, NamedTuple

_current_span: contextvars.ContextVar[Span] = contextvars.ContextVar("current_span")


class Span:
    label: tuple[str, ...]
    metric: Callable[[], float]
    start_time: float | None
    stop_time: float | None
    subspans: list[Span]
    counters: Counter[Hashable]
    # ^ NOTE: `Counter` does support non-int values according to the docs,
    # though type annotations don't indicate this.
    _token: contextvars.Token | None
    _failure: str | None

    def __init__(
        self,
        label: str | tuple[str, ...],
        metric: Callable[[], float] = time.perf_counter,
    ) -> None:
        self.label = (label,) if isinstance(label, str) else label
        self.metric = metric
        self.start_time = None
        self.stop_time = None
        self.subspans = []
        self.counters = Counter()
        self._token = None
        self._failure = None

    @property
    def total_time(self) -> float:
        assert self.start_time is not None
        assert self.stop_time is not None
        return self.stop_time - self.start_time

    @cached_property
    def other_time(self) -> float:
        assert self.start_time is not None
        assert self.stop_time is not None
        return sum(span.total_time for span in self.subspans)

    @property
    def own_time(self) -> float:
        assert self.start_time is not None
        assert self.stop_time is not None
        return self.total_time - self.other_time

    @property
    def done(self) -> bool:
        return self.start_time is not None and self.stop_time is not None

    @property
    def running(self) -> bool:
        return self.start_time is not None and self.stop_time is None

    def start(self) -> None:
        assert self.start_time is None
        assert self.stop_time is None
        self.start_time = self.metric()

    def stop(self) -> None:
        assert self.start_time is not None
        assert self.stop_time is None
        self.stop_time = self.metric()

    def set_failed(self, failure: str) -> None:
        assert not self.done
        assert self._failure is None
        self._failure = failure

    @contextmanager
    def as_current(self) -> Iterator[None]:
        self._set_current()
        try:
            yield
        finally:
            self._unset_current()

    def __enter__(self) -> Span:
        self.start()
        self._set_current()
        return self

    def __exit__(self, *exc: object) -> None:
        self._unset_current()
        self.stop()

    def _set_current(self) -> None:
        assert not self._token
        self._token = _current_span.set(self)

    def _unset_current(self) -> None:
        assert self._token
        _current_span.reset(self._token)
        self._token = None

    def _subspan(
        self,
        label: str | tuple[str, ...],
        metric: Callable[[], float] = time.perf_counter,
    ) -> Span:
        assert (
            self.start_time is not None
        ), "Cannot create sub-span for a span that has not started"
        assert (
            self.stop_time is None
        ), "Cannot create sub-span for a span that has already stopped"
        # TODO allow different metrics, or always use `self.metric`?
        span = Span(label, metric)
        self.subspans.append(span)
        return span

    def flat(self, *, prefix: tuple[str, ...] = ()) -> Iterator[FlatSpan]:
        """
        Iterator over the flattened span tree.

        For example, turns this::

            |---- run ---------------------------------------|
            |---- execute ------||-- deserialize --|
            |---- fetch --|

        into this::

            |---- fetch --||exec||-- deserialize --||--run---|

        Specifically, you'd get spans with these labels and `total_time` values::

            [
                FlatSpan(("run", "execute", "fetch"), 15),
                FlatSpan(("run", "execute", "own-time"), 6),
                FlatSpan(("run", "deserialize"), 19),
                FlatSpan(("run", "own-time"), 10),
            ]
        """
        assert self.start_time is not None
        assert self.stop_time is not None

        label = prefix + self.label
        for sub in self.subspans:
            yield from sub.flat(prefix=label)

        yield FlatSpan(
            label + (("own-time",) if self.subspans else ()),
            self.own_time,
            self.counters,
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<"
            f"{self.label!r}, "
            f"total_time={self.total_time if self.done else '...'}, "
            f"start_time={self.start_time}, "
            f"stop_time={self.stop_time}, "
            ">"
        )


class FlatSpan(NamedTuple):
    label: tuple[str, ...]
    own_time: float
    counters: Counter[Hashable]


def current_span() -> Span | None:
    return _current_span.get(None)


def get_span(
    label: str | tuple[str, ...],
    metric: Callable[[], float] = time.perf_counter,
) -> Span:
    "Get a span, nested under any current span"
    try:
        parent = _current_span.get()
    except LookupError:
        span = Span(label, metric)
    else:
        # assert metric is parent.metric, (metric, parent.metric)
        span = parent._subspan(label)

    return span


@contextmanager
def trace(
    label: str | tuple[str, ...],
    metric: Callable[[], float] = time.perf_counter,
) -> Iterator[Span]:
    "Contextmanager or decorator to create, start, and stop a nested span"
    with get_span(label, metric) as span:
        yield span
