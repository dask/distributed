from __future__ import annotations

import time

from distributed.span import FlatSpan, Span, current_span, get_span, meter


def make_span(label: str, total_time: float, parent: Span | None = None) -> Span:
    span = Span(label)
    span.start_time = 0
    span.stop_time = total_time
    if parent:
        parent.subspans.append(span)
    return span


def test_span_times_empty():
    root = make_span("root", 5)
    assert root.label == ("root",)
    assert root.total_time == 5
    assert root.other_time == 0
    assert root.own_time == 5
    assert not root.subspans


def test_span_times_subs():
    root = make_span("root", 5)

    sub1 = make_span("sub1", 2, parent=root)
    make_span("subsub1", 1, parent=sub1)

    sub2 = make_span("sub2", 1, parent=root)

    assert root.total_time == 5
    assert root.other_time == 3
    assert root.own_time == 2

    assert sub1.total_time == 2
    assert sub1.other_time == 1
    assert sub1.own_time == 1

    assert sub2.total_time == 1
    assert sub2.other_time == 0
    assert sub2.own_time == 1


def test_get_span():
    assert current_span() is None

    root = get_span("root")

    assert current_span() is None

    with root:
        assert current_span() is root
        sub = get_span("sub")
        assert current_span() is root
        assert root.subspans == [sub]

    assert current_span() is None


def test_meter():
    assert current_span() is None

    with meter("outer") as outer:
        assert current_span() is outer
        time.sleep(0.1)

        with meter("inner") as inner:
            assert current_span() is inner
            time.sleep(0.2)

        assert current_span() is outer
        time.sleep(0.1)

    assert current_span() is None

    assert outer.total_time >= 0.4
    assert inner.total_time >= 0.2
    assert outer.other_time == inner.total_time


def test_flat_empty():
    flat = list(make_span("root", 5).flat())
    assert flat == [FlatSpan(("root",), 5)]


def test_flat_basic():
    root = make_span("root", 10)
    sub1 = make_span("sub1", 5, parent=root)
    make_span("subsub11", 2, parent=sub1)
    make_span("subsub12", 3, parent=sub1)
    sub2 = make_span("sub2", 4, parent=root)
    make_span("subsub21", 2, parent=sub2)

    assert list(root.flat()) == [
        FlatSpan(
            (
                "root",
                "sub1",
                "subsub11",
            ),
            2,
        ),
        FlatSpan(
            (
                "root",
                "sub1",
                "subsub12",
            ),
            3,
        ),
        FlatSpan(("root", "sub1"), 0),
        FlatSpan(
            (
                "root",
                "sub2",
                "subsub21",
            ),
            2,
        ),
        FlatSpan(("root", "sub2"), 2),
        FlatSpan(("root",), 1),
    ]
