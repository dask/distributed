from __future__ import annotations

import dataclasses
import sys
import threading
from collections.abc import Iterator, Sequence
from time import sleep

import pytest
from tlz import first

from distributed.compatibility import WINDOWS
from distributed.metrics import time
from distributed.profile import (
    call_stack,
    create,
    identifier,
    info_frame,
    ll_get_stack,
    llprocess,
    lock,
    merge,
    plot_data,
    process,
    watch,
)


def test_basic():
    def test_g():
        sleep(0.01)

    def test_h():
        sleep(0.02)

    def test_f():
        for _ in range(100):
            test_g()
            test_h()

    thread = threading.Thread(target=test_f)
    thread.daemon = True
    thread.start()

    state = create()

    for _ in range(100):
        sleep(0.02)
        frame = sys._current_frames()[thread.ident]
        process(frame, None, state)

    assert state["count"] == 100
    d = state
    while len(d["children"]) == 1:
        d = first(d["children"].values())

    assert d["count"] == 100
    assert "test_f" in str(d["description"])
    g = [c for c in d["children"].values() if "test_g" in str(c["description"])][0]
    h = [c for c in d["children"].values() if "test_h" in str(c["description"])][0]

    assert g["count"] < h["count"]
    assert 95 < g["count"] + h["count"] <= 100

    pd = plot_data(state)
    assert len(set(map(len, pd.values()))) == 1  # all same length
    assert len(set(pd["color"])) > 1  # different colors


@pytest.mark.skipif(
    WINDOWS, reason="no low-level profiler support for Windows available"
)
def test_basic_low_level():
    pytest.importorskip("stacktrace")

    state = create()

    for _ in range(100):
        sleep(0.02)
        frame = sys._current_frames()[threading.get_ident()]
        llframes = {threading.get_ident(): ll_get_stack(threading.get_ident())}
        for f in llframes.values():
            if f is not None:
                llprocess(f, None, state)

    assert state["count"] == 100
    children = state.get("children")
    assert children
    expected = "<low-level>"
    for k, v in zip(children.keys(), children.values()):
        desc = v.get("description")
        assert desc
        filename = desc.get("filename")
        assert expected in k and filename == expected


def test_merge():
    a1 = {
        "count": 5,
        "identifier": "root",
        "description": "a",
        "children": {
            "b": {
                "count": 3,
                "description": "b-func",
                "identifier": "b",
                "children": {},
            },
            "c": {
                "count": 2,
                "description": "c-func",
                "identifier": "c",
                "children": {},
            },
        },
    }

    a2 = {
        "count": 4,
        "description": "a",
        "identifier": "root",
        "children": {
            "d": {
                "count": 2,
                "description": "d-func",
                "children": {},
                "identifier": "d",
            },
            "c": {
                "count": 2,
                "description": "c-func",
                "children": {},
                "identifier": "c",
            },
        },
    }

    expected = {
        "count": 9,
        "identifier": "root",
        "description": "a",
        "children": {
            "b": {
                "count": 3,
                "description": "b-func",
                "identifier": "b",
                "children": {},
            },
            "d": {
                "count": 2,
                "description": "d-func",
                "identifier": "d",
                "children": {},
            },
            "c": {
                "count": 4,
                "description": "c-func",
                "identifier": "c",
                "children": {},
            },
        },
    }

    assert merge(a1, a2) == expected


def test_merge_empty():
    assert merge() == create()
    assert merge(create()) == create()
    assert merge(create(), create()) == create()


def test_call_stack():
    frame = sys._current_frames()[threading.get_ident()]
    L = call_stack(frame)
    assert isinstance(L, list)
    assert all(isinstance(s, str) for s in L)
    assert "test_call_stack" in str(L[-1])


def test_identifier():
    frame = sys._current_frames()[threading.get_ident()]
    assert identifier(frame) == identifier(frame)
    assert identifier(None) == identifier(None)


def test_watch():
    stop_called = threading.Event()
    watch_thread = None
    start = time()

    def stop():
        if not stop_called.is_set():  # Run setup code
            nonlocal watch_thread
            nonlocal start
            watch_thread = threading.current_thread()
            start = time()
            stop_called.set()
        return time() > start + 0.500

    try:
        log = watch(interval="10ms", cycle="50ms", stop=stop)

        stop_called.wait(2)
        sleep(0.5)
        assert 1 < len(log) < 10
    finally:
        stop_called.wait()
        watch_thread.join()


def test_watch_requires_lock_to_run():
    start = time()

    stop_profiling_called = threading.Event()
    profiling_thread = None

    def stop_profiling():
        if not stop_profiling_called.is_set():  # Run setup code
            nonlocal profiling_thread
            nonlocal start
            profiling_thread = threading.current_thread()
            start = time()
            stop_profiling_called.set()
        return time() > start + 0.500

    release_lock = threading.Event()

    def block_lock():
        with lock:
            release_lock.wait()

    start_threads = threading.active_count()

    # Block the lock over the entire duration of watch
    blocking_thread = threading.Thread(target=block_lock, name="Block Lock")
    blocking_thread.daemon = True

    try:
        blocking_thread.start()

        log = watch(interval="10ms", cycle="50ms", stop=stop_profiling)

        start = time()  # wait until thread starts up
        while threading.active_count() < start_threads + 2:
            assert time() < start + 2
            sleep(0.01)

        sleep(0.5)
        assert len(log) == 0
        release_lock.set()
    finally:
        release_lock.set()
        stop_profiling_called.wait()
        blocking_thread.join()
        profiling_thread.join()


@dataclasses.dataclass(frozen=True)
class FakeCode:
    co_filename: str
    co_name: str
    co_firstlineno: int
    co_lnotab: bytes
    co_lines_seq: Sequence[tuple[int, int, int | None]]
    co_code: bytes

    def co_lines(self) -> Iterator[tuple[int, int, int | None]]:
        yield from self.co_lines_seq


FAKE_CODE = FakeCode(
    co_filename="<stdin>",
    co_name="example",
    co_firstlineno=1,
    # https://github.com/python/cpython/blob/b68431fadb3150134ac6ccbf501cdfeaf4c75678/Objects/lnotab_notes.txt#L84
    # generated from:
    # def example():
    #     for i in range(1):
    #         if i >= 0:
    #             pass
    # example.__code__.co_lnotab
    co_lnotab=b"\x00\x01\x0c\x01\x08\x01\x04\xfe",
    # generated with list(example.__code__.co_lines())
    co_lines_seq=[
        (0, 12, 2),
        (12, 20, 3),
        (20, 22, 4),
        (22, 24, None),
        (24, 28, 2),
    ],
    # used in dis.findlinestarts as bytecode_len = len(code.co_code)
    # https://github.com/python/cpython/blob/6f345d363308e3e6ecf0ad518ea0fcc30afde2a8/Lib/dis.py#L457
    co_code=bytes(28),
)


@dataclasses.dataclass(frozen=True)
class FakeFrame:
    f_lasti: int
    f_code: FakeCode
    f_lineno: int | None = None
    f_back: FakeFrame | None = None
    f_globals: dict[str, object] = dataclasses.field(default_factory=dict)


@pytest.mark.parametrize(
    "f_lasti,f_lineno",
    [
        (-1, 1),
        (0, 2),
        (1, 2),
        (11, 2),
        (12, 3),
        (21, 4),
        (22, 4),
        (23, 4),
        (24, 2),
        (25, 2),
        (26, 2),
        (27, 2),
        (100, 2),
    ],
)
def test_info_frame_f_lineno(f_lasti: int, f_lineno: int) -> None:
    assert info_frame(FakeFrame(f_lasti=f_lasti, f_code=FAKE_CODE)) == {  # type: ignore
        "filename": "<stdin>",
        "name": "example",
        "line_number": f_lineno,
        "line": "",
    }


@pytest.mark.parametrize(
    "f_lasti,f_lineno",
    [
        (-1, 1),
        (0, 2),
        (1, 2),
        (11, 2),
        (12, 3),
        (21, 4),
        (22, 4),
        (23, 4),
        (24, 2),
        (25, 2),
        (26, 2),
        (27, 2),
        (100, 2),
    ],
)
def test_call_stack_f_lineno(f_lasti: int, f_lineno: int) -> None:
    assert call_stack(FakeFrame(f_lasti=f_lasti, f_code=FAKE_CODE)) == [  # type: ignore
        f'  File "<stdin>", line {f_lineno}, in example\n\t'
    ]


def test_stack_overflow():
    old = sys.getrecursionlimit()
    sys.setrecursionlimit(300)
    try:
        state = create()
        frame = None

        def f(i):
            if i == 0:
                nonlocal frame
                frame = sys._current_frames()[threading.get_ident()]
                return
            else:
                return f(i - 1)

        f(sys.getrecursionlimit() - 100)
        process(frame, None, state)
        assert state["children"]
        assert state["count"]
        assert merge(state, state, state)

    finally:
        sys.setrecursionlimit(old)
