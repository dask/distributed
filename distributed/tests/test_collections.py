from __future__ import annotations

import heapq
import operator
import pickle
import random
from collections.abc import Mapping

import pytest

from distributed.collections import LRU, HeapSet, sum_mappings


def test_lru():
    l = LRU(maxsize=3)
    l["a"] = 1
    l["b"] = 2
    l["c"] = 3
    assert list(l.keys()) == ["a", "b", "c"]

    # Use "a" and ensure it becomes the most recently used item
    l["a"]
    assert list(l.keys()) == ["b", "c", "a"]

    # Ensure maxsize is respected
    l["d"] = 4
    assert len(l) == 3
    assert list(l.keys()) == ["c", "a", "d"]


class C:
    def __init__(self, k, i):
        self.k = k
        self.i = i

    def __hash__(self):
        return hash(self.k)

    def __eq__(self, other):
        return isinstance(other, C) and other.k == self.k

    def __repr__(self):
        return f"C({self.k}, {self.i})"


def test_heapset():
    heap = HeapSet(key=operator.attrgetter("i"))

    cx = C("x", 2)
    cy = C("y", 1)
    cz = C("z", 3)
    cw = C("w", 4)
    heap.add(cx)
    heap.add(cy)
    heap.add(cz)
    heap.add(cw)
    heap.add(C("x", 0))  # Ignored; x already in heap
    assert len(heap) == 4
    assert repr(heap) == "<HeapSet: 4 items>"

    assert cx in heap
    assert cy in heap
    assert cz in heap
    assert cw in heap

    heap_sorted = heap.sorted()
    # iteration does not empty heap
    assert len(heap) == 4
    assert next(heap_sorted) is cy
    assert next(heap_sorted) is cx
    assert next(heap_sorted) is cz
    assert next(heap_sorted) is cw
    with pytest.raises(StopIteration):
        next(heap_sorted)

    assert set(heap) == {cx, cy, cz, cw}

    assert heap.peek() is cy
    assert heap.pop() is cy
    assert cx in heap
    assert cy not in heap
    assert cz in heap
    assert cw in heap

    assert heap.peek() is cx
    assert heap.pop() is cx
    assert heap.pop() is cz
    assert heap.pop() is cw
    assert not heap
    with pytest.raises(KeyError):
        heap.pop()
    with pytest.raises(KeyError):
        heap.peek()

    # Test out-of-order discard
    heap.add(cx)
    heap.add(cy)
    heap.add(cz)
    heap.add(cw)
    assert heap.peek() is cy

    heap.remove(cy)
    assert cy not in heap
    with pytest.raises(KeyError):
        heap.remove(cy)

    heap.discard(cw)
    assert cw not in heap
    heap.discard(cw)

    assert len(heap) == 2
    assert list(heap.sorted()) == [cx, cz]
    # cy is at the top of heap._heap, but is skipped
    assert heap.peek() is cx
    assert heap.pop() is cx
    assert heap.peek() is cz
    assert heap.pop() is cz
    # heap._heap is not empty
    assert not heap
    with pytest.raises(KeyError):
        heap.peek()
    with pytest.raises(KeyError):
        heap.pop()
    assert list(heap.sorted()) == []

    # Test clear()
    heap.add(cx)
    heap.clear()
    assert not heap
    heap.add(cx)
    assert cx in heap
    # Test discard last element
    heap.discard(cx)
    assert not heap
    heap.add(cx)
    assert cx in heap

    # Test peekn()
    heap.add(cy)
    heap.add(cw)
    heap.add(cz)
    heap.add(cx)
    assert list(heap.peekn(3)) == [cy, cx, cz]
    heap.remove(cz)
    assert list(heap.peekn(10)) == [cy, cx, cw]
    assert list(heap.peekn(0)) == []
    assert list(heap.peekn(-1)) == []
    heap.remove(cy)
    assert list(heap.peekn(1)) == [cx]
    heap.remove(cw)
    assert list(heap.peekn(1)) == [cx]
    heap.remove(cx)
    assert list(heap.peekn(-1)) == []
    assert list(heap.peekn(0)) == []
    assert list(heap.peekn(1)) == []
    assert list(heap.peekn(2)) == []

    # Test resilience to failure in key()
    heap.add(cx)
    bad_key = C("bad_key", 0)
    del bad_key.i
    with pytest.raises(AttributeError):
        heap.add(bad_key)
    assert len(heap) == 1
    assert set(heap) == {cx}

    # Test resilience to failure in weakref.ref()
    class D:
        __slots__ = ("i",)

        def __init__(self, i):
            self.i = i

    with pytest.raises(TypeError):
        heap.add(D("bad_weakref", 2))
    assert len(heap) == 1
    assert set(heap) == {cx}

    # Test resilience to key() returning non-sortable output
    with pytest.raises(TypeError):
        heap.add(C("unsortable_key", None))
    assert len(heap) == 1
    assert set(heap) == {cx}


def assert_heap_sorted(heap: HeapSet) -> None:
    assert heap._sorted
    assert heap._heap == sorted(heap._heap)


def test_heapset_sorted_flag_left():
    heap = HeapSet(key=operator.attrgetter("i"))
    assert heap._sorted
    c1 = C("1", 1)
    c2 = C("2", 2)
    c3 = C("3", 3)
    c4 = C("4", 4)

    heap.add(c4)
    assert not heap._sorted
    heap.add(c3)
    heap.add(c2)
    heap.add(c1)

    list(heap.sorted())
    assert_heap_sorted(heap)

    # `peek` maintains sort if first element is not discarded
    assert heap.peek() is c1
    assert_heap_sorted(heap)

    # `pop` always de-sorts
    assert heap.pop() is c1
    assert not heap._sorted

    list(heap.sorted())

    # discard first element
    heap.discard(c2)
    assert heap.peek() is c3
    assert not heap._sorted

    # popping the last element resets the sorted flag
    assert heap.pop() is c3
    assert heap.pop() is c4
    assert not heap
    assert_heap_sorted(heap)

    # discarding`` the last element resets the sorted flag
    heap.add(c1)
    heap.add(c2)
    assert not heap._sorted
    heap.discard(c1)
    assert not heap._sorted
    heap.discard(c2)
    assert not heap
    assert_heap_sorted(heap)


def test_heapset_sorted_flag_right():
    "Verify right operations don't affect sortedness"
    heap = HeapSet(key=operator.attrgetter("i"))
    c1 = C("1", 1)
    c2 = C("2", 2)
    c3 = C("3", 3)

    heap.add(c2)
    heap.add(c3)
    heap.add(c1)

    assert not heap._sorted
    list(heap.sorted())
    assert_heap_sorted(heap)

    assert heap.peekright() is c3
    assert_heap_sorted(heap)
    assert heap.popright() is c3
    assert_heap_sorted(heap)
    assert heap.popright() is c2
    assert_heap_sorted(heap)

    heap.add(c2)
    assert not heap._sorted
    assert heap.popright() is c2
    assert not heap._sorted
    assert heap.popright() is c1
    assert not heap
    assert_heap_sorted(heap)


@pytest.mark.parametrize("peek", [False, True])
def test_heapset_popright(peek):
    heap = HeapSet(key=operator.attrgetter("i"))
    with pytest.raises(KeyError):
        heap.peekright()
    with pytest.raises(KeyError):
        heap.popright()

    # The heap contains broken weakrefs
    for i in range(200):
        c = C(f"y{i}", random.random())
        heap.add(c)
        if random.random() > 0.7:
            heap.remove(c)

    c0 = heap.peek()
    while len(heap) > 1:
        # These two code paths determine which of the two methods deals with the
        # removal of broken weakrefs
        if peek:
            c1 = heap.peekright()
            assert c1.i >= c0.i
            assert heap.popright() is c1
        else:
            c1 = heap.popright()
            assert c1.i >= c0.i

        # Test that the heap hasn't been corrupted
        h2 = heap._heap[:]
        heapq.heapify(h2)
        assert h2 == heap._heap

    assert heap.peekright() is c0
    assert heap.popright() is c0
    assert not heap


def test_heapset_pickle():
    """Test pickle roundtrip for a HeapSet.

    Note
    ----
    To make this test work with plain pickle and not need cloudpickle, we had to avoid
    lambdas and local classes in our test. Here we're testing that HeapSet doesn't add
    lambdas etc. of its own.
    """
    heap = HeapSet(key=operator.attrgetter("i"))

    # The heap contains broken weakrefs
    for i in range(200):
        c = C(f"y{i}", random.random())
        heap.add(c)
        if random.random() > 0.7:
            heap.remove(c)

    list(heap.sorted())  # trigger sort
    assert heap._sorted
    heap2 = pickle.loads(pickle.dumps(heap))
    assert len(heap) == len(heap2)
    assert not heap2._sorted  # re-heapification may have broken the sort
    # Test that the heap has been re-heapified upon unpickle
    assert len(heap2._heap) < len(heap._heap)
    while heap:
        assert heap.pop() == heap2.pop()


def test_heapset_sort_duplicate():
    """See https://github.com/dask/distributed/issues/6951"""
    heap = HeapSet(key=operator.attrgetter("i"))
    c1 = C("x", 1)
    c2 = C("2", 2)

    heap.add(c1)
    heap.add(c2)
    heap.discard(c1)
    heap.add(c1)

    assert list(heap.sorted()) == [c1, c2]


class ReadOnlyMapping(Mapping):
    def __init__(self, d: Mapping):
        self.d = d

    def __getitem__(self, item):
        return self.d[item]

    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)


def test_sum_mappings():
    a = {"x": 1, "y": 1.2, "z": [3, 4]}
    b = ReadOnlyMapping({"w": 7, "y": 3.4, "z": [5, 6]})
    c = iter([("y", 0.2), ("y", -0.5)])
    actual = sum_mappings(iter([a, b, c]))
    assert isinstance(actual, dict)
    assert actual == {"x": 1, "y": 4.3, "z": [3, 4, 5, 6], "w": 7}
    assert isinstance(actual["x"], int)  # Not 1.0
    assert list(actual) == ["x", "y", "z", "w"]

    d = {"x0": 1, "x1": 2, "y0": 4}
    actual = sum_mappings([((k[0], v) for k, v in d.items())])
    assert actual == {"x": 3, "y": 4}
