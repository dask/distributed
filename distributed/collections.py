from __future__ import annotations

import heapq
import itertools
import weakref
from collections import OrderedDict, UserDict
from collections.abc import Callable, Hashable, Iterator
from typing import (  # TODO move to collections.abc (requires Python >=3.9)
    AbstractSet,
    Any,
    Iterable,
    MutableSet,
    TypeVar,
    cast,
)

T = TypeVar("T", bound=Hashable)


# TODO change to UserDict[K, V] (requires Python >=3.9)
class LRU(UserDict):
    """Limited size mapping, evicting the least recently looked-up key when full"""

    def __init__(self, maxsize: float):
        super().__init__()
        self.data = OrderedDict()
        self.maxsize = maxsize

    def __getitem__(self, key):
        value = super().__getitem__(key)
        cast(OrderedDict, self.data).move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if len(self) >= self.maxsize:
            cast(OrderedDict, self.data).popitem(last=False)
        super().__setitem__(key, value)


class HeapSet(MutableSet[T]):
    """A set-like where the `pop` method returns the smallest item, as sorted by an
    arbitrary key function. Ties are broken by oldest first.

    Values must be compatible with :mod:`weakref`.
    """

    __slots__ = ("key", "_data", "_heap", "_inc")
    key: Callable[[T], Any]
    _data: set[T]
    _inc: int
    _heap: list[tuple[Any, int, weakref.ref[T]]]

    def __init__(self, *, key: Callable[[T], Any]):
        # FIXME https://github.com/python/mypy/issues/708
        self.key = key  # type: ignore
        self._data = set()
        self._inc = 0
        self._heap = []

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items>"

    def __contains__(self, value: object) -> bool:
        return value in self._data

    def __bool__(self) -> bool:
        return bool(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def add(self, value: T) -> None:
        if value in self._data:
            return
        k = self.key(value)  # type: ignore
        vref = weakref.ref(value)
        heapq.heappush(self._heap, (k, self._inc, vref))
        self._data.add(value)
        self._inc += 1

    def discard(self, value: T) -> None:
        self._data.discard(value)
        if not self._data:
            self._heap.clear()

    def peek(self) -> T:
        """Get the smallest element without removing it"""
        if not self._data:
            raise KeyError("peek into empty set")
        while True:
            value = self._heap[0][2]()
            if value in self._data:
                return value
            heapq.heappop(self._heap)

    def pop(self) -> T:
        if not self._data:
            raise KeyError("pop from an empty set")
        while True:
            _, _, vref = heapq.heappop(self._heap)
            value = vref()
            if value in self._data:
                self._data.discard(value)
                return value

    def popright(self) -> T:
        "Remove and return one of the largest elements (not necessarily the largest)!"
        if not self._data:
            raise KeyError("popright from an empty set")
        while True:
            _, _, vref = self._heap.pop()
            value = vref()
            if value is not None and value in self._data:
                self._data.discard(value)
                return value

    def topk(self, k: int) -> Iterator[T]:
        # TODO confirm big-O values here
        "Iterator over the largest K elements. This is O(k*logn) for k < n // 2, O(n*logn) otherwise."
        k = min(k, len(self))
        if k == 1:
            yield self.peek()
        elif k >= len(self) // 2:
            return itertools.islice(self.sorted(), k)
        else:
            # FIXME though neat, with all the list mutation this is probably always slower than sorting inplace.
            elems: list[tuple[Any, int, weakref.ref[T]]] = []
            try:
                while len(elems) < k:
                    elem = heapq.heappop(self._heap)
                    value = elem[-1]()
                    if value is not None and value in self._data:
                        # NOTE: we're in a broken state during iteration, since the value exists
                        # in the set but not the heap. As with all Python iterators, mutating
                        # while iterating is undefined.
                        elems.append(elem)
                        yield value
            finally:
                self._heap = elems + self._heap

    def __iter__(self) -> Iterator[T]:
        """Iterate over all elements. This is a O(n) operation which returns the
        elements in pseudo-random order.
        """
        return iter(self._data)

    def sorted(self) -> Iterator[T]:
        """Iterate over all elements. This is a O(n*logn) operation which returns the
        elements in order, from smallest to largest according to the key and insertion
        order.
        """
        self._heap.sort()  # A sorted list maintains the heap invariant
        for _, _, vref in self._heap:
            value = vref()
            if value in self._data:
                yield value

    def clear(self) -> None:
        self._data.clear()
        self._heap.clear()


class OrderedSet(MutableSet[T]):
    """
    A insertion-ordered set.

    All operations are O(1) complexity.

    Equality tests between OrderedSet objects are order-sensitive. Equality tests
    between OrderedSet objects and other AbstractSet objects are order-insensitive like
    regular sets.
    """

    __slots__ = ("_data",)
    _data: dict[T, None]

    def __init__(self, iterable: Iterable[T] | None = None) -> None:
        if iterable:
            self._data = dict.fromkeys(iterable)
        else:
            self._data = {}

    def add(self, value: T) -> None:
        self._data[value] = None
        # NOTE: updating an already-existing item in a dict does not change iteration order

    def discard(self, value: T) -> None:
        self._data.pop(value, None)

    def clear(self) -> None:
        self._data.clear()

    def copy(self) -> OrderedSet[T]:
        new = type(self)()
        new._data = self._data.copy()
        return new

    def pop(self) -> T:
        "Remove and return the last-inserted item"
        if not self._data:
            raise KeyError("pop on an empty set")
        return self._data.popitem()[0]

    def popleft(self) -> T:
        "Remove and return the first-inserted item"
        if not self._data:
            raise KeyError("popleft on an empty set")
        first = next(iter(self._data))
        self._data.pop(first)
        return first

    def peek(self) -> T:
        if not self._data:
            raise KeyError("peek into empty set")
        return next(reversed(self._data))

    def peekleft(self) -> T:
        if not self._data:
            raise KeyError("peekleft into empty set")
        return next(iter(self._data))

    def rotate(self, n=1) -> None:
        """
        Rotate the OrderedSet ``n`` steps to the right.

        Note that each rotation is an O(1) operation, so the time-complexity
        is equivalent to ``n``.
        """
        if n == 0:
            return
        if n < 0:
            raise ValueError(f"{type(self).__name__} can only be rotated to the right")
        n = n % len(self)
        for _ in range(n):
            self.add(self.popleft())

    def update(self, iterable: Iterable[T]) -> None:
        for x in iterable:
            self._data[x] = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(map(str, self))})>"

    def __contains__(self, value: object) -> bool:
        return value in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[T]:
        """Iterate over all elements in insertion order."""
        return iter(self._data)

    def __reverse__(self) -> Iterator[T]:
        """Iterate over all elements in reverse insertion order."""
        return reversed(self._data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return len(other._data) == len(self._data) and all(
                a == b for a, b in zip(self._data, other._data)
            )
        if isinstance(other, AbstractSet):
            return self._data.keys() == other

        return NotImplemented
