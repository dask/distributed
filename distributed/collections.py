from __future__ import annotations

import heapq
import weakref
from collections import OrderedDict, UserDict
from collections.abc import Callable, Iterator
from typing import MutableSet  # TODO move to collections.abc (requires Python >=3.9)
from typing import Any, TypeVar, cast

T = TypeVar("T")


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
        self.key = key  # type: ignore
        self._data = set()
        self._inc = 0
        self._heap = []

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {len(self)} items>"

    def __contains__(self, value: object) -> bool:
        return value in self._data

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def add(self, value: T) -> None:
        if value in self._data:
            return
        self._data.add(value)
        i = self._inc
        self._inc += 1
        k = self.key(value)  # type: ignore
        heapq.heappush(self._heap, (k, i, weakref.ref(value)))

    def discard(self, value: T) -> None:
        self._data.discard(value)

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
                self._data.remove(value)
                return value

    def sorted(self) -> list[T]:
        """Return a list containing all elements, from smallest to largest according to
        the key and insertion order.
        """
        out = []
        for _, _, vref in sorted(self._heap):
            value = vref()
            if value in self._data:
                out.append(value)
        return out
