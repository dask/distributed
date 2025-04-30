from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Iterator
from typing import Protocol, TypeVar


class _Comparable(Protocol):

    @abstractmethod
    def __ge__(self, other: _Comparable) -> bool: ...


X = TypeVar("X", bound=_Comparable)
Y = TypeVar("Y", bound=_Comparable)


def ffill(x: Iterable[X], xp: Iterable[X], fp: Iterable[Y], left: Y) -> Iterator[Y]:
    """Forward-fill interpolation

    Parameters
    ----------
    x:
        Output x series. Must be monotonic ascending.
    xp:
        Input x series. Must be strictly monotonic ascending.
    fp:
        Input y series. If it contains more or less elements than xp, the two series
        will be clipped to the shortest one (like in :func:`zip`).
    left:
        Value to yield for x < xp[0]

    Yields
    ------
    Forward-fill interpolated elements from fp matching x

    Examples
    --------
    >>> list(ffill([0.5, 2.2, 2.3, 4.5], [1, 2, 3], "abc", "-"))
    ["-", "b", "b", "c"]
    """
    it = zip(xp, fp)
    xp_done = False
    xp1, fp1 = None, left
    for xi in x:
        while not xp_done and (xp1 is None or xi >= xp1):
            fp0 = fp1
            try:
                xp1, fp1 = next(it)
            except StopIteration:
                xp_done = True
        yield fp0
