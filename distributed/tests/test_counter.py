from __future__ import annotations

import pytest

from distributed.counter import Counter

try:
    from distributed.counter import Digest
except ImportError:
    Digest = None  # type: ignore


@pytest.mark.parametrize(
    "CD,size",
    [
        (Counter, lambda d: sum(d.values())),
        pytest.param(
            Digest,
            lambda x: x.size(),
            marks=pytest.mark.skipif(not Digest, reason="no crick library"),
        ),
    ],
)
def test_digest(CD, size):
    c = CD()
    c.add(1)
    c.add(2)
    assert size(c.components[0]) == 2

    c.shift()
    assert 0 < size(c.components[0]) < 2
    assert 0 < size(c.components[1]) < 1
    assert sum(size(d) for d in c.components) == 2

    for i in range(len(c.components) - 1):
        assert size(c.components[i]) >= size(c.components[i + 1])

    c.add(3)

    assert sum(size(d) for d in c.components) == c.size()


def test_counter():
    c = Counter()
    c.add(1)

    for _ in range(5):
        c.shift()
        assert abs(sum(cc[1] for cc in c.components) - 1) < 1e-13
