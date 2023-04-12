from __future__ import annotations

from collections import Counter

from distributed.compatibility import randbytes


def test_randbytes():
    x = randbytes(256_000)
    assert isinstance(x, bytes)
    assert len(x) == 256_000
    c = Counter(x)
    for i in range(256):
        assert 800 < c[i] < 1200
