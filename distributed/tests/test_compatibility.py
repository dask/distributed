from __future__ import annotations

import random
from collections import Counter

from distributed.compatibility import randbytes


def test_randbytes():
    x = randbytes(256_000)
    assert isinstance(x, bytes)
    assert len(x) == 256_000
    c = Counter(x)
    for i in range(256):
        assert 800 < c[i] < 1200, (i, c[i])


def test_randbytes_seed():
    state = random.getstate()
    try:
        random.seed(0)
        assert randbytes(8) == b'\xcd\x07,\xd8\xbeo\x9fb'
    finally:
        random.setstate(state)
