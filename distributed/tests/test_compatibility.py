from __future__ import annotations

import random
from collections import Counter

import pytest

from distributed.compatibility import randbytes


def test_randbytes():
    with pytest.warns(
        DeprecationWarning,
        match=r"randbytes is deprecated and will be removed in a future release; "
        r"use random\.randbytes instead\.",
    ):
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
        with pytest.warns(
            DeprecationWarning,
            match=r"randbytes is deprecated and will be removed in a future release; "
            r"use random\.randbytes instead\.",
        ):
            assert randbytes(8) == b"\xcd\x07,\xd8\xbeo\x9fb"
    finally:
        random.setstate(state)
