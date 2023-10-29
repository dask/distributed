from __future__ import annotations

import pytest

from distributed.protocol.utils import host_array, host_array_from_buffers


def test_host_array():
    a = host_array(5)
    a[:3] = b"abc"
    a[3:] = b"de"
    assert bytes(a) == b"abcde"


def test_host_array_from_buffers():
    a = host_array_from_buffers([b"abc", b"de"])
    a[:1] = b"f"
    assert bytes(a) == b"fbcde"


def test_host_array_from_buffers_numpy():
    """Test for word sizes larger than 1 byte"""
    np = pytest.importorskip("numpy")
    a = host_array_from_buffers(
        [np.array([1, 2], dtype="u1"), np.array([3, 4], dtype="u8")]
    )
    assert a.nbytes == 18
