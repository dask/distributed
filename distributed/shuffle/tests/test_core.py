from __future__ import annotations

import pytest

from distributed.shuffle._core import _mean_shard_size


def test_mean_shard_size():
    assert _mean_shard_size([]) == 0
    assert _mean_shard_size([b""]) == 0
    assert _mean_shard_size([b"123", b"45678"]) == 4
    # Don't fully iterate over large collections
    assert _mean_shard_size([b"12" * n for n in range(1000)]) == 9
    # Support any Buffer object
    assert _mean_shard_size([b"12", bytearray(b"1234"), memoryview(b"123456")]) == 4
    # Recursion into lists or tuples; ignore int
    assert _mean_shard_size([(1, 2, [3, b"123456"])]) == 6
    # Don't blindly call sizeof() on unexpected objects
    with pytest.raises(TypeError):
        _mean_shard_size([1.2])
    with pytest.raises(TypeError):
        _mean_shard_size([{1: 2}])


def test_mean_shard_size_numpy():
    """Test that _mean_shard_size doesn't call len() on multi-byte data types"""
    np = pytest.importorskip("numpy")
    assert _mean_shard_size([np.zeros(10, dtype="u1")]) == 10
    assert _mean_shard_size([np.zeros(10, dtype="u8")]) == 80
