from __future__ import annotations

import pytest

from distributed.shuffle._pickle import pickle_bytelist, unpickle_bytestream


def test_pickle():
    frames = pickle_bytelist("abc") + pickle_bytelist(123)
    bin = b"".join(frames)
    objs = list(unpickle_bytestream(bin))
    assert objs == ["abc", 123]


def test_pickle_numpy():
    np = pytest.importorskip("numpy")
    a = np.array([1, 2, 3])
    frames = pickle_bytelist(a)
    bin = b"".join(frames)
    [a2] = unpickle_bytestream(bin)
    assert (a2 == a).all()


def test_pickle_zero_copy():
    np = pytest.importorskip("numpy")
    a = np.array([1, 2, 3])
    frames = pickle_bytelist(a)
    a[0] = 4  # Test that pickle_bytelist does not deep copy
    bin = bytearray(b"".join(frames))  # Deep-copies buffers
    [a2] = unpickle_bytestream(bin)
    a2[1] = 5  # Test that unpickle_bytelist does not deep copy
    [a3] = unpickle_bytestream(bin)
    expect = np.array([4, 5, 3])
    assert (a3 == expect).all()
