from __future__ import annotations

import pytest

from distributed.protocol.utils import host_array
from distributed.protocol.utils_test import get_host_array


def test_get_host_array():
    np = pytest.importorskip("numpy")

    a = np.array([1, 2, 3])
    assert get_host_array(a) is a
    assert get_host_array(a[1:]) is a
    assert get_host_array(a[1:][1:]) is a

    buf = host_array(3)
    a = np.frombuffer(buf, dtype="u1")
    assert get_host_array(a) is buf.obj
    assert get_host_array(a[1:]) is buf.obj
    a = np.frombuffer(buf[1:], dtype="u1")
    assert get_host_array(a) is buf.obj

    for buf in (b"123", bytearray(b"123")):
        a = np.frombuffer(buf, dtype="u1")
        assert get_host_array(a) is buf
        a = np.frombuffer(memoryview(buf), dtype="u1")
        assert get_host_array(a) is buf
