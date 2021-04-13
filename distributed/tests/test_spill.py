import pytest

pytest.importorskip("zict")

from dask.sizeof import sizeof

from distributed.spill import SpillBuffer


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    assert buf.spilled_total == 0

    a, b, c, d = "a" * 100, "b" * 100, "c" * 100, "d" * 100
    s = sizeof(a)
    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < s < 200

    buf["a"] = a
    assert buf.spilled_total == 0
    assert buf["a"] == a
    buf["b"] = b
    assert buf.spilled_total == 0
    buf["c"] = c
    assert buf.spilled_total == s
    assert buf.spilled_by_key == {"a": s}
    assert buf["a"] == a
    assert buf.spilled_total == s
    assert buf.spilled_by_key == {"b": s}

    buf["d"] = d
    assert buf.spilled_total == s * 2
    assert buf.spilled_by_key == {"b": s, "c": s}

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert buf.spilled_total == s * 2
    assert buf.spilled_by_key == {"b": s, "c": s}
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert buf.spilled_total == s
    assert buf.spilled_by_key == {"c": s}
    with pytest.raises(KeyError):
        buf["b"]
