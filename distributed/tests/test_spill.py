import pytest

pytest.importorskip("zict")

from dask.sizeof import sizeof

from distributed.spill import SpillBuffer


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert not buf.spilled_by_key
    assert buf.spilled_total == 0

    a, b, c, d = "a" * 100, "b" * 100, "c" * 100, "d" * 100
    s = sizeof(a)
    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < s < 200

    buf["a"] = a
    assert not buf.disk
    assert not buf.spilled_by_key
    assert buf.spilled_total == 0
    assert buf["a"] == a

    buf["b"] = b
    assert not buf.disk
    assert not buf.spilled_by_key
    assert buf.spilled_total == 0

    buf["c"] = c
    assert set(buf.disk) == {"a"}
    assert buf.spilled_by_key == {"a": s}
    assert buf.spilled_total == s

    assert buf["a"] == a
    assert set(buf.disk) == {"b"}
    assert buf.spilled_by_key == {"b": s}
    assert buf.spilled_total == s

    buf["d"] = d
    assert set(buf.disk) == {"b", "c"}
    assert buf.spilled_by_key == {"b": s, "c": s}
    assert buf.spilled_total == s * 2

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert set(buf.disk) == {"b", "c"}
    assert buf.spilled_by_key == {"b": s, "c": s}
    assert buf.spilled_total == s * 2
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert set(buf.disk) == {"c"}
    assert buf.spilled_by_key == {"c": s}
    assert buf.spilled_total == s
    with pytest.raises(KeyError):
        buf["b"]

    # Updating a spilled key moves it to the top of the LRU and to memory
    buf["c"] = c * 2
    assert set(buf.disk) == {"d"}
    assert buf.spilled_by_key == {"d": s}
    assert buf.spilled_total == s

    # Single key is larger than target and goes directly into slow
    e = "e" * 500
    slarge = sizeof(e)
    buf["e"] = e
    assert set(buf.disk) == {"d", "e"}
    assert buf.spilled_by_key == {"d": s, "e": slarge}
    assert buf.spilled_total == s + slarge

    # Updating a spilled key with another larger than target updates slow directly
    buf["d"] = "d" * 500
    assert set(buf.disk) == {"d", "e"}
    assert buf.spilled_by_key == {"d": slarge, "e": slarge}
    assert buf.spilled_total == slarge * 2
