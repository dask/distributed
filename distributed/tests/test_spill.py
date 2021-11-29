import pytest

from dask.sizeof import sizeof

from distributed.protocol import serialize_bytelist
from distributed.spill import SpillBuffer


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0

    a, b, c, d = "a" * 100, "b" * 100, "c" * 100, "d" * 100
    s = sizeof(a)  # this is size of a in memory

    pickle = serialize_bytelist(a)
    pickle_size = sum(len(frame) for frame in pickle)

    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < s < 200

    buf["a"] = a
    assert not buf.disk
    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0
    assert buf["a"] == a

    buf["b"] = b
    assert not buf.disk
    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0

    buf["c"] = c
    assert set(buf.disk) == {"a"}
    assert buf.slow.weight_by_key == {"a": pickle_size}
    assert buf.slow.total_weight == pickle_size

    assert buf["a"] == a
    assert set(buf.disk) == {"b"}
    assert buf.slow.weight_by_key == {"b": pickle_size}
    assert buf.slow.total_weight == pickle_size

    buf["d"] = d
    assert set(buf.disk) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": pickle_size, "c": pickle_size}
    assert buf.slow.total_weight == pickle_size * 2

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert set(buf.disk) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": pickle_size, "c": pickle_size}
    assert buf.slow.total_weight == pickle_size * 2
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert set(buf.disk) == {"c"}
    assert buf.slow.weight_by_key == {"c": pickle_size}
    assert buf.slow.total_weight == pickle_size
    with pytest.raises(KeyError):
        buf["b"]

    # Updating a spilled key moves it to the top of the LRU and to memory
    buf["c"] = c * 2
    assert set(buf.disk) == {"d"}
    assert buf.slow.weight_by_key == {"d": pickle_size}
    assert buf.slow.total_weight == pickle_size

    # Single key is larger than target and goes directly into slow
    e = "e" * 500
    pickle_large = serialize_bytelist(e)
    pickle_large_size = sum(len(frame) for frame in pickle_large)

    buf["e"] = e
    assert set(buf.disk) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": pickle_size, "e": pickle_large_size}
    assert buf.slow.total_weight == pickle_size + pickle_large_size

    # Updating a spilled key with another larger than target updates slow directly
    buf["d"] = "d" * 500
    assert set(buf.disk) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": pickle_large_size, "e": pickle_large_size}
    assert buf.slow.total_weight == pickle_large_size * 2
