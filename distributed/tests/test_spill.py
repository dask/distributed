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

    pickle_size = sum(len(frame) for frame in serialize_bytelist(a))

    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < s < 200

    buf["a"] = a
    assert not buf.slow
    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0
    assert buf["a"] == a

    buf["b"] = b
    assert not buf.slow
    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0

    buf["c"] = c
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": pickle_size}
    assert buf.slow.total_weight == pickle_size

    assert buf["a"] == a
    assert set(buf.slow) == {"b"}
    assert buf.slow.weight_by_key == {"b": pickle_size}
    assert buf.slow.total_weight == pickle_size

    buf["d"] = d
    assert set(buf.slow) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": pickle_size, "c": pickle_size}
    assert buf.slow.total_weight == pickle_size * 2

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert set(buf.slow) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": pickle_size, "c": pickle_size}
    assert buf.slow.total_weight == pickle_size * 2
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert set(buf.slow) == {"c"}
    assert buf.slow.weight_by_key == {"c": pickle_size}
    assert buf.slow.total_weight == pickle_size
    with pytest.raises(KeyError):
        buf["b"]

    # Updating a spilled key moves it to the top of the LRU and to memory
    buf["c"] = c * 2
    assert set(buf.slow) == {"d"}
    assert buf.slow.weight_by_key == {"d": pickle_size}
    assert buf.slow.total_weight == pickle_size

    # Single key is larger than target and goes directly into slow
    e = "e" * 500
    pickle_large_size = sum(len(frame) for frame in serialize_bytelist(e))

    buf["e"] = e
    assert set(buf.slow) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": pickle_size, "e": pickle_large_size}
    assert buf.slow.total_weight == pickle_size + pickle_large_size

    # Updating a spilled key with another larger than target updates slow directly
    buf["d"] = "d" * 500
    assert set(buf.slow) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": pickle_large_size, "e": pickle_large_size}
    assert buf.slow.total_weight == pickle_large_size * 2


def test_spillbuffer_maxlim(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0

    a, b, c, d = "a" * 200, "b" * 100, "c" * 100, "d" * 200

    psize_a = sum(len(frame) for frame in serialize_bytelist(a))  # this is size in disk

    # size of a is bigger than target and is smaller than max_spill, key should be in slow
    buf["a"] = a
    assert not buf.fast
    assert not buf.fast.weights
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize_a}
    assert buf.slow.total_weight == psize_a
    assert buf["a"] == a

    # size of b is smaller than target key should be in fast
    sb = sizeof(b)
    buf["b"] = b
    assert set(buf.fast) == {"b"}
    assert buf.fast.weights == {"b": sb}
    assert buf["b"] == b
    assert buf.fast.total_weight == sb

    # size of c is smaller than target but b+c > target, c should stay in fast and b move to slow
    # since the max_spill limit has not been reached yet
    sc = sizeof(c)
    psize_b = sum(len(frame) for frame in serialize_bytelist(b))

    buf["c"] = c
    assert set(buf.fast) == {"c"}
    assert buf.fast.weights == {"c": sc}
    assert buf["c"] == c
    assert buf.fast.total_weight == sc

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize_a, "b": psize_b}
    assert buf.slow.total_weight == psize_a + psize_b

    # size of d > target, d should go to slow but slow reached the max_spill limit then d
    # will end up on fast with c (which can't be move to slow because it won't fit either)
    sd = sizeof(d)

    buf["d"] = d
    assert set(buf.fast) == {"c", "d"}
    assert buf.fast.weights == {"c": sc, "d": sd}
    assert buf["d"] == d
    assert buf.fast.total_weight == sc + sd

    # Overwrite a key that was in slow, but the size of the new key is larger than max_spill
    # We overwrite `a` with something bigger than max_spill, it will try to write in slow
    # it will trigger exception and locate it in fast, where it will trigger total_weight being
    # bigger than the target and the LRU dict has more than one element, causing this to evict
    # `c` (lower priority) with no compalins since we have room on slow. But this condition
    # self.total_weight > self.n and len(self.d) > 1 is still true on the LRU (we have `a` and `d`
    # here) so it'll try to evict `d` but it doesn't fit in slow, which will trigger another exception
    # kepping it finally on fast

    a_large = "a" * 500
    psize_alarge = sum(len(frame) for frame in serialize_bytelist(a_large))
    assert psize_alarge > 600  # size of max_spill

    buf["a"] = a_large
    assert set(buf.fast) == {"a", "d"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == sd + sizeof(a_large)
    assert buf.slow.total_weight == 2 * psize_b

    # Overwrite a key that was in fast, but the size of the new key is larger than max_spill
    # We overwrite `d` with something bigger than max_spill, it will try to write in slow
    # it will trigger exception and locate it in fast, where it will trigger total_weight being
    # bigger than the target and the LRU dict has more than one element, this will try to evict
    # `a` (lower priority) but it will fail because it doesn't fit in slow, which will trigger
    # another exception kepping it finally on fast

    d_large = "d" * 500
    buf["d"] = d_large
    assert set(buf.fast) == {"a", "d"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == 2 * sizeof(a_large)
    assert buf.slow.total_weight == 2 * psize_b
