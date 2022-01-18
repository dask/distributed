import logging
import subprocess
from distutils.version import LooseVersion

import pytest
import zict

from dask.sizeof import sizeof

from distributed.compatibility import WINDOWS
from distributed.protocol import serialize_bytelist
from distributed.spill import SpillBuffer
from distributed.utils_test import captured_logger


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == 0
    assert buf.spilled_total == 0

    a, b, c, d = "a" * 100, "b" * 100, "c" * 100, "d" * 100
    inmem_size = sizeof(a)
    pickle_size = sum(len(frame) for frame in serialize_bytelist(a))

    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < inmem_size < 200
    # assert 100 < pickle_size < 200 #this is not true pickled size = 229
    assert inmem_size != pickle_size

    buf["a"] = a
    assert not buf.slow
    assert buf.fast.weights == {"a": inmem_size}
    assert buf.fast.total_weight == inmem_size
    assert buf.slow.weight_by_key == {}
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


@pytest.mark.skipif(
    LooseVersion(zict.__version__) <= "2.0.0",
    reason="requires zict version > 2.0.0 or higher",
)
def test_spillbuffer_maxlim(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600)

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
    with captured_logger(logging.getLogger("distributed.spill")) as logs_d:
        buf["d"] = d

    assert "disk reached capacity" in logs_d.getvalue()

    assert set(buf.fast) == {"c", "d"}
    assert buf.fast.weights == {"c": sc, "d": sd}
    assert buf["d"] == d
    assert buf.fast.total_weight == sc + sd

    # Overwrite a key that was in slow, but the size of the new key is larger than max_spill
    # We overwrite `a` with something bigger than max_spill, it will try to write in slow
    # it will trigger exception and locate it in fast, where it will trigger total_weight being
    # bigger than the target and the LRU dict has more than one element, causing this to evict
    # `c` (lower priority) with no complains since we have room on slow. But this condition
    # self.total_weight > self.n and len(self.d) > 1 is still true on the LRU (we have `a` and `d`
    # here) so it'll try to evict `d` but it doesn't fit in slow, which will trigger another exception
    # kepping it finally on fast

    a_large = "a" * 500
    psize_alarge = sum(len(frame) for frame in serialize_bytelist(a_large))
    assert psize_alarge > 600  # size of max_spill

    with captured_logger(logging.getLogger("distributed.spill")) as logs_alarge:
        buf["a"] = a_large

    assert "disk reached capacity" in logs_alarge.getvalue()

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
    with captured_logger(logging.getLogger("distributed.spill")) as logs_dlarge:
        buf["d"] = d_large

    assert "disk reached capacity" in logs_dlarge.getvalue()

    assert set(buf.fast) == {"a", "d"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == 2 * sizeof(a_large)
    assert buf.slow.total_weight == 2 * psize_b


class MyError(Exception):
    pass


def test_spillbuffer_bad_key(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600)

    class Bad:
        def __init__(self, size):
            self.size = size

        def __getstate__(self):
            raise MyError()

        def __sizeof__(self):
            return int(self.size)

    # bad data individually larger than spill threshold target 200
    a = Bad(size=200)
    assert sizeof(a) > 200

    with pytest.raises(Exception):
        with captured_logger(logging.getLogger("distributed.spill")) as logs_bad_key:
            buf["a"] = a

    assert "Failed to pickle" in logs_bad_key.getvalue()

    b = Bad(size=100)  # this is small enough to fit in memory/fast

    buf["b"] = b
    assert set(buf.fast) == {"b"}

    c = 100 * "c"
    with captured_logger(logging.getLogger("distributed.spill")) as logs_bad_key_mem:
        # This will go to fast and try to kick b out,
        # but keep b in fast since it's not pickable
        buf["c"] = c

    assert "Failed to pickle" in logs_bad_key_mem.getvalue()
    assert set(buf.fast) == {"b", "c"}


@pytest.mark.skipif(WINDOWS, reason="Needs chmod")
def test_spillbuffer_oserror(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=800)

    a, b, c, d = (
        "a" * 200,
        "b" * 100,
        "c" * 200,
        "d" * 100,
    )

    # let's have something in fast and something in slow
    buf["a"] = a
    buf["b"] = b
    assert set(buf.fast) == {"b"}
    assert set(buf.slow) == {"a"}

    # modify permissions of disk to be read only
    subprocess.call(["chmod", "-R", "-w", str(tmpdir)])

    # Add key > than target
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_slow:
        buf["c"] = c

    assert "Spill to disk failed" in logs_oserror_slow.getvalue()
    assert set(buf.fast) == {"b", "c"}
    assert set(buf.slow) == {"a"}

    assert buf.slow.weight_by_key == {"a": 329}
    assert buf.fast.weights == {"b": 149, "c": 249}

    del buf["c"]
    assert set(buf.fast) == {"b"}
    assert set(buf.slow) == {"a"}

    # add key to fast which is small than target but when add it triggers spill, which triggers OSError
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_evict:
        buf["d"] = d

    assert "Spill to disk failed" in logs_oserror_evict.getvalue()
    assert set(buf.fast) == {"b", "d"}
    assert set(buf.slow) == {"a"}

    assert buf.slow.weight_by_key == {"a": 329}
    assert buf.fast.weights == {"b": 149, "d": 149}
