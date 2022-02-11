import logging
import os

import pytest

zict = pytest.importorskip("zict")
from packaging.version import parse as parse_version

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


requires_zict_210 = pytest.mark.skipif(
    parse_version(zict.__version__) <= parse_version("2.0.0"),
    reason="requires zict version > 2.0.0",
)


@requires_zict_210
def test_spillbuffer_maxlim(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600, min_log_interval=0)

    a, b, c, d, e = "a" * 200, "b" * 100, "c" * 100, "d" * 200, "e" * 100

    psize_a = sum(len(frame) for frame in serialize_bytelist(a))  # size on disk

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

    # size of e < target but e+c > target, this will trigger movement of c to slow
    # but the max spill limit prevents it. Resulting in e remaining in fast
    se = sizeof(e)

    with captured_logger(logging.getLogger("distributed.spill")) as logs_e:
        buf["e"] = e

    assert "disk reached capacity" in logs_e.getvalue()

    assert set(buf.fast) == {"c", "e"}
    assert buf.fast.weights == {"c": sc, "e": se}
    assert buf["e"] == e
    assert buf.fast.total_weight == sc + se

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize_a, "b": psize_b}
    assert buf.slow.total_weight == psize_a + psize_b

    # size of d > target, d should go to slow but slow reached the max_spill limit then d
    # will end up on fast with c (which can't be move to slow because it won't fit either)
    sd = sizeof(d)
    with captured_logger(logging.getLogger("distributed.spill")) as logs_d:
        buf["d"] = d

    assert "disk reached capacity" in logs_d.getvalue()

    assert set(buf.fast) == {"c", "d", "e"}
    assert buf.fast.weights == {"c": sc, "d": sd, "e": se}
    assert buf["d"] == d
    assert buf.fast.total_weight == sc + sd + se

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize_a, "b": psize_b}
    assert buf.slow.total_weight == psize_a + psize_b

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

    assert set(buf.fast) == {"a", "d", "e"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == sd + sizeof(a_large) + se
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

    assert set(buf.fast) == {"a", "d", "e"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == 2 * sizeof(a_large) + se
    assert buf.slow.total_weight == 2 * psize_b


class MyError(Exception):
    pass


class Bad:
    def __init__(self, size):
        self.size = size

    def __getstate__(self):
        raise MyError()

    def __sizeof__(self):
        return self.size


@requires_zict_210
def test_spillbuffer_fail_to_serialize(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600, min_log_interval=0)

    # bad data individually larger than spill threshold target 200
    a = Bad(size=201)

    # Exception caught in the worker
    with pytest.raises(TypeError, match="Could not serialize"):
        with captured_logger(logging.getLogger("distributed.spill")) as logs_bad_key:
            buf["a"] = a

    # spill.py must remain silent because we're already logging in worker.py
    assert not logs_bad_key.getvalue()
    assert not set(buf.fast)
    assert not set(buf.slow)

    b = Bad(size=100)  # this is small enough to fit in memory/fast

    buf["b"] = b
    assert set(buf.fast) == {"b"}

    c = 100 * "c"
    with captured_logger(logging.getLogger("distributed.spill")) as logs_bad_key_mem:
        # This will go to fast and try to kick b out,
        # but keep b in fast since it's not pickable
        buf["c"] = c

    # worker.py won't intercept the exception here, so spill.py must dump the traceback
    logs_value = logs_bad_key_mem.getvalue()
    assert "Failed to pickle" in logs_value  # from distributed.spill
    assert "Traceback" in logs_value  # from distributed.spill
    assert set(buf.fast) == {"b", "c"}
    assert buf.fast.total_weight == sizeof(b) + sizeof(c)
    assert not set(buf.slow)


@requires_zict_210
@pytest.mark.skipif(WINDOWS, reason="Needs chmod")
def test_spillbuffer_oserror(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=800, min_log_interval=0)

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

    # modify permissions of disk to be read only.
    # This causes writes to raise OSError, just like in case of disk full.
    os.chmod(tmpdir, 0o555)

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

    # add key to fast which is smaller than target but when added it triggers spill, which triggers OSError
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_evict:
        buf["d"] = d

    assert "Spill to disk failed" in logs_oserror_evict.getvalue()
    assert set(buf.fast) == {"b", "d"}
    assert set(buf.slow) == {"a"}

    assert buf.slow.weight_by_key == {"a": 329}
    assert buf.fast.weights == {"b": 149, "d": 149}


@requires_zict_210
def test_spillbuffer_evict(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300, min_log_interval=0)

    a_bad = Bad(size=100)
    a = "a" * 100
    sa_bad = sizeof(a_bad)
    sa = sizeof(a)
    sa_pickle = sum(len(frame) for frame in serialize_bytelist(a))

    buf["a"] = a

    assert set(buf.fast) == {"a"}
    assert not set(buf.slow)
    assert buf.fast.weights == {"a": sa}

    # successful eviction
    weight = buf.evict()
    assert weight == sa

    assert not buf.fast
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": sa_pickle}

    buf["a_bad"] = a_bad

    assert set(buf.fast) == {"a_bad"}
    assert buf.fast.weights == {"a_bad": sa_bad}
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": sa_pickle}

    # unsuccessful eviction
    with captured_logger(logging.getLogger("distributed.spill")) as logs_evict_key:
        weight = buf.evict()
    assert weight == -1

    assert "Failed to pickle" in logs_evict_key.getvalue()
    # bad keys stays in fast
    assert set(buf.fast) == {"a_bad"}
    assert buf.fast.weights == {"a_bad": sa_bad}
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": sa_pickle}
