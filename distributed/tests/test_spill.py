from __future__ import annotations

import logging
import os

import pytest

from dask.sizeof import sizeof

from distributed.compatibility import WINDOWS
from distributed.protocol import serialize_bytelist
from distributed.spill import SpillBuffer, has_zict_210
from distributed.utils_test import captured_logger

requires_zict_210 = pytest.mark.skipif(
    not has_zict_210,
    reason="requires zict version >= 2.1.0",
)


def psize(*objs) -> tuple[int, int]:
    return (
        sum(sizeof(o) for o in objs),
        sum(len(frame) for obj in objs for frame in serialize_bytelist(obj)),
    )


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == (0, 0)
    assert buf.spilled_total == (0, 0)

    a, b, c, d = "a" * 100, "b" * 99, "c" * 98, "d" * 97

    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < sizeof(a) < 200
    assert psize(a)[0] != psize(a)[1]

    buf["a"] = a
    assert not buf.slow
    assert buf.fast.weights == {"a": sizeof(a)}
    assert buf.fast.total_weight == sizeof(a)
    assert buf.slow.weight_by_key == {}
    assert buf.slow.total_weight == (0, 0)
    assert buf["a"] == a

    buf["b"] = b
    assert not buf.slow
    assert not buf.slow.weight_by_key
    assert buf.slow.total_weight == (0, 0)

    buf["c"] = c
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize(a)}
    assert buf.slow.total_weight == psize(a)

    assert buf["a"] == a
    assert set(buf.slow) == {"b"}
    assert buf.slow.weight_by_key == {"b": psize(b)}
    assert buf.slow.total_weight == psize(b)

    buf["d"] = d
    assert set(buf.slow) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": psize(b), "c": psize(c)}
    assert buf.slow.total_weight == psize(b, c)

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert set(buf.slow) == {"b", "c"}
    assert buf.slow.weight_by_key == {"b": psize(b), "c": psize(c)}
    assert buf.slow.total_weight == psize(b, c)
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert set(buf.slow) == {"c"}
    assert buf.slow.weight_by_key == {"c": psize(c)}
    assert buf.slow.total_weight == psize(c)
    with pytest.raises(KeyError):
        buf["b"]

    # Updating a spilled key moves it to the top of the LRU and to memory
    buf["c"] = c * 2
    assert set(buf.slow) == {"d"}
    assert buf.slow.weight_by_key == {"d": psize(d)}
    assert buf.slow.total_weight == psize(d)

    # Single key is larger than target and goes directly into slow
    e = "e" * 500

    buf["e"] = e
    assert set(buf.slow) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": psize(d), "e": psize(e)}
    assert buf.slow.total_weight == psize(d, e)

    # Updating a spilled key with another larger than target updates slow directly
    d = "d" * 500
    buf["d"] = d
    assert set(buf.slow) == {"d", "e"}
    assert buf.slow.weight_by_key == {"d": psize(d), "e": psize(e)}
    assert buf.slow.total_weight == psize(d, e)


@requires_zict_210
def test_spillbuffer_maxlim(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600, min_log_interval=0)

    a, b, c, d, e = "a" * 200, "b" * 100, "c" * 99, "d" * 199, "e" * 98

    # size of a is bigger than target and is smaller than max_spill;
    # key should be in slow
    buf["a"] = a
    assert not buf.fast
    assert not buf.fast.weights
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize(a)}
    assert buf.slow.total_weight == psize(a)
    assert buf["a"] == a

    # size of b is smaller than target key should be in fast
    buf["b"] = b
    assert set(buf.fast) == {"b"}
    assert buf.fast.weights == {"b": sizeof(b)}
    assert buf["b"] == b
    assert buf.fast.total_weight == sizeof(b)

    # size of c is smaller than target but b+c > target, c should stay in fast and b
    # move to slow since the max_spill limit has not been reached yet

    buf["c"] = c
    assert set(buf.fast) == {"c"}
    assert buf.fast.weights == {"c": sizeof(c)}
    assert buf["c"] == c
    assert buf.fast.total_weight == sizeof(c)

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize(a), "b": psize(b)}
    assert buf.slow.total_weight == psize(a, b)

    # size of e < target but e+c > target, this will trigger movement of c to slow
    # but the max spill limit prevents it. Resulting in e remaining in fast

    with captured_logger(logging.getLogger("distributed.spill")) as logs_e:
        buf["e"] = e

    assert "disk reached capacity" in logs_e.getvalue()

    assert set(buf.fast) == {"c", "e"}
    assert buf.fast.weights == {"c": sizeof(c), "e": sizeof(e)}
    assert buf["e"] == e
    assert buf.fast.total_weight == sizeof(c) + sizeof(e)

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize(a), "b": psize(b)}
    assert buf.slow.total_weight == psize(a, b)

    # size of d > target, d should go to slow but slow reached the max_spill limit then
    # d will end up on fast with c (which can't be move to slow because it won't fit
    # either)
    with captured_logger(logging.getLogger("distributed.spill")) as logs_d:
        buf["d"] = d

    assert "disk reached capacity" in logs_d.getvalue()

    assert set(buf.fast) == {"c", "d", "e"}
    assert buf.fast.weights == {"c": sizeof(c), "d": sizeof(d), "e": sizeof(e)}
    assert buf["d"] == d
    assert buf.fast.total_weight == sizeof(c) + sizeof(d) + sizeof(e)

    assert set(buf.slow) == {"a", "b"}
    assert buf.slow.weight_by_key == {"a": psize(a), "b": psize(b)}
    assert buf.slow.total_weight == psize(a, b)

    # Overwrite a key that was in slow, but the size of the new key is larger than
    # max_spill

    a_large = "a" * 500
    assert psize(a_large)[1] > 600  # size of max_spill

    with captured_logger(logging.getLogger("distributed.spill")) as logs_alarge:
        buf["a"] = a_large

    assert "disk reached capacity" in logs_alarge.getvalue()

    assert set(buf.fast) == {"a", "d", "e"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == sizeof(d) + sizeof(a_large) + sizeof(e)
    assert buf.slow.total_weight == psize(b, c)

    # Overwrite a key that was in fast, but the size of the new key is larger than
    # max_spill

    d_large = "d" * 501
    with captured_logger(logging.getLogger("distributed.spill")) as logs_dlarge:
        buf["d"] = d_large

    assert "disk reached capacity" in logs_dlarge.getvalue()

    assert set(buf.fast) == {"a", "d", "e"}
    assert set(buf.slow) == {"b", "c"}
    assert buf.fast.total_weight == sizeof(a_large) + sizeof(d_large) + sizeof(e)
    assert buf.slow.total_weight == psize(b, c)


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

    c = "c" * 100
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
        "c" * 201,
        "d" * 101,
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

    assert buf.slow.weight_by_key == {"a": psize(a)}
    assert buf.fast.weights == {"b": sizeof(b), "c": sizeof(c)}

    del buf["c"]
    assert set(buf.fast) == {"b"}
    assert set(buf.slow) == {"a"}

    # add key to fast which is smaller than target but when added it triggers spill,
    # which triggers OSError
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_evict:
        buf["d"] = d

    assert "Spill to disk failed" in logs_oserror_evict.getvalue()
    assert set(buf.fast) == {"b", "d"}
    assert set(buf.slow) == {"a"}

    assert buf.slow.weight_by_key == {"a": psize(a)}
    assert buf.fast.weights == {"b": sizeof(b), "d": sizeof(d)}


@requires_zict_210
def test_spillbuffer_evict(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300, min_log_interval=0)

    a_bad = Bad(size=100)
    a = "a" * 100

    buf["a"] = a

    assert set(buf.fast) == {"a"}
    assert not set(buf.slow)
    assert buf.fast.weights == {"a": sizeof(a)}

    # successful eviction
    weight = buf.evict()
    assert weight == sizeof(a)

    assert not buf.fast
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize(a)}

    buf["a_bad"] = a_bad

    assert set(buf.fast) == {"a_bad"}
    assert buf.fast.weights == {"a_bad": sizeof(a_bad)}
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize(a)}

    # unsuccessful eviction
    with captured_logger(logging.getLogger("distributed.spill")) as logs_evict_key:
        weight = buf.evict()
    assert weight == -1

    assert "Failed to pickle" in logs_evict_key.getvalue()
    # bad keys stays in fast
    assert set(buf.fast) == {"a_bad"}
    assert buf.fast.weights == {"a_bad": sizeof(a_bad)}
    assert set(buf.slow) == {"a"}
    assert buf.slow.weight_by_key == {"a": psize(a)}
