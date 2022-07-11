from __future__ import annotations

import logging
import os
import uuid

import pytest

from dask.sizeof import sizeof

from distributed import profile
from distributed.compatibility import WINDOWS
from distributed.protocol import serialize_bytelist
from distributed.spill import SpillBuffer, has_zict_210, has_zict_220
from distributed.utils_test import captured_logger

requires_zict_210 = pytest.mark.skipif(
    not has_zict_210,
    reason="requires zict version >= 2.1.0",
)
requires_zict_220 = pytest.mark.skipif(
    not has_zict_220,
    reason="requires zict version >= 2.2.0",
)


def psize(*objs: object) -> tuple[int, int]:
    return (
        sum(sizeof(o) for o in objs),
        sum(len(frame) for obj in objs for frame in serialize_bytelist(obj)),
    )


def assert_buf(buf: SpillBuffer, expect_fast: dict, expect_slow: dict) -> None:
    # assertions on fast
    assert dict(buf.fast) == expect_fast
    assert buf.fast.weights == {k: sizeof(v) for k, v in expect_fast.items()}
    assert buf.fast.total_weight == sum(sizeof(v) for v in expect_fast.values())
    for k, v in buf.fast.items():
        assert buf[k] is v

    # assertions on slow
    assert set(buf.slow) == expect_slow.keys()
    slow = buf.slow.data if has_zict_220 else buf.slow  # type: ignore
    assert slow.weight_by_key == {k: psize(v) for k, v in expect_slow.items()}
    total_weight = psize(*expect_slow.values())
    assert slow.total_weight == total_weight
    assert buf.spilled_total == total_weight


def test_spillbuffer(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300)
    # Convenience aliases
    assert buf.memory is buf.fast
    assert buf.disk is buf.slow

    assert_buf(buf, {}, {})

    a, b, c, d = "a" * 100, "b" * 99, "c" * 98, "d" * 97

    # Test assumption made by this test, mostly for non CPython implementations
    assert 100 < sizeof(a) < 200
    assert psize(a)[0] != psize(a)[1]

    buf["a"] = a
    assert_buf(buf, {"a": a}, {})
    assert buf["a"] == a

    buf["b"] = b
    assert_buf(buf, {"a": a, "b": b}, {})

    buf["c"] = c
    assert_buf(buf, {"b": b, "c": c}, {"a": a})

    assert buf["a"] == a
    assert_buf(buf, {"a": a, "c": c}, {"b": b})

    buf["d"] = d
    assert_buf(buf, {"a": a, "d": d}, {"b": b, "c": c})

    # Deleting an in-memory key does not automatically move spilled keys back to memory
    del buf["a"]
    assert_buf(buf, {"d": d}, {"b": b, "c": c})
    with pytest.raises(KeyError):
        buf["a"]

    # Deleting a spilled key updates the metadata
    del buf["b"]
    assert_buf(buf, {"d": d}, {"c": c})
    with pytest.raises(KeyError):
        buf["b"]

    # Updating a spilled key moves it to the top of the LRU and to memory
    c2 = c * 2
    buf["c"] = c2
    assert_buf(buf, {"c": c2}, {"d": d})

    # Single key is larger than target and goes directly into slow
    e = "e" * 500

    buf["e"] = e
    assert_buf(buf, {"c": c2}, {"d": d, "e": e})

    # Updating a spilled key with another larger than target updates slow directly
    d = "d" * 500
    buf["d"] = d
    assert_buf(buf, {"c": c2}, {"d": d, "e": e})


@requires_zict_210
def test_spillbuffer_maxlim(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=200, max_spill=600, min_log_interval=0)

    a, b, c, d, e = "a" * 200, "b" * 100, "c" * 99, "d" * 199, "e" * 98

    # size of a is bigger than target and is smaller than max_spill;
    # key should be in slow
    buf["a"] = a
    assert_buf(buf, {}, {"a": a})
    assert buf["a"] == a

    # size of b is smaller than target key should be in fast
    buf["b"] = b
    assert_buf(buf, {"b": b}, {"a": a})

    # size of c is smaller than target but b+c > target, c should stay in fast and b
    # move to slow since the max_spill limit has not been reached yet
    buf["c"] = c
    assert_buf(buf, {"c": c}, {"a": a, "b": b})

    # size of e < target but e+c > target, this will trigger movement of c to slow
    # but the max spill limit prevents it. Resulting in e remaining in fast

    with captured_logger(logging.getLogger("distributed.spill")) as logs_e:
        buf["e"] = e

    assert "disk reached capacity" in logs_e.getvalue()
    assert_buf(buf, {"c": c, "e": e}, {"a": a, "b": b})

    # size of d > target, d should go to slow but slow reached the max_spill limit then
    # d will end up on fast with c (which can't be move to slow because it won't fit
    # either)
    with captured_logger(logging.getLogger("distributed.spill")) as logs_d:
        buf["d"] = d

    assert "disk reached capacity" in logs_d.getvalue()
    assert_buf(buf, {"c": c, "d": d, "e": e}, {"a": a, "b": b})

    # Overwrite a key that was in slow, but the size of the new key is larger than
    # max_spill

    a_large = "a" * 500
    assert psize(a_large)[1] > 600  # size of max_spill

    with captured_logger(logging.getLogger("distributed.spill")) as logs_alarge:
        buf["a"] = a_large

    assert "disk reached capacity" in logs_alarge.getvalue()
    assert_buf(buf, {"a": a_large, "d": d, "e": e}, {"b": b, "c": c})

    # Overwrite a key that was in fast, but the size of the new key is larger than
    # max_spill

    d_large = "d" * 501
    with captured_logger(logging.getLogger("distributed.spill")) as logs_dlarge:
        buf["d"] = d_large

    assert "disk reached capacity" in logs_dlarge.getvalue()
    assert_buf(buf, {"a": a_large, "d": d_large, "e": e}, {"b": b, "c": c})


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
    assert_buf(buf, {}, {})

    b = Bad(size=100)  # this is small enough to fit in memory/fast

    buf["b"] = b
    assert_buf(buf, {"b": b}, {})

    c = "c" * 100
    with captured_logger(logging.getLogger("distributed.spill")) as logs_bad_key_mem:
        # This will go to fast and try to kick b out,
        # but keep b in fast since it's not pickable
        buf["c"] = c

    # worker.py won't intercept the exception here, so spill.py must dump the traceback
    logs_value = logs_bad_key_mem.getvalue()
    assert "Failed to pickle" in logs_value  # from distributed.spill
    assert "Traceback" in logs_value  # from distributed.spill
    assert_buf(buf, {"b": b, "c": c}, {})


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
    assert_buf(buf, {"b": b}, {"a": a})

    # modify permissions of disk to be read only.
    # This causes writes to raise OSError, just like in case of disk full.
    os.chmod(tmpdir, 0o555)

    # Add key > than target
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_slow:
        buf["c"] = c

    assert "Spill to disk failed" in logs_oserror_slow.getvalue()
    assert_buf(buf, {"b": b, "c": c}, {"a": a})

    del buf["c"]
    assert_buf(buf, {"b": b}, {"a": a})

    # add key to fast which is smaller than target but when added it triggers spill,
    # which triggers OSError
    with captured_logger(logging.getLogger("distributed.spill")) as logs_oserror_evict:
        buf["d"] = d

    assert "Spill to disk failed" in logs_oserror_evict.getvalue()
    assert_buf(buf, {"b": b, "d": d}, {"a": a})


@requires_zict_210
def test_spillbuffer_evict(tmpdir):
    buf = SpillBuffer(str(tmpdir), target=300, min_log_interval=0)

    bad = Bad(size=100)
    a = "a" * 100

    buf["a"] = a
    assert_buf(buf, {"a": a}, {})

    # successful eviction
    weight = buf.evict()
    assert weight == sizeof(a)
    assert_buf(buf, {}, {"a": a})

    buf["bad"] = bad
    assert_buf(buf, {"bad": bad}, {"a": a})

    # unsuccessful eviction
    with captured_logger(logging.getLogger("distributed.spill")) as logs_evict_key:
        weight = buf.evict()
    assert weight == -1

    assert "Failed to pickle" in logs_evict_key.getvalue()
    # bad keys stays in fast
    assert_buf(buf, {"bad": bad}, {"a": a})


class NoWeakRef:
    """A class which
    1. reports an arbitrary managed memory usage
    2. does not support being targeted by weakref.ref()
    3. has a property `id` which changes every time it is unpickled
    """

    __slots__ = ("size", "id")

    def __init__(self, size):
        self.size = size
        self.id = uuid.uuid4()

    def __sizeof__(self):
        return self.size

    def __reduce__(self):
        return (type(self), (self.size,))


class SupportsWeakRef(NoWeakRef):
    __slots__ = ("__weakref__",)


@pytest.mark.parametrize(
    "cls,expect_cached",
    [
        (SupportsWeakRef, has_zict_220),
        (NoWeakRef, False),
    ],
)
@pytest.mark.parametrize("size", [60, 110])
def test_weakref_cache(tmpdir, cls, expect_cached, size):
    buf = SpillBuffer(str(tmpdir), target=100)

    # Run this test twice:
    # - x is smaller than target and is evicted by y;
    # - x is individually larger than target and it never touches fast
    x = cls(size)
    buf["x"] = x
    if size < 100:
        buf["y"] = cls(60)  # spill x
    assert "x" in buf.slow

    # Test that we update the weakref cache on setitem
    assert (buf["x"] is x) == expect_cached

    # Do not use id_x = id(x), as in CPython id's are C memory addresses and are reused
    # by PyMalloc when you descope objects, so a brand new object might end up having
    # the same id as a deleted one
    id_x = x.id
    del x

    # Ensure that the profiler has stopped and released all references to x so that it can be garbage-collected
    with profile.lock:
        pass

    if size < 100:
        buf["y"]
    assert "x" in buf.slow

    x2 = buf["x"]
    assert x2.id != id_x
    if size < 100:
        buf["y"]
    assert "x" in buf.slow

    # Test that we update the weakref cache on getitem
    assert (buf["x"] is x2) == expect_cached
