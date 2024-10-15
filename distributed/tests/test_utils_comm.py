from __future__ import annotations

import asyncio
import random
from unittest import mock

import pytest

from dask._task_spec import TaskRef
from dask.optimization import SubgraphCallable

from distributed import wait
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.core import ConnectionPool, Status
from distributed.utils_comm import (
    DoNotUnpack,
    gather_from_workers,
    pack_data,
    retry,
    subs_multiple,
    unpack_remotedata,
)
from distributed.utils_test import BarrierGetData, BrokenComm, gen_cluster, inc


def test_pack_data():
    data = {"x": 1}
    assert pack_data(("x", "y"), data) == (1, "y")
    assert pack_data({"a": "x", "b": "y"}, data) == {"a": 1, "b": "y"}
    assert pack_data({"a": ["x"], "b": "y"}, data) == {"a": [1], "b": "y"}


def test_subs_multiple():
    data = {"x": 1, "y": 2}
    assert subs_multiple((sum, [0, "x", "y", "z"]), data) == (sum, [0, 1, 2, "z"])
    assert subs_multiple((sum, [0, ["x", "y", "z"]]), data) == (sum, [0, [1, 2, "z"]])

    dsk = {"a": (sum, ["x", "y"])}
    assert subs_multiple(dsk, data) == {"a": (sum, [1, 2])}

    # Tuple key
    data = {"x": 1, ("y", 0): 2}
    dsk = {"a": (sum, ["x", ("y", 0)])}
    assert subs_multiple(dsk, data) == {"a": (sum, [1, 2])}


@gen_cluster(client=True, nthreads=[("", 1)] * 10)
async def test_gather_from_workers_missing_replicas(c, s, *workers):
    """When a key is replicated on multiple workers, but the who_has is slightly
    obsolete, gather_from_workers, retries fetching from all known holders of a replica
    until it finds the key
    """
    a = random.choice(workers)
    x = await c.scatter({"x": 1}, workers=a.address)
    assert len(s.workers) == 10
    assert len(s.tasks["x"].who_has) == 1

    rpc = await ConnectionPool()
    data, missing, failed, bad_workers = await gather_from_workers(
        {"x": [w.address for w in workers]}, rpc=rpc
    )

    assert data == {"x": 1}
    assert missing == []
    assert failed == []
    assert bad_workers == []


@gen_cluster(client=True)
async def test_gather_from_workers_permissive(c, s, a, b):
    """gather_from_workers fetches multiple keys, of which some are missing.
    Test that the available data is returned with a note for missing data.
    """
    rpc = await ConnectionPool()
    x = await c.scatter({"x": 1}, workers=a.address)

    data, missing, failed, bad_workers = await gather_from_workers(
        {"x": [a.address], "y": [b.address]}, rpc=rpc
    )

    assert data == {"x": 1}
    assert missing == ["y"]
    assert failed == []
    assert bad_workers == []


class BrokenConnectionPool(ConnectionPool):
    async def connect(self, address, *args, **kwargs):
        return BrokenComm()


@gen_cluster(client=True)
async def test_gather_from_workers_permissive_flaky(c, s, a, b):
    """gather_from_workers fails to connect to a worker"""
    x = await c.scatter({"x": 1}, workers=a.address)

    rpc = await BrokenConnectionPool()
    data, missing, failed, bad_workers = await gather_from_workers(
        {"x": [a.address]}, rpc=rpc
    )

    assert data == {}
    assert missing == ["x"]
    assert failed == []
    assert bad_workers == [a.address]


@gen_cluster(
    client=True,
    nthreads=[],
    config={"distributed.worker.memory.pause": False},
)
async def test_gather_from_workers_busy(c, s):
    """gather_from_workers receives a 'busy' response from a worker"""
    async with BarrierGetData(s.address, barrier_count=2) as w:
        x = await c.scatter({"x": 1}, workers=[w.address])
        await wait(x)
        # Throttle to 1 simultaneous connection
        w.status = Status.paused

        rpc1 = await ConnectionPool()
        rpc2 = await ConnectionPool()
        out1, out2 = await asyncio.gather(
            gather_from_workers({"x": [w.address]}, rpc=rpc1),
            gather_from_workers({"x": [w.address]}, rpc=rpc2),
        )
        assert w.barrier_count == -1  # w.get_data() has been hit 3 times
        assert out1 == out2 == ({"x": 1}, [], [], [])


@pytest.mark.parametrize("when", ["pickle", "unpickle"])
@gen_cluster(client=True)
async def test_gather_from_workers_serialization_error(c, s, a, b, when):
    """A task fails to (de)serialize. Tasks from other workers are fetched
    successfully.
    """

    class BadReduce:
        def __reduce__(self):
            if when == "pickle":
                1 / 0
            else:
                return lambda: 1 / 0, ()

    rpc = await ConnectionPool()
    x = c.submit(BadReduce, key="x", workers=[a.address])
    y = c.submit(inc, 1, key="y", workers=[a.address])
    z = c.submit(inc, 2, key="z", workers=[b.address])
    await wait([x, y, z])
    data, missing, failed, bad_workers = await gather_from_workers(
        {"x": [a.address], "y": [a.address], "z": [b.address]}, rpc=rpc
    )

    assert data == {"z": 3}
    assert missing == []
    # x and y were serialized together with a single call to pickle; can't tell which
    # raised
    assert failed == ["x", "y"]
    assert bad_workers == []


def test_retry_no_exception(cleanup):
    n_calls = 0
    retval = object()

    async def coro():
        nonlocal n_calls
        n_calls += 1
        return retval

    async def f():
        return await retry(coro, count=0, delay_min=-1, delay_max=-1)

    assert asyncio_run(f(), loop_factory=get_loop_factory()) is retval
    assert n_calls == 1


def test_retry0_raises_immediately(cleanup):
    # test that using max_reties=0 raises after 1 call

    n_calls = 0

    async def coro():
        nonlocal n_calls
        n_calls += 1
        raise RuntimeError(f"RT_ERROR {n_calls}")

    async def f():
        return await retry(coro, count=0, delay_min=-1, delay_max=-1)

    with pytest.raises(RuntimeError, match="RT_ERROR 1"):
        asyncio_run(f(), loop_factory=get_loop_factory())

    assert n_calls == 1


def test_retry_does_retry_and_sleep(cleanup):
    # test the retry and sleep pattern of `retry`
    n_calls = 0

    class MyEx(Exception):
        pass

    async def coro():
        nonlocal n_calls
        n_calls += 1
        raise MyEx(f"RT_ERROR {n_calls}")

    sleep_calls = []

    async def my_sleep(amount):
        sleep_calls.append(amount)
        return

    async def f():
        return await retry(
            coro,
            retry_on_exceptions=(MyEx,),
            count=5,
            delay_min=1.0,
            delay_max=6.0,
            jitter_fraction=0.0,
        )

    with mock.patch("asyncio.sleep", my_sleep):
        with pytest.raises(MyEx, match="RT_ERROR 6"):
            asyncio_run(f(), loop_factory=get_loop_factory())

    assert n_calls == 6
    assert sleep_calls == [0.0, 1.0, 3.0, 6.0, 6.0]


def test_unpack_remotedata():
    def assert_eq(keys1: set[TaskRef], keys2: set[TaskRef]) -> None:
        if len(keys1) != len(keys2):
            assert False
        if not keys1:
            assert True
        if not all(isinstance(k, TaskRef) for k in keys1 & keys2):
            assert False
        assert sorted([k.key for k in keys1]) == sorted([k.key for k in keys2])

    assert unpack_remotedata(1) == (1, set())
    assert unpack_remotedata(()) == ((), set())

    res, keys = unpack_remotedata(TaskRef("mykey"))
    assert res == "mykey"
    assert_eq(keys, {TaskRef("mykey")})

    # Check unpack of SC that contains a wrapped key
    sc = SubgraphCallable({"key": (TaskRef("data"),)}, outkey="key", inkeys=["arg1"])
    dsk = (sc, "arg1")
    res, keys = unpack_remotedata(dsk)
    assert res[0] != sc  # Notice, the first item (the SC) has been changed
    assert res[1:] == ("arg1", "data")
    assert_eq(keys, {TaskRef("data")})

    # Check unpack of SC when it takes a wrapped key as argument
    sc = SubgraphCallable({"key": ("arg1",)}, outkey="key", inkeys=[TaskRef("arg1")])
    dsk = (sc, "arg1")
    res, keys = unpack_remotedata(dsk)
    assert res == (sc, "arg1")  # Notice, the first item (the SC) has NOT been changed
    assert_eq(keys, set())


def test_unpack_remotedata_custom_tuple():
    # We don't want to recurse into custom tuples. This is used as a sentinel to
    # avoid recursion for performance reasons if we know that there are no
    # nested futures. This test case is not how this feature should be used in
    # practice.

    akey = TaskRef("a")

    ordinary_tuple = (1, 2, akey)
    dont_recurse = DoNotUnpack(ordinary_tuple)

    res, keys = unpack_remotedata(ordinary_tuple)
    assert res is not ordinary_tuple
    assert res == (1, 2, "a")
    assert all(not isinstance(item, TaskRef) for item in res)
    assert keys == {akey}
    res, keys = unpack_remotedata(dont_recurse)
    assert not keys
    assert res is dont_recurse
