from __future__ import annotations

import asyncio
from unittest import mock

import pytest

from dask.optimization import SubgraphCallable

from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.core import ConnectionPool
from distributed.utils_comm import (
    WrappedKey,
    gather_from_workers,
    pack_data,
    retry,
    subs_multiple,
    unpack_remotedata,
)
from distributed.utils_test import NO_AMM, BlockedGetData, BrokenComm, gen_cluster


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


@gen_cluster(client=True)
async def test_gather_from_workers_permissive(c, s, a, b):
    rpc = await ConnectionPool()
    x = await c.scatter({"x": 1}, workers=a.address)

    data, missing = await gather_from_workers(["x", "y"], s.get_who_has, rpc=rpc)

    assert data == {"x": 1}
    assert missing == {"y"}


class BrokenConnectionPool(ConnectionPool):
    async def connect(self, *args, **kwargs):
        return BrokenComm()


@gen_cluster(client=True)
async def test_gather_from_workers_permissive_flaky(c, s, a, b):
    x = await c.scatter({"x": 1}, workers=a.address)

    rpc = await BrokenConnectionPool()
    data, missing = await gather_from_workers(["x"], s.get_who_has, rpc=rpc)

    assert data == {}
    assert missing == {"x"}


@gen_cluster(client=True, nthreads=[], config=NO_AMM)
async def test_gather_from_workers_cancelled_error(c, s):
    """Something somewhere in the networking stack raises CancelledError while
    gather_from_workers is running

    See Also
    --------
    test_worker.py::test_gather_dep_cancelled_error
    test_worker.py::test_get_data_cancelled_error
    https://github.com/dask/distributed/issues/8006
    """
    rpc = await ConnectionPool()
    async with BlockedGetData(s.address) as a, BlockedGetData(s.address) as b:
        a.block_get_data.set()
        b.block_get_data.set()
        x = await c.scatter({"x": 1}, broadcast=True)
        assert len(s.tasks["x"].who_has) == 2
        a.in_get_data.clear()
        b.in_get_data.clear()
        a.block_get_data.clear()
        b.block_get_data.clear()

        fut = asyncio.create_task(
            gather_from_workers(keys=["x"], who_has=s.get_who_has, rpc=rpc)
        )
        await asyncio.wait(
            [
                asyncio.create_task(a.in_get_data.wait()),
                asyncio.create_task(b.in_get_data.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        tasks = {
            task for task in asyncio.all_tasks() if "get-data-from-" in task.get_name()
        }
        assert tasks
        # There should be only one task but cope with finding more just in case a
        # previous test didn't properly clean up
        for task in tasks:
            task.cancel()

        a.block_get_data.set()
        b.block_get_data.set()
        # gather_from_workers retries transparently from the other worker
        assert await fut == ({"x": 1}, set())


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
    def assert_eq(keys1: set[WrappedKey], keys2: set[WrappedKey]) -> None:
        if len(keys1) != len(keys2):
            assert False
        if not keys1:
            assert True
        if not all(isinstance(k, WrappedKey) for k in keys1 & keys2):
            assert False
        assert sorted([k.key for k in keys1]) == sorted([k.key for k in keys2])

    assert unpack_remotedata(1) == (1, set())
    assert unpack_remotedata(()) == ((), set())

    res, keys = unpack_remotedata(WrappedKey("mykey"))
    assert res == "mykey"
    assert_eq(keys, {WrappedKey("mykey")})

    # Check unpack of SC that contains a wrapped key
    sc = SubgraphCallable({"key": (WrappedKey("data"),)}, outkey="key", inkeys=["arg1"])
    dsk = (sc, "arg1")
    res, keys = unpack_remotedata(dsk)
    assert res[0] != sc  # Notice, the first item (the SC) has been changed
    assert res[1:] == ("arg1", "data")
    assert_eq(keys, {WrappedKey("data")})

    # Check unpack of SC when it takes a wrapped key as argument
    sc = SubgraphCallable({"key": ("arg1",)}, outkey="key", inkeys=[WrappedKey("arg1")])
    dsk = (sc, "arg1")
    res, keys = unpack_remotedata(dsk)
    assert res == (sc, "arg1")  # Notice, the first item (the SC) has NOT been changed
    assert_eq(keys, set())
