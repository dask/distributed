import asyncio
from unittest import mock

import pytest

from distributed.core import ConnectionPool
from distributed.utils_comm import gather_from_workers, pack_data, retry, subs_multiple
from distributed.utils_test import BrokenComm, gen_cluster


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

    data, missing, bad_workers = await gather_from_workers(
        {"x": [a.address], "y": [b.address]}, rpc=rpc
    )

    assert data == {"x": 1}
    assert list(missing) == ["y"]


class BrokenConnectionPool(ConnectionPool):
    async def connect(self, *args, **kwargs):
        return BrokenComm()


@gen_cluster(client=True)
async def test_gather_from_workers_permissive_flaky(c, s, a, b):
    x = await c.scatter({"x": 1}, workers=a.address)

    rpc = await BrokenConnectionPool()
    data, missing, bad_workers = await gather_from_workers({"x": [a.address]}, rpc=rpc)

    assert missing == {"x": [a.address]}
    assert bad_workers == [a.address]


def test_retry_no_exception(cleanup):
    n_calls = 0
    retval = object()

    async def coro():
        nonlocal n_calls
        n_calls += 1
        return retval

    async def f():
        return await retry(coro, count=0, delay_min=-1, delay_max=-1)

    assert asyncio.run(f()) is retval
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
        asyncio.run(f())

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
            asyncio.run(f())

    assert n_calls == 6
    assert sleep_calls == [0.0, 1.0, 3.0, 6.0, 6.0]
