"""
Various functional tests for TLS networking.
Most are taken from other test files and adapted.
"""
from __future__ import annotations

import asyncio

from tlz import merge

from distributed import Client, Nanny, Queue, Scheduler, Worker, wait, worker_client
from distributed.core import Status
from distributed.metrics import time
from distributed.utils_test import (
    NO_AMM,
    double,
    gen_test,
    gen_tls_cluster,
    inc,
    slowadd,
    slowinc,
    tls_config,
)


@gen_tls_cluster(client=True)
async def test_basic(c, s, a, b):
    pass


@gen_tls_cluster(client=True)
async def test_Queue(c, s, a, b):
    assert s.address.startswith("tls://")

    x = await Queue("x")
    y = await Queue("y")

    size = await x.qsize()
    assert size == 0

    future = c.submit(inc, 1)

    await x.put(future)

    future2 = await x.get()
    assert future.key == future2.key


@gen_tls_cluster(client=True)
async def test_client_submit(c, s, a, b):
    assert s.address.startswith("tls://")

    x = c.submit(inc, 10)
    result = await x
    assert result == 11

    yy = [c.submit(slowinc, i) for i in range(10)]
    results = []
    for y in yy:
        results.append(await y)
    assert results == list(range(1, 11))


@gen_tls_cluster(client=True)
async def test_gather(c, s, a, b):
    assert s.address.startswith("tls://")

    x = c.submit(inc, 10)
    y = c.submit(inc, x)

    result = await c._gather(x)
    assert result == 11
    result = await c._gather([x])
    assert result == [11]
    result = await c._gather({"x": x, "y": [y]})
    assert result == {"x": 11, "y": [12]}


@gen_tls_cluster(client=True)
async def test_scatter(c, s, a, b):
    assert s.address.startswith("tls://")

    d = await c._scatter({"y": 20})
    ts = s.tasks["y"]
    assert ts.who_has
    assert ts.nbytes > 0
    yy = await c._gather([d["y"]])
    assert yy == [20]


@gen_tls_cluster(client=True, Worker=Nanny)
async def test_nanny(c, s, a, b):
    assert s.address.startswith("tls://")
    for n in [a, b]:
        assert isinstance(n, Nanny)
        assert n.address.startswith("tls://")
        assert n.worker_address.startswith("tls://")
    assert set(s.workers) == {n.worker_address for n in [a, b]}

    x = c.submit(inc, 10)
    result = await x
    assert result == 11


@gen_tls_cluster(
    client=True,
    config=merge(
        NO_AMM,
        {
            "distributed.worker.memory.rebalance.measure": "managed",
            "distributed.worker.memory.rebalance.sender-min": 0,
            "distributed.worker.memory.rebalance.sender-recipient-gap": 0,
        },
    ),
)
async def test_rebalance(c, s, a, b):
    """Test Client.rebalance(). This test is just to test the TLS Client wrapper around
    Scheduler.rebalance(); for more thorough tests on the latter see test_scheduler.py.
    """
    assert a.address.startswith("tls://")

    futures = await c.scatter(range(100), workers=[a.address])
    assert len(a.data) == 100
    assert len(b.data) == 0
    await c.rebalance()
    assert len(a.data) == 50
    assert len(b.data) == 50


@gen_tls_cluster(client=True, nthreads=[("tls://127.0.0.1", 2)] * 2)
async def test_work_stealing(c, s, a, b):
    [x] = await c._scatter([1], workers=a.address)
    futures = c.map(slowadd, range(50), [x] * 50, delay=0.1)
    await asyncio.sleep(0.1)
    await wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10


@gen_tls_cluster(client=True)
async def test_worker_client(c, s, a, b):
    def func(x):
        with worker_client() as c:
            x = c.submit(inc, x)
            y = c.submit(double, x)
            result = x.result() + y.result()
            return result

    x, y = c.map(func, [10, 20])
    xx, yy = await c._gather([x, y])

    assert xx == 10 + 1 + (10 + 1) * 2
    assert yy == 20 + 1 + (20 + 1) * 2


@gen_tls_cluster(client=True, nthreads=[("tls://127.0.0.1", 1)] * 2)
async def test_worker_client_gather(c, s, a, b):
    a_address = a.address
    b_address = b.address
    assert a_address.startswith("tls://")
    assert b_address.startswith("tls://")
    assert a_address != b_address

    def func():
        with worker_client() as ee:
            x = ee.submit(inc, 1, workers=a_address)
            y = ee.submit(inc, 2, workers=b_address)

            xx, yy = ee.gather([x, y])
        return xx, yy

    future = c.submit(func)
    result = await future

    assert result == (2, 3)


@gen_tls_cluster(client=True)
async def test_worker_client_executor(c, s, a, b):
    def mysum():
        with worker_client() as c:
            with c.get_executor() as e:
                return sum(e.map(double, range(30)))

    future = c.submit(mysum)
    result = await future
    assert result == 30 * 29


@gen_tls_cluster(client=True, Worker=Nanny)
async def test_retire_workers(c, s, a, b):
    assert set(s.workers) == {a.worker_address, b.worker_address}
    await c.retire_workers(workers=[a.worker_address], close_workers=True)
    assert set(s.workers) == {b.worker_address}

    start = time()
    while a.status != Status.closed:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_test()
async def test_security_dict_input_no_security():
    async with Scheduler(dashboard_address=":0", security={}) as s:
        async with Worker(s.address, security={}):
            async with Client(s.address, security={}, asynchronous=True) as c:
                result = await c.submit(inc, 1)
                assert result == 2


@gen_test()
async def test_security_dict_input():
    conf = tls_config()
    ca_file = conf["distributed"]["comm"]["tls"]["ca-file"]
    client = conf["distributed"]["comm"]["tls"]["client"]["cert"]
    worker = conf["distributed"]["comm"]["tls"]["worker"]["cert"]
    scheduler = conf["distributed"]["comm"]["tls"]["scheduler"]["cert"]

    async with Scheduler(
        host="localhost",
        dashboard_address=":0",
        security={"tls_ca_file": ca_file, "tls_scheduler_cert": scheduler},
    ) as s:
        assert s.address.startswith("tls://")
        async with Worker(
            s.address, security={"tls_ca_file": ca_file, "tls_worker_cert": worker}
        ) as w:
            assert w.address.startswith("tls://")
            async with Client(
                s.address,
                security={"tls_ca_file": ca_file, "tls_client_cert": client},
                asynchronous=True,
            ) as c:
                result = await c.submit(inc, 1)
                assert result == 2
