from __future__ import annotations

import asyncio
import random
import threading
from collections import defaultdict
from time import sleep

import pytest

import dask
from dask import delayed

from distributed import (
    Client,
    as_completed,
    get_client,
    get_worker,
    wait,
    worker_client,
)
from distributed.metrics import time
from distributed.utils_test import double, gen_cluster, inc, slowinc


@gen_cluster(client=True)
async def test_submit_from_worker(c, s, a, b):
    def func(x):
        with worker_client() as c:
            x = c.submit(inc, x)
            y = c.submit(double, x)
            result = x.result() + y.result()
            return result

    x, y = c.map(func, [10, 20])
    xx, yy = await c.gather([x, y])

    assert xx == 10 + 1 + (10 + 1) * 2
    assert yy == 20 + 1 + (20 + 1) * 2

    assert len(s.transition_log) > 10
    assert len([id for id in s.clients if id.lower().startswith("client")]) == 1


@gen_cluster(client=True)
async def test_submit_from_worker_async(c, s, a, b):
    async def func(x):
        with worker_client() as c:
            x = c.submit(inc, x)
            y = c.submit(double, x)
            return await x + await y

    x, y = c.map(func, [10, 20])
    xx, yy = await c.gather([x, y])

    assert xx == 10 + 1 + (10 + 1) * 2
    assert yy == 20 + 1 + (20 + 1) * 2

    assert len(s.transition_log) > 10
    assert len([id for id in s.clients if id.lower().startswith("client")]) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_scatter_from_worker(c, s, a, b):
    def func():
        with worker_client() as c:
            futures = c.scatter([1, 2, 3, 4, 5])
            assert isinstance(futures, (list, tuple))
            assert len(futures) == 5

            x = dict(get_worker().data)
            y = {f.key: i for f, i in zip(futures, [1, 2, 3, 4, 5])}
            assert x == y

            total = c.submit(sum, futures)
            return total.result()

    future = c.submit(func)
    result = await future
    assert result == sum([1, 2, 3, 4, 5])

    def func():
        with worker_client() as c:
            correct = True
            for data in [[1, 2], (1, 2), {1, 2}]:
                futures = c.scatter(data)
                correct &= type(futures) == type(data)

            o = object()
            futures = c.scatter({"x": o})
            correct &= get_worker().data["x"] is o
            return correct

    future = c.submit(func)
    result = await future
    assert result is True

    start = time()
    while not all(ws.nthreads == 1 for ws in s.workers.values()):
        await asyncio.sleep(0.1)
        assert time() < start + 5


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_scatter_singleton(c, s, a, b):
    np = pytest.importorskip("numpy")

    def func():
        with worker_client() as c:
            x = np.ones(5)
            future = c.scatter(x)
            assert future.type == np.ndarray

    await c.submit(func)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_gather_multi_machine(c, s, a, b):
    a_address = a.address
    b_address = b.address
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


@gen_cluster(client=True)
async def test_same_loop(c, s, a, b):
    def f():
        with worker_client() as lc:
            return lc.loop is get_worker().loop

    future = c.submit(f)
    result = await future
    assert result


def test_sync(client):
    def mysum():
        result = 0
        sub_tasks = [delayed(double)(i) for i in range(100)]

        with worker_client() as lc:
            futures = lc.compute(sub_tasks)
            for f in as_completed(futures):
                result += f.result()
        return result

    assert delayed(mysum)().compute() == 9900


@gen_cluster(client=True)
async def test_async(c, s, a, b):
    def mysum():
        result = 0
        sub_tasks = [delayed(double)(i) for i in range(100)]

        with worker_client() as lc:
            futures = lc.compute(sub_tasks)
            for f in as_completed(futures):
                result += f.result()
        return result

    future = c.compute(delayed(mysum)())
    await future

    start = time()
    while len(a.data) + len(b.data) > 1:
        await asyncio.sleep(0.1)
        assert time() < start + 3


@gen_cluster(client=True)
async def test_nested_worker_clients(c, s, a, b):
    def mysum():
        result = 0
        with worker_client() as lc:
            futures = lc.map(double, range(100))
            for f in as_completed(futures):
                result += f.result()
        return result

    def inc_mysum():
        with worker_client() as lc:
            future = lc.submit(inc, mysum())
            result = future.result()
        return result

    future = c.submit(inc_mysum)
    result = await future
    assert result == 9901


@gen_cluster(client=True, nthreads=[("127.0.0.1", 3)])
async def test_separate_thread_false(c, s, a):
    a.count = 0

    def f(i):
        with worker_client(separate_thread=False) as client:
            get_worker().count += 1
            assert get_worker().count <= 3
            sleep(random.random() / 40)
            assert get_worker().count <= 3
            get_worker().count -= 1
        return i

    futures = c.map(f, range(20))
    results = await c._gather(futures)
    assert list(results) == list(range(20))


@gen_cluster(client=True)
async def test_client_executor(c, s, a, b):
    def mysum():
        with worker_client() as c:
            with c.get_executor() as e:
                return sum(e.map(double, range(30)))

    future = c.submit(mysum)
    result = await future
    assert result == 30 * 29


def test_dont_override_default_get(loop):
    import dask.bag as db

    def f(x):
        with worker_client() as c:
            return True

    b = db.from_sequence([1, 2])
    b2 = b.map(f)

    with Client(
        loop=loop, processes=False, set_as_default=True, dashboard_address=":0"
    ) as c:
        assert dask.base.get_scheduler() == c.get
        for _ in range(2):
            b2.compute()

        assert dask.base.get_scheduler() == c.get


@gen_cluster(client=True)
async def test_local_client_warning(c, s, a, b):
    from distributed import local_client

    def func(x):
        with pytest.warns(
            UserWarning, match=r"local_client has moved to worker_client"
        ):
            cmgr = local_client()

        with cmgr as c:
            return c.submit(inc, x).result()

    future = c.submit(func, 10)
    result = await future
    assert result == 11


@gen_cluster(client=True)
async def test_closing_worker_doesnt_close_client(c, s, a, b):
    def func(x):
        get_client()
        return

    await wait(c.map(func, range(10)))
    await a.close()
    assert c.status == "running"


def test_timeout(client):
    def func():
        with worker_client(timeout=0) as wc:
            print("hello")

    future = client.submit(func)
    with pytest.raises(EnvironmentError):
        result = future.result()


def test_secede_without_stealing_issue_1262():
    """
    Tests that seceding works with the Stealing extension disabled
    https://github.com/dask/distributed/issues/1262
    """

    # run the loop as an inner function so all workers are closed
    # and exceptions can be examined
    @gen_cluster(
        client=True,
        scheduler_kwargs={"extensions": {}},
        worker_kwargs={"extensions": {}},
    )
    async def secede_test(c, s, a, b):
        def func(x):
            with worker_client() as wc:
                y = wc.submit(lambda: 1 + x)
                return wc.gather(y)

        f = await c.gather(c.submit(func, 1))

        return c, s, a, b, f

    c, s, a, b, f = secede_test()

    assert f == 2


@gen_cluster(client=True)
async def test_compute_within_worker_client(c, s, a, b):
    @dask.delayed
    def f():
        with worker_client():
            return dask.delayed(lambda x: x)(1).compute()

    result = await c.compute(f())
    assert result == 1


@gen_cluster(client=True)
async def test_worker_client_rejoins(c, s, a, b):
    def f():
        with worker_client():
            pass

        return threading.current_thread() in get_worker().executor._threads

    result = await c.submit(f)
    assert result


@pytest.mark.xfail(
    reason="Flaky due to https://github.com/dask/distributed/issues/5915"
)
@gen_cluster()
async def test_submit_different_names(s, a, b):
    # https://github.com/dask/distributed/issues/2058
    da = pytest.importorskip("dask.array")
    c = await Client(
        "localhost:" + s.address.split(":")[-1], loop=s.loop, asynchronous=True
    )
    try:
        X = c.persist(da.random.uniform(size=(100, 10), chunks=50))
        await wait(X)

        fut = await c.submit(lambda x: x.sum().compute(), X)
        assert fut > 0
    finally:
        await c.close()


@gen_cluster(client=True)
async def test_secede_does_not_claim_worker(c, s, a, b):
    """A seceded task must not block the task running it. Tasks scheduled from
    within should be evenly distributed"""

    # https://github.com/dask/distributed/issues/5332
    def get_addr(x):
        w = get_worker()
        slowinc(x)
        return w.address

    def long_running():
        with worker_client() as client:
            futs = client.map(get_addr, range(100))
            workers = defaultdict(int)
            for f in futs:
                workers[f.result()] += 1
            return dict(workers)

    res = await c.submit(long_running)
    assert len(res) == 2
    assert res[a.address] > 25
    assert res[b.address] > 25


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_log_event(c, s, a):
    # Run a task that spawns a worker client
    def f(x):
        with worker_client(timeout=10, separate_thread=True) as wc:
            x = wc.submit(inc, x)
            y = wc.submit(double, x)
            result = x.result() + y.result()
            return result

    future = c.submit(f, 1)
    result = await future
    assert result == 6

    # Ensure a corresponding event is logged
    for topic in ["worker-get-client", "worker-client"]:
        events = [msg for t, msg in s.get_events().items() if t == topic]
        assert len(events) == 1
        assert events[0][0][1] == {
            "worker": a.address,
            "timeout": 10,
            "client": c.id,
        }


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_log_event_implicit(c, s, a):
    # Run a task that spawns a worker client
    def f(x):
        x = delayed(inc)(x)
        y = delayed(double)(x)
        result = x.compute() + y.compute()
        return result

    future = c.submit(f, 1)
    result = await future
    assert result == 6

    # Ensure a corresponding event is logged
    events = [
        msg for topic, msg in s.get_events().items() if topic == "worker-get-client"
    ]
    assert len(events) == 1
    assert events[0][0][1] == {
        "worker": a.address,
        "timeout": 5,
        "client": c.id,
    }
    # Do not log a `worker-client` since this client was created implicitly
    events = [msg for topic, msg in s.get_events().items() if topic == "worker-client"]
    assert len(events) == 0
