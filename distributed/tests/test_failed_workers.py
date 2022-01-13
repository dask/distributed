import asyncio
import os
import random
from contextlib import suppress
from time import sleep
from unittest import mock

import pytest
from tlz import first, partition_all

from dask import delayed

from distributed import Client, Nanny, wait
from distributed.comm import CommClosedError
from distributed.compatibility import MACOS
from distributed.metrics import time
from distributed.scheduler import COMPILED
from distributed.utils import CancelledError, sync
from distributed.utils_test import (
    captured_logger,
    cluster,
    div,
    gen_cluster,
    inc,
    slowadd,
    slowinc,
)

pytestmark = pytest.mark.ci1


def test_submit_after_failed_worker_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a["proc"]().terminate()
            total = c.submit(sum, L)
            assert total.result() == sum(map(inc, range(10)))


@gen_cluster(client=True, timeout=60, active_rpc_timeout=10)
async def test_submit_after_failed_worker_async(c, s, a, b):
    n = await Nanny(s.address, nthreads=2, loop=s.loop)
    while len(s.workers) < 3:
        await asyncio.sleep(0.1)

    L = c.map(inc, range(10))
    await wait(L)

    s.loop.add_callback(n.kill)
    total = c.submit(sum, L)
    result = await total
    assert result == sum(map(inc, range(10)))

    await n.close()


@gen_cluster(client=True, timeout=60)
async def test_submit_after_failed_worker(c, s, a, b):
    L = c.map(inc, range(10))
    await wait(L)
    await a.close()

    total = c.submit(sum, L)
    result = await total
    assert result == sum(map(inc, range(10)))


@pytest.mark.slow
def test_gather_after_failed_worker(loop):
    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            L = c.map(inc, range(10))
            wait(L)
            a["proc"]().terminate()
            result = c.gather(L)
            assert result == list(map(inc, range(10)))


@pytest.mark.slow
@gen_cluster(client=True, Worker=Nanny, nthreads=[("127.0.0.1", 1)] * 4, timeout=60)
async def test_gather_then_submit_after_failed_workers(c, s, w, x, y, z):
    L = c.map(inc, range(20))
    await wait(L)

    w.process.process._process.terminate()
    total = c.submit(sum, L)

    for _ in range(3):
        await wait(total)
        addr = first(s.tasks[total.key].who_has).address
        for worker in [x, y, z]:
            if worker.worker_address == addr:
                worker.process.process._process.terminate()
                break

        result = await c.gather([total])
        assert result == [sum(map(inc, range(20)))]


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_failed_worker_without_warning(c, s, a, b):
    L = c.map(inc, range(10))
    await wait(L)

    original_pid = a.pid
    with suppress(CommClosedError):
        await c._run(os._exit, 1, workers=[a.worker_address])
    start = time()
    while a.pid == original_pid:
        await asyncio.sleep(0.01)
        assert time() - start < 10

    await asyncio.sleep(0.5)

    start = time()
    while len(s.nthreads) < 2:
        await asyncio.sleep(0.01)
        assert time() - start < 10

    await wait(L)

    L2 = c.map(inc, range(10, 20))
    await wait(L2)
    assert all(len(keys) > 0 for keys in s.has_what.values())
    nthreads2 = dict(s.nthreads)

    await c.restart()

    L = c.map(inc, range(10))
    await wait(L)
    assert all(len(keys) > 0 for keys in s.has_what.values())

    assert not (set(nthreads2) & set(s.nthreads))  # no overlap


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_restart(c, s, a, b):
    assert s.nthreads == {a.worker_address: 1, b.worker_address: 2}

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(div, 1, 0)
    await y

    assert set(s.who_has) == {x.key, y.key}

    f = await c.restart()
    assert f is c

    assert len(s.workers) == 2
    assert not any(ws.occupancy for ws in s.workers.values())

    assert not s.who_has

    assert x.cancelled()
    assert y.cancelled()
    assert z.cancelled()
    assert z.key not in s.exceptions

    assert not s.who_wants
    assert not any(cs.wants_what for cs in s.clients.values())


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_restart_cleared(c, s, a, b):
    x = 2 * delayed(1) + 1
    f = c.compute(x)
    await wait([f])

    await c.restart()

    for coll in [s.tasks, s.unrunnable]:
        assert not coll


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
def test_restart_sync_no_center(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            x = c.submit(inc, 1)
            c.restart()
            assert x.cancelled()
            y = c.submit(inc, 2)
            assert y.result() == 3
            assert len(c.nthreads()) == 2


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
def test_restart_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            x = c.submit(div, 1, 2)
            x.result()

            assert sync(loop, c.scheduler.who_has)
            c.restart()
            assert not sync(loop, c.scheduler.who_has)
            assert x.cancelled()
            assert len(c.nthreads()) == 2

            with pytest.raises(CancelledError):
                x.result()

            y = c.submit(div, 1, 3)
            assert y.result() == 1 / 3


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_restart_fast(c, s, a, b):
    L = c.map(sleep, range(10))

    start = time()
    await c.restart()
    assert time() - start < 10
    assert len(s.nthreads) == 2

    assert all(x.status == "cancelled" for x in L)

    x = c.submit(inc, 1)
    result = await x
    assert result == 2


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
def test_worker_doesnt_await_task_completion(loop):
    with cluster(nanny=True, nworkers=1) as (s, [w]):
        with Client(s["address"], loop=loop) as c:
            future = c.submit(sleep, 100)
            sleep(0.1)
            start = time()
            c.restart()
            stop = time()
            assert stop - start < 5


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
def test_restart_fast_sync(loop):
    with cluster(nanny=True) as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            L = c.map(sleep, range(10))

            start = time()
            c.restart()
            assert time() - start < 10
            assert len(c.nthreads()) == 2

            assert all(x.status == "cancelled" for x in L)

            x = c.submit(inc, 1)
            assert x.result() == 2


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_fast_kill(c, s, a, b):
    L = c.map(sleep, range(10))

    start = time()
    await c.restart()
    assert time() - start < 10

    assert all(x.status == "cancelled" for x in L)

    x = c.submit(inc, 1)
    result = await x
    assert result == 2


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(Worker=Nanny, timeout=60)
async def test_multiple_clients_restart(s, a, b):
    c1 = await Client(s.address, asynchronous=True)
    c2 = await Client(s.address, asynchronous=True)

    x = c1.submit(inc, 1)
    y = c2.submit(inc, 2)
    xx = await x
    yy = await y
    assert xx == 2
    assert yy == 3

    await c1.restart()

    assert x.cancelled()
    start = time()
    while not y.cancelled():
        await asyncio.sleep(0.01)
        assert time() < start + 5

    await c1.close()
    await c2.close()


@gen_cluster(Worker=Nanny, timeout=60)
async def test_restart_scheduler(s, a, b):
    assert len(s.nthreads) == 2
    pids = (a.pid, b.pid)
    assert pids[0]
    assert pids[1]

    await s.restart()

    assert len(s.nthreads) == 2
    pids2 = (a.pid, b.pid)
    assert pids2[0]
    assert pids2[1]
    assert pids != pids2


@gen_cluster(Worker=Nanny, client=True, timeout=60)
async def test_forgotten_futures_dont_clean_up_new_futures(c, s, a, b):
    x = c.submit(inc, 1)
    await c.restart()
    y = c.submit(inc, 1)
    del x
    import gc

    gc.collect()
    await asyncio.sleep(0.1)
    await y


@pytest.mark.slow
@pytest.mark.flaky(condition=MACOS, reruns=10, reruns_delay=5)
@gen_cluster(client=True, timeout=60, active_rpc_timeout=10)
async def test_broken_worker_during_computation(c, s, a, b):
    s.allowed_failures = 100
    n = await Nanny(s.address, nthreads=2, loop=s.loop)

    start = time()
    while len(s.nthreads) < 3:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    N = 256
    expected_result = N * (N + 1) // 2
    i = 0
    L = c.map(inc, range(N), key=["inc-%d-%d" % (i, j) for j in range(N)])
    while len(L) > 1:
        i += 1
        L = c.map(
            slowadd,
            *zip(*partition_all(2, L)),
            key=["add-%d-%d" % (i, j) for j in range(len(L) // 2)],
        )

    await asyncio.sleep(random.random() / 20)
    with suppress(CommClosedError):  # comm will be closed abrupty
        await c._run(os._exit, 1, workers=[n.worker_address])

    await asyncio.sleep(random.random() / 20)
    while len(s.workers) < 3:
        await asyncio.sleep(0.01)

    with suppress(
        CommClosedError, EnvironmentError
    ):  # perhaps new worker can't be contacted yet
        await c._run(os._exit, 1, workers=[n.worker_address])

    [result] = await c.gather(L)
    assert isinstance(result, int)
    assert result == expected_result

    await n.close()


@pytest.mark.xfail(COMPILED, reason="Fails with cythonized scheduler")
@gen_cluster(client=True, Worker=Nanny, timeout=60)
async def test_restart_during_computation(c, s, a, b):
    xs = [delayed(slowinc)(i, delay=0.01) for i in range(50)]
    ys = [delayed(slowinc)(i, delay=0.01) for i in xs]
    zs = [delayed(slowadd)(x, y, delay=0.01) for x, y in zip(xs, ys)]
    total = delayed(sum)(zs)
    result = c.compute(total)

    await asyncio.sleep(0.5)
    assert s.rprocessing
    await c.restart()
    assert not s.rprocessing

    assert len(s.nthreads) == 2
    assert not s.tasks


class SlowTransmitData:
    def __init__(self, data, delay=0.1):
        self.delay = delay
        self.data = data

    def __reduce__(self):
        import time

        time.sleep(self.delay)
        return (SlowTransmitData, (self.delay,))

    def __sizeof__(self) -> int:
        # Ensure this is offloaded to avoid blocking loop
        import dask
        from dask.utils import parse_bytes

        return parse_bytes(dask.config.get("distributed.comm.offload")) + 1


@gen_cluster(client=True)
async def test_worker_who_has_clears_after_failed_connection(c, s, a, b):
    """This test is very sensitive to cluster state consistency. Timeouts often
    indicate subtle deadlocks. Be mindful when marking flaky/repeat/etc."""
    n = await Nanny(s.address, nthreads=2, loop=s.loop)

    while len(s.nthreads) < 3:
        await asyncio.sleep(0.01)

    def slow_ser(x, delay):
        return SlowTransmitData(x, delay=delay)

    n_worker_address = n.worker_address
    futures = c.map(
        slow_ser,
        range(20),
        delay=0.1,
        key=["f%d" % i for i in range(20)],
        workers=[n_worker_address],
        allow_other_workers=True,
    )

    def sink(*args):
        pass

    await wait(futures)
    result_fut = c.submit(sink, futures, workers=a.address)

    with suppress(CommClosedError):
        await c._run(os._exit, 1, workers=[n_worker_address])

    while len(s.workers) > 2:
        await asyncio.sleep(0.01)

    await result_fut

    assert not a.has_what.get(n_worker_address)
    assert not any(n_worker_address in s for ts in a.tasks.values() for s in ts.who_has)

    await n.close()


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1), ("127.0.0.1", 2), ("127.0.0.1", 3)],
)
async def test_worker_same_host_replicas_missing(c, s, a, b, x):
    # See GH4784
    def mock_address_host(addr):
        # act as if A and X are on the same host
        nonlocal a, b, x
        if addr in [a.address, x.address]:
            return "A"
        else:
            return "B"

    with mock.patch("distributed.worker.get_address_host", mock_address_host):
        futures = c.map(
            slowinc,
            range(20),
            delay=0.1,
            key=["f%d" % i for i in range(20)],
            workers=[a.address],
            allow_other_workers=True,
        )
        await wait(futures)

        # replicate data to avoid the scheduler retriggering the computation
        # retriggering cleans up the state nicely but doesn't reflect real world
        # scenarios where there may be replicas on the cluster, e.g. they are
        # replicated as a dependency somewhere else
        await c.replicate(futures, n=2, workers=[a.address, b.address])

        def sink(*args):
            pass

        # Since A and X are mocked to be co-located, X will consistently pick A
        # to fetch data from. It will never succeed since we're removing data
        # artificially, without notifying the scheduler.
        # This can only succeed if B handles the missing data properly by
        # removing A from the known sources of keys
        a.handle_free_keys(keys=["f1"], stimulus_id="Am I evil?")  # Yes, I am!
        result_fut = c.submit(sink, futures, workers=x.address)

        await result_fut


@pytest.mark.slow
@gen_cluster(client=True, timeout=60, Worker=Nanny, nthreads=[("127.0.0.1", 1)])
async def test_restart_timeout_on_long_running_task(c, s, a):
    with captured_logger("distributed.scheduler") as sio:
        future = c.submit(sleep, 3600)
        await asyncio.sleep(0.1)
        await c.restart()

    text = sio.getvalue()
    assert "timeout" not in text.lower()


@pytest.mark.slow
@gen_cluster(client=True, scheduler_kwargs={"worker_ttl": "500ms"})
async def test_worker_time_to_live(c, s, a, b):
    from distributed.scheduler import heartbeat_interval

    # worker removal is also controlled by 10 * heartbeat
    assert set(s.workers) == {a.address, b.address}
    interval = 10 * heartbeat_interval(len(s.workers)) + 0.5

    a.periodic_callbacks["heartbeat"].stop()
    await asyncio.sleep(0.010)
    assert set(s.workers) == {a.address, b.address}

    start = time()
    while set(s.workers) == {a.address, b.address}:
        await asyncio.sleep(interval)
        assert time() < start + interval + 0.1

    set(s.workers) == {b.address}


@gen_cluster()
async def test_forget_data_not_supposed_to_have(s, a, b):
    """
    If a depednecy fetch finishes on a worker after the scheduler already
    released everything, the worker might be stuck with a redundant replica
    which is never cleaned up.
    """
    # FIXME: Replace with "blackbox test" which shows an actual example where
    # this situation is provoked if this is even possible.
    # If this cannot be constructed, the entire superfuous_data handler and its
    # corresponding pieces on the scheduler side may be removed
    from distributed.worker import TaskState

    ts = TaskState("key")
    ts.state = "flight"
    a.tasks["key"] = ts
    recommendations = {ts: ("memory", 123)}
    a.transitions(recommendations, stimulus_id="test")

    assert a.data
    while a.data:
        await asyncio.sleep(0.001)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1) for _ in range(3)],
    config={"distributed.comm.timeouts.connect": "1s"},
    Worker=Nanny,
)
async def test_failing_worker_with_additional_replicas_on_cluster(c, s, *workers):
    """
    If a worker detects a missing dependency, the scheduler is notified. If no
    other replica is available, the dependency is rescheduled. A reschedule
    typically causes a lot of state to be reset. However, if another replica is
    available, we'll need to ensure that the worker can detect outdated state
    and correct its state.
    """

    def slow_transfer(x, delay=0.1):
        return SlowTransmitData(x, delay=delay)

    def dummy(*args, **kwargs):
        return

    import psutil

    proc = psutil.Process(workers[1].pid)
    f1 = c.submit(
        slow_transfer,
        1,
        key="f1",
        workers=[workers[0].worker_address],
    )
    # We'll schedule tasks on two workers, s.t. f1 is replicated. We will
    # suspend one of the workers and kill the origin worker of f1 such that a
    # comm failure causes the worker to handle a missing dependency. It will ask
    # the schedule such that it knows that a replica is available on f2 and
    # reschedules the fetch
    f2 = c.submit(dummy, f1, pure=False, key="f2", workers=[workers[1].worker_address])
    f3 = c.submit(dummy, f1, pure=False, key="f3", workers=[workers[2].worker_address])

    await wait(f1)
    proc.suspend()

    await wait(f3)
    await workers[0].close()

    proc.resume()
    await c.gather([f1, f2, f3])
