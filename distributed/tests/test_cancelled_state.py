import asyncio
from unittest import mock

import distributed
from distributed.core import CommClosedError
from distributed.utils_test import _LockedCommPool, gen_cluster, inc, slowinc


async def wait_for_state(key, state, dask_worker):
    while key not in dask_worker.tasks or dask_worker.tasks[key].state != state:
        await asyncio.sleep(0.005)


async def wait_for_cancelled(key, dask_worker):
    while key in dask_worker.tasks:
        if dask_worker.tasks[key].state == "cancelled":
            return
        await asyncio.sleep(0.005)
    assert False


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_release(c, s, a):
    fut = c.submit(slowinc, 1, delay=1)
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_cancelled(fut.key, a)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_reschedule(c, s, a):
    fut = c.submit(slowinc, 1, delay=1)
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_cancelled(fut.key, a)
    fut = c.submit(slowinc, 1, delay=0.1)
    await fut


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_add_as_dependency(c, s, a):
    fut = c.submit(slowinc, 1, delay=1)
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_cancelled(fut.key, a)

    fut = c.submit(slowinc, 1, delay=1)
    fut = c.submit(slowinc, fut, delay=1)
    await fut


@gen_cluster(client=True)
async def test_abort_execution_to_fetch(c, s, a, b):
    fut = c.submit(slowinc, 1, delay=2, key="f1", workers=[a.worker_address])
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_cancelled(fut.key, a)

    # While the first worker is still trying to compute f1, we'll resubmit it to
    # another worker with a smaller delay. The key is still the same
    fut = c.submit(inc, 1, key="f1", workers=[b.worker_address])
    # then, a must switch the execute to fetch. Instead of doing so, it will
    # simply re-use the currently computing result.
    fut = c.submit(inc, fut, workers=[a.worker_address], key="f2")
    await fut


@gen_cluster(client=True)
async def test_worker_find_missing(c, s, a, b):
    fut = c.submit(inc, 1, workers=[a.address])
    await fut
    # We do not want to use proper API since it would ensure that the cluster is
    # informed properly
    del a.data[fut.key]
    del a.tasks[fut.key]

    # Actually no worker has the data; the scheduler is supposed to reschedule
    assert await c.submit(inc, fut, workers=[b.address]) == 3


@gen_cluster(client=True)
async def test_worker_stream_died_during_comm(c, s, a, b):
    write_queue = asyncio.Queue()
    write_event = asyncio.Event()
    b.rpc = _LockedCommPool(
        b.rpc,
        write_queue=write_queue,
        write_event=write_event,
    )
    fut = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
    await fut
    # Actually no worker has the data; the scheduler is supposed to reschedule
    res = c.submit(inc, fut, workers=[b.address])

    await write_queue.get()
    await a.close()
    write_event.set()

    await res
    assert any("receive-dep-failed" in msg for msg in b.log)


@gen_cluster(client=True)
async def test_flight_to_executing_via_cancelled_resumed(c, s, a, b):
    lock = asyncio.Lock()
    await lock.acquire()

    async def wait_and_raise(*args, **kwargs):
        async with lock:
            raise CommClosedError()

    with mock.patch.object(
        distributed.worker,
        "get_data_from_worker",
        side_effect=wait_and_raise,
    ):
        fut1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
        fut2 = c.submit(inc, fut1, workers=[b.address])

        await wait_for_state(fut1.key, "flight", b)

        # Close in scheduler to ensure we transition and reschedule task properly
        await s.close_worker(worker=a.address)
        await wait_for_state(fut1.key, "resumed", b)

    lock.release()
    assert await fut2 == 3

    b_story = b.story(fut1.key)
    assert any("receive-dep-failed" in msg for msg in b_story)
    assert any("missing-dep" in msg for msg in b_story)
    assert any("cancelled" in msg for msg in b_story)
    assert any("resumed" in msg for msg in b_story)
