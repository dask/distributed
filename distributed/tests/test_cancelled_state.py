import asyncio
from unittest import mock

import pytest

import distributed
from distributed import Worker
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


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_executing_cancelled_error(c, s, w):
    """One worker with one thread. We provoke an executing->cancelled transition
    and let the task err. This test ensures that there is no residual state
    (e.g. a semaphore) left blocking the thread"""
    lock = distributed.Lock()
    await lock.acquire()

    async def wait_and_raise(*args, **kwargs):
        async with lock:
            raise RuntimeError()

    fut = c.submit(wait_and_raise)
    await wait_for_state(fut.key, "executing", w)

    fut.release()
    await wait_for_state(fut.key, "cancelled", w)
    await lock.release()

    # At this point we do not fetch the result of the future since the future
    # itself would raise a cancelled exception. At this point we're concerned
    # about the worker. The task should transition over error to be eventually
    # forgotten since we no longer hold a ref.
    while fut.key in w.tasks:
        await asyncio.sleep(0.01)

    # Everything should still be executing as usual after this
    await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))

    # Everything above this line should be generically true, regardless of
    # refactoring. Below verifies some implementation specific test assumptions

    story = w.story(fut.key)
    start_finish = [(msg[1], msg[2], msg[3]) for msg in story if len(msg) == 7]
    assert ("executing", "released", "cancelled") in start_finish
    assert ("cancelled", "error", "error") in start_finish
    assert ("error", "released", "released") in start_finish


@gen_cluster(client=True)
async def test_flight_cancelled_error(c, s, a, b):
    """One worker with one thread. We provoke an flight->cancelled transition
    and let the task err."""
    lock = asyncio.Lock()
    await lock.acquire()

    async def wait_and_raise(*args, **kwargs):
        async with lock:
            raise RuntimeError()

    with mock.patch.object(
        distributed.worker,
        "get_data_from_worker",
        side_effect=wait_and_raise,
    ):
        fut1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
        fut2 = c.submit(inc, fut1, workers=[b.address])

        await wait_for_state(fut1.key, "flight", b)
        fut2.release()
        fut1.release()
        await wait_for_state(fut1.key, "cancelled", b)

    lock.release()
    # At this point we do not fetch the result of the future since the future
    # itself would raise a cancelled exception. At this point we're concerned
    # about the worker. The task should transition over error to be eventually
    # forgotten since we no longer hold a ref.
    while fut1.key in b.tasks:
        await asyncio.sleep(0.01)

    # Everything should still be executing as usual after this
    await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))


class LargeButForbiddenSerialization:
    def __reduce__(self):
        raise RuntimeError("I will never serialize!")

    def __sizeof__(self) -> int:
        """Ensure this is immediately tried to spill"""
        return 1_000_000_000_000


def test_ensure_spilled_immediately(tmpdir):
    """See also test_value_raises_during_spilling"""
    import sys

    from distributed.spill import SpillBuffer

    mem_target = 1000
    buf = SpillBuffer(tmpdir, target=mem_target)
    buf["key"] = 1

    obj = LargeButForbiddenSerialization()
    assert sys.getsizeof(obj) > mem_target
    with pytest.raises(
        TypeError,
        match=f"Could not serialize object of type {LargeButForbiddenSerialization.__name__}",
    ):
        buf["error"] = obj


@gen_cluster(client=True, nthreads=[])
async def test_value_raises_during_spilling(c, s):
    """See also test_ensure_spilled_immediately"""

    # Use a worker with a default memory limit
    async with Worker(
        s.address,
    ) as w:

        def produce_evil_data():
            return LargeButForbiddenSerialization()

        fut = c.submit(produce_evil_data)

        await wait_for_state(fut.key, "error", w)

        with pytest.raises(
            TypeError,
            match=f"Could not serialize object of type {LargeButForbiddenSerialization.__name__}",
        ):
            await fut

        # Everything should still be executing as usual after this
        await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))
