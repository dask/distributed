import asyncio

import pytest

import distributed
from distributed import Event, Lock, Worker
from distributed.client import wait
from distributed.utils_test import (
    BlockedGetData,
    _LockedCommPool,
    assert_story,
    gen_cluster,
    inc,
    slowinc,
    wait_for_state,
)


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
    ev = Event()

    def f(ev):
        ev.wait()
        return 123

    fut = c.submit(f, ev, key="f1", workers=[a.worker_address])
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_cancelled(fut.key, a)

    # While the first worker is still trying to compute f1, we'll resubmit it to
    # another worker with a smaller delay. The key is still the same
    fut = c.submit(inc, 1, key="f1", workers=[b.worker_address])
    # then, a must switch the execute to fetch. Instead of doing so, it will
    # simply re-use the currently computing result.
    fut = c.submit(inc, fut, workers=[a.worker_address], key="f2")
    await wait_for_state("f2", "waiting", a)
    await ev.set()
    assert await fut == 124  # It would be 3 if the result was copied from b
    del fut
    while "f1" in a.tasks:
        await asyncio.sleep(0.01)

    assert_story(
        a.story("f1"),
        [
            ("f1", "compute-task", "released"),
            ("f1", "released", "waiting", "waiting", {"f1": "ready"}),
            ("f1", "waiting", "ready", "ready", {"f1": "executing"}),
            ("f1", "ready", "executing", "executing", {}),
            ("free-keys", ("f1",)),
            ("f1", "executing", "released", "cancelled", {}),
            ("f1", "cancelled", "fetch", "resumed", {}),
            ("f1", "resumed", "memory", "memory", {"f2": "ready"}),
            ("free-keys", ("f1",)),
            ("f1", "memory", "released", "released", {}),
            ("f1", "released", "forgotten", "forgotten", {}),
        ],
    )


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


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_flight_to_executing_via_cancelled_resumed(c, s, b):

    block_get_data = asyncio.Lock()
    block_compute = Lock()
    enter_get_data = asyncio.Event()
    await block_get_data.acquire()

    class BrokenWorker(Worker):
        async def get_data(self, comm, *args, **kwargs):
            enter_get_data.set()
            async with block_get_data:
                comm.abort()

    def blockable_compute(x, lock):
        with lock:
            return x + 1

    async with BrokenWorker(s.address) as a:
        await c.wait_for_workers(2)
        fut1 = c.submit(
            blockable_compute,
            1,
            lock=block_compute,
            workers=[a.address],
            allow_other_workers=True,
            key="fut1",
        )
        fut2 = c.submit(inc, fut1, workers=[b.address], key="fut2")

        await enter_get_data.wait()
        await block_compute.acquire()

        # Close in scheduler to ensure we transition and reschedule task properly
        await s.close_worker(worker=a.address, stimulus_id="test")
        await wait_for_state(fut1.key, "resumed", b)

        block_get_data.release()
        await block_compute.release()
        assert await fut2 == 3

        b_story = b.story(fut1.key)
        assert any("receive-dep-failed" in msg for msg in b_story)
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
    # Queue up another task to ensure this is not affected by our error handling
    fut2 = c.submit(inc, 1)
    await wait_for_state(fut2.key, "ready", w)

    fut.release()
    await wait_for_state(fut.key, "cancelled", w)
    await lock.release()

    # At this point we do not fetch the result of the future since the future
    # itself would raise a cancelled exception. At this point we're concerned
    # about the worker. The task should transition over error to be eventually
    # forgotten since we no longer hold a ref.
    while fut.key in w.tasks:
        await asyncio.sleep(0.01)

    assert await fut2 == 2
    # Everything should still be executing as usual after this
    await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))

    # Everything above this line should be generically true, regardless of
    # refactoring. Below verifies some implementation specific test assumptions

    story = w.story(fut.key)
    start_finish = [(msg[1], msg[2], msg[3]) for msg in story if len(msg) == 7]
    assert ("executing", "released", "cancelled") in start_finish
    assert ("cancelled", "error", "error") in start_finish
    assert ("error", "released", "released") in start_finish


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_flight_cancelled_error(c, s, b):
    """One worker with one thread. We provoke an flight->cancelled transition
    and let the task err."""
    lock = asyncio.Lock()
    await lock.acquire()

    class BrokenWorker(Worker):
        block_get_data = True

        async def get_data(self, comm, *args, **kwargs):
            if self.block_get_data:
                async with lock:
                    comm.abort()
            return await super().get_data(comm, *args, **kwargs)

    async with BrokenWorker(s.address) as a:
        await c.wait_for_workers(2)
        fut1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
        fut2 = c.submit(inc, fut1, workers=[b.address])
        await wait_for_state(fut1.key, "flight", b)
        fut2.release()
        fut1.release()
        await wait_for_state(fut1.key, "cancelled", b)
        lock.release()
        # At this point we do not fetch the result of the future since the
        # future itself would raise a cancelled exception. At this point we're
        # concerned about the worker. The task should transition over error to
        # be eventually forgotten since we no longer hold a ref.
        while fut1.key in b.tasks:
            await asyncio.sleep(0.01)
        a.block_get_data = False
        # Everything should still be executing as usual after this
        assert await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_in_flight_lost_after_resumed(c, s, b):
    lock_executing = Lock()

    def block_execution(lock):
        lock.acquire()
        return 1

    async with BlockedGetData(s.address) as a:
        fut1 = c.submit(
            block_execution,
            lock_executing,
            workers=[a.address],
            key="fut1",
        )
        # Ensure fut1 is in memory but block any further execution afterwards to
        # ensure we control when the recomputation happens
        await wait(fut1)
        fut2 = c.submit(inc, fut1, workers=[b.address], key="fut2")

        # This ensures that B already fetches the task, i.e. after this the task
        # is guaranteed to be in flight
        await a.in_get_data.wait()
        assert fut1.key in b.state.tasks
        assert b.state.tasks[fut1.key].state == "flight"

        s.set_restrictions({fut1.key: [a.address, b.address]})
        # It is removed, i.e. get_data is guaranteed to fail and f1 is scheduled
        # to be recomputed on B
        await s.remove_worker(a.address, stimulus_id="foo", close=False, safe=True)

        while not b.state.tasks[fut1.key].state == "resumed":
            await asyncio.sleep(0.01)

        fut1.release()
        fut2.release()

        while not b.state.tasks[fut1.key].state == "cancelled":
            await asyncio.sleep(0.01)

        a.block_get_data.set()
        while b.state.tasks:
            await asyncio.sleep(0.01)

    assert_story(
        b.state.story(fut1.key),
        expect=[
            # The initial free-keys is rejected
            ("free-keys", (fut1.key,)),
            (fut1.key, "resumed", "released", "cancelled", {}),
            # After gather_dep receives the data, the task is forgotten
            (fut1.key, "cancelled", "memory", "released", {fut1.key: "forgotten"}),
        ],
    )


@gen_cluster(client=True)
async def test_cancelled_error(c, s, a, b):
    executing = Event()
    lock_executing = Lock()
    await lock_executing.acquire()

    def block_execution(event, lock):
        event.set()
        with lock:
            raise RuntimeError()

    fut1 = c.submit(
        block_execution,
        executing,
        lock_executing,
        workers=[b.address],
        allow_other_workers=True,
        key="fut1",
    )

    await executing.wait()
    assert b.tasks[fut1.key].state == "executing"
    fut1.release()
    while b.tasks[fut1.key].state == "executing":
        await asyncio.sleep(0.01)
    await lock_executing.release()
    while b.tasks:
        await asyncio.sleep(0.01)

    assert_story(
        b.story(fut1.key),
        [
            (fut1.key, "executing", "released", "cancelled", {}),
            (fut1.key, "cancelled", "error", "error", {fut1.key: "released"}),
        ],
    )


@gen_cluster(client=True, nthreads=[("", 1, {"resources": {"A": 1}})])
async def test_cancelled_error_with_resources(c, s, a):
    executing = Event()
    lock_executing = Lock()
    await lock_executing.acquire()

    def block_execution(event, lock):
        event.set()
        with lock:
            raise RuntimeError()

    fut1 = c.submit(
        block_execution,
        executing,
        lock_executing,
        resources={"A": 1},
        key="fut1",
    )

    await executing.wait()
    fut2 = c.submit(inc, 1, resources={"A": 1}, key="fut2")

    while fut2.key not in a.tasks:
        await asyncio.sleep(0.01)

    fut1.release()
    while a.tasks[fut1.key].state == "executing":
        await asyncio.sleep(0.01)
    await lock_executing.release()

    assert await fut2 == 2


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_cancelled_resumed_after_flight_with_dependencies(c, s, w2, w3):
    # See https://github.com/dask/distributed/pull/6327#discussion_r872231090
    block_get_data_1 = asyncio.Lock()
    enter_get_data_1 = asyncio.Event()
    await block_get_data_1.acquire()

    class BlockGetDataWorker(Worker):
        def __init__(self, *args, get_data_event, get_data_lock, **kwargs):
            self._get_data_event = get_data_event
            self._get_data_lock = get_data_lock
            super().__init__(*args, **kwargs)

        async def get_data(self, comm, *args, **kwargs):
            self._get_data_event.set()
            async with self._get_data_lock:
                return await super().get_data(comm, *args, **kwargs)

    async with await BlockGetDataWorker(
        s.address,
        get_data_event=enter_get_data_1,
        get_data_lock=block_get_data_1,
        name="w1",
    ) as w1:

        f1 = c.submit(inc, 1, key="f1", workers=[w1.address])
        f2 = c.submit(inc, 2, key="f2", workers=[w1.address])
        f3 = c.submit(sum, [f1, f2], key="f3", workers=[w1.address])

        await wait(f3)
        f4 = c.submit(inc, f3, key="f4", workers=[w2.address])

        await enter_get_data_1.wait()
        s.set_restrictions(
            {
                f1.key: {w3.address},
                f2.key: {w3.address},
                f3.key: {w2.address},
            }
        )
        await s.remove_worker(w1.address, stimulus_id="stim-id")

        await wait_for_state(f3.key, "resumed", w2)
        assert_story(
            w2.log,
            [
                (f3.key, "flight", "released", "cancelled", {}),
                # ...
                (f3.key, "cancelled", "waiting", "resumed", {}),
            ],
        )
    # w1 closed

    assert await f4 == 6


@pytest.mark.parametrize("wait_for_processing", [True, False])
@pytest.mark.parametrize("raise_error", [True, False])
@gen_cluster(client=True)
async def test_resumed_cancelled_handle_compute(
    c, s, a, b, raise_error, wait_for_processing
):
    """
    Given the history of a task
    Executing -> Cancelled -> Fetch -> Resumed

    A handle_compute should properly restore executing
    """
    # This test is heavily using set_restrictions to simulate certain scheduler
    # decisions of placing keys

    lock_compute = Lock()
    await lock_compute.acquire()
    enter_compute = Event()

    def block(x, lock, enter_event):
        enter_event.set()
        with lock:
            if raise_error:
                raise RuntimeError("test error")
            else:
                return x + 1

    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[a.address])
    f3 = c.submit(
        block,
        f2,
        lock=lock_compute,
        enter_event=enter_compute,
        key="f3",
        workers=[b.address],
    )

    f4 = c.submit(sum, [f1, f3], workers=[b.address])

    await enter_compute.wait()

    async def release_all_futures():
        futs = [f1, f2, f3, f4]
        for fut in futs:
            fut.release()

        while any(fut.key in s.tasks for fut in futs):
            await asyncio.sleep(0.05)

    await release_all_futures()
    await wait_for_state(f3.key, "cancelled", b)

    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[a.address])
    f3 = c.submit(inc, f2, key="f3", workers=[a.address])
    f4 = c.submit(sum, [f1, f3], workers=[b.address])

    await wait_for_state(f3.key, "resumed", b)
    await release_all_futures()

    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[a.address])
    f3 = c.submit(inc, f2, key="f3", workers=[b.address])
    f4 = c.submit(sum, [f1, f3], workers=[b.address])

    if wait_for_processing:
        await wait_for_state(f3.key, "processing", s)

    await lock_compute.release()

    if not raise_error:
        assert await f4 == 4 + 2

        assert_story(
            b.story(f3.key),
            expect=[
                (f3.key, "ready", "executing", "executing", {}),
                (f3.key, "executing", "released", "cancelled", {}),
                (f3.key, "cancelled", "fetch", "resumed", {}),
                (f3.key, "resumed", "memory", "memory", {}),
            ],
        )

    else:
        with pytest.raises(RuntimeError, match="test error"):
            await f3

        assert_story(
            b.story(f3.key),
            expect=[
                (f3.key, "ready", "executing", "executing", {}),
                (f3.key, "executing", "released", "cancelled", {}),
                (f3.key, "cancelled", "fetch", "resumed", {}),
                (f3.key, "resumed", "error", "error", {}),
            ],
        )
