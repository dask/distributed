from __future__ import annotations

import asyncio

import pytest

import distributed
from distributed import Event, Lock, Worker
from distributed.client import wait
from distributed.utils_test import (
    BlockedExecute,
    BlockedGatherDep,
    BlockedGetData,
    _LockedCommPool,
    assert_story,
    gen_cluster,
    inc,
    lock_inc,
    slowinc,
    wait_for_state,
    wait_for_stimulus,
)
from distributed.worker_state_machine import (
    ComputeTaskEvent,
    Execute,
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    FreeKeysEvent,
    GatherDep,
    GatherDepNetworkFailureEvent,
    GatherDepSuccessEvent,
    TaskFinishedMsg,
)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_release(c, s, a):
    lock = Lock()
    async with lock:
        fut = c.submit(lock_inc, 1, lock=lock)
        await wait_for_state(fut.key, "executing", a)
        fut.release()
        await wait_for_state(fut.key, "released", a)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_reschedule(c, s, a):
    lock = Lock()
    async with lock:
        fut = c.submit(lock_inc, 1, lock=lock)
        await wait_for_state(fut.key, "executing", a)
        fut.release()
        await wait_for_state(fut.key, "released", a)
        fut = c.submit(lock_inc, 1, lock=lock)
    await fut


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_add_as_dependency(c, s, a):
    lock = Lock()
    async with lock:
        fut = c.submit(lock_inc, 1, lock=lock)
        await wait_for_state(fut.key, "executing", a)
        fut.release()
        await wait_for_state(fut.key, "released", a)

        fut = c.submit(lock_inc, 1, lock=lock)
        fut = c.submit(inc, fut)
    await fut


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_abort_execution_to_fetch(c, s, a):
    ev = Event()

    def f(ev):
        ev.wait()
        return 123

    fut = c.submit(f, ev, key="f1", workers=[a.address])
    await wait_for_state(fut.key, "executing", a)
    fut.release()
    await wait_for_state(fut.key, "released", a)

    async with BlockedGetData(s.address) as b:
        # While the first worker is still trying to compute f1, we'll resubmit it to
        # another worker. The key is still the same.
        fut = c.submit(inc, 1, key="f1", workers=[b.address])
        # Then, a must switch the task to fetch, albeit it's still executing in the
        # background.
        fut = c.submit(inc, fut, workers=[a.address], key="f2")
        await wait_for_state("f1", "flight", a)
        await ev.set()
        # The completion of the original compute is ignored
        await asyncio.sleep(0.1)
        assert a.state.tasks["f1"].state == "flight"
        b.block_get_data.set()

        # It would be 124 if the result was from the original compute
        assert await fut == 3
        del fut
        while "f1" in a.state.tasks:
            await asyncio.sleep(0.01)


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
    assert any("receive-dep-failed" in msg for msg in b.state.log)


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
        await wait(fut1)
        await block_compute.acquire()
        fut2 = c.submit(inc, fut1, workers=[b.address], key="fut2")

        await enter_get_data.wait()

        # Close in scheduler to ensure we transition and reschedule task properly
        await s.remove_worker(a.address, stimulus_id="test", close=False)
        await wait_for_stimulus(ComputeTaskEvent, b, key="fut1")

        block_get_data.release()
        await block_compute.release()
        assert await fut2 == 3

        assert_story(
            b.state.story("fut1"),
            [
                ("fut1", "flight", "released", "released", {"fut1": "forgotten"}),
                ("fut1", "released", "forgotten", "released", {}),
                ("fut1", "released", "waiting", "waiting", {"fut1": "ready"}),
                ("fut1", "waiting", "ready", "ready", {"fut1": "executing"}),
                ("fut1", "ready", "executing", "executing", {}),
                ("receive-dep-failed", a.address, {"fut1"}),
                ("fut1", "executing", "memory", "memory", {}),
            ],
        )


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

    f1 = c.submit(wait_and_raise, key="f1")
    await wait_for_state(f1.key, "executing", w)
    # Queue up another task to ensure this is not affected by our error handling
    f2 = c.submit(inc, 1, key="f2")
    await wait_for_state(f2.key, "ready", w)

    f1.release()
    await wait_for_state(f1.key, "released", w)
    await lock.release()

    # At this point we do not fetch the result of the future since the future
    # itself would raise a cancelled exception. At this point we're concerned
    # about the worker. The task should transition over error to be eventually
    # forgotten since we no longer hold a ref.
    while f1.key in w.state.tasks:
        await asyncio.sleep(0.01)

    assert await f2 == 2
    # Everything should still be executing as usual after this
    assert await c.submit(sum, c.map(inc, range(10))) == sum(map(inc, range(10)))

    # Everything above this line should be generically true, regardless of
    # refactoring. Below verifies some implementation specific test assumptions

    assert_story(
        w.state.story(f1.key),
        [
            (f1.key, "executing", "released", "released", {f1.key: "forgotten"}),
            # Task can't be released while it's still running
            (f1.key, "released", "forgotten", "released", {}),
            # Task completion event arrived
            (f1.key, "released", "forgotten", "forgotten", {}),
        ],
    )


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_flight_cancelled_error(c, s, b):
    """One worker with one thread. We provoke a flight->cancelled transition
    and let the task err.
    """
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
        await wait_for_state(fut1.key, "released", b)
        lock.release()
        # At this point we do not fetch the result of the future since the
        # future itself would raise a cancelled exception. At this point we're
        # concerned about the worker. The task should transition over error to
        # be eventually forgotten since we no longer hold a ref.
        while fut1.key in b.state.tasks:
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
    assert b.state.tasks[fut1.key].state == "executing"
    fut1.release()
    while b.state.tasks[fut1.key].state == "executing":
        await asyncio.sleep(0.01)
    await lock_executing.release()
    while b.state.tasks:
        await asyncio.sleep(0.01)

    assert_story(
        b.state.story(fut1.key),
        [
            (fut1.key, "executing", "released", "released", {fut1.key: "forgotten"}),
            (fut1.key, "released", "forgotten", "released", {}),
            (fut1.key, "released", "forgotten", "forgotten", {}),
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

    while fut2.key not in a.state.tasks:
        await asyncio.sleep(0.01)

    fut1.release()
    while a.state.tasks[fut1.key].state == "executing":
        await asyncio.sleep(0.01)
    await lock_executing.release()

    assert await fut2 == 2


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_cancelled_resumed_after_flight_with_dependencies(c, s, a):
    """A task is in flight from b to a.
    While a is waiting, b dies. The scheduler notices before a and reschedules the
    task on a itself (as the only surviving replica was just lost).
    Test that the worker eventually computes the task.

    See https://github.com/dask/distributed/pull/6327#discussion_r872231090
    See test_cancelled_resumed_after_flight_with_dependencies_workerstate below.
    """
    async with await BlockedGetData(s.address) as b:
        x = c.submit(inc, 1, key="x", workers=[b.address], allow_other_workers=True)
        y = c.submit(inc, x, key="y", workers=[a.address])
        await b.in_get_data.wait()

        # Make b dead to s, but not to a
        await s.remove_worker(b.address, stimulus_id="stim-id")

        # Wait for the scheduler to reschedule x on a.
        # We want the comms from the scheduler to reach a before b closes the RPC
        # channel, causing a.gather_dep() to raise OSError.
        await wait_for_stimulus(ComputeTaskEvent, a, key="x")

    # b closed; a.gather_dep() fails.
    assert await y == 3


def test_cancelled_resumed_after_flight_with_dependencies_workerstate(ws):
    """Same as test_cancelled_resumed_after_flight_with_dependencies, but testing the
    WorkerState in isolation
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        # Create task x and put it in flight from ws2
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        # The scheduler realises that ws2 is unresponsive, although ws doesn't know yet.
        # Having lost the last surviving replica of x, the scheduler cancels all of its
        # dependents. This also cancels x.
        FreeKeysEvent(keys=["y"], stimulus_id="s2"),
        # The scheduler reschedules x on another worker, which just happens to be one
        # that was previously fetching it. This immediately generates an Execute
        # instruction, even if the GatherDep instruction isn't complete yet.
        ComputeTaskEvent.dummy("x", stimulus_id="s3"),
        # After ~30s, the TCP socket with ws2 finally times out and collapses.
        # This results in a no-op.
        GatherDepNetworkFailureEvent(worker=ws2, total_nbytes=1, stimulus_id="s4"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s1"),
        Execute(key="x", stimulus_id="s3"),
    ]
    assert ws.tasks["x"].state == "executing"


@pytest.mark.parametrize("wait_for_processing", [True, False])
@pytest.mark.parametrize("raise_error", [True, False])
@gen_cluster(client=True)
async def test_resumed_cancelled_handle_compute(
    c, s, a, b, raise_error, wait_for_processing
):
    """
    Given the history of a task
    executing -> cancelled -> resumed(fetch)

    A handle_compute should properly restore executing.
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

    f4 = c.submit(sum, [f1, f3], key="f4", workers=[b.address])

    await enter_compute.wait()

    async def release_all_futures():
        futs = [f1, f2, f3, f4]
        for fut in futs:
            fut.release()

        while any(fut.key in s.tasks for fut in futs):
            await asyncio.sleep(0.05)

    await release_all_futures()
    await wait_for_state(f3.key, "released", b)

    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[a.address])
    f3 = c.submit(inc, f2, key="f3", workers=[a.address])
    f4 = c.submit(sum, [f1, f3], key="f4", workers=[b.address])

    await wait_for_state(f3.key, "memory", b)
    await release_all_futures()

    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[a.address])
    f3 = c.submit(inc, f2, key="f3", workers=[b.address])
    f4 = c.submit(sum, [f1, f3], key="f4", workers=[b.address])

    if wait_for_processing:
        await wait_for_state(f3.key, "processing", s)

    await lock_compute.release()

    if not raise_error:
        assert await f4 == 4 + 2

        assert_story(
            b.state.story(f3.key),
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
            b.state.story(f3.key),
            expect=[
                (f3.key, "ready", "executing", "executing", {}),
                (f3.key, "executing", "released", "cancelled", {}),
                (f3.key, "cancelled", "fetch", "resumed", {}),
                (f3.key, "resumed", "error", "error", {}),
            ],
        )


@pytest.mark.parametrize("intermediate_state", ["resumed", "cancelled"])
@pytest.mark.parametrize("close_worker", [False, True])
@gen_cluster(client=True, config={"distributed.comm.timeouts.connect": "500ms"})
async def test_deadlock_cancelled_after_inflight_before_gather_from_worker(
    c, s, a, x, intermediate_state, close_worker
):
    """If a task was transitioned to in-flight, the gather_dep coroutine was scheduled
    but a cancel request came in before gather_data_from_worker was issued. This might
    corrupt the state machine if the cancelled key is not properly handled.

    See also
    --------
    test_workerstate_deadlock_cancelled_after_inflight_before_gather_from_worker
    """
    fut1 = c.submit(slowinc, 1, workers=[a.address], key="f1")
    fut1B = c.submit(slowinc, 2, workers=[x.address], key="f1B")
    fut2 = c.submit(sum, [fut1, fut1B], workers=[x.address], key="f2")
    await fut2

    async with BlockedGatherDep(s.address, name="b") as b:
        fut3 = c.submit(inc, fut2, workers=[b.address], key="f3")

        await wait_for_state(fut2.key, "flight", b)

        s.set_restrictions(worker={fut1B.key: a.address, fut2.key: b.address})

        await b.in_gather_dep.wait()

        await s.remove_worker(
            address=x.address,
            safe=True,
            close=close_worker,
            stimulus_id="remove-worker",
        )

        await wait_for_state(fut2.key, intermediate_state, b, interval=0)

        b.block_gather_dep.set()
        await fut3


def test_workerstate_executing_to_executing(ws_with_running_task):
    """Test state loops:

    - executing -> cancelled -> executing
    - executing -> long-running -> cancelled -> long-running

    Test that the task immediately reverts to its original state.
    """
    ws = ws_with_running_task
    ts = ws.tasks["x"]
    prev_state = ts.state

    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("x", resource_restrictions={"R": 1}, stimulus_id="s2"),
    )
    assert not instructions
    assert ws.tasks["x"] is ts
    assert ts.state == prev_state


def test_workerstate_flight_to_flight(ws):
    """Test state loop:

    flight -> cancelled -> flight

    Test that the task immediately reverts to its original state.
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        FreeKeysEvent(keys=["y", "x"], stimulus_id="s2"),
        ComputeTaskEvent.dummy("z", who_has={"x": [ws2]}, stimulus_id="s3"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s1")
    ]
    assert ws.tasks["x"].state == "flight"


def test_workerstate_executing_success_to_fetch(ws_with_running_task):
    """Test state loops:

    - executing -> cancelled -> resumed(fetch)
    - executing -> long-running -> cancelled -> resumed(fetch)

    The task execution later terminates successfully.
    Test that the task completion is ignored and the task transitions to flight

    See also: test_workerstate_executing_failure_to_fetch
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        ExecuteSuccessEvent.dummy("x", 123, stimulus_id="s3"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s2")
    ]
    assert ws.tasks["x"].state == "flight"


def test_workerstate_executing_failure_to_fetch(ws_with_running_task):
    """Test state loops:

    - executing -> cancelled -> resumed(fetch)
    - executing -> long-running -> cancelled -> resumed(fetch)

    The task execution later terminates with a failure.
    This is an edge case interaction between work stealing and a task that does not
    deterministically succeed or fail when run multiple times or on different workers.

    Test that the task is fetched from the other worker. This is to avoid having to deal
    with cancelling the dependent, which would require interaction with the scheduler
    and increase the complexity of the use case.

    See also: test_workerstate_executing_success_to_fetch
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        ExecuteFailureEvent.dummy("x", stimulus_id="s3"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s2")
    ]
    assert ws.tasks["x"].state == "flight"


def test_workerstate_flight_skips_executing_on_success(ws):
    """Test state loop

    flight -> cancelled -> resumed(waiting) -> memory

    gather_dep later terminates successfully.
    Test that the task is not executed and is in memory.
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        FreeKeysEvent(keys=["y", "x"], stimulus_id="s2"),
        ComputeTaskEvent.dummy("x", stimulus_id="s3"),
        GatherDepSuccessEvent(
            worker=ws2, total_nbytes=1, data={"x": 123}, stimulus_id="s4"
        ),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s1"),
        TaskFinishedMsg.match(key="x", stimulus_id="s4"),
    ]
    assert ws.tasks["x"].state == "memory"
    assert ws.data["x"] == 123


@pytest.mark.parametrize("block_queue", [False, True])
def test_workerstate_flight_failure_to_executing(ws, block_queue):
    """Test state loop

    flight -> cancelled -> resumed(waiting)

    gather_dep later terminates with a failure.
    Test that the task is executed.
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        FreeKeysEvent(keys=["y", "x"], stimulus_id="s2"),
        ComputeTaskEvent.dummy("x", stimulus_id="s3"),
    )
    assert instructions == [
        GatherDep(stimulus_id="s1", worker=ws2, to_gather={"x"}, total_nbytes=1),
    ]
    assert ws.tasks["x"].state == "resumed"

    if block_queue:
        instructions = ws.handle_stimulus(
            ComputeTaskEvent.dummy(key="z", stimulus_id="s4"),
            GatherDepNetworkFailureEvent(worker=ws2, total_nbytes=1, stimulus_id="s5"),
            ExecuteSuccessEvent.dummy(key="z", stimulus_id="s6"),
        )
        assert instructions == [
            Execute(key="z", stimulus_id="s4"),
            TaskFinishedMsg.match(key="z", stimulus_id="s6"),
            Execute(key="x", stimulus_id="s6"),
        ]
        assert_story(
            ws.story("x"),
            [
                ("x", "resumed", "fetch", "released", {"x": "waiting"}),
                ("x", "released", "waiting", "waiting", {"x": "ready"}),
                ("x", "waiting", "ready", "ready", {}),
            ],
        )

    else:
        instructions = ws.handle_stimulus(
            GatherDepNetworkFailureEvent(worker=ws2, total_nbytes=1, stimulus_id="s5"),
        )
        assert instructions == [
            Execute(key="x", stimulus_id="s5"),
        ]
        assert_story(
            ws.story("x"),
            [
                ("x", "resumed", "fetch", "released", {"x": "waiting"}),
                ("x", "released", "waiting", "waiting", {"x": "ready"}),
                ("x", "waiting", "ready", "ready", {"x": "executing"}),
            ],
        )

    assert ws.tasks["x"].state == "executing"


def test_workerstate_resumed_fetch_to_executing(ws_with_running_task):
    """Test state loops:

    - executing -> cancelled -> resumed(fetch) -> cancelled -> executing
    - executing -> long-running -> cancelled -> resumed(fetch) -> long-running

    See also: test_workerstate_resumed_waiting_to_flight
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    prev_state = ws.tasks["x"].state

    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        FreeKeysEvent(keys=["y", "x"], stimulus_id="s3"),
        ComputeTaskEvent.dummy("x", resource_restrictions={"R": 1}, stimulus_id="s4"),
    )
    assert not instructions
    assert ws.tasks["x"].state == prev_state


def test_workerstate_resumed_waiting_to_flight(ws):
    """Test state loop:

    - flight -> cancelled -> resumed(waiting) -> cancelled -> flight

    See also: test_workerstate_resumed_fetch_to_executing
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        FreeKeysEvent(keys=["y", "x"], stimulus_id="s2"),
        ComputeTaskEvent.dummy("x", resource_restrictions={"R": 1}, stimulus_id="s3"),
        FreeKeysEvent(keys=["x"], stimulus_id="s4"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s5"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, stimulus_id="s1", total_nbytes=1),
    ]
    assert ws.tasks["x"].state == "flight"


@pytest.mark.parametrize("critical_section", ["execute", "deserialize_task"])
@pytest.mark.parametrize("resume_inside_critical_section", [False, True])
@gen_cluster(client=True, nthreads=[])
async def test_execute_preamble_early_cancel_1(
    c, s, critical_section, resume_inside_critical_section
):
    """Test multiple race conditions in the preamble of Worker.execute(), which used to
    cause a task to remain permanently in resumed state or to crash the worker through
    `fail_hard` in case of very tight timings when resuming a task.

    The task is resumed to executing state before Worker.execute() terminates.

    See also
    --------
    https://github.com/dask/distributed/issues/6869
    https://github.com/dask/dask/issues/9330
    test_execute_preamble_early_cancel_2
    test_execute_preamble_early_cancel_3
    test_worker.py::test_execute_preamble_abort_retirement
    """
    async with BlockedExecute(s.address, validate=True) as a:
        if critical_section == "execute":
            in_ev = a.in_execute
            block_ev = a.block_execute
            a.block_deserialize_task.set()
        else:
            assert critical_section == "deserialize_task"
            in_ev = a.in_deserialize_task
            block_ev = a.block_deserialize_task
            a.block_execute.set()

        x = c.submit(inc, 1, key="x")
        await in_ev.wait()

        x.release()
        await wait_for_state("x", "released", a)

        async def resume():
            x = c.submit(inc, 1, key="x")
            await wait_for_state("x", "executing", a)
            return x

        if resume_inside_critical_section:
            x = await resume()

        # Unblock Worker.execute().
        # The method should detect the cancelled status and perform an early exit.
        block_ev.set()
        await a.in_execute_exit.wait()

        if not resume_inside_critical_section:
            x = await resume()

        # Finally let the done_callback of Worker.execute run
        a.block_execute_exit.set()

        # Test that x does not get stuck.
        assert await x == 2


@pytest.mark.parametrize("critical_section", ["execute", "deserialize_task"])
@gen_cluster(client=True, nthreads=[])
async def test_execute_preamble_early_cancel_2(c, s, critical_section):
    """Test multiple race conditions in the preamble of Worker.execute(), which used to
    cause a task to remain permanently in resumed state or to crash the worker through
    `fail_hard` in case of very tight timings when resuming a task.

    The task is resumed to executing state before Worker.execute() terminates.
    On the second execution, the run_spec changes, and some dependencies are no longer
    in memory.

    See also
    --------
    https://github.com/dask/distributed/issues/6869
    https://github.com/dask/dask/issues/9330
    test_execute_preamble_early_cancel_1
    test_execute_preamble_early_cancel_3
    test_worker.py::test_execute_preamble_abort_retirement
    """
    async with BlockedExecute(s.address, validate=True) as a:
        a.block_execute_exit.set()  # Not interesting in this test
        if critical_section == "execute":
            in_ev = a.in_execute
            block_ev = a.block_execute
            a.block_deserialize_task.set()
        else:
            assert critical_section == "deserialize_task"
            in_ev = a.in_deserialize_task
            block_ev = a.block_deserialize_task
            a.block_execute.set()

        x = (await c.scatter({"x": 1}))["x"]
        y = c.submit(inc, x, key="y")
        x.release()
        await in_ev.wait()
        assert a.state.tasks["x"].state == "memory"
        assert a.state.tasks["y"].state == "executing"

        y.release()
        while "x" in a.data:
            await asyncio.sleep(0.01)
        assert a.state.tasks["x"].state == "released"
        assert a.state.tasks["y"].state == "released"
        assert a.state.executing

        y = c.submit(sum, [1], key="y")
        await asyncio.sleep(0.5)
        assert_story(a.state.log, [], strict=True)
        await wait_for_state("y", "executing", a)

        # Unblock Worker.execute().
        # The method should detect that the task is in executing state, but from a
        # different compute-task event, and perform an early exit.
        block_ev.set()

        # Test that the task does not get stuck and that Worker.execute() does not
        # attempt reading x from a.state.data.
        # The output is correct for the new tak that was submitted.
        assert await y == 1

        assert "x" not in a.state.tasks
        assert a.state.tasks["y"].state == "memory"


@pytest.mark.parametrize("critical_section", ["execute", "deserialize_task"])
@pytest.mark.parametrize("resume_inside_critical_section", [False, True])
@gen_cluster(client=True, nthreads=[])
async def test_execute_preamble_early_cancel_3(
    c, s, critical_section, resume_inside_critical_section
):
    """Test multiple race conditions in the preamble of Worker.execute(), which used to
    cause a task to remain permanently in resumed state or to crash the worker through
    `fail_hard` in case of very tight timings when resuming a task.

    There is a recommendation to fetch, which starts Worker.gather_dep() and sends the
    task into flight state before Worker.execute() terminates.

    See also
    --------
    https://github.com/dask/distributed/issues/6869
    https://github.com/dask/dask/issues/9330
    test_execute_preamble_early_cancel_1
    test_execute_preamble_early_cancel_2
    test_worker.py::test_execute_preamble_abort_retirement
    """
    async with BlockedExecute(s.address, validate=True) as a, BlockedGetData(
        s.address, validate=True
    ) as b:
        if critical_section == "execute":
            in_ev = a.in_execute
            block_ev = a.block_execute
            a.block_deserialize_task.set()
        else:
            assert critical_section == "deserialize_task"
            in_ev = a.in_deserialize_task
            block_ev = a.block_deserialize_task
            a.block_execute.set()

        async def resume():
            # Note: it's different from the first run on a
            x = c.submit(inc, 2, key="x", workers=[b.address])
            y = c.submit(inc, x, key="y", workers=[a.address])
            await b.in_get_data.wait()
            assert a.state.tasks["x"].state == "flight"
            return y

        x = c.submit(inc, 1, key="x", workers=[a.address])
        await in_ev.wait()

        x.release()
        await wait_for_state("x", "released", a)

        if resume_inside_critical_section:
            y = await resume()

        # Unblock Worker.execute().
        # The method should detect the cancelled status and perform an early exit.
        block_ev.set()
        await a.in_execute_exit.wait()

        if not resume_inside_critical_section:
            y = await resume()

        ts = a.state.tasks["x"]
        assert a.state.executing == {ts}
        assert a.state.in_flight_tasks == {ts}

        # Finally let the done_callback of Worker.execute run
        a.block_execute_exit.set()
        while a.state.executing:
            await asyncio.sleep(0.1)
        assert a.state.in_flight_tasks == {ts}
        assert ts.state == "flight"

        b.block_get_data.set()

        # Test that x does not get stuck.
        # The output of y is 4 because x on b returned 2;
        # if the output of the execution on a was used instead, it would be 3.
        assert await y == 4


@pytest.mark.parametrize("done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent])
def test_double_release(ws_with_running_task, done_ev_cls):
    """Test transition loop:

    executing ->
    (cancel task) -> released ->
    (resume task) -> executing ->
    (cancel task) -> released ->
    (complete task)
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        FreeKeysEvent(keys=["y"], stimulus_id="s3"),
        done_ev_cls.dummy("x", stimulus_id="s4"),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s2")
    ]
