import asyncio
import logging
from contextlib import contextmanager
from itertools import chain

import pytest

from distributed.core import Status
from distributed.protocol.serialize import Serialize
from distributed.utils import recursive_to_dict
from distributed.utils_test import (
    _LockedCommPool,
    assert_story,
    captured_logger,
    gen_cluster,
    inc,
)
from distributed.worker import Worker
from distributed.worker_state_machine import (
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    Instruction,
    ReleaseWorkerDataMsg,
    RescheduleEvent,
    RescheduleMsg,
    SendMessageToScheduler,
    StateMachineEvent,
    TaskState,
    UniqueTaskHeap,
    merge_recs_instructions,
)


async def wait_for_state(key, state, dask_worker):
    while key not in dask_worker.tasks or dask_worker.tasks[key].state != state:
        await asyncio.sleep(0.005)


def test_TaskState_get_nbytes():
    assert TaskState("x", nbytes=123).get_nbytes() == 123
    # Default to distributed.scheduler.default-data-size
    assert TaskState("y").get_nbytes() == 1024


def test_TaskState__to_dict():
    """Tasks that are listed as dependencies or dependents of other tasks are dumped as
    a short repr and always appear in full directly under Worker.tasks. Uninteresting
    fields are omitted.
    """
    x = TaskState("x", state="memory", done=True)
    y = TaskState("y", priority=(0,), dependencies={x})
    x.dependents.add(y)
    actual = recursive_to_dict([x, y])
    assert actual == [
        {
            "key": "x",
            "state": "memory",
            "done": True,
            "dependents": ["<TaskState 'y' released>"],
        },
        {
            "key": "y",
            "state": "released",
            "dependencies": ["<TaskState 'x' memory>"],
            "priority": [0],
        },
    ]


def test_unique_task_heap():
    heap = UniqueTaskHeap()

    for x in range(10):
        ts = TaskState(f"f{x}", priority=(0,))
        ts.priority = (0, 0, 1, x % 3)
        heap.push(ts)

    heap_list = list(heap)
    # iteration does not empty heap
    assert len(heap) == 10
    assert heap_list == sorted(heap_list, key=lambda ts: ts.priority)

    seen = set()
    last_prio = (0, 0, 0, 0)
    while heap:
        peeked = heap.peek()
        ts = heap.pop()
        assert peeked == ts
        seen.add(ts.key)
        assert ts.priority
        assert last_prio <= ts.priority
        last_prio = last_prio

    ts = TaskState("foo", priority=(0,))
    heap.push(ts)
    heap.push(ts)
    assert len(heap) == 1

    assert repr(heap) == "<UniqueTaskHeap: 1 items>"

    assert heap.pop() == ts
    assert not heap

    # Test that we're cleaning the seen set on pop
    heap.push(ts)
    assert len(heap) == 1
    assert heap.pop() == ts

    assert repr(heap) == "<UniqueTaskHeap: 0 items>"


@pytest.mark.parametrize(
    "cls",
    chain(
        [UniqueTaskHeap],
        Instruction.__subclasses__(),
        SendMessageToScheduler.__subclasses__(),
        StateMachineEvent.__subclasses__(),
    ),
)
def test_slots(cls):
    params = [
        k
        for k in dir(cls)
        if not k.startswith("_")
        and k not in ("op", "handled")
        and not callable(getattr(cls, k))
    ]
    inst = cls(**dict.fromkeys(params))
    assert not hasattr(inst, "__dict__")


def test_sendmsg_to_dict():
    # Arbitrary sample class
    smsg = ReleaseWorkerDataMsg(key="x", stimulus_id="test")
    assert smsg.to_dict() == {
        "op": "release-worker-data",
        "key": "x",
        "stimulus_id": "test",
    }


def test_merge_recs_instructions():
    x = TaskState("x")
    y = TaskState("y")
    instr1 = RescheduleMsg(key="foo", stimulus_id="test")
    instr2 = RescheduleMsg(key="bar", stimulus_id="test")
    assert merge_recs_instructions(
        ({x: "memory"}, [instr1]),
        ({y: "released"}, [instr2]),
    ) == (
        {x: "memory", y: "released"},
        [instr1, instr2],
    )

    # Identical recommendations are silently ignored; incompatible ones raise
    assert merge_recs_instructions(({x: "memory"}, []), ({x: "memory"}, [])) == (
        {x: "memory"},
        [],
    )
    with pytest.raises(ValueError):
        merge_recs_instructions(({x: "memory"}, []), ({x: "released"}, []))


def test_event_to_dict():
    ev = RescheduleEvent(stimulus_id="test", key="x")
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2 == ev
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "RescheduleEvent",
        "stimulus_id": "test",
        "handled": 11.22,
        "key": "x",
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert ev3 == ev


def test_executesuccess_to_dict():
    """The potentially very large ExecuteSuccessEvent.value is not stored in the log"""
    ev = ExecuteSuccessEvent(
        stimulus_id="test",
        key="x",
        value=123,
        start=123.4,
        stop=456.7,
        nbytes=890,
        type=int,
    )
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2.value is None
    assert ev.value == 123
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "ExecuteSuccessEvent",
        "stimulus_id": "test",
        "handled": 11.22,
        "key": "x",
        "value": None,
        "nbytes": 890,
        "start": 123.4,
        "stop": 456.7,
        "type": "<class 'int'>",
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert isinstance(ev3, ExecuteSuccessEvent)
    assert ev3.stimulus_id == "test"
    assert ev3.handled == 11.22
    assert ev3.key == "x"
    assert ev3.value is None
    assert ev3.start == 123.4
    assert ev3.stop == 456.7
    assert ev3.nbytes == 890
    assert ev3.type is None


def test_executefailure_to_dict():
    ev = ExecuteFailureEvent(
        stimulus_id="test",
        key="x",
        start=123.4,
        stop=456.7,
        exception=Serialize(ValueError("foo")),
        traceback=Serialize("lose me"),
        exception_text="exc text",
        traceback_text="tb text",
    )
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2 == ev
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "ExecuteFailureEvent",
        "stimulus_id": "test",
        "handled": 11.22,
        "key": "x",
        "start": 123.4,
        "stop": 456.7,
        "exception": "<Serialize: foo>",
        "traceback": "<Serialize: lose me>",
        "exception_text": "exc text",
        "traceback_text": "tb text",
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert isinstance(ev3, ExecuteFailureEvent)
    assert ev3.stimulus_id == "test"
    assert ev3.handled == 11.22
    assert ev3.key == "x"
    assert ev3.start == 123.4
    assert ev3.stop == 456.7
    assert isinstance(ev3.exception, Serialize)
    assert isinstance(ev3.exception.data, Exception)
    assert ev3.traceback is None
    assert ev3.exception_text == "exc text"
    assert ev3.traceback_text == "tb text"


@gen_cluster(client=True)
async def test_fetch_to_compute(c, s, a, b):
    # Block ensure_communicating to ensure we indeed know that the task is in
    # fetch and doesn't leave it accidentally
    old_out_connections, b.total_out_connections = b.total_out_connections, 0
    old_comm_threshold, b.comm_threshold_bytes = b.comm_threshold_bytes, 0

    f1 = c.submit(inc, 1, workers=[a.address], key="f1", allow_other_workers=True)
    f2 = c.submit(inc, f1, workers=[b.address], key="f2")

    await wait_for_state(f1.key, "fetch", b)
    await a.close()

    b.total_out_connections = old_out_connections
    b.comm_threshold_bytes = old_comm_threshold

    await f2

    assert_story(
        b.log,
        # FIXME: This log should be replaced with an
        # StateMachineEvent/Instruction log
        [
            (f2.key, "compute-task", "released"),
            # This is a "please fetch" request. We don't have anything like
            # this, yet. We don't see the request-dep signal in here because we
            # do not wait for the key to be actually scheduled
            (f1.key, "ensure-task-exists", "released"),
            # After the worker failed, we're instructed to forget f2 before
            # something new comes in
            ("free-keys", (f2.key,)),
            (f1.key, "compute-task", "released"),
            (f1.key, "put-in-memory"),
            (f2.key, "compute-task", "released"),
        ],
    )


@gen_cluster(client=True)
async def test_fetch_via_amm_to_compute(c, s, a, b):
    # Block ensure_communicating to ensure we indeed know that the task is in
    # fetch and doesn't leave it accidentally
    old_out_connections, b.total_out_connections = b.total_out_connections, 0
    old_comm_threshold, b.comm_threshold_bytes = b.comm_threshold_bytes, 0

    f1 = c.submit(inc, 1, workers=[a.address], key="f1", allow_other_workers=True)

    await f1
    s.request_acquire_replicas(b.address, [f1.key], stimulus_id="test")

    await wait_for_state(f1.key, "fetch", b)
    await a.close()

    b.total_out_connections = old_out_connections
    b.comm_threshold_bytes = old_comm_threshold

    await f1


@gen_cluster(client=True)
async def test_cancelled_while_in_flight(c, s, a, b):
    event = asyncio.Event()
    a.rpc = _LockedCommPool(a.rpc, write_event=event)

    x = c.submit(inc, 1, key="x", workers=[b.address])
    y = c.submit(inc, x, key="y", workers=[a.address])
    await wait_for_state("x", "flight", a)
    y.release()
    await wait_for_state("x", "cancelled", a)

    # Let the comm from b to a return the result
    event.set()
    # upon reception, x transitions cancelled->forgotten
    while a.tasks:
        await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_in_memory_while_in_flight(c, s, a, b):
    """
    1. A client scatters x to a
    2. The scheduler does not know about scattered keys until the three-way round-trip
       between client, worker, and scheduler has been completed (see Scheduler.scatter)
    3. In the middle of that handshake, a client (not necessarily the same client) calls
       ``{op: compute-task, key: x}`` on b and then
       ``{op: compute-task, key: y, who_has: {x: [b]}`` on a, which triggers a
       gather_dep call to copy x key from b to a.
    4. while x is in flight from b to a, the scatter finishes, which triggers
       update_data, which in turn transitions x from flight to memory.
    5. later on, gather_dep finishes, but the key is already in memory.
    """
    event = asyncio.Event()
    a.rpc = _LockedCommPool(a.rpc, write_event=event)

    x = c.submit(inc, 1, key="x", workers=[b.address])
    y = c.submit(inc, x, key="y", workers=[a.address])
    await wait_for_state("x", "flight", a)
    a.update_data({"x": 3})
    await wait_for_state("x", "memory", a)

    # Let the comm from b to a return the result
    event.set()
    assert await y == 4  # Data in flight from b has been discarded


@contextmanager
def freeze_data_fetching(w: Worker):
    """Prevent any task from transitioning from fetch to flight on the worker while
    inside the context.
    This is not the same as setting the worker to Status=paused, which would also
    inform the Scheduler and prevent further tasks to be enqueued on the worker.
    """
    old_out_connections = w.total_out_connections
    old_comm_threshold = w.comm_threshold_bytes
    w.total_out_connections = 0
    w.comm_threshold_bytes = 0
    yield
    w.total_out_connections = old_out_connections
    w.comm_threshold_bytes = old_comm_threshold
    # Jump-start ensure_communicating
    w.status = Status.paused
    w.status = Status.running


@pytest.mark.parametrize("as_deps", [False, True])
@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_lose_replica_during_fetch(c, s, w1, w2, w3, as_deps):
    """
    as_deps=True
        0. task x is a dependency of y1 and y2
        1. scheduler calls handle_compute("y1", who_has={"x": [w2, w3]}) on w1
        2. x transitions released -> fetch
        3. the network stack is busy, so x does not transition to flight yet.
        4. scheduler calls handle_compute("y2", who_has={"x": [w3]}) on w1
        5. when x finally reaches the top of the data_needed heap, the w1 will not try
           contacting w2

    as_deps=False
        1. scheduler calls handle_acquire_replicas(who_has={"x": [w2, w3]}) on w1
        2. x transitions released -> fetch
        3. the network stack is busy, so x does not transition to flight yet.
        4. scheduler calls handle_acquire_replicas(who_has={"x": [w3]}) on w1
        5. when x finally reaches the top of the data_needed heap, the w1 will not try
           contacting w2
    """
    x = (await c.scatter({"x": 1}, workers=[w2.address, w3.address], broadcast=True))[
        "x"
    ]
    with freeze_data_fetching(w1):
        if as_deps:
            y1 = c.submit(inc, x, key="y1", workers=[w1.address])
        else:
            s.request_acquire_replicas(w1.address, ["x"], stimulus_id="test")

        await wait_for_state("x", "fetch", w1)
        assert w1.tasks["x"].who_has == {w2.address, w3.address}

        assert len(s.tasks["x"].who_has) == 2
        await w2.close()
        while len(s.tasks["x"].who_has) > 1:
            await asyncio.sleep(0.01)

        if as_deps:
            y2 = c.submit(inc, x, key="y2", workers=[w1.address])
        else:
            s.request_acquire_replicas(w1.address, ["x"], stimulus_id="test")

        while w1.tasks["x"].who_has != {w3.address}:
            await asyncio.sleep(0.01)

    await wait_for_state("x", "memory", w1)

    assert_story(
        w1.story("request-dep"),
        [("request-dep", w3.address, {"x"})],
        # This tests that there has been no attempt to contact w2.
        # If the assumption being tested breaks, this will fail 50% of the times.
        strict=True,
    )


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_fetch_to_missing(c, s, a, b):
    """
    1. task x is a dependency of y
    2. scheduler calls handle_compute("y", who_has={"x": [b]}) on a
    3. x transitions released -> fetch -> flight; a connects to b
    4. b responds it's busy. x transitions flight -> fetch
    5. The busy state triggers an RPC call to Scheduler.who_has
    6. the scheduler responds {"x": []}, because w1 in the meantime has lost the key.
    7. x is transitioned fetch -> missing

    """
    x = await c.scatter({"x": 1}, workers=[b.address])
    b.total_in_connections = 0
    # Crucially, unlike with `c.submit(inc, x, workers=[a.address])`, the scheduler
    # doesn't keep track of acquire-replicas requests, so it won't proactively inform a
    # when we call remove_worker later on
    s.request_acquire_replicas(a.address, ["x"], stimulus_id="test")

    # state will flip-flop between fetch and flight every 150ms, which is the retry
    # period for busy workers.
    await wait_for_state("x", "fetch", a)
    assert b.address in a.busy_workers

    # Sever connection between b and s, but not between b and a.
    # If a tries fetching from b after this, b will keep responding {status: busy}.
    b.periodic_callbacks["heartbeat"].stop()
    await s.remove_worker(b.address, close=False, stimulus_id="test")

    await wait_for_state("x", "missing", a)

    assert_story(
        a.story("x"),
        [
            ("x", "ensure-task-exists", "released"),
            ("x", "fetch", "flight", "flight", {}),
            ("x", "flight", "fetch", "fetch", {}),
            ("x", "fetch", "missing", "missing", {}),
        ],
        # There may be a round of find_missing() after this.
        # Due to timings, there also may be multiple attempts to connect from a to b.
        strict=False,
    )


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_self_denounce_missing_data(c, s, a):
    x = c.submit(inc, 1, key="x")
    await x

    y = c.submit(inc, x, key="y")
    # Wait until the scheduler is forwarding the compute-task to the worker, but before
    # the worker has received it
    while "y" not in s.tasks or s.tasks["y"].state != "processing":
        await asyncio.sleep(0)

    # Wipe x from the worker. This simulates the scheduler calling
    # delete_worker_data(a.address, ["x"]), but the RPC call has not returned yet.
    a.handle_free_keys(keys=["x"], stimulus_id="test")

    # At the same time,
    # a->s: {"op": "release-worker-data", keys=["x"]}
    # s->a: {"op": "compute-task", key="y", who_has={"x": [a.address]}}

    with captured_logger("distributed.worker", level=logging.DEBUG) as logger:
        # The compute-task request for y gets stuck in waiting state, because x is not
        # in memory. However, moments later the scheduler is informed that a is missing
        # x; so it releases y and then recomputes both x and y.
        assert await y == 3

    assert (
        f"Scheduler claims worker {a.address} holds data for task "
        "<TaskState 'x' released>, which is not true."
    ) in logger.getvalue()

    assert_story(
        a.story("x", "y"),
        # Note: omitted uninteresting events
        [
            # {"op": "compute-task", key="y", who_has={"x": [a.address]}}
            # reaches the worker
            ("y", "compute-task", "released"),
            ("x", "ensure-task-exists", "released"),
            ("x", "released", "missing", "missing", {}),
            # {"op": "release-worker-data", keys=["x"]} reaches the scheduler, which
            # reacts by releasing both keys and then recomputing them
            ("y", "release-key"),
            ("x", "release-key"),
            ("x", "compute-task", "released"),
            ("x", "executing", "memory", "memory", {}),
            ("y", "compute-task", "released"),
            ("y", "executing", "memory", "memory", {}),
        ],
    )
