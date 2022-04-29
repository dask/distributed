import asyncio
from itertools import chain

import pytest

from distributed.protocol.serialize import Serialize
from distributed.utils import recursive_to_dict
from distributed.utils_test import gen_cluster, inc
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
    instr1 = RescheduleMsg(key="foo", worker="a", stimulus_id="test")
    instr2 = RescheduleMsg(key="bar", worker="b", stimulus_id="test")
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
