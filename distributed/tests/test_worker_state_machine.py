from itertools import chain

import pytest

from distributed.utils import recursive_to_dict
from distributed.worker_state_machine import (
    Instruction,
    ReleaseWorkerDataMsg,
    RescheduleMsg,
    SendMessageToScheduler,
    StateMachineEvent,
    TaskState,
    UniqueTaskHeap,
    merge_recs_instructions,
)


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
        if not k.startswith("_") and k != "op" and not callable(getattr(cls, k))
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
