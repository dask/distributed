from __future__ import annotations

import asyncio
import gc
import pickle
from collections import defaultdict
from collections.abc import Iterator
from time import sleep

import pytest
from tlz import first

from dask.sizeof import sizeof

import distributed.profile as profile
from distributed import Nanny, Worker, wait
from distributed.compatibility import MACOS, WINDOWS
from distributed.protocol.serialize import Serialize
from distributed.scheduler import TaskState as SchedulerTaskState
from distributed.utils import recursive_to_dict
from distributed.utils_test import (
    NO_AMM,
    _LockedCommPool,
    assert_story,
    async_poll_for,
    freeze_data_fetching,
    gen_cluster,
    inc,
    wait_for_state,
)
from distributed.worker_state_machine import (
    AcquireReplicasEvent,
    AddKeysMsg,
    ComputeTaskEvent,
    Execute,
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    FreeKeysEvent,
    GatherDep,
    GatherDepBusyEvent,
    GatherDepFailureEvent,
    GatherDepNetworkFailureEvent,
    GatherDepSuccessEvent,
    Instruction,
    InvalidTaskState,
    InvalidTransition,
    PauseEvent,
    RecommendationsConflict,
    RefreshWhoHasEvent,
    ReleaseWorkerDataMsg,
    RemoveReplicasEvent,
    RemoveWorkerEvent,
    RequestRefreshWhoHasMsg,
    RescheduleEvent,
    RescheduleMsg,
    RetryBusyWorkerEvent,
    RetryBusyWorkerLater,
    SecedeEvent,
    StateMachineEvent,
    TaskErredMsg,
    TaskState,
    TransitionCounterMaxExceeded,
    UnpauseEvent,
    UpdateDataEvent,
    merge_recs_instructions,
)


def test_instruction_match():
    i = ReleaseWorkerDataMsg(key="x", stimulus_id="s1")
    assert i == ReleaseWorkerDataMsg(key="x", stimulus_id="s1")
    assert i != ReleaseWorkerDataMsg(key="y", stimulus_id="s1")
    assert i != ReleaseWorkerDataMsg(key="x", stimulus_id="s2")
    assert i != RescheduleMsg(key="x", stimulus_id="s1")

    assert i == ReleaseWorkerDataMsg.match(key="x")
    assert i == ReleaseWorkerDataMsg.match(stimulus_id="s1")
    assert i != ReleaseWorkerDataMsg.match(key="y")
    assert i != ReleaseWorkerDataMsg.match(stimulus_id="s2")
    assert i != RescheduleMsg.match(key="x")


def test_TaskState_tracking(cleanup):
    gc.collect()
    x = TaskState("x")
    assert len(TaskState._instances) == 1
    assert first(TaskState._instances) == x
    del x
    assert len(TaskState._instances) == 0


def test_TaskState_get_nbytes():
    assert TaskState("x", nbytes=123).get_nbytes() == 123
    # Default to distributed.scheduler.default-data-size
    assert TaskState("y").get_nbytes() == 1024


def test_TaskState_eq():
    """Test that TaskState objects are hashable and that two identical objects compare
    as different. See comment in TaskState.__hash__ for why.
    """
    a = TaskState("x")
    b = TaskState("x")
    assert a != b
    s = {a, b}
    assert len(s) == 2


def test_TaskState__to_dict():
    """Tasks that are listed as dependencies or dependents of other tasks are dumped as
    a short repr and always appear in full directly under Worker.state.tasks.
    Uninteresting fields are omitted.
    """
    x = TaskState("x", state="memory", done=True)
    y = TaskState("y", priority=(0,), dependencies={x})
    x.dependents.add(y)
    actual = recursive_to_dict([x, y])
    assert actual == [
        {
            "key": "x",
            "run_id": -1,
            "state": "memory",
            "done": True,
            "dependents": ["<TaskState 'y' released>"],
        },
        {
            "key": "y",
            "run_id": -1,
            "state": "released",
            "dependencies": ["<TaskState 'x' memory>"],
            "priority": [0],
        },
    ]


def test_TaskState_repr():
    ts = TaskState("x")
    assert str(ts) == "<TaskState 'x' released>"
    ts.state = "cancelled"
    ts.previous = "flight"
    assert str(ts) == "<TaskState 'x' cancelled(flight)>"
    ts.state = "resumed"
    ts.next = "waiting"
    assert str(ts) == "<TaskState 'x' resumed(flight->waiting)>"


def test_WorkerState__to_dict(ws):
    ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has={"x": ["127.0.0.1:1235"]}, nbytes={"x": 123}, stimulus_id="s1"
        )
    )
    ws.handle_stimulus(UpdateDataEvent(data={"y": object()}, stimulus_id="s2"))

    actual = recursive_to_dict(ws)
    # Remove timestamps
    for ev in actual["log"]:
        del ev[-1]
    for stim in actual["stimulus_log"]:
        del stim["handled"]

    expect = {
        "address": "127.0.0.1:1",
        "busy_workers": [],
        "constrained": [],
        "data": {"y": None},
        "data_needed": {},
        "executing": [],
        "has_what": {"127.0.0.1:1235": ["x"]},
        "in_flight_tasks": ["x"],
        "in_flight_workers": {"127.0.0.1:1235": ["x"]},
        "log": [
            ["x", "ensure-task-exists", "released", "s1"],
            ["x", "released", "fetch", "fetch", {}, "s1"],
            ["gather-dependencies", "127.0.0.1:1235", ["x"], "s1"],
            ["x", "fetch", "flight", "flight", {}, "s1"],
            ["y", "receive-from-scatter", "released", "s2"],
            ["y", "put-in-memory", "s2"],
            ["y", "released", "memory", "memory", {}, "s2"],
        ],
        "long_running": [],
        "missing_dep_flight": [],
        "nthreads": 1,
        "ready": [],
        "running": True,
        "stimulus_log": [
            {
                "cls": "AcquireReplicasEvent",
                "stimulus_id": "s1",
                "who_has": {"x": ["127.0.0.1:1235"]},
                "nbytes": {"x": 123},
            },
            {
                "cls": "UpdateDataEvent",
                "data": {"y": None},
                "stimulus_id": "s2",
            },
        ],
        "tasks": {
            "x": {
                "coming_from": "127.0.0.1:1235",
                "key": "x",
                "run_id": -1,
                "nbytes": 123,
                "priority": [1],
                "state": "flight",
                "who_has": ["127.0.0.1:1235"],
            },
            "y": {
                "key": "y",
                "run_id": -1,
                "nbytes": sizeof(object()),
                "state": "memory",
            },
        },
        "task_counts": {"['x', 'flight']": 1, "['y', 'memory']": 1},
        "task_cumulative_elapsed": {
            "['x', 'flight']": "SNIP",
            "['y', 'memory']": "SNIP",
        },
        "transition_counter": 3,
    }

    # timings data (a few microseconds each)
    for k in actual["task_cumulative_elapsed"]:
        actual["task_cumulative_elapsed"][k] = "SNIP"

    assert actual == expect


def test_WorkerState_pickle(ws):
    """Test pickle round-trip.

    Big caveat
    ----------
    WorkerState, on its own, can be serialized with pickle; it doesn't need cloudpickle.
    A WorkerState extracted from a Worker might, as data contents may only be
    serializable with cloudpickle. Some objects created externally and not designed
    for network transfer - namely, the SpillBuffer - may not be serializable at all.
    """
    ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has={"x": ["127.0.0.1:1235"]}, nbytes={"x": 123}, stimulus_id="s1"
        )
    )
    ws.handle_stimulus(UpdateDataEvent(data={"y": 123}, stimulus_id="s"))
    ws2 = pickle.loads(pickle.dumps(ws))
    assert ws2.tasks.keys() == {"x", "y"}
    assert ws2.data == {"y": 123}


@pytest.mark.parametrize(
    "cls,kwargs",
    [
        (
            InvalidTransition,
            dict(key="x", start="released", finish="waiting", story=[]),
        ),
        (
            TransitionCounterMaxExceeded,
            dict(key="x", start="released", finish="waiting", story=[]),
        ),
        (InvalidTaskState, dict(key="x", state="released", story=[])),
    ],
)
@pytest.mark.parametrize("positional", [False, True])
def test_pickle_exceptions(cls, kwargs, positional):
    if positional:
        e = cls(*kwargs.values())
    else:
        e = cls(**kwargs)
    e2 = pickle.loads(pickle.dumps(e))
    assert type(e2) is type(e)
    for k, v in kwargs.items():
        assert getattr(e2, k) == v


def traverse_subclasses(cls: type) -> Iterator[type]:
    yield cls
    for subcls in cls.__subclasses__():
        yield from traverse_subclasses(subcls)


@pytest.mark.parametrize(
    "cls",
    [
        pytest.param(
            TaskState,
        ),
        *traverse_subclasses(Instruction),
        *traverse_subclasses(StateMachineEvent),
    ],
)
def test_slots(cls):
    params = [
        name
        for name, field in cls.__dataclass_fields__.items()
        if field.init and not field.type.startswith("ClassVar")
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
    with pytest.raises(RecommendationsConflict):
        merge_recs_instructions(({x: "memory"}, []), ({x: "released"}, []))


def test_event_to_dict_with_annotations():
    """Test recursive_to_dict(ev), where ev is a subclass of StateMachineEvent that
    defines its own annotations
    """
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


def test_event_to_dict_without_annotations():
    """Test recursive_to_dict(ev), where ev is a subclass of StateMachineEvent that
    does not define its own annotations
    """
    ev = PauseEvent(stimulus_id="test")
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2 == ev
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "PauseEvent",
        "stimulus_id": "test",
        "handled": 11.22,
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert ev3 == ev


def test_computetask_to_dict():
    """The potentially very large ComputeTaskEvent.run_spec is not stored in the log"""

    def f(arg):
        pass

    ev = ComputeTaskEvent(
        key="x",
        who_has={"y": ["w1"]},
        nbytes={"y": 123},
        priority=(0,),
        run_spec=(f, "arg", {}),
        resource_restrictions={},
        actor=False,
        annotations={},
        span_id=None,
        stimulus_id="test",
        run_id=5,
    )
    assert ev.run_spec is not None
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2.handled == 11.22
    assert ev2.run_spec is None
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "ComputeTaskEvent",
        "key": "x",
        "who_has": {"y": ["w1"]},
        "nbytes": {"y": 123},
        "priority": [0],
        "run_spec": None,
        "resource_restrictions": {},
        "actor": False,
        "annotations": {},
        "span_id": None,
        "stimulus_id": "test",
        "handled": 11.22,
        "run_id": 5,
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert isinstance(ev3, ComputeTaskEvent)
    assert ev3.run_spec is None
    assert ev3.priority == (0,)  # List is automatically converted back to tuple


def test_computetask_dummy():
    ev = ComputeTaskEvent.dummy("x", stimulus_id="s")
    assert ev == ComputeTaskEvent(
        key="x",
        who_has={},
        nbytes={},
        priority=(0,),
        run_spec=ComputeTaskEvent.dummy_runspec("x"),
        resource_restrictions={},
        actor=False,
        annotations={},
        span_id=None,
        stimulus_id="s",
        run_id=0,
    )

    # nbytes is generated from who_has if omitted
    ev2 = ComputeTaskEvent.dummy("x", who_has={"y": "127.0.0.1:2"}, stimulus_id="s")
    assert ev2.nbytes == {"y": 1}


def test_updatedata_to_dict():
    """The potentially very large UpdateDataEvent.data is not stored in the log"""
    ev = UpdateDataEvent(
        data={"x": "foo", "y": "bar"},
        stimulus_id="test",
    )
    ev2 = ev.to_loggable(handled=11.22)
    assert ev2.handled == 11.22
    assert ev2.data == {"x": None, "y": None}
    d = recursive_to_dict(ev2)
    assert d == {
        "cls": "UpdateDataEvent",
        "data": {"x": None, "y": None},
        "stimulus_id": "test",
        "handled": 11.22,
    }
    ev3 = StateMachineEvent.from_dict(d)
    assert isinstance(ev3, UpdateDataEvent)
    assert ev3.data == {"x": None, "y": None}


def test_executesuccess_to_dict():
    """The potentially very large ExecuteSuccessEvent.value is not stored in the log"""
    ev = ExecuteSuccessEvent(
        stimulus_id="test",
        key="x",
        run_id=1,
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
        "run_id": 1,
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
    assert ev3.run_id == 1
    assert ev3.value is None
    assert ev3.start == 123.4
    assert ev3.stop == 456.7
    assert ev3.nbytes == 890
    assert ev3.type is None


def test_executesuccess_dummy():
    ev = ExecuteSuccessEvent.dummy("x", stimulus_id="s")
    assert ev == ExecuteSuccessEvent(
        key="x",
        run_id=1,
        value=None,
        start=0.0,
        stop=1.0,
        nbytes=1,
        type=None,
        stimulus_id="s",
    )

    ev2 = ExecuteSuccessEvent.dummy("x", 123, stimulus_id="s")
    assert ev2.value == 123


def test_executefailure_to_dict():
    ev = ExecuteFailureEvent(
        stimulus_id="test",
        key="x",
        run_id=1,
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
        "run_id": 1,
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


def test_executefailure_dummy():
    ev = ExecuteFailureEvent.dummy("x", stimulus_id="s")
    assert ev == ExecuteFailureEvent(
        key="x",
        run_id=1,
        start=None,
        stop=None,
        exception=Serialize(None),
        traceback=None,
        exception_text="",
        traceback_text="",
        stimulus_id="s",
    )


@gen_cluster(client=True)
async def test_fetch_to_compute(c, s, a, b):
    with freeze_data_fetching(b):
        f1 = c.submit(inc, 1, workers=[a.address], key="f1", allow_other_workers=True)
        f2 = c.submit(inc, f1, workers=[b.address], key="f2")
        await wait_for_state(f1.key, "fetch", b)
        await a.close()

    await f2

    assert_story(
        b.state.log,
        # FIXME: This log should be replaced with a StateMachineEvent log
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
    with freeze_data_fetching(b):
        f1 = c.submit(inc, 1, workers=[a.address], key="f1", allow_other_workers=True)
        await f1
        s.request_acquire_replicas(b.address, [f1.key], stimulus_id="test")
        await wait_for_state(f1.key, "fetch", b)
        await a.close()

    await f1

    assert_story(
        b.state.log,
        # FIXME: This log should be replaced with a StateMachineEvent log
        [
            (f1.key, "ensure-task-exists", "released"),
            (f1.key, "released", "fetch", "fetch", {}),
            (f1.key, "compute-task", "fetch"),
            (f1.key, "put-in-memory"),
        ],
    )


@pytest.mark.parametrize("as_deps", [False, True])
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 3,
    config=NO_AMM,
)
async def test_lose_replica_during_fetch(c, s, w1, w2, w3, as_deps):
    """
    as_deps=True
        0. task x is a dependency of y1 and y2
        1. scheduler calls handle_compute("y1", who_has={"x": [w2, w3]}) on w1
        2. x transitions released -> fetch
        3. the network stack is busy, so x does not transition to flight yet.
        4. scheduler calls handle_compute("y2", who_has={"x": [w3]}) on w1
        5. when x finally reaches the top of the data_needed heap, w1 will not try
           contacting w2

    as_deps=False
        1. scheduler calls handle_acquire_replicas(who_has={"x": [w2, w3]}) on w1
        2. x transitions released -> fetch
        3. the network stack is busy, so x does not transition to flight yet.
        4. scheduler calls handle_acquire_replicas(who_has={"x": [w3]}) on w1
        5. when x finally reaches the top of the data_needed heap, w1 will not try
           contacting w2
    """
    x = (await c.scatter({"x": 1}, workers=[w2.address, w3.address], broadcast=True))[
        "x"
    ]

    # Make sure find_missing is not involved
    w1.periodic_callbacks["find-missing"].stop()

    with freeze_data_fetching(w1, jump_start=True):
        if as_deps:
            y1 = c.submit(inc, x, key="y1", workers=[w1.address])
        else:
            s.request_acquire_replicas(w1.address, ["x"], stimulus_id="test")

        await wait_for_state("x", "fetch", w1)
        assert w1.state.tasks["x"].who_has == {w2.address, w3.address}

        assert len(s.tasks["x"].who_has) == 2
        await w2.close()
        await async_poll_for(lambda: len(s.tasks["x"].who_has) == 1, timeout=5)

        if as_deps:
            y2 = c.submit(inc, x, key="y2", workers=[w1.address])
        else:
            s.request_acquire_replicas(w1.address, ["x"], stimulus_id="test")

        await async_poll_for(
            lambda: w1.state.tasks["x"].who_has == {w3.address}, timeout=5
        )

    await wait_for_state("x", "memory", w1)
    assert_story(
        w1.state.story("request-dep"),
        [("request-dep", w3.address, {"x"})],
        # This tests that there has been no attempt to contact w2.
        # If the assumption being tested breaks, this will fail 50% of the times.
        strict=True,
    )


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_fetch_to_missing_on_busy(c, s, a, b):
    """
    1. task x is a dependency of y
    2. scheduler calls handle_compute("y", who_has={"x": [b]}) on a
    3. x transitions released -> fetch -> flight; a connects to b
    4. b responds it's busy. x transitions flight -> fetch
    5. The busy state triggers an RPC call to Scheduler.who_has
    6. the scheduler responds {"x": []}, because w1 in the meantime has lost the key.
    7. x is transitioned fetch -> missing
    """
    # Note: submit and scatter are different. If you lose all workers holding the
    # replicas of a scattered key, the scheduler forgets the task, which in turn would
    # trigger a free-keys response to request-refresh-who-has.
    x = c.submit(inc, 1, key="x", workers=[b.address])
    await x

    b.transfer_outgoing_count_limit = 0
    # Crucially, unlike with `c.submit(inc, x, workers=[a.address])`, the scheduler
    # doesn't keep track of acquire-replicas requests, so it won't proactively inform a
    # when we call remove_worker later on
    s.request_acquire_replicas(a.address, ["x"], stimulus_id="test")

    # state will flip-flop between fetch and flight every 150ms, which is the retry
    # period for busy workers.
    await wait_for_state("x", "fetch", a)
    assert b.address in a.state.busy_workers

    # Sever connection between b and s, but not between b and a.
    # If a tries fetching from b after this, b will keep responding {status: busy}.
    b.periodic_callbacks["heartbeat"].stop()
    await s.remove_worker(b.address, close=False, stimulus_id="test")

    await wait_for_state("x", "missing", a)

    assert_story(
        a.state.story("x"),
        [
            ("x", "ensure-task-exists", "released"),
            ("x", "released", "fetch", "fetch", {}),
            ("gather-dependencies", b.address, {"x"}),
            ("x", "fetch", "flight", "flight", {}),
            ("request-dep", b.address, {"x"}),
            ("gather-dep-busy", b.address, {"x"}),
            ("x", "flight", "fetch", "fetch", {}),
            ("x", "fetch", "missing", "missing", {}),
        ],
        # There may be a round of find_missing() after this.
        # Due to timings, there also may be multiple attempts to connect from a to b.
        strict=False,
    )


def test_new_replica_while_all_workers_in_flight(ws):
    """A task is stuck in 'fetch' state because all workers that hold a replica are in
    flight. While in this state, a new replica appears on a different worker and the
    scheduler informs the waiting worker through a new acquire-replicas or
    compute-task op.

    In real life, this will typically happen when the Active Memory Manager replicates a
    key to multiple workers and some workers are much faster than others to acquire it,
    due to unrelated tasks being in flight, so 2 seconds later the AMM reiterates the
    request, passing a larger who_has.

    Test that, when this happens, the task is immediately acquired from the new worker,
    without waiting for the original replica holders to get out of flight.
    """
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    instructions = ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has={"x": [ws2]},
            nbytes={"x": 1},
            stimulus_id="s1",
        ),
        AcquireReplicasEvent(
            who_has={"y": [ws2]},
            nbytes={"y": 1},
            stimulus_id="s2",
        ),
        AcquireReplicasEvent(
            who_has={"y": [ws2, ws3]},
            nbytes={"y": 1},
            stimulus_id="s3",
        ),
    )
    assert instructions == [
        GatherDep(
            worker=ws2,
            to_gather={"x"},
            total_nbytes=1,
            stimulus_id="s1",
        ),
        GatherDep(
            worker=ws3,
            to_gather={"y"},
            total_nbytes=1,
            stimulus_id="s3",
        ),
    ]
    assert ws.tasks["x"].state == "flight"
    assert ws.tasks["y"].state == "flight"


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
    while a.state.tasks:
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=NO_AMM)
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


@gen_cluster(client=True)
async def test_forget_data_needed(c, s, a, b):
    """
    1. A task transitions to fetch and is added to data_needed
    2. _ensure_communicating runs, but the network is saturated so the task is not
       popped from data_needed
    3. Task is forgotten
    4. Task is recreated from scratch and transitioned to fetch again
    5. BUG: at the moment of writing this test, adding to data_needed silently did
       nothing, because it still contained the forgotten task, which is a different
       TaskState instance which will be no longer updated.
    6. _ensure_communicating runs. It pops the forgotten task and discards it.
    7. We now have a task stuck in fetch state.
    """
    x = c.submit(inc, 1, key="x", workers=[a.address])
    with freeze_data_fetching(b):
        y = c.submit(inc, x, key="y", workers=[b.address])
        await wait_for_state("x", "fetch", b)
        x.release()
        y.release()
        while s.tasks or a.state.tasks or b.state.tasks:
            await asyncio.sleep(0.01)

    x = c.submit(inc, 2, key="x", workers=[a.address])
    y = c.submit(inc, x, key="y", workers=[b.address])
    assert await y == 4


@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_missing_handle_compute_dependency(c, s, w1, w2, w3):
    """Test that it is OK for a dependency to be in state missing if a dependent is
    asked to be computed
    """
    w3.periodic_callbacks["find-missing"].stop()

    f1 = c.submit(inc, 1, key="f1", workers=[w1.address])
    f2 = c.submit(inc, 2, key="f2", workers=[w1.address])
    await wait_for_state(f1.key, "memory", w1)

    w3.handle_stimulus(
        AcquireReplicasEvent(
            who_has={f1.key: [w2.address]}, nbytes={f1.key: 1}, stimulus_id="acquire"
        )
    )
    await wait_for_state(f1.key, "missing", w3)

    f3 = c.submit(sum, [f1, f2], key="f3", workers=[w3.address])

    await f3


@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_missing_to_waiting(c, s, w1, w2, w3):
    w3.periodic_callbacks["find-missing"].stop()

    f1 = c.submit(inc, 1, key="f1", workers=[w1.address], allow_other_workers=True)
    await wait_for_state(f1.key, "memory", w1)

    w3.handle_stimulus(
        AcquireReplicasEvent(
            who_has={f1.key: [w2.address]}, nbytes={f1.key: 1}, stimulus_id="acquire"
        )
    )
    await wait_for_state(f1.key, "missing", w3)

    await w2.close()
    await w1.close()

    await f1


@gen_cluster(client=True, Worker=Nanny)
async def test_task_state_instance_are_garbage_collected(c, s, a, b):
    futs = c.map(inc, range(10))
    red = c.submit(sum, futs)
    f1 = c.submit(inc, red, pure=False)
    f2 = c.submit(inc, red, pure=False)

    async def check(dask_worker):
        while dask_worker.state.tasks:
            await asyncio.sleep(0.01)
        with profile.lock:
            gc.collect()
        assert not TaskState._instances

    await c.gather([f2, f1])
    del futs, red, f1, f2
    await c.run(check)

    async def check(dask_scheduler):
        while dask_scheduler.tasks:
            await asyncio.sleep(0.01)

        gc.collect()
        # Gargabe collection might already be running in which case gc.collect()'s behavior is undefined.
        # Try again and hope for the best.
        while SchedulerTaskState._instances:
            await asyncio.sleep(0.01)
            with profile.lock:
                gc.collect()

    await c.run_on_scheduler(check)


@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_fetch_to_missing_on_refresh_who_has(c, s, w1, w2, w3):
    """
    1. Two tasks, x and y, are only available on a busy worker.
       The worker sends request-refresh-who-has to the scheduler.
    2. The scheduler responds that x has become missing, while y has gained an
       additional replica
    3. The handler for RefreshWhoHasEvent empties x.who_has and recommends a transition
       to missing.
    4. Before the recommendation can be implemented, the same event invokes
       _ensure_communicating to let y to transition to flight. This in turn pops x from
       data_needed - but x has an empty who_has, which is an exceptional situation.
    5. The transition fetch->missing is executed, but x is no longer in
       data_needed - another exceptional situation.
    """
    x = c.submit(inc, 1, key="x", workers=[w1.address])
    y = c.submit(inc, 2, key="y", workers=[w1.address])
    await wait([x, y])
    w1.transfer_outgoing_count_limit = 0
    s.request_acquire_replicas(w3.address, ["x", "y"], stimulus_id="test1")

    # The tasks will now flip-flop between fetch and flight every 150ms
    # (see Worker.retry_busy_worker_later)
    await wait_for_state("x", "fetch", w3)
    await wait_for_state("y", "fetch", w3)
    assert w1.address in w3.state.busy_workers
    # w3 sent {op: request-refresh-who-has, keys: [x, y]}
    # There also may have been enough time for a refresh-who-has message to come back,
    # which reiterated what w3 already knew:
    # {op: refresh-who-has, who_has={x: [w1.address], y: [w1.address]}}

    # Let's instead simulate that, while request-refresh-who-has was in transit,
    # w2 gained a replica of y and w1 closed down.
    # When request-refresh-who-has lands, the scheduler will respond:
    # {op: refresh-who-has, who_has={x: [], y: [w2.address]}}
    w3.handle_stimulus(
        RefreshWhoHasEvent(who_has={"x": [], "y": [w2.address]}, stimulus_id="test2")
    )
    assert w3.state.tasks["x"].state == "missing"
    assert w3.state.tasks["y"].state == "flight"
    assert w3.state.tasks["y"].who_has == {w2.address}


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_fetch_to_missing_on_network_failure(c, s, a):
    """
    1. Two tasks, x and y, are respectively in flight and fetch state from the same
       worker, which holds the only replica of both.
    2. gather_dep for x returns GatherDepNetworkFailureEvent
    3. The event empties has_what, x.who_has, and y.who_has.
    4. The same event invokes _ensure_communicating, which pops y from data_needed
       - but y has an empty who_has, which is an exceptional situation.
       _ensure_communicating recommends a transition to missing for x.
    5. The fetch->missing transition is executed, but y is no longer in data_needed -
       another exceptional situation.
    """
    block_get_data = asyncio.Event()

    class BlockedBreakingWorker(Worker):
        async def get_data(self, comm, *args, **kwargs):
            await block_get_data.wait()
            raise OSError("fake error")

    async with BlockedBreakingWorker(s.address) as b:
        x = c.submit(inc, 1, key="x", workers=[b.address])
        y = c.submit(inc, 2, key="y", workers=[b.address])
        await wait([x, y])
        s.request_acquire_replicas(a.address, ["x"], stimulus_id="test_x")
        await wait_for_state("x", "flight", a)
        s.request_acquire_replicas(a.address, ["y"], stimulus_id="test_y")
        await wait_for_state("y", "fetch", a)

        block_get_data.set()

        await wait_for_state("x", "missing", a)
        await wait_for_state("y", "missing", a)


@gen_cluster()
async def test_deprecated_worker_attributes(s, a, b):
    n = a.state.generation
    msg = (
        "The `Worker.generation` attribute has been moved to "
        "`Worker.state.generation`"
    )
    with pytest.warns(FutureWarning, match=msg):
        assert a.generation == n
    with pytest.warns(FutureWarning, match=msg):
        a.generation -= 1
        assert a.generation == n - 1
    assert a.state.generation == n - 1

    # Old and new names differ
    msg = (
        "The `Worker.in_flight_tasks` attribute has been moved to "
        "`Worker.state.in_flight_tasks_count`"
    )
    with pytest.warns(FutureWarning, match=msg):
        assert a.in_flight_tasks == 0

    with pytest.warns(FutureWarning, match="attribute has been removed"):
        assert a.data_needed == set()
    with pytest.warns(FutureWarning, match="attribute has been removed"):
        assert a.waiting_for_data_count == 0


@pytest.mark.parametrize("n_remote_workers", [1, 2])
@pytest.mark.parametrize(
    "nbytes,n_in_flight_per_worker",
    [
        (int(10e6), 3),
        (int(20e6), 2),
        (int(30e6), 1),
        (int(60e6), 1),
    ],
)
def test_aggregate_gather_deps(ws, nbytes, n_in_flight_per_worker, n_remote_workers):
    ws.transfer_message_bytes_limit = int(50e6)
    wss = [f"127.0.0.1:{2 + i}" for i in range(n_remote_workers)]
    who_has = {f"x{i}": [wss[i // 3]] for i in range(3 * n_remote_workers)}
    instructions = ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has=who_has,
            nbytes={task: nbytes for task in who_has.keys()},
            stimulus_id="s1",
        )
    )
    assert instructions == [
        GatherDep.match(worker=remote, stimulus_id="s1") for remote in wss
    ]
    assert all(
        len(instruction.to_gather) == n_in_flight_per_worker
        for instruction in instructions
    )
    assert len(ws.in_flight_tasks) == n_in_flight_per_worker * n_remote_workers
    assert (
        ws.transfer_incoming_bytes == nbytes * n_in_flight_per_worker * n_remote_workers
    )
    assert ws.transfer_incoming_count == n_remote_workers
    assert ws.transfer_incoming_count_total == n_remote_workers


def test_gather_priority(ws):
    """Test that tasks are fetched in the following order:

    1. by task priority
    2. in case of tie, from local workers first
    3. in case of tie, from the worker with the most tasks queued
    4. in case of tie, from a random worker (which is actually deterministic).
    """
    ws.transfer_incoming_count_limit = 4

    instructions = ws.handle_stimulus(
        PauseEvent(stimulus_id="pause"),
        # Note: tasks fetched by acquire-replicas always have priority=(1, )
        AcquireReplicasEvent(
            who_has={
                # Remote + local
                "x1": ["127.0.0.2:1", "127.0.0.1:2"],
                # Remote. After getting x11 from .1, .2  will have less tasks than .3
                "x2": ["127.0.0.2:1"],
                "x3": ["127.0.0.3:1"],
                "x4": ["127.0.0.3:1"],
                # It will be a random choice between .2, .4, .5, .6, and .7
                "x5": ["127.0.0.4:1"],
                "x6": ["127.0.0.5:1"],
                "x7": ["127.0.0.6:1"],
                # This will be fetched first because it's on the same worker as y
                "x8": ["127.0.0.7:1"],
            },
            # Substantial nbytes prevents transfer_incoming_count_limit to be
            # overridden by transfer_incoming_bytes_throttle_threshold,
            # but it's less than transfer_message_bytes_limit
            nbytes={f"x{i}": 4 * 2**20 for i in range(1, 9)},
            stimulus_id="compute1",
        ),
        # A higher-priority task, even if scheduled later, is fetched first
        ComputeTaskEvent.dummy(
            key="z",
            who_has={"y": ["127.0.0.7:1"]},
            priority=(0,),
            stimulus_id="compute2",
        ),
        UnpauseEvent(stimulus_id="unpause"),
    )

    assert instructions == [
        # Highest-priority task first. Lower priority tasks from the same worker are
        # shoved into the same instruction (up to 50MB worth)
        GatherDep(
            stimulus_id="unpause",
            worker="127.0.0.7:1",
            to_gather={"y", "x8"},
            total_nbytes=1 + 4 * 2**20,
        ),
        # Followed by local workers
        GatherDep(
            stimulus_id="unpause",
            worker="127.0.0.1:2",
            to_gather={"x1"},
            total_nbytes=4 * 2**20,
        ),
        # Followed by remote workers with the most tasks
        GatherDep(
            stimulus_id="unpause",
            worker="127.0.0.3:1",
            to_gather={"x3", "x4"},
            total_nbytes=8 * 2**20,
        ),
        # Followed by other remote workers, randomly.
        # Determinism is guaranteed by a statically-seeded random number generator.
        # FIXME It would have not been deterministic if we instead of multiple keys we
        #       had used a single key with multiple workers, because sets
        #       (like TaskState.who_has) change order at every interpreter restart.
        GatherDep(
            stimulus_id="unpause",
            worker="127.0.0.4:1",
            to_gather={"x5"},
            total_nbytes=4 * 2**20,
        ),
    ]
    expected_bytes = 1 + 4 * 2**20 + 4 * 2**20 + 8 * 2**20 + 4 * 2**20
    assert ws.transfer_incoming_bytes == expected_bytes
    assert ws.transfer_incoming_count == 4
    assert ws.transfer_incoming_count_total == 4


@pytest.mark.parametrize("state", ["executing", "long-running"])
def test_task_acquires_resources(ws, state):
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            key="x", resource_restrictions={"R": 1}, stimulus_id="compute"
        )
    )
    if state == "long-running":
        ws.handle_stimulus(
            SecedeEvent(key="x", compute_duration=1.0, stimulus_id="secede")
        )
    assert ws.tasks["x"].state == state
    assert ws.available_resources == {"R": 0}


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_task_releases_resources(ws_with_running_task, done_ev_cls):
    ws = ws_with_running_task
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(done_ev_cls.dummy("x", stimulus_id="success"))
    assert ws.available_resources == {"R": 1}


def test_task_with_dependencies_acquires_resources(ws):
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "y", who_has={"x": [ws2]}, resource_restrictions={"R": 1}, stimulus_id="s1"
        )
    )
    assert ws.tasks["x"].state == "flight"
    assert ws.tasks["y"].state == "waiting"
    assert ws.available_resources == {"R": 1}

    instructions = ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2, data={"x": 123}, total_nbytes=8, stimulus_id="s2"
        )
    )
    assert instructions == [
        AddKeysMsg(keys=["x"], stimulus_id="s2"),
        Execute(key="y", stimulus_id="s2"),
    ]
    assert ws.tasks["y"].state == "executing"
    assert ws.available_resources == {"R": 0}


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_resumed_task_releases_resources(ws_with_running_task, done_ev_cls):
    ws = ws_with_running_task
    assert ws.available_resources == {"R": 0}
    ws2 = "127.0.0.1:2"

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="cancel"))
    assert ws.tasks["x"].state == "cancelled"
    assert ws.available_resources == {"R": 0}

    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="compute")
    )
    assert not instructions
    assert ws.tasks["x"].state == "resumed"
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(done_ev_cls.dummy("x", stimulus_id="s2"))
    assert ws.available_resources == {"R": 1}


@gen_cluster()
async def test_clean_log(s, a, b):
    """Test that brand new workers start with a clean log"""
    assert not a.state.log
    assert not a.state.stimulus_log


def test_running_task_in_all_running_tasks(ws_with_running_task):
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    ts = ws.tasks["x"]
    assert ts in ws.all_running_tasks

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert ts.state == "cancelled"
    assert ts in ws.all_running_tasks

    ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2")
    )
    assert ts.state == "resumed"
    assert ts in ws.all_running_tasks


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_done_task_not_in_all_running_tasks(ws_with_running_task, done_ev_cls):
    ws = ws_with_running_task
    ts = ws.tasks["x"]
    assert ts in ws.all_running_tasks

    ws.handle_stimulus(done_ev_cls.dummy("x", stimulus_id="s1"))
    assert ts not in ws.all_running_tasks


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_done_resumed_task_not_in_all_running_tasks(ws_with_running_task, done_ev_cls):
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"

    ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2"),
        done_ev_cls.dummy("x", stimulus_id="s3"),
    )
    ts = ws.tasks["x"]
    assert ts not in ws.all_running_tasks


def test_gather_dep_failure(ws):
    """Simulate a task failing to unpickle when it reaches the destination worker after
    a flight.

    See also test_worker_memory.py::test_workerstate_fail_to_pickle_flight,
    where the task instead is gathered successfully, but fails to spill.
    """
    ws2 = "127.0.0.1:2"
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        GatherDepFailureEvent.from_exception(
            Exception(), worker=ws2, total_nbytes=1, stimulus_id="s2"
        ),
    )
    assert instructions == [
        GatherDep(worker=ws2, to_gather={"x"}, total_nbytes=1, stimulus_id="s1"),
        TaskErredMsg.match(key="x", stimulus_id="s2"),
    ]
    assert ws.tasks["x"].state == "error"
    assert ws.tasks["y"].state == "waiting"  # Not ready
    assert ws.transfer_incoming_bytes == 0
    assert ws.transfer_incoming_count == 0
    assert ws.transfer_incoming_count_total == 1

    # FIXME https://github.com/dask/distributed/issues/6705
    ws.validate = False


def test_recompute_erred_task(ws):
    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("x", run_id=1, stimulus_id="s1"),
        ExecuteFailureEvent.dummy("x", run_id=1, stimulus_id="s2"),
        ComputeTaskEvent.dummy("x", run_id=2, stimulus_id="s3"),
    )
    assert instructions == [
        Execute(key="x", stimulus_id="s1"),
        TaskErredMsg.match(key="x", run_id=1, stimulus_id="s2"),
        Execute(key="x", stimulus_id="s3"),
    ]
    assert ws.tasks["x"].state == "executing"


def test_transfer_incoming_metrics(ws):
    assert ws.transfer_incoming_bytes == 0
    assert ws.transfer_incoming_count == 0
    assert ws.transfer_incoming_count_total == 0

    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "b", who_has={"a": [ws2]}, nbytes={"a": 7}, stimulus_id="s1"
        )
    )
    assert ws.transfer_incoming_bytes == 7
    assert ws.transfer_incoming_count == 1
    assert ws.transfer_incoming_count_total == 1

    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2, data={"a": 123}, total_nbytes=7, stimulus_id="s2"
        )
    )
    assert ws.transfer_incoming_bytes == 0
    assert ws.transfer_incoming_count == 0
    assert ws.transfer_incoming_count_total == 1

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "e",
            who_has={"c": [ws2], "d": [ws2]},
            nbytes={"c": 11, "d": 13},
            stimulus_id="s2",
        )
    )
    assert ws.transfer_incoming_bytes == 24
    assert ws.transfer_incoming_count == 1
    assert ws.transfer_incoming_count_total == 2

    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2, data={"c": 123, "d": 234}, total_nbytes=24, stimulus_id="s3"
        )
    )
    assert ws.transfer_incoming_bytes == 0
    assert ws.transfer_incoming_count == 0
    assert ws.transfer_incoming_count_total == 2

    ws3 = "127.0.0.1:3"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "h",
            who_has={"f": [ws2], "g": [ws3]},
            nbytes={"f": 17, "g": 19},
            stimulus_id="s4",
        )
    )
    assert ws.transfer_incoming_bytes == 36
    assert ws.transfer_incoming_count == 2
    assert ws.transfer_incoming_count_total == 4

    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws3, data={"g": 345}, total_nbytes=19, stimulus_id="s5"
        )
    )
    assert ws.transfer_incoming_bytes == 17
    assert ws.transfer_incoming_count == 1
    assert ws.transfer_incoming_count_total == 4

    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2, data={"g": 456}, total_nbytes=17, stimulus_id="s6"
        )
    )
    assert ws.transfer_incoming_bytes == 0
    assert ws.transfer_incoming_count == 0
    assert ws.transfer_incoming_count_total == 4


def test_throttling_does_not_affect_first_transfer(ws):
    ws.transfer_incoming_count_limit = 100
    ws.transfer_incoming_bytes_limit = 100
    ws.transfer_incoming_bytes_throttle_threshold = 1
    ws.transfer_message_bytes_limit = 100
    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "c",
            who_has={"a": [ws2]},
            nbytes={"a": 200},
            stimulus_id="s1",
        )
    )
    assert ws.tasks["a"].state == "flight"


def test_message_target_does_not_affect_first_transfer_on_different_worker(ws):
    ws.transfer_incoming_count_limit = 100
    ws.transfer_incoming_bytes_limit = 600
    ws.transfer_message_bytes_limit = 100
    ws.transfer_incoming_bytes_throttle_threshold = 1
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "c",
            who_has={"a": [ws2], "b": [ws3]},
            nbytes={"a": 200, "b": 200},
            stimulus_id="s1",
        )
    )
    assert ws.tasks["a"].state == "flight"
    assert ws.tasks["b"].state == "flight"


def test_throttle_incoming_transfers_on_count_limit(ws):
    ws.transfer_incoming_count_limit = 1
    ws.transfer_incoming_bytes_limit = 100_000
    ws.transfer_incoming_bytes_throttle_threshold = 1
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    who_has = {"a": [ws2], "b": [ws3]}
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "c",
            who_has=who_has,
            nbytes={"a": 100, "b": 100},
            stimulus_id="s1",
        )
    )
    tasks_by_state = defaultdict(list)
    for ts in ws.tasks.values():
        tasks_by_state[ts.state].append(ts)
    assert len(tasks_by_state["flight"]) == 1
    assert len(tasks_by_state["fetch"]) == 1
    assert ws.transfer_incoming_bytes == 100

    in_flight_task = tasks_by_state["flight"][0]
    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=who_has[in_flight_task.key][0],
            data={in_flight_task.key: 123},
            total_nbytes=100,
            stimulus_id="s2",
        )
    )
    assert tasks_by_state["flight"][0].state == "memory"
    assert tasks_by_state["fetch"][0].state == "flight"
    assert ws.transfer_incoming_bytes == 100


def test_throttling_incoming_transfer_on_transfer_bytes_same_worker(ws):
    ws.transfer_incoming_count_limit = 100
    ws.transfer_incoming_bytes_limit = 250
    ws.transfer_incoming_bytes_throttle_threshold = 1
    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "d",
            who_has={"a": [ws2], "b": [ws2], "c": [ws2]},
            nbytes={"a": 100, "b": 100, "c": 100},
            stimulus_id="s1",
        )
    )
    tasks_by_state = defaultdict(list)
    for ts in ws.tasks.values():
        tasks_by_state[ts.state].append(ts)
    assert ws.transfer_incoming_bytes == 200
    assert len(tasks_by_state["flight"]) == 2
    assert len(tasks_by_state["fetch"]) == 1

    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2,
            data={ts.key: 123 for ts in tasks_by_state["flight"]},
            total_nbytes=200,
            stimulus_id="s2",
        )
    )
    assert all(ts.state == "memory" for ts in tasks_by_state["flight"])
    assert all(ts.state == "flight" for ts in tasks_by_state["fetch"])


def test_throttling_incoming_transfer_on_transfer_bytes_different_workers(ws):
    ws.transfer_incoming_count_limit = 100
    ws.transfer_incoming_bytes_limit = 150
    ws.transfer_incoming_bytes_throttle_threshold = 1
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    who_has = {"a": [ws2], "b": [ws3]}
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "c",
            who_has=who_has,
            nbytes={"a": 100, "b": 100},
            stimulus_id="s1",
        )
    )
    tasks_by_state = defaultdict(list)
    for ts in ws.tasks.values():
        tasks_by_state[ts.state].append(ts)
    assert ws.transfer_incoming_bytes == 100
    assert len(tasks_by_state["flight"]) == 1
    assert len(tasks_by_state["fetch"]) == 1

    in_flight_task = tasks_by_state["flight"][0]
    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=who_has[in_flight_task.key][0],
            data={in_flight_task.key: 123},
            total_nbytes=100,
            stimulus_id="s2",
        )
    )
    assert tasks_by_state["flight"][0].state == "memory"
    assert tasks_by_state["fetch"][0].state == "flight"


def test_do_not_throttle_connections_while_below_threshold(ws):
    ws.transfer_incoming_count_limit = 1
    ws.transfer_incoming_bytes_limit = 200
    ws.transfer_incoming_bytes_throttle_threshold = 50
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    ws4 = "127.0.0.1:4"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "b",
            who_has={"a": [ws2]},
            nbytes={"a": 1},
            stimulus_id="s1",
        )
    )
    assert ws.tasks["a"].state == "flight"

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "d",
            who_has={"c": [ws3]},
            nbytes={"c": 1},
            stimulus_id="s2",
        )
    )
    assert ws.tasks["c"].state == "flight"

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "f",
            who_has={"e": [ws4]},
            nbytes={"e": 100},
            stimulus_id="s3",
        )
    )
    assert ws.tasks["e"].state == "flight"
    assert ws.transfer_incoming_bytes == 102


def test_throttle_on_transfer_bytes_regardless_of_threshold(ws):
    ws.transfer_incoming_count_limit = 1
    ws.transfer_incoming_bytes_limit = 100
    ws.transfer_incoming_bytes_throttle_threshold = 50
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "b",
            who_has={"a": [ws2]},
            nbytes={"a": 1},
            stimulus_id="s1",
        )
    )
    assert ws.tasks["a"].state == "flight"

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "d",
            who_has={"c": [ws3]},
            nbytes={"c": 100},
            stimulus_id="s2",
        )
    )
    assert ws.tasks["c"].state == "fetch"
    assert ws.transfer_incoming_bytes == 1


def test_worker_nbytes(ws_with_running_task):
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    assert ws.nbytes == 0

    # executing->memory
    ws.handle_stimulus(ExecuteSuccessEvent.dummy("x", nbytes=12, stimulus_id="s1"))
    assert ws.nbytes == 12

    # flight->memory
    ws.handle_stimulus(
        AcquireReplicasEvent(who_has={"y": [ws2]}, nbytes={"y": 13}, stimulus_id="s2")
    )
    assert ws.nbytes == 12
    ws.handle_stimulus(
        GatherDepSuccessEvent(
            worker=ws2,
            data={"y": "foo"},
            total_nbytes=13,
            stimulus_id="s3",
        )
    )
    assert ws.nbytes == 12 + 13

    # released -> memory (scatter)
    ws.handle_stimulus(UpdateDataEvent(data={"z": "bar"}, stimulus_id="s3"))
    assert ws.nbytes == 12 + 13 + sizeof("bar")

    # actors
    ws.handle_stimulus(
        ComputeTaskEvent.dummy("w", actor=True, stimulus_id="s4"),
        ExecuteSuccessEvent.dummy("w", nbytes=14, stimulus_id="s5"),
    )
    assert ws.nbytes == 12 + 13 + sizeof("bar") + 14

    # memory -> released by FreeKeysEvent
    ws.handle_stimulus(FreeKeysEvent(keys=["z"], stimulus_id="s6"))
    assert ws.nbytes == 12 + 13 + 14

    # memory -> released by RemoveReplicasEvent
    ws.handle_stimulus(RemoveReplicasEvent(keys=["x", "y", "w"], stimulus_id="s7"))
    assert ws.nbytes == 0


def test_fetch_count(ws):
    ws.transfer_incoming_count_limit = 0
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    assert ws.fetch_count == 0
    # Saturate comms
    # released->fetch->flight
    ws.handle_stimulus(
        AcquireReplicasEvent(who_has={"a": [ws2]}, nbytes={"a": 1}, stimulus_id="s1"),
        AcquireReplicasEvent(
            who_has={"b": [ws2, ws3]}, nbytes={"b": 1}, stimulus_id="s2"
        ),
    )
    assert ws.tasks["b"].coming_from == ws3
    assert ws.fetch_count == 0

    # released->fetch
    # d is in two data_needed heaps
    ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has={"c": [ws2], "d": [ws2, ws3]},
            nbytes={"c": 1, "d": 1},
            stimulus_id="s3",
        )
    )
    assert ws.fetch_count == 2

    # fetch->released
    ws.handle_stimulus(FreeKeysEvent(keys={"c", "d"}, stimulus_id="s4"))
    assert ws.fetch_count == 0

    # flight->missing
    ws.handle_stimulus(
        GatherDepSuccessEvent(worker=ws2, data={}, total_nbytes=0, stimulus_id="s5")
    )
    assert ws.tasks["a"].state == "missing"
    print(ws.tasks)
    assert ws.fetch_count == 0
    assert len(ws.missing_dep_flight) == 1

    # flight->fetch
    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "clog", who_has={"clog_dep": [ws2]}, priority=(-1,), stimulus_id="s6"
        ),
        GatherDepSuccessEvent(worker=ws3, data={}, total_nbytes=0, stimulus_id="s7"),
    )
    assert ws.tasks["b"].state == "fetch"
    assert ws.fetch_count == 1
    assert len(ws.missing_dep_flight) == 1


def test_task_counter(ws):
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    for by_prefix in (False, True):
        assert ws.task_counter.current_count(by_prefix=by_prefix) == {}
        assert ws.task_counter.cumulative_elapsed(by_prefix=by_prefix) == {}

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(
            "('y-123', 7)", who_has={"('x-456', 8)": [ws2]}, stimulus_id="s1"
        ),
        AcquireReplicasEvent(
            who_has={"('x-789', 0)": [ws3], "z": [ws3]},
            nbytes={"('x-789', 0)": 1, "z": 1},
            stimulus_id="s2",
        ),
    )
    assert ws.task_counter.current_count() == {
        ("x", "flight"): 2,
        ("y", "waiting"): 1,
        ("z", "flight"): 1,
    }
    assert ws.task_counter.current_count(by_prefix=False) == {"waiting": 1, "flight": 3}

    def assert_time(actual, expect):
        # timer accuracy in Windows can be very poor;
        # see awful hack in distributed.metrics
        margin_lo = 0.099 if WINDOWS else 0
        # sleep() has been observed to have up to 450ms lag on both
        # MacOSX and Windows GitHub CI
        margin_hi = 0.6 if MACOS or WINDOWS else 0.1
        assert expect - margin_lo <= actual < expect + margin_hi

    sleep(0.1)
    elapsed = ws.task_counter.cumulative_elapsed()
    # Transitory states are not recorded
    assert len(elapsed) == 3
    assert_time(elapsed["x", "flight"], 0.2)
    assert_time(elapsed["y", "waiting"], 0.1)
    assert_time(elapsed["z", "flight"], 0.1)

    elapsed = ws.task_counter.cumulative_elapsed(by_prefix=False)
    assert len(elapsed) == 2
    assert_time(elapsed["flight"], 0.3)
    assert_time(elapsed["waiting"], 0.1)

    # Forgotten keys disappear from current_count() and stop accruing time in
    # cumulative_elapsed()
    ws.handle_stimulus(FreeKeysEvent(keys=["('y-123', 7)"], stimulus_id="s3"))
    assert ws.task_counter.current_count() == {
        ("x", "cancelled"): 1,
        ("x", "flight"): 1,
        ("z", "flight"): 1,
    }
    sleep(0.15)
    elapsed = ws.task_counter.cumulative_elapsed()
    assert len(elapsed) == 4
    assert_time(elapsed["x", "flight"], 0.35)
    assert_time(elapsed["x", "cancelled"], 0.15)
    assert_time(elapsed["y", "waiting"], 0.1)
    assert_time(elapsed["z", "flight"], 0.25)


@pytest.mark.parametrize("outcome", ["fail", "success", "no-key", "busy"])
def test_remove_worker_while_in_flight(ws, outcome):
    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s1"),
        RemoveWorkerEvent(worker=ws2, stimulus_id="s2"),
    )
    ts = ws.tasks["x"]
    assert ts.state == "flight"
    assert not ws.data_needed
    assert not ws.tasks["y"].who_has
    assert not ws.has_what

    if outcome == "fail":
        instructions = ws.handle_stimulus(
            GatherDepNetworkFailureEvent(worker=ws2, total_nbytes=1, stimulus_id="s3")
        )
        assert not instructions
        assert ts.state == "missing"
    elif outcome == "success":
        instructions = ws.handle_stimulus(
            GatherDepSuccessEvent(
                worker=ws2, total_nbytes=1, data={"x": None}, stimulus_id="s3"
            )
        )
        assert instructions == [
            AddKeysMsg(stimulus_id="s3", keys=["x"]),
            Execute(stimulus_id="s3", key="y"),
        ]
        assert ts.state == "memory"
    elif outcome == "no-key":
        instructions = ws.handle_stimulus(
            GatherDepSuccessEvent(worker=ws2, total_nbytes=1, data={}, stimulus_id="s3")
        )
        assert not instructions
        assert ts.state == "missing"
    else:
        assert outcome == "busy"
        instructions = ws.handle_stimulus(
            GatherDepBusyEvent(worker=ws2, total_nbytes=1, stimulus_id="s3")
        )
        assert instructions == [
            RetryBusyWorkerLater(worker=ws2, stimulus_id="s3"),
            RequestRefreshWhoHasMsg(keys=["x"], stimulus_id="s3"),
        ]
        assert ts.state == "missing"
        instructions = ws.handle_stimulus(
            RetryBusyWorkerEvent(worker=ws2, stimulus_id="s4")
        )
        assert not instructions
        assert ts.state == "missing"

    assert not ts.who_has
    assert not ws.has_what


def test_remove_worker_while_in_flight_unused_peer(ws):
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    ws.handle_stimulus(
        AcquireReplicasEvent(who_has={"x": [ws2]}, nbytes={"x": 1}, stimulus_id="s1"),
        AcquireReplicasEvent(
            who_has={"y": [ws2, ws3]}, nbytes={"y": 1}, stimulus_id="s2"
        ),
    )

    ts = ws.tasks["y"]
    assert ts.state == "flight"
    assert ts.who_has == {ws2, ws3}
    assert ts.coming_from == ws3

    ws.handle_stimulus(RemoveWorkerEvent(worker=ws2, stimulus_id="s3"))
    assert ts.state == "flight"
    assert ts.who_has == {ws3}
    assert ts.coming_from == ws3


def test_remove_worker_while_in_fetch(ws):
    ws2 = "127.0.0.1:2"
    ws3 = "127.0.0.1:3"
    ws.handle_stimulus(
        AcquireReplicasEvent(
            who_has={"x": [ws2], "y": [ws3]}, nbytes={"x": 1, "y": 1}, stimulus_id="s1"
        ),
        AcquireReplicasEvent(
            who_has={"w": [ws2], "z": [ws2, ws3]},
            nbytes={"w": 1, "z": 1},
            stimulus_id="s2",
        ),
        RemoveWorkerEvent(worker=ws2, stimulus_id="s3"),
    )
    assert ws.tasks["w"].state == "missing"
    assert ws.tasks["z"].state == "fetch"
    assert not ws.tasks["w"].who_has
    assert ws.tasks["z"].who_has == {ws3}
    assert {k: list(v) for k, v in ws.data_needed.items()} == {ws3: [ws.tasks["z"]]}
    assert ws.has_what == {ws3: {"y", "z"}}


def test_remove_worker_unknown(ws):
    ws2 = "127.0.0.1:2"
    ws.handle_stimulus(RemoveWorkerEvent(worker=ws2, stimulus_id="s3"))
