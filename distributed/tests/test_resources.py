from __future__ import annotations

import asyncio

import pytest

import dask
from dask import delayed
from dask.utils import stringify

from distributed import Lock, Worker
from distributed.client import wait
from distributed.utils_test import NO_AMM, gen_cluster, inc, lock_inc, slowadd, slowinc
from distributed.worker_state_machine import (
    ComputeTaskEvent,
    Execute,
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    FreeKeysEvent,
    LongRunningMsg,
    RescheduleEvent,
    TaskFinishedMsg,
)


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 5}}),
        ("127.0.0.1", 1, {"resources": {"A": 1, "B": 1}}),
    ],
)
async def test_resource_submit(c, s, a, b):
    x = c.submit(inc, 1, resources={"A": 3})
    y = c.submit(inc, 2, resources={"B": 1})
    z = c.submit(inc, 3, resources={"C": 2})

    await wait(x)
    assert x.key in a.data

    await wait(y)
    assert y.key in b.data

    assert s.get_task_status(keys=[z.key]) == {z.key: "no-worker"}

    async with Worker(s.address, resources={"C": 10}) as w:
        await wait(z)
        assert z.key in w.data


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_submit_many_non_overlapping(c, s, a, b):
    futures = [c.submit(inc, i, resources={"A": 1}) for i in range(5)]
    await wait(futures)

    assert len(a.data) == 5
    assert len(b.data) == 0


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 4, {"resources": {"A": 2}}),
        ("127.0.0.1", 4, {"resources": {"A": 1}}),
    ],
)
async def test_submit_many_non_overlapping_2(c, s, a, b):
    futures = c.map(slowinc, range(100), resources={"A": 1}, delay=0.02)

    while len(a.data) + len(b.data) < 100:
        await asyncio.sleep(0.01)
        assert a.state.executing_count <= 2
        assert b.state.executing_count <= 1

    await wait(futures)
    assert a.state.total_resources == a.state.available_resources
    assert b.state.total_resources == b.state.available_resources


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_move(c, s, a, b):
    [x] = await c.scatter([1], workers=b.address)

    future = c.submit(inc, x, resources={"A": 1})

    await wait(future)
    assert a.data[future.key] == 2


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_dont_work_steal(c, s, a, b):
    [x] = await c.scatter([1], workers=a.address)

    futures = [
        c.submit(slowadd, x, i, resources={"A": 1}, delay=0.05) for i in range(10)
    ]

    await wait(futures)
    assert all(f.key in a.data for f in futures)


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_map(c, s, a, b):
    futures = c.map(inc, range(10), resources={"B": 1})
    await wait(futures)
    assert set(b.data) == {f.key for f in futures}
    assert not a.data


@gen_cluster(
    client=True,
    nthreads=[("", 1, {"resources": {"A": 1}}), ("", 1, {"resources": {"B": 1}})],
    config=NO_AMM,
)
async def test_persist(c, s, a, b):
    with dask.annotate(resources={"A": 1}):
        x = delayed(inc)(1, dask_key_name="x")
    with dask.annotate(resources={"B": 1}):
        y = delayed(inc)(x, dask_key_name="y")

    xx, yy = c.persist([x, y], optimize_graph=False)
    await wait([xx, yy])

    assert set(a.data) == {"x"}
    assert set(b.data) == {"x", "y"}


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 11}}),
    ],
)
async def test_compute(c, s, a, b):
    with dask.annotate(resources={"A": 1}):
        x = delayed(inc)(1)
    with dask.annotate(resources={"B": 1}):
        y = delayed(inc)(x)

    yy = c.compute(y, optimize_graph=False)
    await wait(yy)

    assert b.data

    xs = [delayed(inc)(i) for i in range(10, 20)]
    xxs = c.compute(xs, resources={"B": 1})
    await wait(xxs)

    assert len(b.data) > 10


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_get(c, s, a, b):
    dsk = {"x": (inc, 1), "y": (inc, "x")}

    result = await c.get(dsk, "y", resources={"A": 1}, sync=False)
    assert result == 3
    assert "y" in a.data
    assert not b.data


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_persist_multiple_collections(c, s, a, b):
    with dask.annotate(resources={"A": 1}):
        x = delayed(inc)(1)
        y = delayed(inc)(x)

    xx, yy = c.persist([x, y], optimize_graph=False)

    await wait([xx, yy])

    assert x.key in a.data
    assert y.key in a.data
    assert not b.data


@gen_cluster(client=True)
async def test_resources_str(c, s, a, b):
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    await a.set_resources(MyRes=1)

    x = dd.from_pandas(pd.DataFrame({"A": [1, 2], "B": [3, 4]}), npartitions=1)
    y = x.apply(lambda row: row.sum(), axis=1, meta=(None, "int64"))
    yy = y.persist(resources={"MyRes": 1})
    await wait(yy)

    ts_first = s.tasks[stringify(y.__dask_keys__()[0])]
    assert ts_first.resource_restrictions == {"MyRes": 1}
    ts_last = s.tasks[stringify(y.__dask_keys__()[-1])]
    assert ts_last.resource_restrictions == {"MyRes": 1}


@gen_cluster(client=True, nthreads=[("127.0.0.1", 4, {"resources": {"A": 2, "B": 1}})])
async def test_minimum_resource(c, s, a):
    futures = c.map(slowinc, range(30), resources={"A": 1, "B": 1}, delay=0.02)

    while len(a.data) < 30:
        await asyncio.sleep(0.01)
        assert a.state.executing_count <= 1

    await wait(futures)
    assert a.state.total_resources == a.state.available_resources


@pytest.mark.parametrize("swap", [False, True])
@pytest.mark.parametrize("p1,p2,expect_key", [(1, 0, "y"), (0, 1, "x")])
def test_constrained_vs_ready_priority_1(ws, p1, p2, expect_key, swap):
    """If there are both ready and constrained tasks, those with the highest priority
    win (note: on the Worker, priorities have their sign inverted)
    """
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    RR = {"resource_restrictions": {"R": 1}}

    ws.handle_stimulus(ComputeTaskEvent.dummy(key="clog", stimulus_id="clog"))

    stimuli = [
        ComputeTaskEvent.dummy("x", priority=(p1,), stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", priority=(p2,), **RR, stimulus_id="s2"),
    ]
    if swap:
        stimuli = stimuli[::-1]  # This must be inconsequential

    instructions = ws.handle_stimulus(
        *stimuli,
        ExecuteSuccessEvent.dummy("clog", stimulus_id="s3"),
    )
    assert instructions == [
        TaskFinishedMsg.match(key="clog", stimulus_id="s3"),
        Execute(key=expect_key, stimulus_id="s3"),
    ]


@pytest.mark.parametrize("swap", [False, True])
@pytest.mark.parametrize("p1,p2,expect_key", [(1, 0, "y"), (0, 1, "x")])
def test_constrained_vs_ready_priority_2(ws, p1, p2, expect_key, swap):
    """If there are both ready and constrained tasks, but not enough available
    resources, priority is inconsequential - the tasks in the ready queue are picked up.
    """
    ws.nthreads = 2
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    RR = {"resource_restrictions": {"R": 1}}

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(key="clog1", stimulus_id="clog1"),
        ComputeTaskEvent.dummy(key="clog2", **RR, stimulus_id="clog2"),
    )

    # Test that both priorities and order are inconsequential
    stimuli = [
        ComputeTaskEvent.dummy("x", priority=(p1,), stimulus_id="s1"),
        ComputeTaskEvent.dummy("y", priority=(p2,), **RR, stimulus_id="s2"),
    ]
    if swap:
        stimuli = stimuli[::-1]

    instructions = ws.handle_stimulus(
        *stimuli,
        ExecuteSuccessEvent.dummy("clog1", stimulus_id="s3"),
    )
    assert instructions == [
        TaskFinishedMsg.match(key="clog1", stimulus_id="s3"),
        Execute(key="x", stimulus_id="s3"),
    ]


def test_constrained_tasks_respect_priority(ws):
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    RR = {"resource_restrictions": {"R": 1}}

    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy(key="clog", **RR, stimulus_id="clog"),
        ComputeTaskEvent.dummy(key="x1", priority=(1,), **RR, stimulus_id="s1"),
        ComputeTaskEvent.dummy(key="x2", priority=(2,), **RR, stimulus_id="s2"),
        ComputeTaskEvent.dummy(key="x3", priority=(0,), **RR, stimulus_id="s3"),
        ExecuteSuccessEvent.dummy(key="clog", stimulus_id="s4"),  # start x3
        ExecuteSuccessEvent.dummy(key="x3", stimulus_id="s5"),  # start x1
        ExecuteSuccessEvent.dummy(key="x1", stimulus_id="s6"),  # start x2
    )
    assert instructions == [
        Execute(key="clog", stimulus_id="clog"),
        TaskFinishedMsg.match(key="clog", stimulus_id="s4"),
        Execute(key="x3", stimulus_id="s4"),
        TaskFinishedMsg.match(key="x3", stimulus_id="s5"),
        Execute(key="x1", stimulus_id="s5"),
        TaskFinishedMsg.match(key="x1", stimulus_id="s6"),
        Execute(key="x2", stimulus_id="s6"),
    ]


def test_task_cancelled_and_readded_with_resources(ws):
    """See https://github.com/dask/distributed/issues/6710

    A task is enqueued without resources, then cancelled by the client, then re-added
    with the same key, this time with resources.
    Test that resources are respected.
    """
    ws.available_resources = {"R": 1}
    ws.total_resources = {"R": 1}
    RR = {"resource_restrictions": {"R": 1}}

    ws.handle_stimulus(
        ComputeTaskEvent.dummy(key="clog", **RR, stimulus_id="s1"),
        ComputeTaskEvent.dummy(key="x", stimulus_id="s2"),
    )
    ts = ws.tasks["x"]
    assert ts.state == "ready"
    assert ts in ws.ready
    assert ts not in ws.constrained
    assert ts.resource_restrictions == {}

    ws.handle_stimulus(
        FreeKeysEvent(keys=["x"], stimulus_id="clog"),
        ComputeTaskEvent.dummy(key="x", **RR, stimulus_id="s2"),
    )
    ts = ws.tasks["x"]
    assert ts.state == "constrained"
    assert ts not in ws.ready
    assert ts in ws.constrained
    assert ts.resource_restrictions == {"R": 1}


@pytest.mark.skip(reason="")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 2, {"resources": {"A": 1}}),
        ("127.0.0.1", 2, {"resources": {"A": 1}}),
    ],
)
async def test_balance_resources(c, s, a, b):
    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)
    constrained = c.map(inc, range(2), resources={"A": 1})

    await wait(constrained)
    assert any(f.key in a.data for f in constrained)  # share
    assert any(f.key in b.data for f in constrained)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)])
async def test_set_resources(c, s, a):
    await a.set_resources(A=2)
    assert a.state.total_resources["A"] == 2
    assert a.state.available_resources["A"] == 2
    assert s.workers[a.address].resources == {"A": 2}
    lock = Lock()
    async with lock:
        future = c.submit(lock_inc, 1, lock=lock, resources={"A": 1})
        while a.state.available_resources["A"] == 2:
            await asyncio.sleep(0.01)

    await a.set_resources(A=3)
    assert a.state.total_resources["A"] == 3
    assert a.state.available_resources["A"] == 2
    assert s.workers[a.address].resources == {"A": 3}


@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_persist_collections(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(10, chunks=(5,))
    with dask.annotate(resources={"A": 1}):
        y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    ww, yy = c.persist([w, y], optimize_graph=False)

    await wait([ww, yy])

    assert all(stringify(key) in a.data for key in y.__dask_keys__())


@pytest.mark.skip(reason="Should protect resource keys from optimization")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_dont_optimize_out(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(10, chunks=(5,))
    y = x.map_blocks(lambda x: x + 1)
    z = y.map_blocks(lambda x: 2 * x)
    w = z.sum()

    await c.compute(w, resources={tuple(y.__dask_keys__()): {"A": 1}})

    for key in map(stringify, y.__dask_keys__()):
        assert "executing" in str(a.state.story(key))


@pytest.mark.skip(reason="atop fusion seemed to break this")
@gen_cluster(
    client=True,
    nthreads=[
        ("127.0.0.1", 1, {"resources": {"A": 1}}),
        ("127.0.0.1", 1, {"resources": {"B": 1}}),
    ],
)
async def test_full_collections(c, s, a, b):
    dd = pytest.importorskip("dask.dataframe")
    df = dd.demo.make_timeseries(
        freq="60s", partition_freq="1d", start="2000-01-01", end="2000-01-31"
    )
    z = df.x + df.y  # some extra nodes in the graph

    await c.compute(z, resources={tuple(z.dask): {"A": 1}})
    assert a.state.log
    assert not b.state.log


@pytest.mark.parametrize(
    "optimize_graph",
    [
        pytest.param(
            True,
            marks=pytest.mark.xfail(
                reason="don't track resources through optimization"
            ),
        ),
        False,
    ],
)
def test_collections_get(client, optimize_graph, s, a, b):
    da = pytest.importorskip("dask.array")

    async def f(dask_worker):
        await dask_worker.set_resources(**{"A": 1})

    client.run(f, workers=[a["address"]])

    with dask.annotate(resources={"A": 1}):
        x = da.random.random(100, chunks=(10,)) + 1

    x.compute(optimize_graph=optimize_graph)

    def g(dask_worker):
        return len(dask_worker.log)

    logs = client.run(g)
    assert logs[a["address"]]
    assert not logs[b["address"]]


@gen_cluster(config={"distributed.worker.resources.my_resources": 1}, client=True)
async def test_resources_from_config(c, s, a, b):
    info = c.scheduler_info()
    for worker in [a, b]:
        assert info["workers"][worker.address]["resources"] == {"my_resources": 1}


@gen_cluster(
    worker_kwargs=dict(resources={"my_resources": 10}),
    config={"distributed.worker.resources.my_resources": 1},
    client=True,
)
async def test_resources_from_python_override_config(c, s, a, b):
    info = c.scheduler_info()
    for worker in [a, b]:
        assert info["workers"][worker.address]["resources"] == {"my_resources": 10}


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_cancelled_with_resources(ws_with_running_task, done_ev_cls):
    """Test transition loop of a task with resources:

    executing -> cancelled -> released
    """
    ws = ws_with_running_task
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(done_ev_cls.dummy("x", stimulus_id="s2"))
    assert ws.available_resources == {"R": 1}
    assert "x" not in ws.tasks


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_resumed_with_resources(ws_with_running_task, done_ev_cls):
    """Test transition loop of a task with resources:

    executing -> cancelled -> resumed(fetch) -> (complete execution)
    """
    ws = ws_with_running_task
    ws2 = "127.0.0.1:2"
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(
        ComputeTaskEvent.dummy("y", who_has={"x": [ws2]}, stimulus_id="s2")
    )
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(done_ev_cls.dummy("x", stimulus_id="s3"))
    assert ws.available_resources == {"R": 1}


@pytest.mark.parametrize(
    "done_ev_cls", [ExecuteSuccessEvent, ExecuteFailureEvent, RescheduleEvent]
)
def test_resumed_with_different_resources(ws_with_running_task, done_ev_cls):
    """A task with resources is cancelled and then resumed to the same state, but with
    different resources. This is actually possible in case of manual cancellation from
    the client, followed by resubmit.
    """
    ws = ws_with_running_task
    assert ws.available_resources == {"R": 0}
    ts = ws.tasks["x"]
    prev_state = ts.state

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert ws.available_resources == {"R": 0}

    instructions = ws.handle_stimulus(
        ComputeTaskEvent.dummy("x", stimulus_id="s2", resource_restrictions={"R": 0.4})
    )
    if prev_state == "long-running":
        assert instructions == [
            LongRunningMsg(key="x", compute_duration=None, stimulus_id="s2")
        ]
    else:
        assert not instructions
    assert ws.available_resources == {"R": 0}

    ws.handle_stimulus(done_ev_cls.dummy(key="x", stimulus_id="s3"))
    assert ws.available_resources == {"R": 1}
