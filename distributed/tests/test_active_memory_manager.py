from __future__ import annotations

import asyncio
import logging
import random
from contextlib import contextmanager

import pytest

from distributed import Nanny
from distributed.active_memory_manager import (
    ActiveMemoryManagerExtension,
    ActiveMemoryManagerPolicy,
)
from distributed.core import Status
from distributed.utils_test import captured_logger, gen_cluster, inc, slowinc

NO_AMM_START = {"distributed.scheduler.active-memory-manager.start": False}


@contextmanager
def assert_amm_log(expect: list[str]):
    with captured_logger(
        "distributed.active_memory_manager", level=logging.DEBUG
    ) as logger:
        yield
    actual = logger.getvalue().splitlines()
    if len(actual) != len(expect) or any(
        not a.startswith(e) for a, e in zip(actual, expect)
    ):
        raise AssertionError(
            "Log lines mismatch:\n"
            + "\n".join(actual)
            + "\n"
            + "=" * 80
            + "\n"
            + "Does not match:\n"
            + "\n".join(expect)
        )


class DemoPolicy(ActiveMemoryManagerPolicy):
    """Drop or replicate a key n times"""

    def __init__(self, action, key, n, candidates):
        self.action = action
        self.key = key
        self.n = n
        self.candidates = candidates

    def run(self):
        candidates = self.candidates
        if candidates is not None:
            candidates = {
                ws
                for i, ws in enumerate(self.manager.scheduler.workers.values())
                if i in candidates
            }
        for ts in self.manager.scheduler.tasks.values():
            if ts.key == self.key:
                for _ in range(self.n):
                    yield self.action, ts, candidates


def demo_config(action, key="x", n=10, candidates=None, start=False, interval=0.1):
    """Create a dask config for AMM with DemoPolicy"""
    return {
        "distributed.scheduler.active-memory-manager.start": start,
        "distributed.scheduler.active-memory-manager.interval": interval,
        "distributed.scheduler.active-memory-manager.policies": [
            {
                "class": "distributed.tests.test_active_memory_manager.DemoPolicy",
                "action": action,
                "key": key,
                "n": n,
                "candidates": candidates,
            },
        ],
    }


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.active-memory-manager.start": False,
        "distributed.scheduler.active-memory-manager.policies": [],
    },
)
async def test_no_policies(c, s, a, b):
    s.extensions["amm"].run_once()


@gen_cluster(nthreads=[("", 1)] * 4, client=True, config=demo_config("drop", n=5))
async def test_drop(c, s, *workers):
    # Logging is quiet if there are no suggestions
    with assert_amm_log(
        [
            "Running policy: DemoPolicy()",
            "Active Memory Manager run in ",
        ],
    ):
        s.extensions["amm"].run_once()

    futures = await c.scatter({"x": 123}, broadcast=True)
    assert len(s.tasks["x"].who_has) == 4
    # Also test the extension handler
    with assert_amm_log(
        [
            "Running policy: DemoPolicy()",
            "(drop, <TaskState 'x' memory>, None): dropping from ",
            "(drop, <TaskState 'x' memory>, None): dropping from ",
            "(drop, <TaskState 'x' memory>, None): dropping from ",
            "(drop, <TaskState 'x' memory>, None) rejected: less than 2 replicas exist",
            "(drop, <TaskState 'x' memory>, None) rejected: less than 2 replicas exist",
            "Enacting suggestions for 1 tasks:",
            "- <WorkerState ",
            "- <WorkerState ",
            "- <WorkerState ",
            "Active Memory Manager run in ",
        ],
    ):
        s.extensions["amm"].run_once()

    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)
    # The last copy is never dropped even if the policy asks so
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@gen_cluster(client=True, config=demo_config("drop"))
async def test_start_stop(c, s, a, b):
    x = c.submit(lambda: 123, key="x")
    await c.replicate(x, 2)
    assert len(s.tasks["x"].who_has) == 2
    s.extensions["amm"].start()
    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)
    s.extensions["amm"].start()  # Double start is a no-op
    s.extensions["amm"].stop()
    s.extensions["amm"].stop()  # Double stop is a no-op
    # AMM is not running anymore
    await c.replicate(x, 2)
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 2


@gen_cluster(client=True, config=demo_config("drop", start=True, interval=0.1))
async def test_auto_start(c, s, a, b):
    futures = await c.scatter({"x": 123}, broadcast=True)
    # The AMM should run within 0.1s of the broadcast.
    # Add generous extra padding to prevent flakiness.
    await asyncio.sleep(0.5)
    assert len(s.tasks["x"].who_has) == 1


@gen_cluster(client=True, config=demo_config("drop", key="x"))
async def test_add_policy(c, s, a, b):
    p2 = DemoPolicy(action="drop", key="y", n=10, candidates=None)
    p3 = DemoPolicy(action="drop", key="z", n=10, candidates=None)

    # policies parameter can be:
    # - None: get from config
    # - explicit set, which can be empty
    m1 = s.extensions["amm"]
    m2 = ActiveMemoryManagerExtension(s, {p2}, register=False, start=False)
    m3 = ActiveMemoryManagerExtension(s, set(), register=False, start=False)

    assert len(m1.policies) == 1
    assert len(m2.policies) == 1
    assert len(m3.policies) == 0
    m3.add_policy(p3)
    assert len(m3.policies) == 1

    futures = await c.scatter({"x": 1, "y": 2, "z": 3}, broadcast=True)
    m1.run_once()
    while len(s.tasks["x"].who_has) == 2:
        await asyncio.sleep(0.01)

    m2.run_once()
    while len(s.tasks["y"].who_has) == 2:
        await asyncio.sleep(0.01)

    m3.run_once()
    while len(s.tasks["z"].who_has) == 2:
        await asyncio.sleep(0.01)

    with pytest.raises(TypeError):
        m3.add_policy("not a policy")


@gen_cluster(client=True, config=demo_config("drop", key="x", start=False))
async def test_multi_start(c, s, a, b):
    """Multiple AMMs can be started in parallel"""
    p2 = DemoPolicy(action="drop", key="y", n=10, candidates=None)
    p3 = DemoPolicy(action="drop", key="z", n=10, candidates=None)

    # policies parameter can be:
    # - None: get from config
    # - explicit set, which can be empty
    m1 = s.extensions["amm"]
    m2 = ActiveMemoryManagerExtension(s, {p2}, register=False, start=True, interval=0.1)
    m3 = ActiveMemoryManagerExtension(s, {p3}, register=False, start=True, interval=0.1)

    assert not m1.running
    assert m2.running
    assert m3.running

    futures = await c.scatter({"x": 1, "y": 2, "z": 3}, broadcast=True)

    # The AMMs should run within 0.1s of the broadcast.
    # Add generous extra padding to prevent flakiness.
    await asyncio.sleep(0.5)
    assert len(s.tasks["x"].who_has) == 2
    assert len(s.tasks["y"].who_has) == 1
    assert len(s.tasks["z"].who_has) == 1


@gen_cluster(client=True, config=NO_AMM_START)
async def test_not_registered(c, s, a, b):
    futures = await c.scatter({"x": 1}, broadcast=True)
    assert len(s.tasks["x"].who_has) == 2

    class Policy(ActiveMemoryManagerPolicy):
        def run(self):
            yield "drop", s.tasks["x"], None

    amm = ActiveMemoryManagerExtension(s, {Policy()}, register=False, start=False)
    amm.run_once()
    assert amm is not s.extensions["amm"]

    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)


def test_client_proxy_sync(client):
    assert not client.amm.running()
    client.amm.start()
    assert client.amm.running()
    client.amm.stop()
    assert not client.amm.running()
    client.amm.run_once()


@gen_cluster(client=True, config=NO_AMM_START)
async def test_client_proxy_async(c, s, a, b):
    assert not await c.amm.running()
    await c.amm.start()
    assert await c.amm.running()
    await c.amm.stop()
    assert not await c.amm.running()
    await c.amm.run_once()


@gen_cluster(client=True, config=demo_config("drop"))
async def test_drop_not_in_memory(c, s, a, b):
    """ts.who_has is empty"""
    x = c.submit(slowinc, 1, key="x")
    while "x" not in s.tasks:
        await asyncio.sleep(0.01)
    assert not x.done()
    s.extensions["amm"].run_once()
    assert await x == 2


@gen_cluster(client=True, config=demo_config("drop"))
async def test_drop_with_waiter(c, s, a, b):
    """Tasks with a waiter are never dropped"""
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    y1 = c.submit(slowinc, x, delay=0.4, key="y1", workers=[a.address])
    y2 = c.submit(slowinc, x, delay=0.8, key="y2", workers=[b.address])
    for key in ("y1", "y2"):
        while key not in s.tasks or s.tasks[key].state != "processing":
            await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert {ws.address for ws in s.tasks["x"].who_has} == {a.address, b.address}
    assert await y1 == 2
    # y1 is finished so there's a worker available without a waiter
    s.extensions["amm"].run_once()
    while {ws.address for ws in s.tasks["x"].who_has} != {b.address}:
        await asyncio.sleep(0.01)
    assert not y2.done()


@gen_cluster(client=True, config=NO_AMM_START)
async def test_double_drop(c, s, a, b):
    """An AMM drop policy runs once to drop one of the two replicas of a key.
    Then it runs again, before the recommendations from the first iteration had the time
    to either be enacted or rejected, and chooses a different worker to drop from.

    Test that, in this use case, the last replica of a key is never dropped.
    """
    futures = await c.scatter({"x": 1}, broadcast=True)
    assert len(s.tasks["x"].who_has) == 2
    ws_iter = iter(s.workers.values())

    class Policy(ActiveMemoryManagerPolicy):
        def run(self):
            yield "drop", s.tasks["x"], {next(ws_iter)}

    amm = ActiveMemoryManagerExtension(s, {Policy()}, register=False, start=False)
    amm.run_once()
    amm.run_once()
    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@gen_cluster(client=True, config=demo_config("drop"))
async def test_double_drop_stress(c, s, a, b):
    """AMM runs many times before the recommendations of the first run are enacted"""
    futures = await c.scatter({"x": 1}, broadcast=True)
    assert len(s.tasks["x"].who_has) == 2
    for _ in range(10):
        s.extensions["amm"].run_once()
    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@pytest.mark.slow
@gen_cluster(
    nthreads=[("", 1)] * 4,
    Worker=Nanny,
    client=True,
    worker_kwargs={"memory_limit": "2 GiB"},
    config=demo_config("drop", n=1),
)
async def test_drop_from_worker_with_least_free_memory(c, s, *nannies):
    a1, a2, a3, a4 = s.workers.keys()
    ws1, ws2, ws3, ws4 = s.workers.values()

    futures = await c.scatter({"x": 1}, broadcast=True)
    assert s.tasks["x"].who_has == {ws1, ws2, ws3, ws4}
    # Allocate enough RAM to be safely more than unmanaged memory
    clog = c.submit(lambda: "x" * 2 ** 29, workers=[a3])  # 512 MiB
    # await wait(clog) is not enough; we need to wait for the heartbeats
    while ws3.memory.optimistic < 2 ** 29:
        await asyncio.sleep(0.01)
    s.extensions["amm"].run_once()

    while s.tasks["x"].who_has != {ws1, ws2, ws4}:
        await asyncio.sleep(0.01)


@gen_cluster(
    nthreads=[("", 1)] * 8,
    client=True,
    config=demo_config("drop", n=1, candidates={5, 6}),
)
async def test_drop_with_candidates(c, s, *workers):
    futures = await c.scatter({"x": 1}, broadcast=True)
    s.extensions["amm"].run_once()
    wss = list(s.workers.values())
    expect1 = {wss[0], wss[1], wss[2], wss[3], wss[4], wss[6], wss[7]}
    expect2 = {wss[0], wss[1], wss[2], wss[3], wss[4], wss[5], wss[7]}
    while s.tasks["x"].who_has not in (expect1, expect2):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=demo_config("drop", candidates=set()))
async def test_drop_with_empty_candidates(c, s, a, b):
    """Key is not dropped as the plugin proposes an empty set of candidates,
    not to be confused with None
    """
    futures = await c.scatter({"x": 1}, broadcast=True)
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 2


@gen_cluster(
    client=True, nthreads=[("", 1)] * 3, config=demo_config("drop", candidates={2})
)
async def test_drop_from_candidates_without_key(c, s, *workers):
    """Key is not dropped as none of the candidates hold a replica"""
    ws0, ws1, ws2 = s.workers.values()
    x = (await c.scatter({"x": 1}, workers=[ws0.address]))["x"]
    y = c.submit(inc, x, key="y", workers=[ws1.address])
    await y
    assert s.tasks["x"].who_has == {ws0, ws1}

    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert s.tasks["x"].who_has == {ws0, ws1}


@gen_cluster(client=True, config=demo_config("drop", candidates={0}))
async def test_drop_with_bad_candidates(c, s, a, b):
    """Key is not dropped as all candidates hold waiter tasks"""
    ws0, ws1 = s.workers.values()  # Not necessarily a, b; it could be b, a!
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    y = c.submit(slowinc, x, 0.3, key="y", workers=[ws0.address])
    while "y" not in s.tasks:
        await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    await y
    assert s.tasks["x"].who_has == {ws0, ws1}


@gen_cluster(client=True, nthreads=[("", 1)] * 10, config=demo_config("drop", n=1))
async def test_drop_prefers_paused_workers(c, s, *workers):
    x = await c.scatter({"x": 1}, broadcast=True)
    ts = s.tasks["x"]
    assert len(ts.who_has) == 10
    ws = s.workers[workers[3].address]
    workers[3].memory_pause_fraction = 1e-15
    while ws.status != Status.paused:
        await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    while len(ts.who_has) != 9:
        await asyncio.sleep(0.01)
    assert ws not in ts.who_has


@pytest.mark.slow
@gen_cluster(client=True, config=demo_config("drop"))
async def test_drop_with_paused_workers_with_running_tasks_1(c, s, a, b):
    """If there is exactly 1 worker that holds a replica of a task that isn't paused or
    retiring, and there are 1+ paused/retiring workers with the same task, don't drop
    anything.

    Use case 1 (don't drop):
    a is paused and with dependent tasks executing on it
    b is running and has no dependent tasks
    """
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    y = c.submit(slowinc, x, delay=2, key="y", workers=[a.address])
    while "y" not in a.tasks or a.tasks["y"].state != "executing":
        await asyncio.sleep(0.01)
    a.memory_pause_fraction = 1e-15
    while s.workers[a.address].status != Status.paused:
        await asyncio.sleep(0.01)
    assert s.tasks["y"].state == "processing"
    assert a.tasks["y"].state == "executing"

    s.extensions["amm"].run_once()
    await y
    assert len(s.tasks["x"].who_has) == 2


@gen_cluster(client=True, config=demo_config("drop"))
async def test_drop_with_paused_workers_with_running_tasks_2(c, s, a, b):
    """If there is exactly 1 worker that holds a replica of a task that isn't paused or
    retiring, and there are 1+ paused/retiring workers with the same task, don't drop
    anything.

    Use case 2 (drop from a):
    a is paused and has no dependent tasks
    b is running and has no dependent tasks
    """
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    a.memory_pause_fraction = 1e-15
    while s.workers[a.address].status != Status.paused:
        await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert {ws.address for ws in s.tasks["x"].who_has} == {b.address}


@pytest.mark.slow
@pytest.mark.parametrize("pause", [True, False])
@gen_cluster(
    client=True,
    config=demo_config("drop"),
    worker_kwargs={"memory_monitor_interval": "50ms"},
)
async def test_drop_with_paused_workers_with_running_tasks_3_4(c, s, a, b, pause):
    """If there is exactly 1 worker that holds a replica of a task that isn't paused or
    retiring, and there are 1+ paused/retiring workers with the same task, don't drop
    anything.

    Use case 3 (drop from b):
    a is paused and with dependent tasks executing on it
    b is paused and has no dependent tasks

    Use case 4 (drop from b):
    a is running and with dependent tasks executing on it
    b is running and has no dependent tasks
    """
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    y = c.submit(slowinc, x, delay=2.5, key="y", workers=[a.address])
    while "y" not in a.tasks or a.tasks["y"].state != "executing":
        await asyncio.sleep(0.01)

    if pause:
        a.memory_pause_fraction = 1e-15
        b.memory_pause_fraction = 1e-15
        while any(ws.status != Status.paused for ws in s.workers.values()):
            await asyncio.sleep(0.01)

    assert s.tasks["y"].state == "processing"
    assert a.tasks["y"].state == "executing"

    s.extensions["amm"].run_once()
    await y
    assert {ws.address for ws in s.tasks["x"].who_has} == {a.address}


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)] * 3, config=demo_config("drop"))
async def test_drop_with_paused_workers_with_running_tasks_5(c, s, w1, w2, w3):
    """If there is exactly 1 worker that holds a replica of a task that isn't paused or
    retiring, and there are 1+ paused/retiring workers with the same task, don't drop
    anything.

    Use case 5 (drop from w2):
    w1 is paused and with dependent tasks executing on it
    w2 is running and has no dependent tasks
    w3 is running and with dependent tasks executing on it
    """
    x = (await c.scatter({"x": 1}, broadcast=True))["x"]
    y1 = c.submit(slowinc, x, delay=2, key="y1", workers=[w1.address])
    y2 = c.submit(slowinc, x, delay=2, key="y2", workers=[w3.address])
    while (
        "y1" not in w1.tasks
        or w1.tasks["y1"].state != "executing"
        or "y2" not in w3.tasks
        or w3.tasks["y2"].state != "executing"
    ):
        await asyncio.sleep(0.01)
    w1.memory_pause_fraction = 1e-15
    while s.workers[w1.address].status != Status.paused:
        await asyncio.sleep(0.01)
    assert s.tasks["y1"].state == "processing"
    assert s.tasks["y2"].state == "processing"
    assert w1.tasks["y1"].state == "executing"
    assert w3.tasks["y2"].state == "executing"

    s.extensions["amm"].run_once()
    await y1
    await y2
    assert {ws.address for ws in s.tasks["x"].who_has} == {w1.address, w3.address}


@gen_cluster(nthreads=[("", 1)] * 4, client=True, config=demo_config("replicate", n=2))
async def test_replicate(c, s, *workers):
    futures = await c.scatter({"x": 123})
    assert len(s.tasks["x"].who_has) == 1

    s.extensions["amm"].run_once()
    while len(s.tasks["x"].who_has) < 3:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 3

    s.extensions["amm"].run_once()
    while len(s.tasks["x"].who_has) < 4:
        await asyncio.sleep(0.01)

    for w in workers:
        assert w.data["x"] == 123


@gen_cluster(client=True, config=demo_config("replicate"))
async def test_replicate_not_in_memory(c, s, a, b):
    """ts.who_has is empty"""
    x = c.submit(slowinc, 1, key="x")
    while "x" not in s.tasks:
        await asyncio.sleep(0.01)
    assert not x.done()
    s.extensions["amm"].run_once()
    assert await x == 2
    assert len(s.tasks["x"].who_has) == 1
    s.extensions["amm"].run_once()
    while len(s.tasks["x"].who_has) < 2:
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=demo_config("replicate"))
async def test_double_replicate_stress(c, s, a, b):
    """AMM runs many times before the recommendations of the first run are enacted"""
    futures = await c.scatter({"x": 1})
    assert len(s.tasks["x"].who_has) == 1
    for _ in range(10):
        s.extensions["amm"].run_once()
    while len(s.tasks["x"].who_has) < 2:
        await asyncio.sleep(0.01)


@pytest.mark.slow
@gen_cluster(
    nthreads=[("", 1)] * 4,
    Worker=Nanny,
    client=True,
    worker_kwargs={"memory_limit": "2 GiB"},
    config=demo_config("replicate", n=1),
)
async def test_replicate_to_worker_with_most_free_memory(c, s, *nannies):
    a1, a2, a3, a4 = s.workers.keys()
    ws1, ws2, ws3, ws4 = s.workers.values()

    futures = await c.scatter({"x": 1}, workers=[a1])
    assert s.tasks["x"].who_has == {ws1}
    # Allocate enough RAM to be safely more than unmanaged memory
    clog2 = c.submit(lambda: "x" * 2 ** 29, workers=[a2])  # 512 MiB
    clog4 = c.submit(lambda: "x" * 2 ** 29, workers=[a4])  # 512 MiB
    # await wait(clog) is not enough; we need to wait for the heartbeats
    for ws in (ws2, ws4):
        while ws.memory.optimistic < 2 ** 29:
            await asyncio.sleep(0.01)
    s.extensions["amm"].run_once()

    while s.tasks["x"].who_has != {ws1, ws3}:
        await asyncio.sleep(0.01)


@gen_cluster(
    nthreads=[("", 1)] * 8,
    client=True,
    config=demo_config("replicate", n=1, candidates={5, 6}),
)
async def test_replicate_with_candidates(c, s, *workers):
    wss = list(s.workers.values())
    futures = await c.scatter({"x": 1}, workers=[wss[0].address])
    s.extensions["amm"].run_once()
    expect1 = {wss[0], wss[5]}
    expect2 = {wss[0], wss[6]}
    while s.tasks["x"].who_has not in (expect1, expect2):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=demo_config("replicate", candidates=set()))
async def test_replicate_with_empty_candidates(c, s, a, b):
    """Key is not replicated as the plugin proposes an empty set of candidates,
    not to be confused with None
    """
    futures = await c.scatter({"x": 1})
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@gen_cluster(client=True, config=demo_config("replicate", candidates={0}))
async def test_replicate_to_candidates_with_key(c, s, a, b):
    """Key is not replicated as all candidates already hold replicas"""
    ws0, ws1 = s.workers.values()  # Not necessarily a, b; it could be b, a!
    futures = await c.scatter({"x": 1}, workers=[ws0.address])
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert s.tasks["x"].who_has == {ws0}


@gen_cluster(client=True, nthreads=[("", 1)] * 3, config=demo_config("replicate"))
async def test_replicate_avoids_paused_workers_1(c, s, w0, w1, w2):
    w1.memory_pause_fraction = 1e-15
    while s.workers[w1.address].status != Status.paused:
        await asyncio.sleep(0.01)

    futures = await c.scatter({"x": 1}, workers=[w0.address])
    s.extensions["amm"].run_once()
    while "x" not in w2.data:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.2)
    assert "x" not in w1.data


@gen_cluster(client=True, config=demo_config("replicate"))
async def test_replicate_avoids_paused_workers_2(c, s, a, b):
    b.memory_pause_fraction = 1e-15
    while s.workers[b.address].status != Status.paused:
        await asyncio.sleep(0.01)

    futures = await c.scatter({"x": 1}, workers=[a.address])
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert "x" not in b.data


@gen_cluster(
    nthreads=[("", 1)] * 4,
    client=True,
    config={
        "distributed.scheduler.active-memory-manager.start": False,
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
            # Run two instances of the plugin in sequence, to emulate multiple plugins
            # that issues drop suggestions for the same keys
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
        ],
    },
)
async def test_ReduceReplicas(c, s, *workers):
    # Logging is quiet if there are no suggestions
    with assert_amm_log(
        [
            "Running policy: ReduceReplicas()",
            "Running policy: ReduceReplicas()",
            "Active Memory Manager run in ",
        ],
    ):
        s.extensions["amm"].run_once()

    futures = await c.scatter({"x": 123}, broadcast=True)
    assert len(s.tasks["x"].who_has) == 4

    with assert_amm_log(
        [
            "Running policy: ReduceReplicas()",
            "(drop, <TaskState 'x' memory>, None): dropping from <WorkerState ",
            "(drop, <TaskState 'x' memory>, None): dropping from <WorkerState ",
            "(drop, <TaskState 'x' memory>, None): dropping from <WorkerState ",
            "ReduceReplicas: Dropping 3 superfluous replicas of 1 tasks",
            "Running policy: ReduceReplicas()",
            "Enacting suggestions for 1 tasks:",
            "- <WorkerState ",
            "- <WorkerState ",
            "- <WorkerState ",
            "Active Memory Manager run in ",
        ],
    ):
        s.extensions["amm"].run_once()

    while len(s.tasks["x"].who_has) > 1:
        await asyncio.sleep(0.01)


class DropEverything(ActiveMemoryManagerPolicy):
    """Inanely suggest to drop every single key in the cluster"""

    def __init__(self):
        self.i = 0

    def run(self):
        for ts in self.manager.scheduler.tasks.values():
            # Instead of yielding ("drop", ts, None) for each worker, which would result
            # in semi-predictable output about which replica survives, randomly choose a
            # different survivor at each AMM run.
            candidates = list(ts.who_has)
            random.shuffle(candidates)
            for ws in candidates:
                yield "drop", ts, {ws}

        # Stop running after ~2s
        self.i += 1
        if self.i == 20:
            self.manager.policies.remove(self)


async def tensordot_stress(c):
    da = pytest.importorskip("dask.array")

    rng = da.random.RandomState(0)
    a = rng.random((20, 20), chunks=(1, 1))
    b = (a @ a.T).sum().round(3)
    assert await c.compute(b) == 2134.398


@pytest.mark.slow
@pytest.mark.avoid_ci(reason="distributed#5371")
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    Worker=Nanny,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.tests.test_active_memory_manager.DropEverything"},
        ],
    },
)
async def test_drop_stress(c, s, *nannies):
    """A policy which suggests dropping everything won't break a running computation,
    but only slow it down.

    See also: test_ReduceReplicas_stress
    """
    await tensordot_stress(c)


@pytest.mark.slow
@pytest.mark.avoid_ci(reason="distributed#5371")
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    Worker=Nanny,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
        ],
    },
)
async def test_ReduceReplicas_stress(c, s, *nannies):
    """Running ReduceReplicas compulsively won't break a running computation. Unlike
    test_drop_stress above, this test does not stop running after a few seconds - the
    policy must not disrupt the computation too much.
    """
    await tensordot_stress(c)
