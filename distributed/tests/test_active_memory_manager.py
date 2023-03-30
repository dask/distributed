from __future__ import annotations

import asyncio
import logging
import random
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal

import pytest

import dask.config

from distributed import Event, Lock, Scheduler, wait
from distributed.active_memory_manager import (
    ActiveMemoryManagerExtension,
    ActiveMemoryManagerPolicy,
    RetireWorker,
)
from distributed.core import Status
from distributed.utils_test import (
    NO_AMM,
    BlockedGatherDep,
    assert_story,
    captured_logger,
    gen_cluster,
    gen_test,
    inc,
    lock_inc,
    slowinc,
    wait_for_state,
)
from distributed.worker_state_machine import AcquireReplicasEvent


@contextmanager
def assert_amm_log(expect: list[str]) -> Iterator[None]:
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

    def __init__(
        self,
        action: Literal["drop", "replicate"],
        key: str,
        n: int,
        candidates: list[int] | None,
    ):
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


def demo_config(
    action: Literal["drop", "replicate"],
    key: str = "x",
    n: int = 10,
    candidates: list[int] | None = None,
    start: bool = False,
    interval: float = 0.1,
    measure: str = "managed",
) -> dict[str, Any]:
    """Create a dask config for AMM with DemoPolicy"""
    return {
        "distributed.scheduler.active-memory-manager.start": start,
        "distributed.scheduler.active-memory-manager.interval": interval,
        "distributed.scheduler.active-memory-manager.measure": measure,
        "distributed.scheduler.active-memory-manager.policies": [
            {
                "class": "distributed.tests.test_active_memory_manager.DemoPolicy",
                "action": action,
                "key": key,
                "n": n,
                "candidates": candidates,
            },
        ],
        # If pause is required, do it manually by setting Worker.status = Status.paused
        "distributed.worker.memory.pause": False,
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


@gen_cluster(client=True, config=NO_AMM)
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


def test_client_proxy_sync(client_no_amm):
    c = client_no_amm
    assert not c.amm.running()
    c.amm.start()
    assert c.amm.running()
    c.amm.stop()
    assert not c.amm.running()
    c.amm.run_once()


@gen_cluster(client=True, config=NO_AMM)
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


@gen_cluster(client=True, config=NO_AMM)
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


@gen_cluster(nthreads=[("", 1)] * 4, client=True, config=demo_config("drop", n=1))
async def test_drop_from_worker_with_least_free_memory(c, s, *workers):
    ws1, ws2, ws3, ws4 = s.workers.values()

    futures = await c.scatter({"x": 1}, broadcast=True)
    assert s.tasks["x"].who_has == {ws1, ws2, ws3, ws4}
    clog = c.submit(lambda: "x" * 100, workers=[ws3.address])
    await wait(clog)

    s.extensions["amm"].run_once()

    while s.tasks["x"].who_has != {ws1, ws2, ws4}:
        await asyncio.sleep(0.01)


@gen_cluster(
    nthreads=[("", 1)] * 8,
    client=True,
    config=demo_config("drop", n=1, candidates=[5, 6]),
)
async def test_drop_with_candidates(c, s, *workers):
    futures = await c.scatter({"x": 1}, broadcast=True)
    s.extensions["amm"].run_once()
    wss = list(s.workers.values())
    expect1 = {wss[0], wss[1], wss[2], wss[3], wss[4], wss[6], wss[7]}
    expect2 = {wss[0], wss[1], wss[2], wss[3], wss[4], wss[5], wss[7]}
    while s.tasks["x"].who_has not in (expect1, expect2):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=demo_config("drop", candidates=[]))
async def test_drop_with_empty_candidates(c, s, a, b):
    """Key is not dropped as the plugin proposes an empty set of candidates,
    not to be confused with None
    """
    futures = await c.scatter({"x": 1}, broadcast=True)
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 2


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 3,
    config=demo_config("drop", candidates=[2]),
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


@gen_cluster(client=True, config=demo_config("drop", candidates=[0]))
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
    workers[3].status = Status.paused
    while ws.status != Status.paused:
        await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    while len(ts.who_has) != 9:
        await asyncio.sleep(0.01)
    assert ws not in ts.who_has


@gen_cluster(client=True, config=demo_config("drop"))
async def test_drop_with_paused_workers_with_running_tasks_1(c, s, a, b):
    """If there is exactly 1 worker that holds a replica of a task that isn't paused or
    retiring, and there are 1+ paused/retiring workers with the same task, don't drop
    anything.

    Use case 1 (don't drop):
    a is paused and with dependent tasks executing on it
    b is running and has no dependent tasks
    """
    lock = Lock()
    async with lock:
        x = (await c.scatter({"x": 1}, broadcast=True))["x"]
        y = c.submit(lock_inc, x, lock=lock, key="y", workers=[a.address])
        await wait_for_state("y", "executing", a)

        a.status = Status.paused
        while s.workers[a.address].status != Status.paused:
            await asyncio.sleep(0.01)
        assert a.state.tasks["y"].state == "executing"

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
    a.status = Status.paused
    while s.workers[a.address].status != Status.paused:
        await asyncio.sleep(0.01)

    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert {ws.address for ws in s.tasks["x"].who_has} == {b.address}


@pytest.mark.parametrize("pause", [True, False])
@gen_cluster(client=True, config=demo_config("drop"))
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
    lock = Lock()
    async with lock:
        x = (await c.scatter({"x": 1}, broadcast=True))["x"]
        y = c.submit(lock_inc, x, lock, key="y", workers=[a.address])
        await wait_for_state("y", "executing", a)

        if pause:
            a.status = Status.paused
            b.status = Status.paused
            while any(ws.status != Status.paused for ws in s.workers.values()):
                await asyncio.sleep(0.01)

        assert s.tasks["y"].state == "processing"
        assert a.state.tasks["y"].state == "executing"

        s.extensions["amm"].run_once()
    await y
    assert {ws.address for ws in s.tasks["x"].who_has} == {a.address}


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
    lock = Lock()
    async with lock:
        x = (await c.scatter({"x": 1}, broadcast=True))["x"]
        y1 = c.submit(lock_inc, x, lock=lock, key="y1", workers=[w1.address])
        y2 = c.submit(lock_inc, x, lock=lock, key="y2", workers=[w3.address])

        def executing() -> bool:
            return (
                "y1" in w1.state.tasks
                and w1.state.tasks["y1"].state == "executing"
                and "y2" in w3.state.tasks
                and w3.state.tasks["y2"].state == "executing"
            )

        while not executing():
            await asyncio.sleep(0.01)
        w1.status = Status.paused
        while s.workers[w1.address].status != Status.paused:
            await asyncio.sleep(0.01)
        assert executing()

        s.extensions["amm"].run_once()
        while {ws.address for ws in s.tasks["x"].who_has} != {w1.address, w3.address}:
            await asyncio.sleep(0.01)
        assert executing()


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


@gen_cluster(nthreads=[("", 1)] * 4, client=True, config=demo_config("replicate", n=1))
async def test_replicate_to_worker_with_most_free_memory(c, s, *workers):
    ws1, ws2, ws3, ws4 = s.workers.values()

    x = await c.scatter({"x": 1}, workers=[ws1.address])
    clogs = await c.scatter([2, 3], workers=[ws2.address, ws4.address])

    assert s.tasks["x"].who_has == {ws1}
    s.extensions["amm"].run_once()

    while s.tasks["x"].who_has != {ws1, ws3}:
        await asyncio.sleep(0.01)


@gen_cluster(
    nthreads=[("", 1)] * 8,
    client=True,
    config=demo_config("replicate", n=1, candidates=[5, 6]),
)
async def test_replicate_with_candidates(c, s, *workers):
    wss = list(s.workers.values())
    futures = await c.scatter({"x": 1}, workers=[wss[0].address])
    s.extensions["amm"].run_once()
    expect1 = {wss[0], wss[5]}
    expect2 = {wss[0], wss[6]}
    while s.tasks["x"].who_has not in (expect1, expect2):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=demo_config("replicate", candidates=[]))
async def test_replicate_with_empty_candidates(c, s, a, b):
    """Key is not replicated as the plugin proposes an empty set of candidates,
    not to be confused with None
    """
    futures = await c.scatter({"x": 1})
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@gen_cluster(client=True, config=demo_config("replicate", candidates=[0]))
async def test_replicate_to_candidates_with_key(c, s, a, b):
    """Key is not replicated as all candidates already hold replicas"""
    ws0, ws1 = s.workers.values()  # Not necessarily a, b; it could be b, a!
    futures = await c.scatter({"x": 1}, workers=[ws0.address])
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert s.tasks["x"].who_has == {ws0}


@gen_cluster(client=True, nthreads=[("", 1)] * 3, config=demo_config("replicate"))
async def test_replicate_avoids_paused_workers_1(c, s, w0, w1, w2):
    w1.status = Status.paused
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
    b.status = Status.paused
    while s.workers[b.address].status != Status.paused:
        await asyncio.sleep(0.01)

    futures = await c.scatter({"x": 1}, workers=[a.address])
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert "x" not in b.data


@gen_test()
async def test_bad_measure():
    with dask.config.set(
        {"distributed.scheduler.active-memory-manager.measure": "notexist"}
    ):
        with pytest.raises(ValueError) as e:
            await Scheduler(dashboard_address=":0")

    assert "measure must be one of " in str(e.value)


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


@pytest.mark.parametrize("start_amm", [False, True])
@gen_cluster(client=True)
async def test_RetireWorker_amm_on_off(c, s, a, b, start_amm):
    """retire_workers must work both with and without the AMM started"""
    if start_amm:
        await c.amm.start()
    else:
        await c.amm.stop()

    futures = await c.scatter({"x": 1}, workers=[a.address])
    await c.retire_workers([a.address])
    assert a.address not in s.workers
    assert "x" in b.data


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.policies": [],
    },
)
async def test_RetireWorker_no_remove(c, s, a, b):
    """Test RetireWorker behaviour on retire_workers(..., remove=False)"""

    x = await c.scatter({"x": "x"}, workers=[a.address])
    await c.retire_workers([a.address], close_workers=False, remove=False)
    # Wait 2 AMM iterations
    # retire_workers may return before all keys have been dropped from a
    while s.tasks["x"].who_has != {s.workers[b.address]}:
        await asyncio.sleep(0.01)
    assert a.address in s.workers
    assert a.status == Status.closing_gracefully
    assert s.workers[a.address].status == Status.closing_gracefully
    # Policy has been removed without waiting for worker to disappear from
    # Scheduler.workers
    assert not s.extensions["amm"].policies


@pytest.mark.parametrize("use_ReduceReplicas", [False, True])
@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.measure": "managed",
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
        ],
    },
)
async def test_RetireWorker_with_ReduceReplicas(c, s, *workers, use_ReduceReplicas):
    """RetireWorker and ReduceReplicas work well with each other.

    If ReduceReplicas is enabled,
    1. On the first AMM iteration, either ReduceReplicas or RetireWorker (arbitrarily
       depending on which comes first in the iteration of
       ActiveMemoryManagerExtension.policies) deletes non-unique keys, choosing from
       workers to be retired first. At the same time, RetireWorker replicates unique
       keys.
    2. On the second AMM iteration, either ReduceReplicas or RetireWorker deletes the
       keys replicated at the previous round from the worker to be retired.

    If ReduceReplicas is not enabled, all drops are performed by RetireWorker.

    This test fundamentally relies on workers in the process of being retired to be
    always picked first by ActiveMemoryManagerExtension._find_dropper.
    """
    ws_a, ws_b = s.workers.values()
    if not use_ReduceReplicas:
        s.extensions["amm"].policies.clear()

    x = c.submit(lambda: "x", key="x", workers=[ws_a.address])
    y = c.submit(lambda: "y", key="y", workers=[ws_a.address])
    z = c.submit(lambda x: None, x, key="z", workers=[ws_b.address])  # copy x to ws_b
    # Make sure that the worker NOT being retired has the most RAM usage to test that
    # it is not being picked first since there's a retiring worker.
    w = c.submit(lambda: "w" * 100, key="w", workers=[ws_b.address])
    await wait([x, y, z, w])

    await c.retire_workers([ws_a.address], remove=False)
    # retire_workers may return before all keys have been dropped from a
    while ws_a.has_what:
        await asyncio.sleep(0.01)
    assert {ts.key for ts in ws_b.has_what} == {"x", "y", "z", "w"}


@gen_cluster(client=True, nthreads=[("", 1)] * 3, config=NO_AMM)
async def test_RetireWorker_all_replicas_are_being_retired(c, s, w1, w2, w3):
    """There are multiple replicas of a key, but they all reside on workers that are
    being retired
    """
    ws1 = s.workers[w1.address]
    ws2 = s.workers[w2.address]
    ws3 = s.workers[w3.address]
    fut = await c.scatter({"x": "x"}, workers=[w1.address, w2.address], broadcast=True)
    assert s.tasks["x"].who_has == {ws1, ws2}
    await c.retire_workers([w1.address, w2.address])
    assert s.tasks["x"].who_has == {ws3}


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        # test that we're having a manual amm.run_once() "kick" from retire_workers
        "distributed.scheduler.active-memory-manager.interval": 999,
        "distributed.scheduler.active-memory-manager.policies": [],
    },
)
async def test_RetireWorker_no_recipients(c, s, w1, w2, w3, w4):
    """All workers are retired at once.

    Test use cases:
    1. (w1) worker contains no data -> it is retired
    2. (w2) worker contains unique data -> it is not retired
    3. (w3, w4) worker contains non-unique data, but all replicas are on workers that
       are being retired -> all but one are retired
    """
    x = await c.scatter({"x": "x"}, workers=[w2.address])
    y = await c.scatter({"y": "y"}, workers=[w3.address, w4.address], broadcast=True)

    out = await c.retire_workers([w1.address, w2.address, w3.address, w4.address])

    assert set(out) in ({w1.address, w3.address}, {w1.address, w4.address})
    assert not s.extensions["amm"].policies
    assert set(s.workers) in ({w2.address, w3.address}, {w2.address, w4.address})
    # After a Scheduler -> Worker -> Scheduler roundtrip, workers that failed to retire
    # went back from closing_gracefully to running and can run tasks
    while any(ws.status != Status.running for ws in s.workers.values()):
        await asyncio.sleep(0.01)
    assert await c.submit(inc, 1) == 2


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 999,
        "distributed.scheduler.active-memory-manager.policies": [],
        "distributed.worker.memory.pause": False,
    },
)
async def test_RetireWorker_all_recipients_are_paused(c, s, a, b):
    ws_a = s.workers[a.address]
    ws_b = s.workers[b.address]

    b.status = Status.paused
    while ws_b.status != Status.paused:
        await asyncio.sleep(0.01)

    x = await c.scatter("x", workers=[a.address])
    out = await c.retire_workers([a.address])
    assert out == {}
    assert not s.extensions["amm"].policies
    assert set(s.workers) == {a.address, b.address}

    # After a Scheduler -> Worker -> Scheduler roundtrip, workers that failed to
    # retire went back from closing_gracefully to running and can run tasks
    while ws_a.status != Status.running:
        await asyncio.sleep(0.01)
    assert await c.submit(inc, 1) == 2


@gen_cluster(
    client=True,
    config={
        # Don't use one-off AMM instance
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.policies": [],
    },
)
async def test_RetireWorker_new_keys_arrive_after_all_keys_moved_away(c, s, a, b):
    """
    If all keys have been moved off a worker, but then new keys arrive (due to task
    completion or `gather_dep`) before the worker has actually closed, make sure we
    still retire it (instead of hanging forever).

    This test is timing-sensitive. If it runs too slowly, it *should* `pytest.skip`
    itself.

    See https://github.com/dask/distributed/issues/6223 for motivation.
    """
    ws_a = s.workers[a.address]
    ws_b = s.workers[b.address]
    event = Event()

    # Put 200 keys on the worker, so `_track_retire_worker` will sleep for 0.5s
    xs = c.map(lambda x: x, range(200), workers=[a.address])
    await wait(xs)

    # Put an extra task on the worker, which we will allow to complete once the `xs`
    # have been replicated.
    extra = c.submit(
        lambda: event.wait("2s"),
        workers=[a.address],
        allow_other_workers=True,
        key="extra",
    )
    await wait_for_state(extra.key, "executing", a)

    t = asyncio.create_task(c.retire_workers([a.address]))

    amm = s.extensions["amm"]
    while not amm.policies:
        await asyncio.sleep(0)
    policy = next(iter(amm.policies))
    assert isinstance(policy, RetireWorker)

    # Wait for all `xs` to be replicated.
    while len(ws_b.has_what) != len(xs):
        await asyncio.sleep(0)

    # `_track_retire_worker` _should_ now be sleeping for 0.5s, because there were >=200
    # keys on A. In this test, everything from the beginning of the transfers needs to
    # happen within 0.5s.

    # Simulate waiting for the policy to run again.
    # Note that the interval at which the policy runs is inconsequential for this test.
    amm.run_once()

    # The policy has removed itself, because all `xs` have been replicated.
    assert not amm.policies
    assert policy.done(), {ts.key: ts.who_has for ts in ws_a.has_what}

    # But what if a new key arrives now while `_track_retire_worker` is still (maybe)
    # sleeping? Let `extra` complete and wait for it to hit the scheduler.
    await event.set()
    await wait(extra)

    if a.address not in s.workers:
        # It took more than 0.5s to get here, and the scheduler closed our worker. Dang.
        pytest.xfail(
            "Timing didn't work out: `_track_retire_worker` finished before "
            "`extra` completed."
        )

    # `retire_workers` doesn't hang
    await t
    assert a.address not in s.workers
    assert not amm.policies

    # `extra` was not transferred from `a` to `b`. Instead, it was recomputed on `b`.
    story = b.state.story(extra.key)
    assert_story(
        story,
        [
            (extra.key, "compute-task", "released"),
            (extra.key, "released", "waiting", "waiting", {"extra": "ready"}),
            (extra.key, "waiting", "ready", "ready", {"extra": "executing"}),
        ],
    )

    # `extra` completes successfully and is fetched from the other worker.
    await extra.result()


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.worker-ttl": "500ms",
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.05,
        "distributed.scheduler.active-memory-manager.measure": "managed",
        "distributed.scheduler.active-memory-manager.policies": [],
    },
)
async def test_RetireWorker_faulty_recipient(c, s, w1, w2):
    """RetireWorker requests to replicate a key onto an unresponsive worker.
    The AMM will iterate multiple times, repeating the command, until eventually the
    scheduler declares the worker dead and removes it from the pool; at that point the
    AMM will choose another valid worker and complete the job.
    """
    # w1 is being retired
    # w3 has the lowest RAM usage and is chosen as a recipient, but is unresponsive

    x = c.submit(lambda: 123, key="x", workers=[w1.address])
    await wait(x)
    # Fill w2 with dummy data so that it's got the highest memory usage
    # among the workers that are not being retired (w2 and w3).
    clutter = await c.scatter(456, workers=[w2.address])

    async with BlockedGatherDep(s.address) as w3:
        await c.wait_for_workers(3)

        retire_fut = asyncio.create_task(c.retire_workers([w1.address]))
        # w3 is chosen as the recipient for x, because it's got the lowest memory usage
        await w3.in_gather_dep.wait()

        # AMM unfruitfully sends to w3 a new {op: acquire-replicas} message every 0.05s
        while (
            sum(isinstance(ev, AcquireReplicasEvent) for ev in w3.state.stimulus_log)
            < 3
        ):
            await asyncio.sleep(0.01)

        assert not retire_fut.done()

    # w3 has been shut down. At this point, AMM switches to w2.
    await retire_fut

    assert w1.address not in s.workers
    assert w3.address not in s.workers
    assert dict(w2.data) == {"x": 123, clutter.key: 456}


class Counter:
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1


@gen_cluster(client=True, config=demo_config("drop"))
async def test_dont_drop_actors(c, s, a, b):
    x = c.submit(Counter, key="x", actor=True, workers=[a.address])
    y = c.submit(lambda cnt: cnt.increment(), x, key="y", workers=[b.address])
    await wait([x, y])
    assert len(s.tasks["x"].who_has) == 2
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 2


@gen_cluster(client=True, config=demo_config("replicate"))
async def test_dont_replicate_actors(c, s, a, b):
    x = c.submit(Counter, key="x", actor=True)
    await wait(x)
    assert len(s.tasks["x"].who_has) == 1
    s.extensions["amm"].run_once()
    await asyncio.sleep(0.2)
    assert len(s.tasks["x"].who_has) == 1


@pytest.mark.parametrize("has_proxy", [False, True])
@gen_cluster(client=True, config=NO_AMM)
async def test_RetireWorker_with_actor(c, s, a, b, has_proxy):
    """A worker holding one or more original actor objects cannot be retired"""
    x = c.submit(Counter, key="x", actor=True, workers=[a.address])
    await wait(x)
    assert "x" in a.state.actors

    if has_proxy:
        y = c.submit(
            lambda cnt: cnt.increment().result(), x, key="y", workers=[b.address]
        )
        await wait(y)
        assert "x" in b.data
        assert "y" in b.data

    with captured_logger("distributed.active_memory_manager", logging.WARNING) as log:
        out = await c.retire_workers([a.address])
    assert out == {}
    assert "it holds actor(s)" in log.getvalue()
    assert "x" in a.state.actors

    if has_proxy:
        assert "x" in b.data
        assert "y" in b.data


@gen_cluster(client=True, config=NO_AMM)
async def test_RetireWorker_with_actor_proxy(c, s, a, b):
    """A worker holding an Actor proxy object can be retired as normal."""
    x = c.submit(Counter, key="x", actor=True, workers=[a.address])
    y = c.submit(lambda cnt: cnt.increment().result(), x, key="y", workers=[b.address])
    await wait(y)
    assert "x" in a.state.actors
    assert "x" in b.data
    assert "y" in b.data

    out = await c.retire_workers([b.address])
    assert out.keys() == {b.address}
    assert "x" in a.state.actors
    assert "y" in a.data


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
    a = rng.random((10, 10), chunks=(1, 1))
    # dask.array.core.PerformanceWarning: Increasing number of chunks by factor of 10
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        b = (a @ a.T).sum().round(3)
    assert await c.compute(b) == 245.394


@pytest.mark.slow
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    config=NO_AMM,
)
async def test_noamm_stress(c, s, *workers):
    """Test the tensordot_stress helper without AMM. This is to figure out if a
    stability issue is AMM-specific or not.
    """
    await tensordot_stress(c)


@pytest.mark.slow
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.measure": "managed",
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.tests.test_active_memory_manager.DropEverything"},
        ],
    },
)
async def test_drop_stress(c, s, *workers):
    """A policy which suggests dropping everything won't break a running computation,
    but only slow it down.

    See also: test_ReduceReplicas_stress
    """
    await tensordot_stress(c)


@pytest.mark.slow
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 4,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.measure": "managed",
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
        ],
    },
)
async def test_ReduceReplicas_stress(c, s, *workers):
    """Running ReduceReplicas compulsively won't break a running computation. Unlike
    test_drop_stress above, this test does not stop running after a few seconds - the
    policy must not disrupt the computation too much.
    """
    await tensordot_stress(c)


@pytest.mark.slow
@pytest.mark.parametrize("use_ReduceReplicas", [False, True])
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 10,
    config={
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": 0.1,
        "distributed.scheduler.active-memory-manager.measure": "managed",
        "distributed.scheduler.active-memory-manager.policies": [
            {"class": "distributed.active_memory_manager.ReduceReplicas"},
        ],
    },
    scheduler_kwargs={"transition_counter_max": 500_000},
    worker_kwargs={"transition_counter_max": 500_000},
)
async def test_RetireWorker_stress(c, s, *workers, use_ReduceReplicas):
    """It is safe to retire the best part of a cluster in the middle of a computation"""
    if not use_ReduceReplicas:
        s.extensions["amm"].policies.clear()

    addrs = list(s.workers)
    random.shuffle(addrs)
    print(f"Removing all workers except {addrs[-1]}")

    # Note: Scheduler._lock effectively prevents multiple calls to retire_workers from
    # running at the same time. However, the lock only exists for the benefit of legacy
    # (non-AMM) rebalance() and replicate() methods. Once the lock is removed, these
    # calls will become parallel and the test *should* continue working.

    tasks = [asyncio.create_task(tensordot_stress(c))]
    await asyncio.sleep(1)
    tasks.append(asyncio.create_task(c.retire_workers(addrs[0:2])))
    await asyncio.sleep(1)
    tasks.append(asyncio.create_task(c.retire_workers(addrs[2:5])))
    await asyncio.sleep(1)
    tasks.append(asyncio.create_task(c.retire_workers(addrs[5:9])))

    await asyncio.gather(*tasks)
    assert set(s.workers) == {addrs[9]}
