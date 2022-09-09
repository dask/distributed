from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import math
import random
import sys
import weakref
from collections import defaultdict
from operator import mul
from time import sleep

import numpy as np
import pytest
from tlz import sliding_window

import dask
from dask.utils import key_split

from distributed import Event, Lock, Nanny, Worker, profile, wait, worker_client
from distributed.compatibility import LINUX
from distributed.core import Status
from distributed.metrics import time
from distributed.system import MEMORY_LIMIT
from distributed.utils_test import (
    SizeOf,
    captured_logger,
    freeze_batched_send,
    gen_cluster,
    gen_nbytes,
    inc,
    nodebug_setup_module,
    nodebug_teardown_module,
    slowadd,
    slowidentity,
    slowinc,
)
from distributed.worker_state_machine import (
    ExecuteSuccessEvent,
    FreeKeysEvent,
    StealRequestEvent,
)

pytestmark = pytest.mark.ci1

# Most tests here are timing-dependent
setup_module = nodebug_setup_module
teardown_module = nodebug_teardown_module


class Sizeof:
    """Helper class for creating task results of a given size"""

    def __init__(self, nbytes):
        self._nbytes = nbytes - sys.getsizeof(object)

    def __sizeof__(self) -> int:
        return self._nbytes


@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_work_stealing(c, s, a, b):
    [x] = await c._scatter([1], workers=a.address)
    futures = c.map(slowadd, range(50), [x] * 50)
    await wait(futures)
    assert len(a.data) > 10
    assert len(b.data) > 10


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_dont_steal_expensive_data_fast_computation(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = c.submit(np.arange, 1000000, workers=a.address)
    await wait([x])
    future = c.submit(np.sum, [1], workers=a.address)  # learn that sum is fast
    await wait([future])

    cheap = [
        c.submit(np.sum, x, pure=False, workers=a.address, allow_other_workers=True)
        for i in range(10)
    ]
    await wait(cheap)
    assert len(s.tasks[x.key].who_has) == 1
    assert len(b.data) == 0
    assert len(a.data) == 12


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_steal_cheap_data_slow_computation(c, s, a, b):
    x = c.submit(slowinc, 100, delay=0.1)  # learn that slowinc is slow
    await wait(x)

    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    await wait(futures)
    assert abs(len(a.data) - len(b.data)) <= 5


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_steal_expensive_data_slow_computation(c, s, a, b):
    np = pytest.importorskip("numpy")

    x = c.submit(slowinc, 1, delay=0.2, workers=a.address)
    await wait(x)  # learn that slowinc is slow

    x = c.submit(np.arange, 1_000_000, workers=a.address)  # put expensive data
    await wait(x)

    slow = [c.submit(slowinc, x, delay=0.1, pure=False) for _ in range(20)]
    await wait(slow)
    assert len(s.tasks[x.key].who_has) > 1

    assert b.data  # not empty


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 10)
async def test_worksteal_many_thieves(c, s, *workers):
    x = c.submit(slowinc, -1, delay=0.1)
    await x

    xs = c.map(slowinc, [x] * 100, pure=False, delay=0.1)

    await wait(xs)

    for ws in s.workers.values():
        assert 2 < len(ws.has_what) < 30

    assert len(s.tasks[x.key].who_has) > 1
    assert sum(len(ws.has_what) for ws in s.workers.values()) < 150


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 2,
    config={"distributed.scheduler.work-stealing-interval": "10ms"},
)
async def test_stop_plugin(c, s, a, b):
    steal = s.extensions["stealing"]

    await steal.stop()
    futs = c.map(slowinc, range(10), workers=[a.address], allow_other_workers=True)
    await c.gather(futs)
    assert len(a.data) == 10

    # nothing happens
    for _ in range(10):
        await steal.stop()


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 2,
    config={"distributed.scheduler.work-stealing-interval": "1ms"},
)
async def test_stop_in_flight(c, s, a, b):
    steal = s.extensions["stealing"]
    num_tasks = 10
    futs = c.map(
        slowinc, range(num_tasks), workers=[a.address], allow_other_workers=True
    )
    while not steal.in_flight:
        await asyncio.sleep(0)

    assert steal.in_flight
    await steal.stop()
    assert not steal.in_flight

    assert len(a.data) != num_tasks
    del futs
    while s.tasks or a.state.tasks or b.state.tasks:
        await asyncio.sleep(0.1)

    futs = c.map(
        slowinc, range(num_tasks), workers=[a.address], allow_other_workers=True
    )
    await c.gather(futs)
    assert len(a.data) == num_tasks

    del futs
    while s.tasks or a.state.tasks or b.state.tasks:
        await asyncio.sleep(0.1)
    event = Event()

    def block(x, event):
        event.wait()
        return x + 1

    futs = c.map(
        block,
        range(num_tasks),
        event=event,
        workers=[a.address],
        allow_other_workers=True,
    )
    while not len(a.state.tasks) == num_tasks:
        await asyncio.sleep(0.01)
    assert len(b.state.tasks) == 0
    await steal.start()
    await event.set()
    await c.gather(futs)
    assert len(a.state.tasks) != num_tasks
    assert len(b.state.tasks) != 0


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 2,
    config={"distributed.scheduler.work-stealing-interval": "10ms"},
)
async def test_allow_tasks_stolen_before_first_completes(c, s, a, b):
    # https://github.com/dask/distributed/issues/5564
    from distributed import Semaphore

    steal = s.extensions["stealing"]
    await steal.stop()
    lock = await Semaphore(max_leases=1)

    # We will reuse the same function such that multiple dispatches have the
    # same task prefix. This ensures that we have tasks queued up but all of
    # them are still classified as unknown.
    # The lock allows us to control the duration of the first task without
    # delaying test runtime or flakyness
    def blocked_task(x, lock):
        if x == 0:
            with lock:
                return x
        return x

    async with lock:
        first = c.submit(blocked_task, 0, lock, workers=[a.address], key="f-0")
        while first.key not in a.state.tasks:
            await asyncio.sleep(0.001)
        # Ensure the task is indeed blocked
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(first, 0.01)

        more_tasks = c.map(
            blocked_task,
            # zero is a sentinel for using the lock.
            # Start counting at one for non-blocking funcs
            range(1, 11),
            lock=lock,
            workers=[a.address],
            key=[f"f-{ix}" for ix in range(1, 11)],
            allow_other_workers=True,
        )
        # All tasks are put on A since this is what we asked for. Only work
        # stealing should rebalance the tasks once we allow for it
        while not len(a.state.tasks) == 11:
            await asyncio.sleep(0.1)

        assert len(b.state.tasks) == 0

        await steal.start()
        # A is still blocked by executing task f-1 so this can only pass if
        # workstealing moves the tasks to B
        await asyncio.sleep(5)
        await c.gather(more_tasks)
        assert len(b.data) == 10
    await first


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 2,
    config={"distributed.scheduler.work-stealing-interval": "10ms"},
)
async def test_eventually_steal_unknown_functions(c, s, a, b):
    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    await wait(futures)
    assert not s.unknown_durations
    assert len(a.data) >= 3, [len(a.data), len(b.data)]
    assert len(b.data) >= 3, [len(a.data), len(b.data)]


@pytest.mark.skip(reason="")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_steal_related_tasks(e, s, a, b, c):
    futures = e.map(
        slowinc, range(20), delay=0.05, workers=a.address, allow_other_workers=True
    )

    await wait(futures)

    nearby = 0
    for f1, f2 in sliding_window(2, futures):
        if s.tasks[f1.key].who_has == s.tasks[f2.key].who_has:
            nearby += 1

    assert nearby > 10


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 10)
async def test_dont_steal_fast_tasks_compute_time(c, s, *workers):
    def do_nothing(x, y=None):
        pass

    xs = c.map(do_nothing, range(10), workers=workers[0].address)
    await wait(xs)

    futures = c.map(do_nothing, range(100), y=xs)

    await wait(futures)

    assert len(set.union(*(s.tasks[x.key].who_has for x in xs))) == 1
    assert len(s.workers[workers[0].address].has_what) == len(xs) + len(futures)


@gen_cluster(client=True)
async def test_dont_steal_fast_tasks_blocklist(c, s, a, b):
    # create a dependency
    x = c.submit(slowinc, 1, workers=[b.address])

    # If the blocklist of fast tasks is tracked somewhere else, this needs to be
    # changed. This test requires *any* key which is blocked.
    from distributed.stealing import fast_tasks

    blocked_key = next(iter(fast_tasks))

    def fast_blocked(x, y=None):
        # The task should observe a certain computation time such that we can
        # ensure that it is not stolen due to the blocking. If it is too
        # fast, the standard mechanism shouldn't allow stealing
        import time

        time.sleep(0.01)

    futures = c.map(
        fast_blocked,
        range(100),
        y=x,
        # Submit the task to one worker but allow it to be distributed else,
        # i.e. this is not a task restriction
        workers=[a.address],
        allow_other_workers=True,
        key=blocked_key,
    )

    await wait(futures)

    # The +1 is the dependency we initially submitted to worker B
    assert len(s.workers[a.address].has_what) == 101
    assert len(s.workers[b.address].has_what) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_new_worker_steals(c, s, a):
    await wait(c.submit(slowinc, 1, delay=0.01))

    futures = c.map(slowinc, range(100), delay=0.05)
    total = c.submit(sum, futures)
    while len(a.state.tasks) < 10:
        await asyncio.sleep(0.01)

    async with Worker(s.address, nthreads=1, memory_limit=MEMORY_LIMIT) as b:
        result = await total
        assert result == sum(map(inc, range(100)))

        for w in (a, b):
            assert all(isinstance(v, int) for v in w.data.values())

        assert b.data


@gen_cluster(client=True)
async def test_work_steal_no_kwargs(c, s, a, b):
    await wait(c.submit(slowinc, 1, delay=0.05))

    futures = c.map(
        slowinc, range(100), workers=a.address, allow_other_workers=True, delay=0.05
    )

    await wait(futures)

    assert 20 < len(a.data) < 80
    assert 20 < len(b.data) < 80

    total = c.submit(sum, futures)
    result = await total

    assert result == sum(map(inc, range(100)))


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.1", 2)])
async def test_dont_steal_worker_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.1, workers=a.address)

    while len(a.state.tasks) + len(b.state.tasks) < 100:
        await asyncio.sleep(0.01)

    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0

    s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)

    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.1", 2), ("127.0.0.1", 2)]
)
async def test_steal_worker_restrictions(c, s, wa, wb, wc):
    future = c.submit(slowinc, 1, delay=0.1, workers={wa.address, wb.address})
    await future

    ntasks = 100
    futures = c.map(slowinc, range(ntasks), delay=0.1, workers={wa.address, wb.address})

    while sum(len(w.state.tasks) for w in [wa, wb, wc]) < ntasks:
        await asyncio.sleep(0.01)

    assert 0 < len(wa.state.tasks) < ntasks
    assert 0 < len(wb.state.tasks) < ntasks
    assert len(wc.state.tasks) == 0

    s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)

    assert 0 < len(wa.state.tasks) < ntasks
    assert 0 < len(wb.state.tasks) < ntasks
    assert len(wc.state.tasks) == 0


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.2", 1)])
async def test_dont_steal_host_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.1, workers="127.0.0.1")
    while len(a.state.tasks) + len(b.state.tasks) < 100:
        await asyncio.sleep(0.01)
    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0

    result = s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)
    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.2", 2)])
async def test_steal_host_restrictions(c, s, wa, wb):
    future = c.submit(slowinc, 1, delay=0.10, workers=wa.address)
    await future

    ntasks = 100
    futures = c.map(slowinc, range(ntasks), delay=0.1, workers="127.0.0.1")
    while len(wa.state.tasks) < ntasks:
        await asyncio.sleep(0.01)
    assert len(wa.state.tasks) == ntasks
    assert len(wb.state.tasks) == 0

    async with Worker(s.address, nthreads=1) as wc:
        start = time()
        while not wc.state.tasks or len(wa.state.tasks) == ntasks:
            await asyncio.sleep(0.01)
            assert time() < start + 3

        await asyncio.sleep(0.1)
        assert 0 < len(wa.state.tasks) < ntasks
        assert len(wb.state.tasks) == 0
        assert 0 < len(wc.state.tasks) < ntasks


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1, {"resources": {"A": 2}}), ("127.0.0.1", 1)]
)
async def test_dont_steal_resource_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.1, resources={"A": 1})
    while len(a.state.tasks) + len(b.state.tasks) < 100:
        await asyncio.sleep(0.01)
    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0

    result = s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)
    assert len(a.state.tasks) == 100
    assert len(b.state.tasks) == 0


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1, {"resources": {"A": 2}})])
async def test_steal_resource_restrictions(c, s, a):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.2, resources={"A": 1})
    while len(a.state.tasks) < 101:
        await asyncio.sleep(0.01)
    assert len(a.state.tasks) == 101

    async with Worker(s.address, nthreads=1, resources={"A": 4}) as b:
        while not b.state.tasks or len(a.state.tasks) == 101:
            await asyncio.sleep(0.01)

        assert len(b.state.tasks) > 0
        assert len(a.state.tasks) < 101


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1, {"resources": {"A": 2, "C": 1}})])
async def test_steal_resource_restrictions_asym_diff(c, s, a):
    # See https://github.com/dask/distributed/issues/5565
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.2, resources={"A": 1})
    while len(a.state.tasks) < 101:
        await asyncio.sleep(0.01)
    assert len(a.state.tasks) == 101

    async with Worker(s.address, nthreads=1, resources={"A": 4, "B": 5}) as b:
        while not b.state.tasks or len(a.state.tasks) == 101:
            await asyncio.sleep(0.01)

        assert len(b.state.tasks) > 0
        assert len(a.state.tasks) < 101


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 5,
    config={"distributed.scheduler.work-stealing-interval": "20ms"},
)
async def test_balance_without_dependencies(c, s, *workers):
    def slow(x):
        y = random.random() * 0.1
        sleep(y)
        return y

    futures = c.map(slow, range(100))
    await wait(futures)

    durations = [sum(w.data.values()) for w in workers]
    assert max(durations) / min(durations) < 3


@gen_cluster(client=True, nthreads=[("127.0.0.1", 4)] * 2)
async def test_dont_steal_executing_tasks(c, s, a, b):
    futures = c.map(
        slowinc, range(4), delay=0.1, workers=a.address, allow_other_workers=True
    )

    await wait(futures)
    assert len(a.data) == 4
    assert len(b.data) == 0


@gen_cluster(client=True)
async def test_dont_steal_executing_tasks_2(c, s, a, b):
    steal = s.extensions["stealing"]

    future = c.submit(slowinc, 1, delay=0.5, workers=a.address)
    while not a.state.executing_count:
        await asyncio.sleep(0.01)

    steal.move_task_request(
        s.tasks[future.key], s.workers[a.address], s.workers[b.address]
    )
    await asyncio.sleep(0.1)
    assert a.state.tasks[future.key].state == "executing"
    assert not b.state.executing_count


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    config={
        "distributed.scheduler.default-task-durations": {"slowidentity": 0.2},
        "distributed.scheduler.work-stealing-interval": "20ms",
    },
)
async def test_dont_steal_few_saturated_tasks_many_workers(c, s, a, *rest):
    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(2)]

    await wait(futures)

    assert len(a.data) == 3
    assert not any(w.state.tasks for w in rest)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    worker_kwargs={"memory_limit": MEMORY_LIMIT},
    config={
        "distributed.scheduler.default-task-durations": {"slowidentity": 0.2},
        "distributed.scheduler.work-stealing-interval": "20ms",
    },
)
async def test_steal_when_more_tasks(c, s, a, *rest):
    x = c.submit(mul, b"0", 50000000, workers=a.address)  # 50 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(20)]

    start = time()
    while not any(w.state.tasks for w in rest):
        await asyncio.sleep(0.01)
        assert time() < start + 1


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    config={
        "distributed.scheduler.default-task-durations": {
            "slowidentity": 0.2,
            "slow2": 1,
        },
        "distributed.scheduler.work-stealing-interval": "20ms",
    },
)
async def test_steal_more_attractive_tasks(c, s, a, *rest):
    def slow2(x):
        sleep(1)
        return x

    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(10)]
    future = c.submit(slow2, x, priority=-1)

    while not any(w.state.tasks for w in rest):
        await asyncio.sleep(0.01)

    # good future moves first
    assert any(future.key in w.state.tasks for w in rest)


async def assert_balanced(inp, expected, recompute_saturation, c, s, *workers):
    steal = s.extensions["stealing"]
    await steal.stop()
    ev = Event()

    def block(*args, event, **kwargs):
        event.wait()

    counter = itertools.count()

    futures = []
    for w, ts in zip(workers, inp):
        for t in sorted(ts, reverse=True):
            if t:
                [dat] = await c.scatter(
                    [SizeOf(int(t * s.bandwidth))], workers=w.address
                )
            else:
                dat = 123
            i = next(counter)
            f = c.submit(
                block,
                dat,
                event=ev,
                key="%d-%d" % (int(t), i),
                workers=w.address,
                allow_other_workers=True,
                pure=False,
                priority=-i,
            )
            futures.append(f)

    while len([ts for ts in s.tasks.values() if ts.processing_on]) < len(futures):
        await asyncio.sleep(0.001)
    if recompute_saturation:
        for ws in s.workers.values():
            s._reevaluate_occupancy_worker(ws)
    try:
        for _ in range(10):
            steal.balance()
            await steal.stop()

            result = [
                sorted(
                    (int(key_split(ts.key)) for ts in s.workers[w.address].processing),
                    reverse=True,
                )
                for w in workers
            ]

            result2 = sorted(result, reverse=True)
            expected2 = sorted(expected, reverse=True)

            if result2 == expected2:
                # Release the threadpools
                return
    finally:
        await ev.set()
    raise Exception(f"Expected: {expected2}; got: {result2}")


@pytest.mark.parametrize("recompute_saturation", [True, False])
@pytest.mark.parametrize(
    "inp,expected",
    [
        pytest.param([[1], []], [[1], []], id="don't move unnecessarily"),
        pytest.param([[0, 0], []], [[0], [0]], id="balance"),
        pytest.param(
            [[0.1, 0.1], []], [[0], [0]], id="balance even if results in even"
        ),
        pytest.param([[0, 0, 0], []], [[0, 0], [0]], id="don't over balance"),
        pytest.param(
            [[0, 0], [0, 0, 0], []], [[0, 0], [0, 0], [0]], id="move from larger"
        ),
        pytest.param([[0, 0, 0], [0], []], [[0, 0], [0], [0]], id="move to smaller"),
        pytest.param([[0, 1], []], [[1], [0]], id="choose easier first"),
        pytest.param([[0, 0, 0, 0], [], []], [[0, 0], [0], [0]], id="spread evenly"),
        pytest.param([[1, 0, 2, 0], [], []], [[2, 1], [0], [0]], id="move easier"),
        pytest.param(
            [[1, 1, 1], []], [[1, 1], [1]], id="be willing to move costly items"
        ),
        pytest.param(
            [[1, 1, 1, 1], []], [[1, 1, 1], [1]], id="but don't move too many"
        ),
        pytest.param(
            [[0, 0], [0, 0], [0, 0], []],
            [[0, 0], [0, 0], [0], [0]],
            id="no one clearly saturated",
        ),
        # NOTE: There is a timing issue that workers may already start executing
        # tasks before we call balance, i.e. the workers will reject the
        # stealing request and we end up with a different end result.
        # Particularly tests with many input tasks are more likely to fail since
        # the test setup takes longer and allows the workers more time to
        # schedule a task on the threadpool
        pytest.param(
            [[4, 2, 2, 2, 2, 1, 1], [4, 2, 1, 1], [], [], []],
            [[4, 2, 2, 2], [4, 2, 1, 1], [2], [1], [1]],
            id="balance multiple saturated workers",
        ),
    ],
)
def test_balance(inp, expected, recompute_saturation):
    async def test_balance_(*args, **kwargs):
        await assert_balanced(inp, expected, recompute_saturation, *args, **kwargs)

    config = {
        "distributed.scheduler.default-task-durations": {str(i): 1 for i in range(10)}
    }
    gen_cluster(client=True, nthreads=[("", 1)] * len(inp), config=config)(
        test_balance_
    )()


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2, Worker=Nanny, timeout=60)
async def test_restart(c, s, a, b):
    futures = c.map(
        slowinc, range(100), delay=0.01, workers=a.address, allow_other_workers=True
    )
    while not s.workers[b.worker_address].processing:
        await asyncio.sleep(0.01)

    # Unknown tasks are never stolen therefore wait for a measurement
    while not any(s.tasks[f.key].state == "memory" for f in futures):
        await asyncio.sleep(0.01)

    steal = s.extensions["stealing"]
    assert any(x for L in steal.stealable.values() for x in L)

    await c.restart()

    assert not any(x for L in steal.stealable.values() for x in L)


@gen_cluster(client=True)
async def test_do_not_steal_communication_heavy_tasks(c, s, a, b):
    # Never steal unreasonably large tasks
    steal = s.extensions["stealing"]
    x = c.submit(gen_nbytes, int(s.bandwidth) * 1000, workers=a.address, pure=False)
    y = c.submit(gen_nbytes, int(s.bandwidth) * 1000, workers=a.address, pure=False)

    def block_reduce(x, y, event):
        event.wait()
        return None

    event = Event()
    futures = [
        c.submit(
            block_reduce,
            x,
            y,
            event=event,
            pure=False,
            workers=a.address,
            allow_other_workers=True,
        )
        for i in range(10)
    ]
    while not a.state.tasks:
        await asyncio.sleep(0.1)
    steal.balance()
    await steal.stop()
    await event.set()
    await c.gather(futures)
    assert not b.data


@gen_cluster(
    client=True,
    config={"distributed.scheduler.default-task-durations": {"slowadd": 0.001}},
)
async def test_steal_communication_heavy_tasks(c, s, a, b):
    steal = s.extensions["stealing"]
    await steal.stop()
    x = c.submit(mul, b"0", int(s.bandwidth), workers=a.address)
    y = c.submit(mul, b"1", int(s.bandwidth), workers=b.address)

    futures = [
        c.submit(
            slowadd,
            x,
            y,
            delay=1,
            pure=False,
            workers=a.address,
            allow_other_workers=True,
        )
        for i in range(10)
    ]

    while not any(f.key in s.tasks and s.tasks[f.key].processing_on for f in futures):
        await asyncio.sleep(0.01)

    steal.balance()

    await steal.stop()


@gen_cluster(client=True)
async def test_steal_twice(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    await wait(x)

    futures = [c.submit(slowadd, x, i, delay=0.2) for i in range(100)]

    while len(s.tasks) < 100:  # tasks are all allocated
        await asyncio.sleep(0.01)
    if math.isinf(s.WORKER_SATURATION):
        # Wait for b to start stealing tasks
        while len(b.state.tasks) < 30:
            await asyncio.sleep(0.01)
    else:
        # Wait for b to complete some tasks
        while len(b.data) < 8:
            await asyncio.sleep(0.01)

    # Army of new workers arrives to help
    async with contextlib.AsyncExitStack() as stack:
        # This is pretty timing sensitive
        workers = [stack.enter_async_context(Worker(s.address)) for _ in range(10)]
        workers = await asyncio.gather(*workers)

        await wait(futures)

        # Note: this includes a and b
        empty_workers = [ws for ws in s.workers.values() if not ws.has_what]
        assert (
            len(empty_workers) < 3
        ), f"Too many workers without keys ({len(empty_workers)} out of {len(s.workers)})"
        # This also tests that some tasks were stolen from b
        # (see `while len(b.state.tasks) < 30` above)
        # If queuing is enabled, then there was nothing to steal from b,
        # so this just tests the queue was balanced not-terribly.
        assert max(len(ws.has_what) for ws in s.workers.values()) < 30

        assert a.state.in_flight_tasks_count == 0
        assert b.state.in_flight_tasks_count == 0


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 3,
    config={"distributed.worker.memory.pause": False},
)
async def test_paused_workers_must_not_steal(c, s, w1, w2, w3):
    w2.status = Status.paused
    while s.workers[w2.address].status != Status.paused:
        await asyncio.sleep(0.01)

    x = c.submit(inc, 1, workers=w1.address)
    await wait(x)

    futures = [c.submit(slowadd, x, i, delay=0.1) for i in range(10)]
    await wait(futures)

    assert w1.data
    assert not w2.data
    assert w3.data


@gen_cluster(client=True)
async def test_dont_steal_already_released(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.05, workers=a.address)
    key = future.key
    while key not in a.state.tasks:
        await asyncio.sleep(0.05)

    del future

    while key in a.state.tasks and a.state.tasks[key].state != "released":
        await asyncio.sleep(0.05)

    a.handle_stimulus(StealRequestEvent(key=key, stimulus_id="test"))
    assert len(a.batched_stream.buffer) == 1
    msg = a.batched_stream.buffer[0]
    assert msg["op"] == "steal-response"
    assert msg["key"] == key
    assert msg["state"] in [None, "released"]

    with captured_logger(
        logging.getLogger("distributed.stealing"), level=logging.DEBUG
    ) as stealing_logs:
        msg = f"Key released between request and confirm: {key}"
        while msg not in stealing_logs.getvalue():
            await asyncio.sleep(0.05)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_dont_steal_long_running_tasks(c, s, a, b):
    def long(delay):
        with worker_client() as c:
            sleep(delay)

    await c.submit(long, 0.1)  # learn duration
    await c.submit(inc, 1)  # learn duration

    long_tasks = c.map(long, [0.5, 0.6], workers=a.address, allow_other_workers=True)
    while sum(len(ws.processing) for ws in s.workers.values()) < 2:  # let them start
        await asyncio.sleep(0.01)

    start = time()
    while any(t.key in s.extensions["stealing"].key_stealable for t in long_tasks):
        await asyncio.sleep(0.01)
        assert time() < start + 1

    na = a.state.executing_count
    nb = b.state.executing_count

    incs = c.map(inc, range(100), workers=a.address, allow_other_workers=True)

    await asyncio.sleep(0.2)

    await wait(long_tasks)

    for t in long_tasks:
        assert (
            sum(log[1] == "executing" for log in a.state.story(t))
            + sum(log[1] == "executing" for log in b.state.story(t))
        ) <= 1


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 5)] * 2,
    config={"distributed.scheduler.work-stealing-interval": "20ms"},
)
async def test_cleanup_repeated_tasks(c, s, a, b):
    class Foo:
        pass

    await c.submit(slowidentity, -1, delay=0.1)
    objects = [c.submit(Foo, pure=False, workers=a.address) for _ in range(50)]

    x = c.map(
        slowidentity, objects, workers=a.address, allow_other_workers=True, delay=0.05
    )
    del objects
    await wait(x)
    assert a.data and b.data
    assert len(a.data) + len(b.data) > 10
    ws = weakref.WeakSet()
    ws.update(a.data.values())
    ws.update(b.data.values())
    del x

    start = time()
    while a.data or b.data:
        await asyncio.sleep(0.01)
        assert time() < start + 1

    assert not s.tasks

    with profile.lock:
        assert not list(ws)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_lose_task(c, s, a, b):
    with captured_logger("distributed.stealing") as log:
        s.periodic_callbacks["stealing"].interval = 1
        for _ in range(100):
            futures = c.map(
                slowinc,
                range(10),
                delay=0.01,
                pure=False,
                workers=a.address,
                allow_other_workers=True,
            )
            await asyncio.sleep(0.01)
            del futures

    out = log.getvalue()
    assert "Error" not in out


@pytest.mark.parametrize("interval, expected", [(None, 100), ("500ms", 500), (2, 2)])
@gen_cluster(nthreads=[], config={"distributed.scheduler.work-stealing": False})
async def test_parse_stealing_interval(s, interval, expected):
    from distributed.scheduler import WorkStealing

    if interval:
        ctx = dask.config.set(
            {"distributed.scheduler.work-stealing-interval": interval}
        )
    else:
        ctx = contextlib.nullcontext()
    with ctx:
        ws = WorkStealing(s)
        await ws.start()
        assert s.periodic_callbacks["stealing"].callback_time == expected


@gen_cluster(client=True)
async def test_balance_with_longer_task(c, s, a, b):
    np = pytest.importorskip("numpy")

    await c.submit(slowinc, 0, delay=0)  # scheduler learns that slowinc is very fast
    x = await c.scatter(np.arange(10000), workers=[a.address])
    y = c.submit(
        slowinc, 1, delay=5, workers=[a.address], priority=1
    )  # a surprisingly long task
    z = c.submit(
        slowadd, x, 1, workers=[a.address], allow_other_workers=True, priority=0
    )  # a task after y, suggesting a, but open to b

    # Allow task to be learned, otherwise it will not be stolen
    _ = c.submit(slowadd, x, 2, workers=[b.address])
    await z
    assert not y.done()
    assert z.key in b.data


@gen_cluster(client=True)
async def test_blocklist_shuffle_split(c, s, a, b):

    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")
    npart = 10
    df = dd.from_pandas(pd.DataFrame({"A": range(100), "B": 1}), npartitions=npart)
    graph = df.shuffle(
        "A",
        shuffle="tasks",
        # If we don't have enough partitions, we'll fall back to a simple shuffle
        max_branch=npart - 1,
    ).sum()
    res = c.compute(graph)

    while not s.tasks:
        await asyncio.sleep(0.005)
    prefixes = set(s.task_prefixes.keys())
    from distributed.stealing import fast_tasks

    blocked = fast_tasks & prefixes
    assert blocked
    assert any(["split" in prefix for prefix in blocked])

    stealable = s.extensions["stealing"].stealable
    while not res.done():
        for tasks_per_level in stealable.values():
            for tasks in tasks_per_level:
                for ts in tasks:
                    assert ts.prefix.name not in fast_tasks
                    assert "split" not in ts.prefix.name
        await asyncio.sleep(0.001)
    await res


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 3,
    config={
        "distributed.scheduler.work-stealing-interval": 1_000_000,
    },
)
async def test_steal_concurrent_simple(c, s, *workers):
    steal = s.extensions["stealing"]
    w0 = workers[0]
    w1 = workers[1]
    w2 = workers[2]
    futs1 = c.map(
        slowinc,
        range(10),
        key=[f"f1-{ix}" for ix in range(10)],
        workers=[w0.address],
    )

    while not w0.state.tasks:
        await asyncio.sleep(0.1)

    # ready is a heap but we don't need last, just not the next
    victim_key = w0.state.ready.peekright().key
    victim_ts = s.tasks[victim_key]

    ws0 = s.workers[w0.address]
    ws1 = s.workers[w1.address]
    ws2 = s.workers[w2.address]
    steal.move_task_request(victim_ts, ws0, ws1)
    steal.move_task_request(victim_ts, ws0, ws2)

    await c.gather(futs1)

    # First wins
    assert ws1.has_what
    assert not ws2.has_what


# FIXME shouldn't consistently fail, may be an actual bug?
@pytest.mark.skipif(
    math.isfinite(dask.config.get("distributed.scheduler.worker-saturation")),
    reason="flaky with queuing active",
)
@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.work-stealing-interval": 1_000_000,
    },
)
async def test_steal_reschedule_reset_in_flight_occupancy(c, s, *workers):
    # https://github.com/dask/distributed/issues/5370
    steal = s.extensions["stealing"]
    w0 = workers[0]
    roots = c.map(
        inc,
        range(6),
        key=[f"r-{ix}" for ix in range(6)],
    )

    def block(x, event):
        event.wait()
        return x + 1

    event = Event()
    futs1 = [
        c.submit(block, f, event=event, key=f"f1-{ix}")
        for f in roots
        for ix in range(4)
    ]
    while not w0.state.ready:
        await asyncio.sleep(0.01)

    # ready is a heap but we don't need last, just not the next
    victim_key = w0.state.ready.peekright().key
    victim_ts = s.tasks[victim_key]

    wsA = victim_ts.processing_on
    other_workers = [ws for ws in s.workers.values() if ws != wsA]
    wsB = other_workers[0]

    steal.move_task_request(victim_ts, wsA, wsB)

    s.reschedule(victim_key, stimulus_id="test")
    await event.set()
    await c.gather(futs1)

    del futs1

    assert all(v == 0 for v in steal.in_flight_occupancy.values())


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.work-stealing-interval": 10,
    },
)
async def test_get_story(c, s, a, b):
    steal = s.extensions["stealing"]
    futs = c.map(slowinc, range(100), workers=[a.address], allow_other_workers=True)
    collect = c.submit(sum, futs)
    await collect
    key = next(iter(b.state.tasks))
    ts = s.tasks[key]
    msgs = steal.story(key)
    msgs_ts = steal.story(ts)
    assert msgs
    assert msgs == msgs_ts
    assert all(isinstance(m, tuple) for m in msgs)


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.work-stealing-interval": 1_000_000,
    },
)
async def test_steal_worker_dies_same_ip(c, s, w0, w1):
    # https://github.com/dask/distributed/issues/5370
    steal = s.extensions["stealing"]
    ev = Event()
    futs1 = c.map(
        lambda _, ev: ev.wait(),
        range(10),
        ev=ev,
        key=[f"f1-{ix}" for ix in range(10)],
        workers=[w0.address],
        allow_other_workers=True,
    )
    while not w0.active_keys:
        await asyncio.sleep(0.01)

    # ready is a heap but we don't need last, just not the next
    victim_key = w0.state.ready.peekright().key
    victim_ts = s.tasks[victim_key]

    wsA = victim_ts.processing_on
    assert wsA.address == w0.address
    wsB = s.workers[w1.address]

    steal.move_task_request(victim_ts, wsA, wsB)
    len_before = len(s.events["stealing"])
    with freeze_batched_send(w0.batched_stream):
        while not any(
            isinstance(event, StealRequestEvent) for event in w0.state.stimulus_log
        ):
            await asyncio.sleep(0.1)
        async with contextlib.AsyncExitStack() as stack:
            # Block batched stream of w0 to ensure the steal-confirmation doesn't
            # arrive at the scheduler before we want it to
            await w1.close()
            # Kill worker wsB
            # Restart new worker with same IP, name, etc.
            while w1.address in s.workers:
                await asyncio.sleep(0.1)

            w_new = await stack.enter_async_context(
                Worker(
                    s.address,
                    host=w1.host,
                    port=w1.port,
                    name=w1.name,
                )
            )
            wsB2 = s.workers[w_new.address]
            assert wsB2.address == wsB.address
            assert wsB2 is not wsB
            assert wsB2 != wsB
            assert hash(wsB2) != hash(wsB)

    # Wait for the steal response to arrive
    while len_before == len(s.events["stealing"]):
        await asyncio.sleep(0.1)

    assert victim_ts.processing_on != wsB

    await w_new.close(executor_wait=False)
    await ev.set()
    await c.gather(futs1)


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 3,
    config={
        "distributed.scheduler.work-stealing-interval": 1_000_000,
    },
)
async def test_reschedule_concurrent_requests_deadlock(c, s, *workers):
    # https://github.com/dask/distributed/issues/5370
    steal = s.extensions["stealing"]
    w0 = workers[0]
    ev = Event()
    futs1 = c.map(
        lambda _, ev: ev.wait(),
        range(10),
        ev=ev,
        key=[f"f1-{ix}" for ix in range(10)],
        workers=[w0.address],
        allow_other_workers=True,
    )
    while not w0.active_keys:
        await asyncio.sleep(0.01)

    # ready is a heap but we don't need last, just not the next
    victim_key = list(w0.active_keys)[0]

    victim_ts = s.tasks[victim_key]

    wsA = victim_ts.processing_on
    other_workers = [ws for ws in s.workers.values() if ws != wsA]
    wsB = other_workers[0]
    wsC = other_workers[1]

    steal.move_task_request(victim_ts, wsA, wsB)

    s.set_restrictions(worker={victim_key: [wsB.address]})
    s.reschedule(victim_key, stimulus_id="test")
    assert wsB == victim_ts.processing_on
    # move_task_request is not responsible for respecting worker restrictions
    steal.move_task_request(victim_ts, wsB, wsC)

    # Let tasks finish
    await ev.set()
    await c.gather(futs1)

    assert victim_ts.who_has != {wsC}
    msgs = steal.story(victim_ts)
    msgs = [msg[:-1] for msg in msgs]  # Remove random IDs

    # There are three possible outcomes
    expect1 = [
        ("stale-response", victim_key, "executing", wsA.address),
        ("already-computing", victim_key, "executing", wsB.address, wsC.address),
    ]
    expect2 = [
        ("already-computing", victim_key, "executing", wsB.address, wsC.address),
        ("already-aborted", victim_key, "executing", wsA.address),
    ]
    # This outcome appears only in ~2% of the runs
    expect3 = [
        ("already-computing", victim_key, "executing", wsB.address, wsC.address),
        ("already-aborted", victim_key, "memory", wsA.address),
    ]
    assert msgs in (expect1, expect2, expect3)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_correct_bad_time_estimate(c, s, *workers):
    """Initial time estimation causes the task to not be considered for
    stealing. Following occupancy readjustments will re-enlist the keys since
    the duration estimate is now significant.

    This is done during reevaluate occupancy
    """
    steal = s.extensions["stealing"]
    future = c.submit(slowinc, 1, delay=0)
    await wait(future)
    futures = [c.submit(slowinc, future, delay=0.1, pure=False) for i in range(20)]
    while not any(f.key in s.tasks for f in futures):
        await asyncio.sleep(0.001)
    assert not any(s.tasks[f.key] in steal.key_stealable for f in futures)
    await asyncio.sleep(0.5)
    assert any(s.tasks[f.key] in steal.key_stealable for f in futures)
    await wait(futures)
    assert all(w.data for w in workers), [sorted(w.data) for w in workers]


@gen_cluster(client=True)
async def test_steal_stimulus_id_unique(c, s, a, b):
    steal = s.extensions["stealing"]
    num_futs = 1_000
    async with Lock() as lock:

        def blocked(x, lock):
            lock.acquire()

        # Setup all tasks on worker 0 such that victim/thief relation is the
        # same for all tasks.
        futures = c.map(blocked, range(num_futs), lock=lock, workers=[a.address])
        # Ensure all tasks are assigned to the worker since otherwise the
        # move_task_request fails.
        while len(a.state.tasks) != num_futs:
            await asyncio.sleep(0.1)
        tasks = [s.tasks[f.key] for f in futures]
        w0 = s.workers[a.address]
        w1 = s.workers[b.address]
        # Generating the move task requests as fast as possible increases the
        # chance of duplicates if the uniqueness is not guaranteed.
        for ts in tasks:
            steal.move_task_request(ts, w0, w1)
        # Values stored in in_flight are used for response verification.
        # Therefore all stimulus IDs are stored here and must be unique
        stimulus_ids = {dct["stimulus_id"] for dct in steal.in_flight.values()}
        assert len(stimulus_ids) == num_futs
        await c.cancel(futures)


def test_steal_worker_state(ws_with_running_task):
    ws = ws_with_running_task

    ws.handle_stimulus(FreeKeysEvent(keys=["x"], stimulus_id="s1"))
    assert ws.available_resources == {"R": 0}
    assert ws.tasks["x"].state == "cancelled"

    instructions = ws.handle_stimulus(ExecuteSuccessEvent.dummy("x", stimulus_id="s2"))
    assert not instructions
    assert "x" not in ws.tasks
    assert "x" not in ws.data
    assert ws.available_resources == {"R": 1}


@pytest.mark.slow()
@gen_cluster(nthreads=[("", 1)] * 4, client=True)
async def test_steal_very_fast_tasks(c, s, *workers):
    # Ensure that very fast tasks are allowed to be stolen
    root = dask.delayed(lambda n: "x" * n)(
        dask.utils.parse_bytes("1MiB"), dask_key_name="root"
    )

    @dask.delayed
    def func(*args):
        import time

        time.sleep(0.002)

    ntasks = 1000
    results = [func(root, i) for i in range(ntasks)]
    futs = c.compute(results)
    await c.gather(futs)

    ntasks_per_worker = np.array([len(w.data) for w in workers])
    ideal = ntasks / len(workers)
    assert (ntasks_per_worker > ideal * 0.5).all(), (ideal, ntasks_per_worker)
    assert (ntasks_per_worker < ideal * 1.5).all(), (ideal, ntasks_per_worker)


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_willing_to_move_costly_items(recompute_saturation):
    dependencies = {"a": 1, "b": 1, "c": 1}
    dependency_placement = [["a", "b", "c"], []]
    task_placement = [[["a"], ["b"], ["c"]], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [2, 1]

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_but_dont_move_too_many(recompute_saturation):
    dependencies = {"a": 1, "b": 1, "c": 1, "d": 1}
    dependency_placement = [["a", "b", "c", "d"], []]
    task_placement = [[["a"], ["b"], ["c"], ["d"]], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [3, 1]

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_even_with_replica(recompute_saturation):
    dependencies = {"a": 1}
    dependency_placement = [["a"], ["a"]]
    task_placement = [[["a"], ["a"]], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [
            1,
            1,
        ]

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_to_replica(recompute_saturation):
    dependencies = {"a": 1}
    dependency_placement = [["a"], ["a"], []]
    task_placement = [[["a"], ["a"], ["a"]], [], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [
            2,
            1,
            0,
        ]  # Note: The success of this test currently depends on worker ordering

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_to_larger_dependency(recompute_saturation):
    dependencies = {"a": 2, "b": 1}
    dependency_placement = [["a", "b"], ["a"], ["b"]]
    task_placement = [[["a", "b"], ["a", "b"], ["a", "b"]], [], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [
            2,
            1,
            0,
        ]  # Note: The success of this test currently depends on worker ordering

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_move_to_busier_with_dependency(recompute_saturation):
    dependencies = {"a": 2, "b": 1}
    dependency_placement = [["a"], ["a", "b"], []]
    task_placement = [[["a"], ["a"], ["a"], ["a"], ["a"], ["a"]], [["b"]], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [
            3,
            3,
            1,
        ]  # Note: The success of this test currently depends on worker ordering

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


@pytest.mark.parametrize("recompute_saturation", [True, False])
def test_balance_when_moving_dependency(recompute_saturation):
    dependencies = {"a": 1}
    dependency_placement = [["a"], []]
    task_placement = [[["a"], ["a"], ["a"], ["a"], ["a"], ["a"], ["a"]], []]

    def _correct_placement(actual):
        actual_task_counts = [len(placed) for placed in actual]
        return actual_task_counts == [
            4,
            3,
        ]  # Note: The success of this test currently depends on worker ordering

    async def _run_test(*args, **kwargs):
        await _run_balance_test(
            dependencies,
            dependency_placement,
            task_placement,
            _correct_placement,
            recompute_saturation,
            *args,
            **kwargs,
        )

    config = {"distributed.scheduler.unknown-task-duration": "1s"}
    gen_cluster(
        client=True,
        nthreads=[("", 1)] * len(task_placement),
        config=config,
    )(_run_test)()


async def _run_balance_test(
    dependencies,
    dependency_placement,
    task_placement,
    correct_placement_fn,
    recompute_saturation,
    c,
    s,
    *workers,
):
    steal = s.extensions["stealing"]
    await steal.stop()
    ev = Event()

    dependency_futures = await _place_dependencies(
        dependencies, dependency_placement, c, s, workers
    )

    futures = await _place_tasks(ev, task_placement, dependency_futures, c, s, workers)
    if recompute_saturation:
        for ws in s.workers.values():
            s._reevaluate_occupancy_worker(ws)
    try:
        for _ in range(20):
            steal.balance()

            while steal.in_flight:
                await asyncio.sleep(0.001)

            result = _get_task_placement(s, workers)

            if correct_placement_fn(result):
                # Release the threadpools
                return
    finally:
        await ev.set()

    raise AssertionError(result)


async def _place_dependencies(dependencies, placement, c, s, workers):
    dependencies_to_workers = defaultdict(set)
    for worker_idx, placed in enumerate(placement):
        for dependency in placed:
            dependencies_to_workers[dependency].add(workers[worker_idx].address)

    futures = {}
    for name, multiplier in dependencies.items():
        worker_addresses = dependencies_to_workers[name]
        futs = await c.scatter(
            {name: Sizeof(int(multiplier * s.bandwidth))},
            workers=worker_addresses,
            broadcast=True,
        )
        futures[name] = futs[name]

    await c.gather(futures.values())

    _assert_dependency_placement(placement, workers)

    return futures


def _assert_dependency_placement(expected, workers):
    actual = []
    for worker in workers:
        actual.append(list(worker.state.tasks.keys()))

    assert actual == expected


async def _place_tasks(ev, placement, dependency_futures, c, s, workers):
    def block(*args, event, **kwargs):
        event.wait()

    counter = itertools.count()
    futures = []
    for worker_idx, tasks in enumerate(placement):
        for dependencies in tasks:
            i = next(counter)
            dep_key = "".join(sorted(dependencies))
            key = f"{dep_key}-{i}"
            f = c.submit(
                block,
                [dependency_futures[dependency] for dependency in dependencies],
                event=ev,
                key=key,
                workers=workers[worker_idx].address,
                allow_other_workers=True,
                pure=False,
                priority=-i,
            )
            futures.append(f)

    while len([ts for ts in s.tasks.values() if ts.processing_on]) < len(futures):
        await asyncio.sleep(0.001)

    assert_task_placement(placement, s, workers)

    return futures


def _get_task_placement(s, workers):
    actual = []
    for w in workers:
        actual.append(
            [list(key_split(ts.key)) for ts in s.workers[w.address].processing]
        )
    return actual


def _equal_placement(actual, expected):
    return _comparable_placement(actual) == _comparable_placement(expected)


def _comparable_placement(placement):
    return [sorted(placed) for placed in placement]


def assert_task_placement(expected, s, workers):
    actual = _get_task_placement(s, workers)
    assert _equal_placement(actual, expected)
