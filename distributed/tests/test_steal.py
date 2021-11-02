import asyncio
import itertools
import logging
import random
import sys
import weakref
from operator import mul
from time import sleep

import pytest
from tlz import concat, sliding_window

import dask

from distributed import Nanny, Worker, wait, worker_client
from distributed.compatibility import LINUX, WINDOWS
from distributed.config import config
from distributed.metrics import time
from distributed.scheduler import key_split
from distributed.system import MEMORY_LIMIT
from distributed.utils_test import (
    captured_logger,
    gen_cluster,
    inc,
    nodebug_setup_module,
    nodebug_teardown_module,
    slowadd,
    slowidentity,
    slowinc,
)

pytestmark = pytest.mark.ci1

# Most tests here are timing-dependent
setup_module = nodebug_setup_module
teardown_module = nodebug_teardown_module


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 2), ("127.0.0.2", 2)])
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
    assert len(s.who_has[x.key]) == 1
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


@pytest.mark.avoid_ci
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_steal_expensive_data_slow_computation(c, s, a, b):
    np = pytest.importorskip("numpy")

    x = c.submit(slowinc, 100, delay=0.2, workers=a.address)
    await wait(x)  # learn that slowinc is slow

    x = c.submit(np.arange, 1000000, workers=a.address)  # put expensive data
    await wait(x)

    slow = [c.submit(slowinc, x, delay=0.1, pure=False) for i in range(20)]
    await wait(slow)
    assert len(s.who_has[x.key]) > 1

    assert b.data  # not empty


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 10)
async def test_worksteal_many_thieves(c, s, *workers):
    x = c.submit(slowinc, -1, delay=0.1)
    await x

    xs = c.map(slowinc, [x] * 100, pure=False, delay=0.1)

    await wait(xs)

    for w, keys in s.has_what.items():
        assert 2 < len(keys) < 30

    assert len(s.who_has[x.key]) > 1
    assert sum(map(len, s.has_what.values())) < 150


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_dont_steal_unknown_functions(c, s, a, b):
    futures = c.map(inc, range(100), workers=a.address, allow_other_workers=True)
    await wait(futures)
    assert len(a.data) >= 95, [len(a.data), len(b.data)]


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
        if s.who_has[f1.key] == s.who_has[f2.key]:
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

    assert len(set.union(*(s.who_has[x.key] for x in xs))) == 1
    assert len(s.has_what[workers[0].address]) == len(xs) + len(futures)


@gen_cluster(client=True)
async def test_dont_steal_fast_tasks_blacklist(c, s, a, b):
    # create a dependency
    x = c.submit(slowinc, 1, workers=[b.address])

    # If the blacklist of fast tasks is tracked somewhere else, this needs to be
    # changed. This test requires *any* key which is blacklisted.
    from distributed.stealing import fast_tasks

    blacklisted_key = next(iter(fast_tasks))

    def fast_blacklisted(x, y=None):
        # The task should observe a certain computation time such that we can
        # ensure that it is not stolen due to the blacklisting. If it is too
        # fast, the standard mechanism shouldn't allow stealing
        import time

        time.sleep(0.01)

    futures = c.map(
        fast_blacklisted,
        range(100),
        y=x,
        # Submit the task to one worker but allow it to be distributed else,
        # i.e. this is not a task restriction
        workers=[a.address],
        allow_other_workers=True,
        key=blacklisted_key,
    )

    await wait(futures)

    # The +1 is the dependency we initially submitted to worker B
    assert len(s.has_what[a.address]) == 101
    assert len(s.has_what[b.address]) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_new_worker_steals(c, s, a):
    await wait(c.submit(slowinc, 1, delay=0.01))

    futures = c.map(slowinc, range(100), delay=0.05)
    total = c.submit(sum, futures)
    while len(a.tasks) < 10:
        await asyncio.sleep(0.01)

    b = await Worker(s.address, loop=s.loop, nthreads=1, memory_limit=MEMORY_LIMIT)

    result = await total
    assert result == sum(map(inc, range(100)))

    for w in [a, b]:
        assert all(isinstance(v, int) for v in w.data.values())

    assert b.data

    await b.close()


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

    while len(a.tasks) + len(b.tasks) < 100:
        await asyncio.sleep(0.01)

    assert len(a.tasks) == 100
    assert len(b.tasks) == 0

    result = s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)

    assert len(a.tasks) == 100
    assert len(b.tasks) == 0


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.1", 2), ("127.0.0.1", 2)]
)
async def test_steal_worker_restrictions(c, s, wa, wb, wc):
    future = c.submit(slowinc, 1, delay=0.1, workers={wa.address, wb.address})
    await future

    ntasks = 100
    futures = c.map(slowinc, range(ntasks), delay=0.1, workers={wa.address, wb.address})

    while sum(len(w.tasks) for w in [wa, wb, wc]) < ntasks:
        await asyncio.sleep(0.01)

    assert 0 < len(wa.tasks) < ntasks
    assert 0 < len(wb.tasks) < ntasks
    assert len(wc.tasks) == 0

    s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)

    assert 0 < len(wa.tasks) < ntasks
    assert 0 < len(wb.tasks) < ntasks
    assert len(wc.tasks) == 0


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.2", 1)])
async def test_dont_steal_host_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.1, workers="127.0.0.1")
    while len(a.tasks) + len(b.tasks) < 100:
        await asyncio.sleep(0.01)
    assert len(a.tasks) == 100
    assert len(b.tasks) == 0

    result = s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)
    assert len(a.tasks) == 100
    assert len(b.tasks) == 0


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1), ("127.0.0.2", 2)])
async def test_steal_host_restrictions(c, s, wa, wb):
    future = c.submit(slowinc, 1, delay=0.10, workers=wa.address)
    await future

    ntasks = 100
    futures = c.map(slowinc, range(ntasks), delay=0.1, workers="127.0.0.1")
    while len(wa.tasks) < ntasks:
        await asyncio.sleep(0.01)
    assert len(wa.tasks) == ntasks
    assert len(wb.tasks) == 0

    wc = await Worker(s.address, nthreads=1)

    start = time()
    while not wc.tasks or len(wa.tasks) == ntasks:
        await asyncio.sleep(0.01)
        assert time() < start + 3

    await asyncio.sleep(0.1)
    assert 0 < len(wa.tasks) < ntasks
    assert len(wb.tasks) == 0
    assert 0 < len(wc.tasks) < ntasks


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1, {"resources": {"A": 2}}), ("127.0.0.1", 1)]
)
async def test_dont_steal_resource_restrictions(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.1, resources={"A": 1})
    while len(a.tasks) + len(b.tasks) < 100:
        await asyncio.sleep(0.01)
    assert len(a.tasks) == 100
    assert len(b.tasks) == 0

    result = s.extensions["stealing"].balance()

    await asyncio.sleep(0.1)
    assert len(a.tasks) == 100
    assert len(b.tasks) == 0


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1, {"resources": {"A": 2}})])
async def test_steal_resource_restrictions(c, s, a):
    future = c.submit(slowinc, 1, delay=0.10, workers=a.address)
    await future

    futures = c.map(slowinc, range(100), delay=0.2, resources={"A": 1})
    while len(a.tasks) < 101:
        await asyncio.sleep(0.01)
    assert len(a.tasks) == 101

    b = await Worker(s.address, loop=s.loop, nthreads=1, resources={"A": 4})

    while not b.tasks or len(a.tasks) == 101:
        await asyncio.sleep(0.01)

    assert len(b.tasks) > 0
    assert len(a.tasks) < 101

    await b.close()


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 5)
async def test_balance_without_dependencies(c, s, *workers):
    s.extensions["stealing"]._pc.callback_time = 20

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
    while not a.executing_count:
        await asyncio.sleep(0.01)

    steal.move_task_request(
        s.tasks[future.key], s.workers[a.address], s.workers[b.address]
    )
    await asyncio.sleep(0.1)
    assert a.tasks[future.key].state == "executing"
    assert not b.executing_count


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    config={"distributed.scheduler.default-task-durations": {"slowidentity": 0.2}},
)
async def test_dont_steal_few_saturated_tasks_many_workers(c, s, a, *rest):
    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(2)]

    await wait(futures)

    assert len(a.data) == 3
    assert not any(w.tasks for w in rest)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    worker_kwargs={"memory_limit": MEMORY_LIMIT},
    config={"distributed.scheduler.default-task-durations": {"slowidentity": 0.2}},
)
async def test_steal_when_more_tasks(c, s, a, *rest):
    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 50000000, workers=a.address)  # 50 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(20)]

    start = time()
    while not any(w.tasks for w in rest):
        await asyncio.sleep(0.01)
        assert time() < start + 1


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 10,
    config={
        "distributed.scheduler.default-task-durations": {
            "slowidentity": 0.2,
            "slow2": 1,
        }
    },
)
async def test_steal_more_attractive_tasks(c, s, a, *rest):
    def slow2(x):
        sleep(1)
        return x

    s.extensions["stealing"]._pc.callback_time = 20
    x = c.submit(mul, b"0", 100000000, workers=a.address)  # 100 MB
    await wait(x)

    futures = [c.submit(slowidentity, x, pure=False, delay=0.2) for i in range(10)]
    future = c.submit(slow2, x, priority=-1)

    while not any(w.tasks for w in rest):
        await asyncio.sleep(0.01)

    # good future moves first
    assert any(future.key in w.tasks for w in rest)


def func(x):
    sleep(1)


async def assert_balanced(inp, expected, c, s, *workers):
    steal = s.extensions["stealing"]
    steal._pc.stop()

    counter = itertools.count()
    tasks = list(concat(inp))
    data_seq = itertools.count()

    futures = []
    for w, ts in zip(workers, inp):
        for t in sorted(ts, reverse=True):
            if t:
                [dat] = await c.scatter([next(data_seq)], workers=w.address)
                ts = s.tasks[dat.key]
                # Ensure scheduler state stays consistent
                old_nbytes = ts.nbytes
                ts.nbytes = int(s.bandwidth * t)
                for ws in ts.who_has:
                    ws.nbytes += ts.nbytes - old_nbytes
            else:
                dat = 123
            i = next(counter)
            f = c.submit(
                func,
                dat,
                key="%d-%d" % (int(t), i),
                workers=w.address,
                allow_other_workers=True,
                pure=False,
                priority=-i,
            )
            futures.append(f)

    while len(s.rprocessing) < len(futures):
        await asyncio.sleep(0.001)

    for i in range(10):
        steal.balance()

        while steal.in_flight:
            await asyncio.sleep(0.001)

        result = [
            sorted((int(key_split(k)) for k in s.processing[w.address]), reverse=True)
            for w in workers
        ]

        result2 = sorted(result, reverse=True)
        expected2 = sorted(expected, reverse=True)

        if config.get("pdb-on-err"):
            if result2 != expected2:
                import pdb

                pdb.set_trace()

        if result2 == expected2:
            return
    raise Exception(f"Expected: {expected2}; got: {result2}")


@pytest.mark.parametrize(
    "inp,expected",
    [
        ([[1], []], [[1], []]),  # don't move unnecessarily
        ([[0, 0], []], [[0], [0]]),  # balance
        ([[0.1, 0.1], []], [[0], [0]]),  # balance even if results in even
        ([[0, 0, 0], []], [[0, 0], [0]]),  # don't over balance
        ([[0, 0], [0, 0, 0], []], [[0, 0], [0, 0], [0]]),  # move from larger
        ([[0, 0, 0], [0], []], [[0, 0], [0], [0]]),  # move to smaller
        ([[0, 1], []], [[1], [0]]),  # choose easier first
        ([[0, 0, 0, 0], [], []], [[0, 0], [0], [0]]),  # spread evenly
        ([[1, 0, 2, 0], [], []], [[2, 1], [0], [0]]),  # move easier
        ([[1, 1, 1], []], [[1, 1], [1]]),  # be willing to move costly items
        ([[1, 1, 1, 1], []], [[1, 1, 1], [1]]),  # but don't move too many
        (
            [[0, 0], [0, 0], [0, 0], []],  # no one clearly saturated
            [[0, 0], [0, 0], [0], [0]],
        ),
        (
            [[4, 2, 2, 2, 2, 1, 1], [4, 2, 1, 1], [], [], []],
            [[4, 2, 2, 2, 2], [4, 2, 1], [1], [1], [1]],
        ),
        pytest.param(
            [[1, 1, 1, 1, 1, 1, 1], [1, 1], [1, 1], [1, 1], []],
            [[1, 1, 1, 1, 1], [1, 1], [1, 1], [1, 1], [1, 1]],
            # Can't mark as flaky as when it fails it does so every time for some reason
            marks=pytest.mark.xfail(
                reason="Some uncertainty based on executing stolen task"
            ),
        ),
    ],
)
def test_balance(inp, expected):
    async def test(*args, **kwargs):
        await assert_balanced(inp, expected, *args, **kwargs)

    test = gen_cluster(
        client=True,
        nthreads=[("127.0.0.1", 1)] * len(inp),
        config={
            "distributed.scheduler.default-task-durations": {
                str(i): 1 for i in range(10)
            }
        },
    )(test)
    test()


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2, Worker=Nanny, timeout=60)
async def test_restart(c, s, a, b):
    futures = c.map(
        slowinc, range(100), delay=0.01, workers=a.address, allow_other_workers=True
    )
    while not s.processing[b.worker_address]:
        await asyncio.sleep(0.01)

    # Unknown tasks are never stolen therefore wait for a measurement
    while not any(s.tasks[f.key].state == "memory" for f in futures):
        await asyncio.sleep(0.01)

    steal = s.extensions["stealing"]
    assert any(st for st in steal.stealable_all)
    assert any(x for L in steal.stealable.values() for x in L)

    await c.restart()

    assert not any(x for x in steal.stealable_all)
    assert not any(x for L in steal.stealable.values() for x in L)


@gen_cluster(
    client=True,
    config={"distributed.scheduler.default-task-durations": {"slowadd": 0.001}},
)
async def test_steal_communication_heavy_tasks(c, s, a, b):
    steal = s.extensions["stealing"]
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

    while not any(f.key in s.rprocessing for f in futures):
        await asyncio.sleep(0.01)

    steal.balance()
    while steal.in_flight:
        await asyncio.sleep(0.001)

    assert s.processing[b.address]


@pytest.mark.flaky(
    condition=WINDOWS and sys.version_info[:2] == (3, 7),
    reruns=20,
    reruns_delay=5,
    reason="b.in_flight_tasks == 1",
)
@gen_cluster(client=True)
async def test_steal_twice(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    await wait(x)

    futures = [c.submit(slowadd, x, i, delay=0.2) for i in range(100)]

    while len(s.tasks) < 100:  # tasks are all allocated
        await asyncio.sleep(0.01)

    # Army of new workers arrives to help
    workers = await asyncio.gather(*(Worker(s.address, loop=s.loop) for _ in range(20)))

    await wait(futures)

    has_what = dict(s.has_what)  # take snapshot
    empty_workers = [w for w, keys in has_what.items() if not len(keys)]
    if len(empty_workers) > 2:
        pytest.fail(
            "Too many workers without keys (%d out of %d)"
            % (len(empty_workers), len(has_what))
        )
    assert max(map(len, has_what.values())) < 30

    assert a.in_flight_tasks == 0
    assert b.in_flight_tasks == 0

    await c._close()
    await asyncio.gather(*(w.close() for w in workers))


@gen_cluster(client=True)
async def test_dont_steal_already_released(c, s, a, b):
    future = c.submit(slowinc, 1, delay=0.05, workers=a.address)
    key = future.key
    while key not in a.tasks:
        await asyncio.sleep(0.05)

    del future

    while key in a.tasks and a.tasks[key].state != "released":
        await asyncio.sleep(0.05)

    a.handle_steal_request(key=key, stimulus_id="test")
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
    while sum(map(len, s.processing.values())) < 2:  # let them start
        await asyncio.sleep(0.01)

    start = time()
    while any(t.key in s.extensions["stealing"].key_stealable for t in long_tasks):
        await asyncio.sleep(0.01)
        assert time() < start + 1

    na = a.executing_count
    nb = b.executing_count

    incs = c.map(inc, range(100), workers=a.address, allow_other_workers=True)

    await asyncio.sleep(0.2)

    await wait(long_tasks)

    for t in long_tasks:
        assert (
            sum(log[1] == "executing" for log in a.story(t))
            + sum(log[1] == "executing" for log in b.story(t))
        ) <= 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 5)] * 2)
async def test_cleanup_repeated_tasks(c, s, a, b):
    class Foo:
        pass

    s.extensions["stealing"]._pc.callback_time = 20
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

    assert not s.who_has
    assert not any(s.has_what.values())

    assert not list(ws)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_lose_task(c, s, a, b):
    with captured_logger("distributed.stealing") as log:
        s.periodic_callbacks["stealing"].interval = 1
        for i in range(100):
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


@gen_cluster(client=True)
async def test_worker_stealing_interval(c, s, a, b):
    from distributed.scheduler import WorkStealing

    ws = WorkStealing(s)
    assert ws._pc.callback_time == 100

    with dask.config.set({"distributed.scheduler.work-stealing-interval": "500ms"}):
        ws = WorkStealing(s)
    assert ws._pc.callback_time == 500

    # Default unit is `ms`
    with dask.config.set({"distributed.scheduler.work-stealing-interval": 2}):
        ws = WorkStealing(s)
    assert ws._pc.callback_time == 2


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
async def test_blacklist_shuffle_split(c, s, a, b):

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

    blacklisted = fast_tasks & prefixes
    assert blacklisted
    assert any(["split" in prefix for prefix in blacklisted])

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

    while not w0.tasks:
        await asyncio.sleep(0.1)

    # ready is a heap but we don't need last, just not the next
    _, victim_key = w0.ready[-1]

    ws0 = s.workers[w0.address]
    ws1 = s.workers[w1.address]
    ws2 = s.workers[w2.address]
    victim_ts = s.tasks[victim_key]
    steal.move_task_request(victim_ts, ws0, ws1)
    steal.move_task_request(victim_ts, ws0, ws2)

    await c.gather(futs1)

    # First wins
    assert ws1.has_what
    assert not ws2.has_what


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
    futs1 = c.map(
        slowinc,
        range(10),
        key=[f"f1-{ix}" for ix in range(10)],
    )
    while not w0.tasks:
        await asyncio.sleep(0.01)

    # ready is a heap but we don't need last, just not the next
    _, victim_key = w0.ready[-1]

    victim_ts = s.tasks[victim_key]

    wsA = victim_ts.processing_on
    other_workers = [ws for ws in s.workers.values() if ws != wsA]
    wsB = other_workers[0]

    steal.move_task_request(victim_ts, wsA, wsB)

    s.reschedule(victim_key)
    await c.gather(futs1)

    del futs1

    assert all(v == 0 for v in steal.in_flight_occupancy.values())


@gen_cluster(
    client=True,
    config={
        "distributed.scheduler.work-stealing-interval": 10,
    },
)
async def test_get_story(c, s, *workers):
    steal = s.extensions["stealing"]
    futs = c.map(
        slowinc, range(100), workers=[workers[0].address], allow_other_workers=True
    )
    collect = c.submit(sum, futs)
    await collect
    key = next(iter(workers[1].tasks))
    ts = s.tasks[key]
    msgs = steal.story(key)
    msgs_ts = steal.story(ts)
    assert msgs
    assert msgs == msgs_ts
    assert all(isinstance(m, tuple) for m in msgs)


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
    futs1 = c.map(
        slowinc,
        range(10),
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
    s.reschedule(victim_key)
    assert wsB == victim_ts.processing_on
    # move_task_request is not responsible for respecting worker restrictions
    steal.move_task_request(victim_ts, wsB, wsC)
    await c.gather(futs1)

    # If this turns out to be overly flaky, the following may be relaxed or
    # removed. The point of this test is to not deadlock but verifying expected
    # state is still a nice thing

    # Either the last request goes through or both have been rejected since the
    # computation was already done by the time the request comes in. This is
    # unfortunately not stable even if we increase the compute time
    if victim_ts.who_has != {wsC}:
        msgs = steal.story(victim_ts)
        assert len(msgs) == 2
        assert all(msg[0] == "already-aborted" for msg in msgs), msgs


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
