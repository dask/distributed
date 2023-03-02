from __future__ import annotations

import asyncio
import re
import secrets
import time
from time import sleep

import pytest

from distributed import Event, Worker, wait
from distributed.comm.utils import OFFLOAD_THRESHOLD
from distributed.compatibility import WINDOWS
from distributed.metrics import context_meter, meter
from distributed.utils_test import (
    BlockedGatherDep,
    BlockedGetData,
    async_wait_for,
    gen_cluster,
    inc,
    wait_for_state,
)


def get_digests(
    w: Worker,
    allow: str = ".*",
    block: str = r"^(latency|tick-duration|transfer-bandwidth|transfer-duration)$",
) -> dict[str, float]:
    # import pprint; pprint.pprint(dict(w.digests_total))
    digests = {
        k: v
        for k, v in w.digests_total.items()
        if re.match(allow, k) and not re.match(block, k)
    }
    assert all(v >= 0 for v in digests.values()), digests
    return digests


@gen_cluster(client=True, config={"distributed.worker.memory.target": 1e-9})
async def test_task_lifecycle(c, s, a, b):
    x = (await c.scatter({"x": "x" * 20_000}, workers=[a.address]))["x"]
    y = (await c.scatter({"y": "y" * 20_000}, workers=[b.address]))["y"]
    assert a.state.tasks["x"].state == "memory"
    assert b.state.tasks["y"].state == "memory"
    with meter() as m:
        z = c.submit("".join, [x, y], key="z", workers=[a.address])
        assert (await z) == "x" * 20_000 + "y" * 20_000
        # The call to Worker.get_data will terminate after the fetch of z returns
        await async_wait_for(
            lambda: "get-data-network-seconds" in a.digests_total, timeout=5
        )

    del x, y, z
    await async_wait_for(lambda: not a.state.tasks, timeout=5)  # For hygene only

    expect = [
        # scatter x
        "scatter-serialize-seconds",
        "scatter-compress-seconds",
        "scatter-disk-write-seconds",
        "scatter-disk-write-count",
        "scatter-disk-write-bytes",
        # a.gather_dep(worker=b.address, keys=["z"])
        "gather-dep-decompress-seconds",
        "gather-dep-deserialize-seconds",
        "gather-dep-network-seconds",
        # Delta to end-to-end runtime as seen from the worker state machine
        "gather-dep-other-seconds",
        # Spill output; added by _transition_to_memory
        "gather-dep-serialize-seconds",
        "gather-dep-compress-seconds",
        "gather-dep-disk-write-seconds",
        "gather-dep-disk-write-count",
        "gather-dep-disk-write-bytes",
        # a.execute()
        # -> Deserialize run_spec
        "execute-deserialize-seconds",
        # -> Unspill inputs
        # (There's also another execute-deserialize-seconds entry)
        "execute-disk-read-seconds",
        "execute-disk-read-count",
        "execute-disk-read-bytes",
        "execute-decompress-seconds",
        # -> Run in thread
        "execute-thread-cpu-seconds",
        "execute-thread-noncpu-seconds",
        # Delta to end-to-end runtime as seen from the worker state machine
        "execute-other-seconds",
        # Spill output; added by _transition_to_memory
        "execute-serialize-seconds",
        "execute-compress-seconds",
        "execute-disk-write-seconds",
        "execute-disk-write-count",
        "execute-disk-write-bytes",
        # a.get_data() (triggered by the client retrieving the Future for z)
        # Unspill
        "get-data-disk-read-seconds",
        "get-data-disk-read-count",
        "get-data-disk-read-bytes",
        "get-data-decompress-seconds",
        "get-data-deserialize-seconds",
        # Send over the network
        "get-data-serialize-seconds",
        "get-data-compress-seconds",
        "get-data-network-seconds",
    ]
    assert list(get_digests(a)) == expect

    assert get_digests(a, allow=".*-count") == {
        "scatter-disk-write-count": 1,
        "execute-disk-read-count": 2,
        "execute-disk-write-count": 1,
        "gather-dep-disk-write-count": 1,
        "get-data-disk-read-count": 1,
    }
    if not WINDOWS:  # Fiddly rounding; see distributed.metrics._WindowsTime
        assert sum(get_digests(a, allow=".*-seconds").values()) <= m.delta


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_async_task(c, s, a):
    """Test that async tasks are metered"""
    await c.submit(asyncio.sleep, 0.1)
    assert a.digests_total["execute-thread-cpu-seconds"] == 0
    assert 0 < a.digests_total["execute-thread-noncpu-seconds"] < 1


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_run_spec_deserialization(c, s, a):
    """Test that deserialization of run_spec is metered"""
    await c.submit(inc, 1, key="x")
    assert 0 < a.digests_total["execute-deserialize-seconds"] < 1


@gen_cluster(client=True)
async def test_offload(c, s, a, b):
    """Test that functions wrapped by offload() are metered"""
    nbytes = int(OFFLOAD_THRESHOLD * 1.1)
    x = c.submit(secrets.token_bytes, nbytes, key="x", workers=[a.address])
    y = c.submit(lambda x: None, x, key="y", workers=[b.address])
    await y

    assert 0 < a.digests_total["get-data-serialize-seconds"] < 1
    assert 0 < b.digests_total["gather-dep-deserialize-seconds"] < 1


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_execute_failed(c, s, a):
    """Tasks that failed to execute are metered as a separate lump total"""
    x = c.submit(lambda: 1 / 0)
    await wait(x)

    assert list(get_digests(a)) == ["execute-failed-seconds"]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_cancelled_execute(c, s, a):
    """cancelled(execute) tasks are metered as a separate lump total"""
    ev = await Event()
    x = c.submit(lambda ev: ev.wait(), ev, key="x")
    await wait_for_state("x", "executing", a)
    del x
    await wait_for_state("x", "cancelled", a)
    await ev.set()
    await async_wait_for(lambda: not a.state.tasks, timeout=5)

    assert list(get_digests(a)) == ["execute-cancelled-seconds"]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_cancelled_flight(c, s, a):
    """cancelled(flight) tasks are metered as a separate lump total"""
    async with BlockedGetData(s.address) as b:
        x = c.submit(inc, 1, key="x", workers=[b.address])
        y = c.submit(inc, x, key="y", workers=[a.address])
        await b.in_get_data.wait()

        del y
        await wait_for_state("x", "cancelled", a)
        b.block_get_data.set()

    assert list(get_digests(a)) == ["gather-dep-cancelled-seconds"]


@gen_cluster(client=True)
async def test_gather_dep_busy(c, s, a, b):
    """gather_dep() calls that failed because the remote peer is busy
    are metered as a separate lump total
    """
    # We will block A for any outgoing communication. This simulates an
    # overloaded worker which will always return "busy" for get_data requests.
    a.transfer_outgoing_count = 10000000

    x = c.submit(inc, 1, key="x", workers=[a.address])
    y = c.submit(inc, x, key="y", workers=[b.address])

    await wait_for_state(y.key, "waiting", b)
    assert b.state.tasks[x.key].state in ("flight", "fetch")
    with pytest.raises(asyncio.TimeoutError):
        await y.result(timeout=0.5)

    assert list(get_digests(b)) == ["gather-dep-busy-seconds"]


@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    config={"distributed.scheduler.active-memory-manager.start": False},
)
async def test_gather_dep_no_task(c, s, w1):
    """gather_dep() calls where the remote peer answers that it doesn't have any of the
    requested keys are metered as a separate lump total
    """
    x = c.submit(inc, 1, key="x", workers=[w1.address])

    async with BlockedGetData(s.address) as w2, BlockedGatherDep(s.address) as w3:
        y = c.submit(inc, x, key="y", workers=[w3.address])
        await w3.in_gather_dep.wait()  # Gather from w1

        # Move x from w1 to w2
        s.request_acquire_replicas(w2.address, ["x"], stimulus_id="ar")
        await async_wait_for(lambda: len(s.tasks["x"].who_has) == 2, timeout=5)
        s.request_remove_replicas(w1.address, ["x"], stimulus_id="rr")
        await async_wait_for(lambda: len(s.tasks["x"].who_has) == 1, timeout=5)

        w3.block_gather_dep.set()
        # 1. w1 will now answer that it does not have the key
        # 2. x will transition to missing on w3
        # 3. w3 will ask the scheduler if there are other replicas
        # 4. the scheduler will answer that now w2 holds a replica
        # 5. w3 will try fetching the key from w2. We block again so that we don't
        #    pollute the metrics with a successful attempt.
        await w2.in_get_data.wait()

        assert list(get_digests(w3)) == ["gather-dep-missing-seconds"]

        w2.block_get_data.set()
        assert await y == 3


@gen_cluster(client=True)
async def test_gather_dep_failed(c, s, a, b):
    """gather_dep() calls where the task fails to deserialize are metered as a
    separate lump total
    """

    class C:
        def __reduce__(self):
            def expand():
                raise Exception()

            return expand, ()

    x = c.submit(C, key="x", workers=[a.address])
    y = c.submit(lambda x: None, x, key="y", workers=[b.address])
    await wait_for_state("x", "error", b)
    assert list(get_digests(b)) == ["gather-dep-failed-seconds"]

    # FIXME https://github.com/dask/distributed/issues/6705
    b.state.validate = False


@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    config={"distributed.comm.timeouts.connect": "500ms"},
)
async def test_gather_dep_network_error(c, s, a):
    """gather_dep() calls where the remote peer fails to respond are metered as a
    separate lump total
    """
    x = c.submit(inc, 1, key="x")
    await wait(x)
    async with BlockedGatherDep(s.address) as b:
        y = c.submit(inc, x, key="y", workers=[b.address])
        await b.in_gather_dep.wait()
        await a.close()
        b.block_gather_dep.set()
        await wait(y)
        assert list(get_digests(b, "gather-dep-")) == ["gather-dep-failed-seconds"]


@gen_cluster(
    nthreads=[("", 1)],
    client=True,
    worker_kwargs={"memory_limit": "10 GiB"},
    config={
        "distributed.worker.memory.target": False,
        "distributed.worker.memory.spill": 0.7,
        "distributed.worker.memory.pause": False,
        "distributed.worker.memory.monitor-interval": "10ms",
    },
)
async def test_memory_monitor(c, s, a):
    a.monitor.get_process_memory = lambda: 800_000_000_000 if a.data.fast else 0
    x = c.submit(inc, 1, key="x")
    await async_wait_for(lambda: a.data.disk, timeout=5)

    # The call to WorkerMemoryMonitor._spill will terminate after SpillBuffer.evict()
    await async_wait_for(
        lambda: "memory-monitor-spill-evloop-released-seconds" in a.digests_total,
        timeout=5,
    )

    assert list(get_digests(a)) == [
        "execute-deserialize-seconds",
        "execute-thread-cpu-seconds",
        "execute-thread-noncpu-seconds",
        "execute-other-seconds",
        "memory-monitor-spill-serialize-seconds",
        "memory-monitor-spill-compress-seconds",
        "memory-monitor-spill-disk-write-seconds",
        "memory-monitor-spill-disk-write-count",
        "memory-monitor-spill-disk-write-bytes",
        "memory-monitor-spill-evloop-released-seconds",
    ]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_user_metrics_sync(c, s, a):
    def f():
        t1 = time.perf_counter() + 0.1
        while time.perf_counter() < t1:
            pass
        sleep(0.1)
        context_meter.digest_metric("I/O", 5, "seconds")

    await wait(c.submit(f))

    assert list(get_digests(a)) == [
        "execute-deserialize-seconds",
        "execute-I/O-seconds",
        "execute-thread-cpu-seconds",
        "execute-thread-noncpu-seconds",
        "execute-other-seconds",
    ]
    assert get_digests(a)["execute-I/O-seconds"] == 5
    assert get_digests(a)["execute-thread-cpu-seconds"] == 0
    assert get_digests(a)["execute-thread-noncpu-seconds"] == 0
    assert get_digests(a)["execute-other-seconds"] == 0


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_user_metrics_async(c, s, a):
    async def f():
        await asyncio.sleep(0.1)
        context_meter.digest_metric("I/O", 5, "seconds")

    await wait(c.submit(f))

    assert list(get_digests(a)) == [
        "execute-deserialize-seconds",
        "execute-I/O-seconds",
        "execute-thread-noncpu-seconds",
        "execute-other-seconds",
    ]
    assert get_digests(a)["execute-I/O-seconds"] == 5
    assert get_digests(a)["execute-thread-noncpu-seconds"] == 0
    assert get_digests(a)["execute-other-seconds"] == 0


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_user_metrics_fail(c, s, a):
    def f():
        context_meter.digest_metric("I/O", 5, "seconds")
        context_meter.digest_metric("I/O", 100, "bytes")
        raise ValueError("foo")

    await wait(c.submit(f))

    assert list(get_digests(a)) == [
        "execute-I/O-bytes",
        "execute-failed-seconds",
    ]
    assert get_digests(a)["execute-I/O-bytes"] == 100
    assert get_digests(a)["execute-failed-seconds"] < 1
