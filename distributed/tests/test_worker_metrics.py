from __future__ import annotations

from collections.abc import Hashable

from distributed import Event, Worker
from distributed.utils_test import async_wait_for, gen_cluster, inc, slowinc, wait_for_state


def get_digests(w: Worker, allow: str | None = None) -> dict[Hashable, float]:
    # import pprint; pprint.pprint(dict(w.digests_total))
    digests = {
        k: v
        for k, v in w.digests_total.items()
        if k
        not in {
            "latency",
            "tick-duration",
            "transfer-bandwidth",
            "transfer-duration",
            "compute-duration",
            "get-data-send-duration",
        }
        and (allow is None or allow in k)
    }
    assert all(v >= 0 for v in digests.values()), digests
    return digests


@gen_cluster(client=True, config={"distributed.worker.memory.target": 1e-9})
async def test_task_lifecycle(c, s, a, b):
    x = (await c.scatter({"x": "x" * 20_000}, workers=[a.address]))["x"]
    y = (await c.scatter({"y": "y" * 20_000}, workers=[b.address]))["y"]
    assert a.state.tasks["x"].state == "memory"
    assert b.state.tasks["y"].state == "memory"

    z = c.submit("".join, [x, y], key=("z-123", 0), workers=[a.address])
    assert (await z) == "x" * 20_000 + "y" * 20_000
    # The call to Worker.get_data will terminate after the fetch of z returns
    # await async_wait_for(
    #     lambda: ("get-data", "network") in a.digests_total, timeout=5
    # )

    del x, y, z
    await async_wait_for(lambda: not a.state.tasks, timeout=5)  # For hygene only

    expect = [
        # scatter({"x": "x" * 20_000}, workers=[a.address])
        ("transition", "x", "released->memory", "serialize"),
        ("transition", "x", "released->memory", "compress"),
        ("transition", "x", "released->memory", "disk-write"),
        ("disk-write",),  # aggregated metric
        ("transition", "x", "released->memory", "own-time"),
        ("transition", "x", "released->memory", "disk-write", "count"),
        ("transition", "x", "released->memory", "disk-write", "nbytes"),
        ("disk-write", "count"),  # aggregated metric
        ("disk-write", "nbytes"),  # aggregated metric
        # a.gather_dep(worker=b.address, keys=["z"])
        ("gather-dep", "decompress"),
        ("gather-dep", "deserialize"),
        ("gather-dep", "own-time"),
        # ("gather-dep", "network"), # TODO
        # Spill output; added by _transition_to_memory
        ("transition", "y", "flight->memory", "serialize"),
        ("transition", "y", "flight->memory", "compress"),
        ("transition", "y", "flight->memory", "disk-write"),
        # note `"(disk-write,)"` doesn't show up again here because it's just incremented
        ("transition", "y", "flight->memory", "own-time"),
        ("transition", "y", "flight->memory", "disk-write", "count"),
        ("transition", "y", "flight->memory", "disk-write", "nbytes"),
        # Delta to end-to-end runtime as seen from the worker state machine
        # ("gather-dep", "other"),  # TODO? the gather-dep and the subsequent spill are not linked. idk if this actually matters.
        # a.execute()
        # -> Deserialize run_spec
        ("execute", "z", "deserialize-task"),
        # -> Unspill inputs
        ("execute", "z", "disk-read"),
        ("disk-read",),  # aggregated metric
        ("execute", "z", "decompress"),
        ("execute", "z", "deserialize"),
        # -> Run in thread
        ("execute", "z", "thread"),
        ("execute", "z", "own-time"),
        # -> Counters from un-spill and execute
        ("execute", "z", "disk-read", "count"),
        ("execute", "z", "disk-read", "nbytes"),
        ("disk-read", "count"),  # aggregated metric
        ("disk-read", "nbytes"),  # aggregated metric
        ("execute", "z", "thread", "thread-time"),
        # Spill output; added by _transition_to_memory
        ("transition", "z", "executing->memory", "serialize"),
        ("transition", "z", "executing->memory", "compress"),
        ("transition", "z", "executing->memory", "disk-write"),
        ("transition", "z", "executing->memory", "own-time"),
        ("transition", "z", "executing->memory", "disk-write", "count"),
        ("transition", "z", "executing->memory", "disk-write", "nbytes"),
        # Delta to end-to-end runtime as seen from the worker state machine
        # ("execute", "z", "other"),  # TODO also don't have this, also don't know if we care
        # a.get_data() (triggered by the client retrieving the Future for z)
        # # Unspill
        ("get-data", "disk-read"),
        ("get-data", "decompress"),
        ("get-data", "deserialize"),
        # # Send over the network
        ("get-data", "serialize"),
        ("get-data", "compress"),
        # ("get-data", "network"),  # TODO
        ("get-data", "own-time"),
        # Counters
        ("get-data", "disk-read", "count"),
        ("get-data", "disk-read", "nbytes"),
    ]
    for k, v in get_digests(a).items():
        print(f"{k!r}: {v!r}")
    assert list(get_digests(a)) == expect

    assert get_digests(a, allow="count") == {
        ("disk-read", "count"): 3,
        ("disk-write", "count"): 3,
        ("execute", "z", "disk-read", "count"): 2,
        ("transition", "x", "released->memory", "disk-write", "count"): 1,
        ("transition", "y", "flight->memory", "disk-write", "count"): 1,
        ("transition", "z", "executing->memory", "disk-write", "count"): 1.0,
        ("get-data", "disk-read", "count"): 1,
    }
    # if not WINDOWS:  # Fiddly rounding; see distributed.metrics._WindowsTime
    #     assert sum(get_digests(a, allow="seconds").values()) <= m.delta


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_basic_execute(c, s, a):
    await c.submit(inc, 1, key="x")
    assert list(get_digests(a)) == [
        ("execute", "x", "deserialize-task"),
        ("execute", "x", "thread"),
        ("execute", "x", "own-time"),
        ("execute", "x", "thread", "thread-time"),
    ]

@gen_cluster(client=True, nthreads=[("", 2)])
async def test_execute_parallel(c, s, a):
    await c.gather(c.map(slowinc, [1, 2], delay=0.1, key=["x-1", "x-2"]))
    digests = get_digests(a)
    assert list(digests) == [
        ("execute", "x", "deserialize-task"),
        ("execute", "x", "thread"),
        ("execute", "x", "own-time"),
        ("execute", "x", "thread", "thread-time"),
    ]
    assert digests[("execute", "x", "thread")] >= 0.2

@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_multiple_workers(c, s, a, b):
    await c.gather(c.map(slowinc, [1, 2], delay=0.1, key=["a-1", "a-2"], workers=[a.address]))
    digests_a = get_digests(a)
    assert list(digests_a) == [
        ("execute", "a", "deserialize-task"),
        ("execute", "a", "thread"),
        ("execute", "a", "own-time"),
        ("execute", "a", "thread", "thread-time"),
    ]
    assert digests_a[("execute", "a", "thread")] >= 0.2

    assert not get_digests(b)

    await c.gather(c.map(slowinc, [1, 2], delay=0.1, key=["b-1", "b-2"], workers=[b.address]))
    digests_b = get_digests(b)
    assert list(digests_b) == [
        ("execute", "b", "deserialize-task"),
        ("execute", "b", "thread"),
        ("execute", "b", "own-time"),
        ("execute", "b", "thread", "thread-time"),
    ]
    assert digests_b[("execute", "b", "thread")] >= 0.2

    assert get_digests(a) != get_digests(b)

    slow = c.submit(slowinc, 1, delay=0.1, key="slow", workers=[a.address])
    fast = c.submit(inc, 1, key="fast", workers=[b.address])
    await c.gather([slow, fast], direct=False)

    digests_a = get_digests(a)
    digests_b = get_digests(b)

    assert digests_a[("execute", "slow", "thread")] > digests_b[("execute", "fast", "thread")]



@gen_cluster(client=True, nthreads=[("", 1)])
async def test_run_spec_deserialization(c, s, a):
    """Test that deserialization of run_spec is metered"""
    await c.submit(inc, 1, key="x")
    assert 0 < a.digests_total["execute", "x", "deserialize-task"] < 1


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

    print(list(get_digests(a)))
    assert list(get_digests(a)) == [("execute", "x", "cancelled")]
