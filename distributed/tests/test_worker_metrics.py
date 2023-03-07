from __future__ import annotations

from collections.abc import Hashable

from distributed import Event, Worker
from distributed.compatibility import WINDOWS
from distributed.utils_test import async_wait_for, gen_cluster, inc, wait_for_state


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
        # a.gather_dep(worker=b.address, keys=["z"])
        ("gather-dep", "decompress"),
        ("gather-dep", "deserialize"),
        ("gather-dep", "deserialize"),
        # ("gather-dep", "network"), # TODO
        # Spill output; added by _transition_to_memory
        ("gather-dep", "serialize"),
        ("gather-dep", "compress"),
        ("gather-dep", "disk-write"),
        ("gather-dep", "disk-write", "count"),
        ("gather-dep", "disk-write", "bytes"),
        # Delta to end-to-end runtime as seen from the worker state machine
        ("gather-dep", "other"),
        # a.execute()
        # -> Deserialize run_spec
        ("execute", "z", "deserialize"),
        # -> Unspill inputs
        # (There's also another execute-deserialize-seconds entry)
        ("execute", "z", "disk-read"),
        ("execute", "z", "disk-read", "count"),
        ("execute", "z", "disk-read", "bytes"),
        ("execute", "z", "decompress"),
        # -> Run in thread
        ("execute", "z", "thread-cpu"),
        ("execute", "z", "thread-noncpu"),
        # Spill output; added by _transition_to_memory
        ("execute", "z", "serialize"),
        ("execute", "z", "compress"),
        ("execute", "z", "disk-write"),
        ("execute", "z", "disk-write", "count"),
        ("execute", "z", "disk-write", "bytes"),
        # Delta to end-to-end runtime as seen from the worker state machine
        ("execute", "z", "other"),
        # a.get_data() (triggered by the client retrieving the Future for z)
        # Unspill
        ("get-data", "disk-read"),
        ("get-data", "disk-read", "count"),
        ("get-data", "disk-read", "bytes"),
        ("get-data", "decompress"),
        ("get-data", "deserialize"),
        # Send over the network
        ("get-data", "serialize"),
        ("get-data", "compress"),
        ("get-data", "network"),
    ]
    print(get_digests(a))
    assert list(get_digests(a)) == expect

    assert get_digests(a, allow="count") == {
        ("execute", "z", "disk-read", "count"): 2,
        ("execute", "z", "disk-write", "count"): 1,
        ("gather-dep", "disk-write", "count"): 1,
        ("get-data", "disk-read", "count"): 1,
    }
    # if not WINDOWS:  # Fiddly rounding; see distributed.metrics._WindowsTime
    #     assert sum(get_digests(a, allow="seconds").values()) <= m.delta


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_basic_execute(c, s, a):
    await c.submit(inc, 1, key="x")
    assert list(get_digests(a)) == [
        ("execute", "x", "thread"),
        ("execute", "x", "own-time"),
        ("execute", "x", "thread", "thread-time"),
    ]


# @gen_cluster(client=True, nthreads=[("", 1)])
# async def test_run_spec_deserialization(c, s, a):
#     """Test that deserialization of run_spec is metered"""
#     await c.submit(inc, 1, key="x")
#     assert 0 < a.digests_total["execute", "x", "deserialize", "seconds"] < 1


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
