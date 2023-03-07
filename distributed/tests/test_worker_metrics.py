from __future__ import annotations

from collections.abc import Hashable

from distributed import Event, Worker
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


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_basic_execute(c, s, a):
    await c.submit(inc, 1, key="x")
    assert list(get_digests(a)) == [
        ("execute", "x", "thread", "thread-cpu"),
        ("execute", "x", "thread"),
        ("execute", "x"),
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
