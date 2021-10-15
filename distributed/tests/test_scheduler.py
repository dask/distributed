import asyncio
import gc
import json
import logging
import operator
import re
import sys
from itertools import product
from textwrap import dedent
from time import sleep
from unittest import mock

import cloudpickle
import psutil
import pytest
from tlz import concat, first, frequencies, merge, valmap

import dask
from dask import delayed
from dask.utils import apply, parse_timedelta, stringify, tmpfile, typename

from distributed import Client, Nanny, Worker, fire_and_forget, wait
from distributed.comm import Comm
from distributed.compatibility import LINUX, WINDOWS
from distributed.core import ConnectionPool, Status, clean_exception, connect, rpc
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.scheduler import MemoryState, Scheduler
from distributed.utils import TimeoutError
from distributed.utils_test import (
    captured_logger,
    cluster,
    dec,
    div,
    gen_cluster,
    gen_test,
    inc,
    nodebug,
    slowadd,
    slowdec,
    slowinc,
    tls_only_security,
    varying,
)
from distributed.worker import dumps_function, dumps_task, get_worker

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle
    except ImportError:
        import pickle
else:
    import pickle

pytestmark = pytest.mark.ci1


alice = "alice:1234"
bob = "bob:1234"


@gen_cluster()
async def test_administration(s, a, b):
    assert isinstance(s.address, str)
    assert s.address in str(s)
    assert str(sum(s.nthreads.values())) in repr(s)
    assert str(len(s.nthreads)) in repr(s)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_respect_data_in_memory(c, s, a):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    await wait([f])

    assert s.tasks[y.key].who_has == {s.workers[a.address]}

    z = delayed(operator.add)(x, y)
    f2 = c.persist(z)
    while f2.key not in s.tasks or not s.tasks[f2.key]:
        assert s.tasks[y.key].who_has
        await asyncio.sleep(0.0001)


@gen_cluster(client=True)
async def test_recompute_released_results(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.persist(y)
    await wait(yy)

    while s.tasks[x.key].who_has or x.key in a.data or x.key in b.data:  # let x go away
        await asyncio.sleep(0.01)

    z = delayed(dec)(x)
    zz = c.compute(z)
    result = await zz
    assert result == 1


@gen_cluster(client=True)
async def test_decide_worker_with_many_independent_leaves(c, s, a, b):
    xs = await asyncio.gather(
        c.scatter(list(range(0, 100, 2)), workers=a.address),
        c.scatter(list(range(1, 100, 2)), workers=b.address),
    )
    xs = list(concat(zip(*xs)))
    ys = [delayed(inc)(x) for x in xs]

    y2s = c.persist(ys)
    await wait(y2s)

    nhits = sum(y.key in a.data for y in y2s[::2]) + sum(
        y.key in b.data for y in y2s[1::2]
    )

    assert nhits > 80


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_decide_worker_with_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers=[a.address, b.address])
    await x
    assert x.key in a.data or x.key in b.data


@pytest.mark.parametrize("ndeps", [0, 1, 4])
@pytest.mark.parametrize(
    "nthreads",
    [
        [("127.0.0.1", 1)] * 5,
        [("127.0.0.1", 3), ("127.0.0.1", 2), ("127.0.0.1", 1)],
    ],
)
def test_decide_worker_coschedule_order_neighbors(ndeps, nthreads):
    @gen_cluster(
        client=True,
        nthreads=nthreads,
        config={"distributed.scheduler.work-stealing": False},
    )
    async def test(c, s, *workers):
        r"""
        Ensure that sibling root tasks are scheduled to the same node, reducing future data transfer.

        We generate a wide layer of "root" tasks (random NumPy arrays). All of those tasks share 0-5
        trivial dependencies. The ``ndeps=0`` and ``ndeps=1`` cases are most common in real-world use
        (``ndeps=1`` is basically ``da.from_array(..., inline_array=False)`` or ``da.from_zarr``).
        The graph is structured like this (though the number of tasks and workers is different):

            |-W1-|  |-W2-| |-W3-|  |-W4-|   < ---- ideal task scheduling

              q       r       s       t      < --- `sum-aggregate-`
             / \     / \     / \     / \
            i   j   k   l   m   n   o   p    < --- `sum-`
            |   |   |   |   |   |   |   |
            a   b   c   d   e   f   g   h    < --- `random-`
            \   \   \   |   |   /   /   /
                   TRIVIAL * 0..5

        Neighboring `random-` tasks should be scheduled on the same worker. We test that generally,
        only one worker holds each row of the array, that the `random-` tasks are never transferred,
        and that there are few transfers overall.
        """
        da = pytest.importorskip("dask.array")
        np = pytest.importorskip("numpy")

        if ndeps == 0:
            x = da.random.random((100, 100), chunks=(10, 10))
        else:

            def random(**kwargs):
                assert len(kwargs) == ndeps
                return np.random.random((10, 10))

            trivial_deps = {f"k{i}": delayed(object()) for i in range(ndeps)}

            # TODO is there a simpler (non-blockwise) way to make this sort of graph?
            x = da.blockwise(
                random,
                "yx",
                new_axes={"y": (10,) * 10, "x": (10,) * 10},
                dtype=float,
                **trivial_deps,
            )

        xx, xsum = dask.persist(x, x.sum(axis=1, split_every=20))
        await xsum

        # Check that each chunk-row of the array is (mostly) stored on the same worker
        primary_worker_key_fractions = []
        secondary_worker_key_fractions = []
        for i, keys in enumerate(x.__dask_keys__()):
            # Iterate along rows of the array.
            keys = {stringify(k) for k in keys}

            # No more than 2 workers should have any keys
            assert sum(any(k in w.data for k in keys) for w in workers) <= 2

            # What fraction of the keys for this row does each worker hold?
            key_fractions = [
                len(set(w.data).intersection(keys)) / len(keys) for w in workers
            ]
            key_fractions.sort()
            # Primary worker: holds the highest percentage of keys
            # Secondary worker: holds the second highest percentage of keys
            primary_worker_key_fractions.append(key_fractions[-1])
            secondary_worker_key_fractions.append(key_fractions[-2])

        # There may be one or two rows that were poorly split across workers,
        # but the vast majority of rows should only be on one worker.
        assert np.mean(primary_worker_key_fractions) >= 0.9
        assert np.median(primary_worker_key_fractions) == 1.0
        assert np.mean(secondary_worker_key_fractions) <= 0.1
        assert np.median(secondary_worker_key_fractions) == 0.0

        # Check that there were few transfers
        unexpected_transfers = []
        for worker in workers:
            for log in worker.incoming_transfer_log:
                keys = log["keys"]
                # The root-ish tasks should never be transferred
                assert not any(k.startswith("random") for k in keys), keys
                # `object-` keys (the trivial deps of the root random tasks) should be transferred
                if any(not k.startswith("object") for k in keys):
                    # But not many other things should be
                    unexpected_transfers.append(list(keys))

        # A transfer at the very end to move aggregated results is fine (necessary with unbalanced workers in fact),
        # but generally there should be very very few transfers.
        assert len(unexpected_transfers) <= 3, unexpected_transfers

    test()


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_move_data_over_break_restrictions(client, s, a, b, c):
    [x] = await client.scatter([1], workers=b.address)
    y = client.submit(inc, x, workers=[a.address, b.address])
    await wait(y)
    assert y.key in a.data or y.key in b.data


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_balance_with_restrictions(client, s, a, b, c):
    [x], [y] = await asyncio.gather(
        client.scatter([[1, 2, 3]], workers=a.address),
        client.scatter([1], workers=c.address),
    )
    z = client.submit(inc, 1, workers=[a.address, c.address])
    await wait(z)

    assert s.tasks[z.key].who_has == {s.workers[c.address]}


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_no_valid_workers(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999")
    while not s.tasks:
        await asyncio.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(x, 0.05)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_no_valid_workers_loose_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers="127.0.0.5:9999", allow_other_workers=True)
    result = await x
    assert result == 2


@gen_cluster(client=True, nthreads=[])
async def test_no_workers(client, s):
    x = client.submit(inc, 1)
    while not s.tasks:
        await asyncio.sleep(0.01)

    assert s.tasks[x.key] in s.unrunnable

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(x, 0.05)


@gen_cluster(nthreads=[])
async def test_retire_workers_empty(s):
    await s.retire_workers(workers=[])


@gen_cluster()
async def test_remove_client(s, a, b):
    s.update_graph(
        tasks={"x": dumps_task((inc, 1)), "y": dumps_task((inc, "x"))},
        dependencies={"x": [], "y": ["x"]},
        keys=["y"],
        client="ident",
    )

    assert s.tasks
    assert s.dependencies

    s.remove_client(client="ident")

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
async def test_server_listens_to_other_ops(s, a, b):
    with rpc(s.address) as r:
        ident = await r.identity()
        assert ident["type"] == "Scheduler"
        assert ident["id"].lower().startswith("scheduler")


@gen_cluster()
async def test_remove_worker_from_scheduler(s, a, b):
    dsk = {("x-%d" % i): (inc, i) for i in range(20)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        dependencies={k: set() for k in dsk},
    )

    assert a.address in s.stream_comms
    await s.remove_worker(address=a.address)
    assert a.address not in s.nthreads
    assert len(s.workers[b.address].processing) == len(dsk)  # b owns everything


@gen_cluster()
async def test_remove_worker_by_name_from_scheduler(s, a, b):
    assert a.address in s.stream_comms
    assert await s.remove_worker(address=a.name) == "OK"
    assert a.address not in s.nthreads
    assert await s.remove_worker(address=a.address) == "already-removed"


@gen_cluster(config={"distributed.scheduler.events-cleanup-delay": "10 ms"})
async def test_clear_events_worker_removal(s, a, b):
    assert a.address in s.events
    assert a.address in s.nthreads
    assert b.address in s.events
    assert b.address in s.nthreads

    await s.remove_worker(address=a.address)
    # Shortly after removal, the events should still be there
    assert a.address in s.events
    assert a.address not in s.nthreads
    s.validate_state()

    start = time()
    while a.address in s.events:
        await asyncio.sleep(0.01)
        assert time() < start + 2
    assert b.address in s.events


@gen_cluster(
    config={"distributed.scheduler.events-cleanup-delay": "10 ms"}, client=True
)
async def test_clear_events_client_removal(c, s, a, b):
    assert c.id in s.events
    s.remove_client(c.id)

    assert c.id in s.events
    assert c.id not in s.clients
    assert c not in s.clients

    s.remove_client(c.id)
    # If it doesn't reconnect after a given time, the events log should be cleared
    start = time()
    while c.id in s.events:
        await asyncio.sleep(0.01)
        assert time() < start + 2


@gen_cluster()
async def test_add_worker(s, a, b):
    w = Worker(s.address, nthreads=3)
    w.data["x-5"] = 6
    w.data["y"] = 1

    dsk = {("x-%d" % i): (inc, i) for i in range(10)}
    s.update_graph(
        tasks=valmap(dumps_task, dsk),
        keys=list(dsk),
        client="client",
        dependencies={k: set() for k in dsk},
    )
    s.validate_state()
    await w
    s.validate_state()

    assert w.ip in s.host_info
    assert s.host_info[w.ip]["addresses"] == {a.address, b.address, w.address}
    await w.close()


@gen_cluster(scheduler_kwargs={"blocked_handlers": ["feed"]})
async def test_blocked_handlers_are_respected(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    response = await comm.read()

    _, exc, _ = clean_exception(response["exception"], response["traceback"])
    assert isinstance(exc, ValueError)
    assert "'feed' handler has been explicitly disallowed" in repr(exc)

    await comm.close()


@gen_cluster(
    nthreads=[], config={"distributed.scheduler.blocked-handlers": ["test-handler"]}
)
def test_scheduler_init_pulls_blocked_handlers_from_config(s):
    assert s.blocked_handlers == ["test-handler"]


@gen_cluster()
async def test_feed(s, a, b):
    def func(scheduler):
        return dumps(dict(scheduler.worker_info))

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.01})

    for i in range(5):
        response = await comm.read()
        expected = dict(s.worker_info)
        assert cloudpickle.loads(response) == expected

    await comm.close()


@gen_cluster()
async def test_feed_setup_teardown(s, a, b):
    def setup(scheduler):
        return 1

    def func(scheduler, state):
        assert state == 1
        return "OK"

    def teardown(scheduler, state):
        scheduler.flag = "done"

    comm = await connect(s.address)
    await comm.write(
        {
            "op": "feed",
            "function": dumps(func),
            "setup": dumps(setup),
            "teardown": dumps(teardown),
            "interval": 0.01,
        }
    )

    for i in range(5):
        response = await comm.read()
        assert response == "OK"

    await comm.close()
    start = time()
    while not hasattr(s, "flag"):
        await asyncio.sleep(0.01)
        assert time() - start < 5


@gen_cluster()
async def test_feed_large_bytestring(s, a, b):
    np = pytest.importorskip("numpy")

    x = np.ones(10000000)

    def func(scheduler):
        y = x
        return True

    comm = await connect(s.address)
    await comm.write({"op": "feed", "function": dumps(func), "interval": 0.05})

    for i in range(5):
        response = await comm.read()
        assert response is True

    await comm.close()


@gen_cluster(client=True)
async def test_delete_data(c, s, a, b):
    d = await c.scatter({"x": 1, "y": 2, "z": 3})

    assert {ts.key for ts in s.tasks.values() if ts.who_has} == {"x", "y", "z"}
    assert set(a.data) | set(b.data) == {"x", "y", "z"}
    assert merge(a.data, b.data) == {"x": 1, "y": 2, "z": 3}

    del d["x"]
    del d["y"]

    start = time()
    while set(a.data) | set(b.data) != {"z"}:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_delete(c, s, a):
    x = c.submit(inc, 1)
    await x
    assert x.key in s.tasks
    assert x.key in a.data

    await c._cancel(x)

    start = time()
    while x.key in a.data:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    assert x.key not in s.tasks

    s.report_on_key(key=x.key)


@gen_cluster()
async def test_filtered_communication(s, a, b):
    c = await connect(s.address)
    f = await connect(s.address)
    await c.write({"op": "register-client", "client": "c", "versions": {}})
    await f.write({"op": "register-client", "client": "f", "versions": {}})
    await c.read()
    await f.read()

    assert set(s.client_comms) == {"c", "f"}

    await c.write(
        {
            "op": "update-graph",
            "tasks": {"x": dumps_task((inc, 1)), "y": dumps_task((inc, "x"))},
            "dependencies": {"x": [], "y": ["x"]},
            "client": "c",
            "keys": ["y"],
        }
    )

    await f.write(
        {
            "op": "update-graph",
            "tasks": {
                "x": dumps_task((inc, 1)),
                "z": dumps_task((operator.add, "x", 10)),
            },
            "dependencies": {"x": [], "z": ["x"]},
            "client": "f",
            "keys": ["z"],
        }
    )
    (msg,) = await c.read()
    assert msg["op"] == "key-in-memory"
    assert msg["key"] == "y"
    (msg,) = await f.read()
    assert msg["op"] == "key-in-memory"
    assert msg["key"] == "z"


def test_dumps_function():
    a = dumps_function(inc)
    assert cloudpickle.loads(a)(10) == 11

    b = dumps_function(inc)
    assert a is b

    c = dumps_function(dec)
    assert a != c


def test_dumps_task():
    d = dumps_task((inc, 1))
    assert set(d) == {"function", "args"}

    f = lambda x, y=2: x + y
    d = dumps_task((apply, f, (1,), {"y": 10}))
    assert cloudpickle.loads(d["function"])(1, 2) == 3
    assert cloudpickle.loads(d["args"]) == (1,)
    assert cloudpickle.loads(d["kwargs"]) == {"y": 10}

    d = dumps_task((apply, f, (1,)))
    assert cloudpickle.loads(d["function"])(1, 2) == 3
    assert cloudpickle.loads(d["args"]) == (1,)
    assert set(d) == {"function", "args"}


@gen_cluster()
async def test_ready_remove_worker(s, a, b):
    s.update_graph(
        tasks={"x-%d" % i: dumps_task((inc, i)) for i in range(20)},
        keys=["x-%d" % i for i in range(20)],
        client="client",
        dependencies={"x-%d" % i: [] for i in range(20)},
    )

    assert all(len(w.processing) > w.nthreads for w in s.workers.values())

    await s.remove_worker(address=a.address)

    assert set(s.workers) == {b.address}
    assert all(len(w.processing) > w.nthreads for w in s.workers.values())


@gen_cluster(client=True, Worker=Nanny, timeout=60)
async def test_restart(c, s, a, b):
    futures = c.map(inc, range(20))
    await wait(futures)

    await s.restart()

    assert len(s.workers) == 2

    for ws in s.workers.values():
        assert not ws.occupancy
        assert not ws.processing

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
async def test_broadcast(s, a, b):
    result = await s.broadcast(msg={"op": "ping"})
    assert result == {a.address: b"pong", b.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, workers=[a.address])
    assert result == {a.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, hosts=[a.ip])
    assert result == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(security=tls_only_security())
async def test_broadcast_tls(s, a, b):
    result = await s.broadcast(msg={"op": "ping"})
    assert result == {a.address: b"pong", b.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, workers=[a.address])
    assert result == {a.address: b"pong"}

    result = await s.broadcast(msg={"op": "ping"}, hosts=[a.ip])
    assert result == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(Worker=Nanny)
async def test_broadcast_nanny(s, a, b):
    result1 = await s.broadcast(msg={"op": "identity"}, nanny=True)
    assert all(d["type"] == "Nanny" for d in result1.values())

    result2 = await s.broadcast(
        msg={"op": "identity"}, workers=[a.worker_address], nanny=True
    )
    assert len(result2) == 1
    assert first(result2.values())["id"] == a.id

    result3 = await s.broadcast(msg={"op": "identity"}, hosts=[a.ip], nanny=True)
    assert result1 == result3


@gen_cluster(nthreads=[])
async def test_worker_name(s):
    w = await Worker(s.address, name="alice")
    assert s.workers[w.address].name == "alice"
    assert s.aliases["alice"] == w.address

    with pytest.raises(ValueError):
        w2 = await Worker(s.address, name="alice")
        await w2.close()

    await w.close()


@gen_cluster(nthreads=[])
async def test_coerce_address(s):
    print("scheduler:", s.address, s.listen_address)
    a = Worker(s.address, name="alice")
    b = Worker(s.address, name=123)
    c = Worker("127.0.0.1", s.port, name="charlie")
    await asyncio.gather(a, b, c)

    assert s.coerce_address("127.0.0.1:8000") == "tcp://127.0.0.1:8000"
    assert s.coerce_address("[::1]:8000") == "tcp://[::1]:8000"
    assert s.coerce_address("tcp://127.0.0.1:8000") == "tcp://127.0.0.1:8000"
    assert s.coerce_address("tcp://[::1]:8000") == "tcp://[::1]:8000"
    assert s.coerce_address("localhost:8000") in (
        "tcp://127.0.0.1:8000",
        "tcp://[::1]:8000",
    )
    assert s.coerce_address("localhost:8000") in (
        "tcp://127.0.0.1:8000",
        "tcp://[::1]:8000",
    )
    assert s.coerce_address(a.address) == a.address
    # Aliases
    assert s.coerce_address("alice") == a.address
    assert s.coerce_address(123) == b.address
    assert s.coerce_address("charlie") == c.address

    assert s.coerce_hostname("127.0.0.1") == "127.0.0.1"
    assert s.coerce_hostname("alice") == a.ip
    assert s.coerce_hostname(123) == b.ip
    assert s.coerce_hostname("charlie") == c.ip
    assert s.coerce_hostname("jimmy") == "jimmy"

    assert s.coerce_address("zzzt:8000", resolve=False) == "tcp://zzzt:8000"
    await asyncio.gather(a.close(), b.close(), c.close())


@gen_cluster(nthreads=[], config={"distributed.scheduler.work-stealing": True})
async def test_config_stealing(s):
    """Regression test for https://github.com/dask/distributed/issues/3409"""
    assert "stealing" in s.extensions


@gen_cluster(nthreads=[], config={"distributed.scheduler.work-stealing": False})
async def test_config_no_stealing(s):
    assert "stealing" not in s.extensions


@pytest.mark.skipif(WINDOWS, reason="num_fds not supported on windows")
@gen_cluster(nthreads=[])
async def test_file_descriptors_dont_leak(s):
    proc = psutil.Process()
    before = proc.num_fds()

    async with Worker(s.address):
        assert proc.num_fds() > before

    while proc.num_fds() > before:
        await asyncio.sleep(0.01)


@gen_cluster()
async def test_update_graph_culls(s, a, b):
    s.update_graph(
        tasks={
            "x": dumps_task((inc, 1)),
            "y": dumps_task((inc, "x")),
            "z": dumps_task((inc, 2)),
        },
        keys=["y"],
        dependencies={"y": "x", "x": [], "z": []},
        client="client",
    )
    assert "z" not in s.tasks
    assert "z" not in s.dependencies


def test_io_loop(loop):
    s = Scheduler(loop=loop, dashboard_address=":0", validate=True)
    assert s.io_loop is loop


@gen_cluster(client=True)
async def test_story(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    await wait([f])

    assert s.transition_log

    story = s.story(x.key)
    assert all(line in s.transition_log for line in story)
    assert len(story) < len(s.transition_log)
    assert all(x.key == line[0] or x.key in line[-2] for line in story)

    assert len(s.story(x.key, y.key)) > len(story)

    assert s.story(x.key) == s.story(s.tasks[x.key])


@gen_cluster(nthreads=[], client=True)
async def test_scatter_no_workers(c, s):
    with pytest.raises(TimeoutError):
        await s.scatter(data={"x": 1}, client="alice", timeout=0.1)

    start = time()
    with pytest.raises(TimeoutError):
        await c.scatter(123, timeout=0.1)
    assert time() < start + 1.5

    w = Worker(s.address, nthreads=3)
    await asyncio.gather(c.scatter(data={"y": 2}, timeout=5), w)

    assert w.data["y"] == 2
    await w.close()


@gen_cluster(nthreads=[])
async def test_scheduler_sees_memory_limits(s):
    w = await Worker(s.address, nthreads=3, memory_limit=12345)

    assert s.workers[w.address].memory_limit == 12345
    await w.close()


@gen_cluster(client=True)
async def test_retire_workers(c, s, a, b):
    [x] = await c.scatter([1], workers=a.address)
    [y] = await c.scatter([list(range(1000))], workers=b.address)

    assert s.workers_to_close() == [a.address]

    workers = await s.retire_workers()
    assert list(workers) == [a.address]
    assert workers[a.address]["nthreads"] == a.nthreads
    assert list(s.nthreads) == [b.address]

    assert s.workers_to_close() == []

    assert s.workers[b.address].has_what == {s.tasks[x.key], s.tasks[y.key]}

    workers = await s.retire_workers()
    assert not workers


@gen_cluster(client=True)
async def test_retire_workers_n(c, s, a, b):
    await s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 1

    await s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 1

    await s.retire_workers(n=1, close_workers=True)
    assert len(s.workers) == 0

    await s.retire_workers(n=0, close_workers=True)
    assert len(s.workers) == 0

    while not (
        a.status in (Status.closed, Status.closing, Status.closing_gracefully)
        and b.status in (Status.closed, Status.closing, Status.closing_gracefully)
    ):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 4)
async def test_workers_to_close(cl, s, *workers):
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"a": 4, "b": 4, "c": 1}}
    ):
        futures = cl.map(slowinc, [1, 1, 1], key=["a-4", "b-4", "c-1"])
        while sum(len(w.processing) for w in s.workers.values()) < 3:
            await asyncio.sleep(0.001)

        wtc = s.workers_to_close()
        assert all(not s.workers[w].processing for w in wtc)
        assert len(wtc) == 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 4)
async def test_workers_to_close_grouped(c, s, *workers):
    groups = {
        workers[0].address: "a",
        workers[1].address: "a",
        workers[2].address: "b",
        workers[3].address: "b",
    }

    def key(ws):
        return groups[ws.address]

    assert set(s.workers_to_close(key=key)) == {w.address for w in workers}

    # Assert that job in one worker blocks closure of group
    future = c.submit(slowinc, 1, delay=0.2, workers=workers[0].address)
    while len(s.rprocessing) < 1:
        await asyncio.sleep(0.001)

    assert set(s.workers_to_close(key=key)) == {workers[2].address, workers[3].address}

    del future

    while len(s.rprocessing) > 0:
        await asyncio.sleep(0.001)

    # Assert that *total* byte count in group determines group priority
    av = await c.scatter("a" * 100, workers=workers[0].address)
    bv = await c.scatter("b" * 75, workers=workers[2].address)
    bv2 = await c.scatter("b" * 75, workers=workers[3].address)

    assert set(s.workers_to_close(key=key)) == {workers[0].address, workers[1].address}


@gen_cluster(client=True)
async def test_retire_workers_no_suspicious_tasks(c, s, a, b):
    future = c.submit(
        slowinc, 100, delay=0.5, workers=a.address, allow_other_workers=True
    )
    await asyncio.sleep(0.2)
    await s.retire_workers(workers=[a.address])

    assert all(ts.suspicious == 0 for ts in s.tasks.values())
    assert all(tp.suspicious == 0 for tp in s.task_prefixes.values())


@pytest.mark.slow
@pytest.mark.skipif(WINDOWS, reason="num_fds not supported on windows")
@gen_cluster(client=True, nthreads=[], timeout=120)
async def test_file_descriptors(c, s):
    await asyncio.sleep(0.1)
    da = pytest.importorskip("dask.array")
    proc = psutil.Process()
    num_fds_1 = proc.num_fds()

    N = 20
    nannies = await asyncio.gather(*(Nanny(s.address, loop=s.loop) for _ in range(N)))

    while len(s.nthreads) < N:
        await asyncio.sleep(0.1)

    num_fds_2 = proc.num_fds()

    await asyncio.sleep(0.2)

    num_fds_3 = proc.num_fds()
    assert num_fds_3 <= num_fds_2 + N  # add some heartbeats

    x = da.random.random(size=(1000, 1000), chunks=(25, 25))
    x = c.persist(x)
    await wait(x)

    num_fds_4 = proc.num_fds()
    assert num_fds_4 <= num_fds_2 + 2 * N

    y = c.persist(x + x.T)
    await wait(y)

    num_fds_5 = proc.num_fds()
    assert num_fds_5 < num_fds_4 + N

    await asyncio.sleep(1)

    num_fds_6 = proc.num_fds()
    assert num_fds_6 < num_fds_5 + N

    await asyncio.gather(*(n.close() for n in nannies))
    await c.close()

    assert not s.rpc.open
    for addr, occ in c.rpc.occupied.items():
        for comm in occ:
            assert comm.closed() or comm.peer_address != s.address, comm
    assert not s.stream_comms

    while proc.num_fds() > num_fds_1 + N:
        await asyncio.sleep(0.01)


@pytest.mark.slow
@nodebug
@gen_cluster(client=True)
async def test_learn_occupancy(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.2)
    while sum(len(ts.who_has) for ts in s.tasks.values()) < 10:
        await asyncio.sleep(0.01)

    assert 100 < s.total_occupancy < 1000
    for w in [a, b]:
        assert 50 < s.workers[w.address].occupancy < 700


@pytest.mark.slow
@nodebug
@gen_cluster(client=True)
async def test_learn_occupancy_2(c, s, a, b):
    future = c.map(slowinc, range(1000), delay=0.2)
    while not any(ts.who_has for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    assert 100 < s.total_occupancy < 1000


@gen_cluster(client=True)
async def test_occupancy_cleardown(c, s, a, b):
    s.validate = False

    # Inject excess values in s.occupancy
    s.workers[a.address].occupancy = 2
    s.total_occupancy += 2
    futures = c.map(slowinc, range(100), delay=0.01)
    await wait(futures)

    # Verify that occupancy values have been zeroed out
    assert abs(s.total_occupancy) < 0.01
    assert all(ws.occupancy == 0 for ws in s.workers.values())


@nodebug
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 30)
async def test_balance_many_workers(c, s, *workers):
    futures = c.map(slowinc, range(20), delay=0.2)
    await wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {0, 1}


@nodebug
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 30)
async def test_balance_many_workers_2(c, s, *workers):
    s.extensions["stealing"]._pc.callback_time = 100000000
    futures = c.map(slowinc, range(90), delay=0.2)
    await wait(futures)
    assert {len(w.has_what) for w in s.workers.values()} == {3}


@gen_cluster(client=True)
async def test_learn_occupancy_multiple_workers(c, s, a, b):
    x = c.submit(slowinc, 1, delay=0.2, workers=a.address)
    await asyncio.sleep(0.05)
    futures = c.map(slowinc, range(100), delay=0.2)

    await wait(x)

    assert not any(v == 0.5 for w in s.workers.values() for v in w.processing.values())


@gen_cluster(client=True)
async def test_include_communication_in_occupancy(c, s, a, b):
    await c.submit(slowadd, 1, 2, delay=0)
    x = c.submit(operator.mul, b"0", int(s.bandwidth), workers=a.address)
    y = c.submit(operator.mul, b"1", int(s.bandwidth * 1.5), workers=b.address)

    z = c.submit(slowadd, x, y, delay=1)
    while z.key not in s.tasks or not s.tasks[z.key].processing_on:
        await asyncio.sleep(0.01)

    ts = s.tasks[z.key]
    assert ts.processing_on == s.workers[b.address]
    assert s.workers[b.address].processing[ts] > 1
    await wait(z)
    del z


@gen_cluster(client=True)
async def test_worker_arrives_with_processing_data(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not any(w.processing for w in s.workers.values()):
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1)
    w.update_data(data={y.key: 3})

    await w

    start = time()

    while len(s.workers) < 3:
        await asyncio.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_worker_breaks_and_returns(c, s, a):
    future = c.submit(slowinc, 1, delay=0.1)
    for i in range(20):
        future = c.submit(slowinc, future, delay=0.1)

    await wait(future)

    await a.batched_stream.comm.close()

    await asyncio.sleep(0.1)
    start = time()
    await wait(future, timeout=10)
    end = time()

    assert end - start < 2

    states = frequencies(ts.state for ts in s.tasks.values())
    assert states == {"memory": 1, "released": 20}


@gen_cluster(client=True, nthreads=[])
async def test_no_workers_to_memory(c, s):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not s.tasks:
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1)
    w.update_data(data={y.key: 3})

    await w

    start = time()

    while not s.workers:
        await asyncio.sleep(0.01)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


@gen_cluster(client=True)
async def test_no_worker_to_memory_restrictions(c, s, a, b):
    with dask.annotate(workers="alice"):
        x = delayed(slowinc)(1, delay=0.4)
        y = delayed(slowinc)(x, delay=0.4)
        z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z], optimize_graph=False)

    while not s.tasks:
        await asyncio.sleep(0.01)

    w = Worker(s.address, nthreads=1, name="alice")
    w.update_data(data={y.key: 3})

    await w

    while len(s.workers) < 3:
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.3)

    assert s.get_task_status(keys={x.key, y.key, z.key}) == {
        x.key: "released",
        y.key: "memory",
        z.key: "processing",
    }

    await w.close()


def test_run_on_scheduler_sync(loop):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            address = c.run_on_scheduler(f)
            assert address == s["address"]

            with pytest.raises(ZeroDivisionError):
                c.run_on_scheduler(div, 1, 0)


@gen_cluster(client=True)
async def test_run_on_scheduler(c, s, a, b):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    response = await c._run_on_scheduler(f)
    assert response == s.address


@gen_cluster(client=True, config={"distributed.scheduler.pickle": False})
async def test_run_on_scheduler_disabled(c, s, a, b):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    with pytest.raises(ValueError, match="disallowed from deserializing"):
        await c._run_on_scheduler(f)


@gen_cluster(client=True)
async def test_close_worker(c, s, a, b):
    assert len(s.workers) == 2

    await s.close_worker(worker=a.address)

    assert len(s.workers) == 1
    assert a.address not in s.workers

    await asyncio.sleep(0.5)

    assert len(s.workers) == 1


@pytest.mark.slow
@gen_cluster(client=True, Worker=Nanny)
async def test_close_nanny(c, s, a, b):
    assert len(s.workers) == 2

    assert a.process.is_alive()
    a_worker_address = a.worker_address
    start = time()
    await s.close_worker(worker=a_worker_address)

    assert len(s.workers) == 1
    assert a_worker_address not in s.workers

    start = time()
    while a.is_alive():
        await asyncio.sleep(0.1)
        assert time() < start + 5

    assert not a.is_alive()
    assert a.pid is None

    for i in range(10):
        await asyncio.sleep(0.1)
        assert len(s.workers) == 1
        assert not a.is_alive()
        assert a.pid is None

    while a.status != Status.closed:
        await asyncio.sleep(0.05)
        assert time() < start + 10


@gen_cluster(client=True)
async def test_retire_workers_close(c, s, a, b):
    await s.retire_workers(close_workers=True)
    assert not s.workers
    while a.status != Status.closed and b.status != Status.closed:
        await asyncio.sleep(0.01)


@gen_cluster(client=True, Worker=Nanny)
async def test_retire_nannies_close(c, s, a, b):
    nannies = [a, b]
    await s.retire_workers(close_workers=True, remove=True)
    assert not s.workers

    start = time()

    while any(n.status != Status.closed for n in nannies):
        await asyncio.sleep(0.05)
        assert time() < start + 10

    assert not any(n.is_alive() for n in nannies)
    assert not s.workers


@gen_cluster(client=True, nthreads=[("127.0.0.1", 2)])
async def test_fifo_submission(c, s, w):
    futures = []
    for i in range(20):
        future = c.submit(slowinc, i, delay=0.1, key="inc-%02d" % i, fifo_timeout=0.01)
        futures.append(future)
        await asyncio.sleep(0.02)
    await wait(futures[-1])
    assert futures[10].status == "finished"


@gen_test()
async def test_scheduler_file():
    with tmpfile() as fn:
        s = await Scheduler(scheduler_file=fn, dashboard_address=":0")
        with open(fn) as f:
            data = json.load(f)
        assert data["address"] == s.address

        c = await Client(scheduler_file=fn, loop=s.loop, asynchronous=True)
        await c.close()
        await s.close()


@pytest.mark.xfail()
@gen_cluster(client=True, nthreads=[])
async def test_non_existent_worker(c, s):
    with dask.config.set({"distributed.comm.timeouts.connect": "100ms"}):
        await s.add_worker(
            address="127.0.0.1:5738", nthreads=2, nbytes={}, host_info={}
        )
        futures = c.map(inc, range(10))
        await asyncio.sleep(0.300)
        assert not s.workers
        assert all(ts.state == "no-worker" for ts in s.tasks.values())


@pytest.mark.parametrize(
    "host", ["tcp://0.0.0.0", "tcp://127.0.0.1", "tcp://127.0.0.1:38275"]
)
@pytest.mark.parametrize(
    "dashboard_address,expect",
    [
        (None, ("::", "0.0.0.0")),
        ("127.0.0.1:0", ("127.0.0.1",)),
    ],
)
@pytest.mark.asyncio
async def test_dashboard_host(host, dashboard_address, expect):
    """Dashboard is accessible from any host by default, but it can be also bound to
    localhost.
    """
    async with Scheduler(host=host, dashboard_address=dashboard_address) as s:
        sock = first(s.http_server._sockets.values())
        assert sock.getsockname()[0] in expect


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata(c, s, a, b):
    start = time() - 1
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    await wait(futures)
    await asyncio.sleep(0.200)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    now = time() + 1
    assert meta
    assert all(start < t < now for t, count in meta["counts"])
    assert all(0 <= count < 30 for t, count in meta["counts"][:4])
    assert not meta["counts"][-1][1]


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata_timeout(c, s, a, b):
    start = time() - 1

    def raise_timeout(*args, **kwargs):
        raise TimeoutError

    b.handlers["profile_metadata"] = raise_timeout

    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    await wait(futures)
    await asyncio.sleep(0.200)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    now = time() + 1
    assert meta
    assert all(start < t < now for t, count in meta["counts"])
    assert all(0 <= count < 30 for t, count in meta["counts"][:4])
    assert not meta["counts"][-1][1]


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "100ms"})
async def test_profile_metadata_keys(c, s, a, b):
    x = c.map(slowinc, range(10), delay=0.05)
    y = c.map(slowdec, range(10), delay=0.05)
    await wait(x + y)

    meta = await s.get_profile_metadata(profile_cycle_interval=0.100)
    assert set(meta["keys"]) == {"slowinc", "slowdec"}
    assert (
        len(meta["counts"]) - 3 <= len(meta["keys"]["slowinc"]) <= len(meta["counts"])
    )


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)

    await wait(futures)

    profile = await s.get_profile()
    assert profile["count"]


@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling_failure(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)

    def raise_timeout(*args, **kwargs):
        raise TimeoutError

    b.handlers["profile"] = raise_timeout
    await wait(futures)

    profile = await s.get_profile()
    assert profile["count"]


@gen_cluster(client=True)
async def test_cancel_fire_and_forget(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.05)
    y = delayed(slowinc)(x, delay=0.05)
    z = delayed(slowinc)(y, delay=0.05)
    w = delayed(slowinc)(z, delay=0.05)
    future = c.compute(w)
    fire_and_forget(future)

    await asyncio.sleep(0.05)
    await future.cancel(force=True)
    assert future.status == "cancelled"
    assert not s.tasks


@gen_cluster(
    client=True, Worker=Nanny, clean_kwargs={"processes": False, "threads": False}
)
async def test_log_tasks_during_restart(c, s, a, b):
    future = c.submit(sys.exit, 0)
    await wait(future)
    assert "exit" in str(s.events)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule(c, s, a, b):
    await c.submit(slowinc, -1, delay=0.1)  # learn cost
    x = c.map(slowinc, range(4), delay=0.1)

    # add much more work onto worker a
    futures = c.map(slowinc, range(10, 20), delay=0.1, workers=a.address)

    while len(s.tasks) < len(x) + len(futures):
        await asyncio.sleep(0.001)

    for future in x:
        s.reschedule(key=future.key)

    # Worker b gets more of the original tasks
    await wait(x)
    assert sum(future.key in b.data for future in x) >= 3
    assert sum(future.key in a.data for future in x) <= 1


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule_warns(c, s, a, b):
    with captured_logger(logging.getLogger("distributed.scheduler")) as sched:
        s.reschedule(key="__this-key-does-not-exist__")

    assert "not found on the scheduler" in sched.getvalue()
    assert "Aborting reschedule" in sched.getvalue()


@gen_cluster(client=True)
async def test_get_task_status(c, s, a, b):
    future = c.submit(inc, 1)
    await wait(future)

    result = await a.scheduler.get_task_status(keys=[future.key])
    assert result == {future.key: "memory"}


@gen_cluster(nthreads=[])
async def test_deque_handler(s):
    from distributed.scheduler import logger

    deque_handler = s._deque_handler
    logger.info("foo123")
    assert len(deque_handler.deque) >= 1
    msg = deque_handler.deque[-1]
    assert "distributed.scheduler" in deque_handler.format(msg)
    assert any(msg.msg == "foo123" for msg in deque_handler.deque)


@gen_cluster(client=True)
async def test_retries(c, s, a, b):
    args = [ZeroDivisionError("one"), ZeroDivisionError("two"), 42]

    future = c.submit(varying(args), retries=3)
    result = await future
    assert result == 42
    assert s.tasks[future.key].retries == 1
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=2, pure=False)
    result = await future
    assert result == 42
    assert s.tasks[future.key].retries == 0
    assert future.key not in s.exceptions

    future = c.submit(varying(args), retries=1, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        await future
    exc_info.match("two")

    future = c.submit(varying(args), retries=0, pure=False)
    with pytest.raises(ZeroDivisionError) as exc_info:
        await future
    exc_info.match("one")


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_missing_data_errant_worker(c, s, w1, w2, w3):
    with dask.config.set({"distributed.comm.timeouts.connect": "1s"}):
        np = pytest.importorskip("numpy")

        x = c.submit(np.random.random, 10000000, workers=w1.address)
        await wait(x)
        await c.replicate(x, workers=[w1.address, w2.address])

        y = c.submit(len, x, workers=w3.address)
        while not w3.tasks:
            await asyncio.sleep(0.001)
        await w1.close()
        await wait(y)


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")

    yy = y.persist()
    await wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    await wait(yyy)

    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_2(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")

    yy = y.persist()
    await wait(yy)

    old = s.story("x", "y")

    zz = z.persist()
    await wait(zz)

    await asyncio.sleep(0.100)
    assert s.story("x", "y") == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_3(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(2, dask_key_name="y")
    z = delayed(inc)(y, dask_key_name="z")
    w = delayed(operator.add)(x, z, dask_key_name="w")

    ww = w.persist()
    await wait(ww)

    old = list(s.transition_log)

    www = w.persist()
    await wait(www)
    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster(client=True)
async def test_dont_recompute_if_persisted_4(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(inc)(x, dask_key_name="y")
    z = delayed(inc)(x, dask_key_name="z")

    yy = y.persist()
    await wait(yy)

    old = s.story("x")

    while s.tasks["x"].state == "memory":
        await asyncio.sleep(0.01)

    yyy, zzz = dask.persist(y, z)
    await wait([yyy, zzz])

    new = s.story("x")
    assert len(new) > len(old)


@gen_cluster(client=True)
async def test_dont_forget_released_keys(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    y = c.submit(inc, x, key="y")
    z = c.submit(dec, x, key="z")
    del x
    await wait([y, z])
    del z

    while "z" in s.tasks:
        await asyncio.sleep(0.01)

    assert "x" in s.tasks


@gen_cluster(client=True)
async def test_dont_recompute_if_erred(c, s, a, b):
    x = delayed(inc)(1, dask_key_name="x")
    y = delayed(div)(x, 0, dask_key_name="y")

    yy = y.persist()
    await wait(yy)

    old = list(s.transition_log)

    yyy = y.persist()
    await wait(yyy)

    await asyncio.sleep(0.100)
    assert list(s.transition_log) == old


@gen_cluster()
async def test_closing_scheduler_closes_workers(s, a, b):
    await s.close()

    start = time()
    while a.status != Status.closed or b.status != Status.closed:
        await asyncio.sleep(0.01)
        assert time() < start + 2


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], worker_kwargs={"resources": {"A": 1}}
)
async def test_resources_reset_after_cancelled_task(c, s, w):
    future = c.submit(sleep, 0.2, resources={"A": 1})

    while not w.executing_count:
        await asyncio.sleep(0.01)

    await future.cancel()

    while w.executing_count:
        await asyncio.sleep(0.01)

    assert not s.workers[w.address].used_resources["A"]
    assert w.available_resources == {"A": 1}

    await c.submit(inc, 1, resources={"A": 1})


@gen_cluster(client=True)
async def test_gh2187(c, s, a, b):
    def foo():
        return "foo"

    def bar(x):
        return x + "bar"

    def baz(x):
        return x + "baz"

    def qux(x):
        sleep(0.1)
        return x + "qux"

    w = c.submit(foo, key="w")
    x = c.submit(bar, w, key="x")
    y = c.submit(baz, x, key="y")
    await y
    z = c.submit(qux, y, key="z")
    del y
    await asyncio.sleep(0.1)
    f = c.submit(bar, x, key="y")
    await f


@gen_cluster(client=True)
async def test_collect_versions(c, s, a, b):
    cs = s.clients[c.id]
    (w1, w2) = s.workers.values()
    assert cs.versions
    assert w1.versions
    assert w2.versions
    assert "dask" in str(cs.versions)
    assert cs.versions == w1.versions == w2.versions


@pytest.mark.xfail(reason="flaky and re-fails on rerun")
@gen_cluster(client=True, config={"distributed.scheduler.idle-timeout": "500ms"})
async def test_idle_timeout(c, s, a, b):
    beginning = time()
    assert s.idle_since <= beginning
    future = c.submit(slowinc, 1)
    await future
    assert s.idle_since is None or s.idle_since > beginning

    assert s.status != Status.closed

    with captured_logger("distributed.scheduler") as logs:
        start = time()
        while s.status != Status.closed:
            await asyncio.sleep(0.01)
            assert time() < start + 3

        start = time()
        while not (a.status == Status.closed and b.status == Status.closed):
            await asyncio.sleep(0.01)
            assert time() < start + 1

    assert "idle" in logs.getvalue()
    assert "500" in logs.getvalue()
    assert "ms" in logs.getvalue()
    assert s.idle_since > beginning


@gen_cluster(client=True, config={"distributed.scheduler.bandwidth": "100 GB"})
async def test_bandwidth(c, s, a, b):
    start = s.bandwidth
    x = c.submit(operator.mul, b"0", 1000001, workers=a.address)
    y = c.submit(lambda x: x, x, workers=b.address)
    await y
    await b.heartbeat()
    assert s.bandwidth < start  # we've learned that we're slower
    assert b.latency
    assert typename(bytes) in s.bandwidth_types
    assert (b.address, a.address) in s.bandwidth_workers

    await a.close()
    assert not s.bandwidth_workers


@gen_cluster(client=True, Worker=Nanny, timeout=60)
async def test_bandwidth_clear(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = c.submit(np.arange, 1000000, workers=[a.worker_address], pure=False)
    y = c.submit(np.arange, 1000000, workers=[b.worker_address], pure=False)
    z = c.submit(operator.add, x, y)  # force communication
    await z

    async def f(dask_worker):
        await dask_worker.heartbeat()

    await c.run(f)

    assert s.bandwidth_workers

    await s.restart()
    assert not s.bandwidth_workers


@gen_cluster()
async def test_workerstate_clean(s, a, b):
    ws = s.workers[a.address].clean()
    assert ws.address == a.address
    b = pickle.dumps(ws)
    assert len(b) < 1000


@gen_cluster(client=True)
async def test_result_type(c, s, a, b):
    x = c.submit(lambda: 1)
    await x

    assert "int" in s.tasks[x.key].type


@gen_cluster()
async def test_close_workers(s, a, b):
    await s.close(close_workers=True)
    assert a.status == Status.closed
    assert b.status == Status.closed


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_test()
async def test_host_address():
    s = await Scheduler(host="127.0.0.2", dashboard_address=":0")
    assert "127.0.0.2" in s.address
    await s.close()


@gen_test()
async def test_dashboard_address():
    pytest.importorskip("bokeh")
    async with Scheduler(dashboard_address="127.0.0.1:8901") as s:
        assert s.services["dashboard"].port == 8901

    async with Scheduler(dashboard_address="127.0.0.1") as s:
        assert s.services["dashboard"].port

    async with Scheduler(dashboard_address="127.0.0.1:8901,127.0.0.1:8902") as s:
        assert s.services["dashboard"].port == 8901

    async with Scheduler(dashboard_address=":8901,:8902") as s:
        assert s.services["dashboard"].port == 8901

    async with Scheduler(dashboard_address=[8901, 8902]) as s:
        assert s.services["dashboard"].port == 8901


@gen_cluster(client=True)
async def test_adaptive_target(c, s, a, b):
    with dask.config.set(
        {"distributed.scheduler.default-task-durations": {"slowinc": 10}}
    ):
        assert s.adaptive_target() == 0
        x = c.submit(inc, 1)
        await x
        assert s.adaptive_target() == 1

        # Long task
        x = c.submit(slowinc, 1, delay=0.5)
        while x.key not in s.tasks:
            await asyncio.sleep(0.01)
        assert s.adaptive_target(target_duration=".1s") == 1  # still one

        L = c.map(slowinc, range(100), delay=0.5)
        while len(s.tasks) < 100:
            await asyncio.sleep(0.01)
        assert 10 < s.adaptive_target(target_duration=".1s") <= 100
        del x, L
        while s.tasks:
            await asyncio.sleep(0.01)
        assert s.adaptive_target(target_duration=".1s") == 0


@gen_test()
async def test_async_context_manager():
    async with Scheduler(dashboard_address=":0") as s:
        assert s.status == Status.running
        async with Worker(s.address) as w:
            assert w.status == Status.running
            assert s.workers
        assert not s.workers


@gen_test()
async def test_allowed_failures_config():
    async with Scheduler(dashboard_address=":0", allowed_failures=10) as s:
        assert s.allowed_failures == 10

    with dask.config.set({"distributed.scheduler.allowed_failures": 100}):
        async with Scheduler(dashboard_address=":0") as s:
            assert s.allowed_failures == 100

    with dask.config.set({"distributed.scheduler.allowed_failures": 0}):
        async with Scheduler(dashboard_address=":0") as s:
            assert s.allowed_failures == 0


@gen_test()
async def test_finished():
    async with Scheduler(dashboard_address=":0") as s:
        async with Worker(s.address) as w:
            pass

    await s.finished()
    await w.finished()


@gen_cluster(nthreads=[], client=True)
async def test_retire_names_str(c, s):
    async with Worker(s.address, name="0") as a, Worker(s.address, name="1") as b:
        futures = c.map(inc, range(10))
        await wait(futures)
        assert a.data and b.data
        await s.retire_workers(names=[0])
        assert all(f.done() for f in futures)
        assert len(b.data) == 10


@gen_cluster(
    client=True, config={"distributed.scheduler.default-task-durations": {"inc": 100}}
)
async def test_get_task_duration(c, s, a, b):
    future = c.submit(inc, 1)
    await future
    assert 10 < s.task_prefixes["inc"].duration_average < 100

    ts_pref1 = s.new_task("inc-abcdefab", None, "released")
    assert 10 < s.get_task_duration(ts_pref1) < 100

    # make sure get_task_duration adds TaskStates to unknown dict
    assert len(s.unknown_durations) == 0
    x = c.submit(slowinc, 1, delay=0.5)
    while len(s.tasks) < 3:
        await asyncio.sleep(0.01)

    ts = s.tasks[x.key]
    assert s.get_task_duration(ts) == 0.5  # default
    assert len(s.unknown_durations) == 1
    assert len(s.unknown_durations["slowinc"]) == 1


@gen_cluster(client=True)
async def test_default_task_duration_splits(c, s, a, b):
    """Ensure that the default task durations for shuffle split tasks are, by default,
    aligned with the task names of dask.dask
    """
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # We don't care about the actual computation here but we'll schedule one anyhow to verify that we're looking for the correct key
    npart = 10
    df = dd.from_pandas(pd.DataFrame({"A": range(100), "B": 1}), npartitions=npart)
    graph = df.shuffle(
        "A",
        shuffle="tasks",
        # If we don't have enough partitions, we'll fall back to a simple shuffle
        max_branch=npart - 1,
    ).sum()
    fut = c.compute(graph)
    await wait(fut)

    split_prefix = [pre for pre in s.task_prefixes.keys() if "split" in pre]
    assert len(split_prefix) == 1
    split_prefix = split_prefix[0]
    default_time = parse_timedelta(
        dask.config.get("distributed.scheduler.default-task-durations")[split_prefix]
    )
    assert default_time <= 1e-6


@gen_test()
async def test_no_danglng_asyncio_tasks():
    start = asyncio.all_tasks()
    async with Scheduler(dashboard_address=":0") as s:
        async with Worker(s.address, name="0"):
            async with Client(s.address, asynchronous=True) as c:
                await c.submit(lambda: 1)

    tasks = asyncio.all_tasks()
    assert tasks == start


class NoSchedulerDelayWorker(Worker):
    """Custom worker class which does not update `scheduler_delay`.

    This worker class is useful for some tests which make time
    comparisons using times reported from workers.
    """

    @property
    def scheduler_delay(self):
        return 0

    @scheduler_delay.setter
    def scheduler_delay(self, value):
        pass


@gen_cluster(client=True, Worker=NoSchedulerDelayWorker)
async def test_task_groups(c, s, a, b):
    start = time()
    da = pytest.importorskip("dask.array")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).persist(optimize_graph=False)
    y = await y
    stop = time()

    tg = s.task_groups[x.name]
    tp = s.task_prefixes["arange"]
    repr(tg)
    repr(tp)
    assert tg.states["memory"] == 0
    assert tg.states["released"] == 5
    assert tp.states["memory"] == 0
    assert tp.states["released"] == 5
    assert tp.groups == [tg]
    assert tg.prefix is tp
    # these must be true since in this simple case there is a 1to1 mapping
    # between prefix and group
    assert tg.duration == tp.duration
    assert tg.nbytes_total == tp.nbytes_total
    # It should map down to individual tasks
    assert tg.nbytes_total == sum(
        ts.get_nbytes() for ts in s.tasks.values() if ts.group is tg
    )
    tg = s.task_groups[y.name]
    assert tg.states["memory"] == 5

    assert s.task_groups[y.name].dependencies == {s.task_groups[x.name]}

    await c.replicate(y)
    # TODO: Are we supposed to track replicated memory here? See also Scheduler.add_keys
    assert "array" in str(tg.types)
    assert "array" in str(tp.types)

    del y

    while s.tasks:
        await asyncio.sleep(0.01)

    assert tg.states["forgotten"] == 5
    assert tg.name not in s.task_groups
    assert tg.start > start
    assert tg.stop < stop
    assert "compute" in tg.all_durations


@gen_cluster(client=True)
async def test_task_prefix(c, s, a, b):
    da = pytest.importorskip("dask.array")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).sum().persist()
    y = await y

    assert s.task_prefixes["sum-aggregate"].states["memory"] == 1

    a = da.arange(101, chunks=(20,))
    b = (a + 1).sum().persist()
    b = await b

    assert s.task_prefixes["sum-aggregate"].states["memory"] == 2


@gen_cluster(
    client=True, Worker=Nanny, config={"distributed.scheduler.allowed-failures": 0}
)
async def test_failing_task_increments_suspicious(client, s, a, b):
    future = client.submit(sys.exit, 0)
    await wait(future)

    assert s.task_prefixes["exit"].suspicious == 1
    assert sum(tp.suspicious for tp in s.task_prefixes.values()) == sum(
        ts.suspicious for ts in s.tasks.values()
    )


@gen_cluster(client=True)
async def test_task_group_non_tuple_key(c, s, a, b):
    da = pytest.importorskip("dask.array")
    np = pytest.importorskip("numpy")
    x = da.arange(100, chunks=(20,))
    y = (x + 1).sum().persist()
    y = await y

    assert s.task_prefixes["sum"].states["released"] == 4
    assert "sum" not in s.task_groups

    f = c.submit(np.sum, [1, 2, 3])
    await f

    assert s.task_prefixes["sum"].states["released"] == 4
    assert s.task_prefixes["sum"].states["memory"] == 1
    assert "sum" in s.task_groups


@gen_cluster(client=True)
async def test_task_unique_groups(c, s, a, b):
    """This test ensure that task groups remain unique when using submit"""
    x = c.submit(sum, [1, 2])
    y = c.submit(len, [1, 2])
    z = c.submit(sum, [3, 4])
    await asyncio.wait([x, y, z])

    assert s.task_prefixes["len"].states["memory"] == 1
    assert s.task_prefixes["sum"].states["memory"] == 2


@gen_cluster(client=True)
async def test_task_group_on_fire_and_forget(c, s, a, b):
    # Regression test for https://github.com/dask/distributed/issues/3465
    with captured_logger("distributed.scheduler") as logs:
        x = await c.scatter(list(range(10)))
        fire_and_forget([c.submit(slowadd, i, x[i]) for i in range(len(x))])
        await asyncio.sleep(1)

    assert "Error transitioning" not in logs.getvalue()


class BrokenComm(Comm):
    peer_address = ""
    local_address = ""

    def close(self):
        pass

    def closed(self):
        pass

    def abort(self):
        pass

    def read(self, deserializers=None):
        raise OSError()

    def write(self, msg, serializers=None, on_error=None):
        raise OSError()


class FlakyConnectionPool(ConnectionPool):
    def __init__(self, *args, failing_connections=0, **kwargs):
        self.cnn_count = 0
        self.failing_connections = failing_connections
        super().__init__(*args, **kwargs)

    async def connect(self, *args, **kwargs):
        self.cnn_count += 1
        if self.cnn_count > self.failing_connections:
            return await super().connect(*args, **kwargs)
        else:
            return BrokenComm()


@gen_cluster(client=True)
async def test_gather_failing_cnn_recover(c, s, a, b):
    orig_rpc = s.rpc
    x = await c.scatter({"x": 1}, workers=a.address)

    s.rpc = await FlakyConnectionPool(failing_connections=1)
    with dask.config.set({"distributed.comm.retry.count": 1}):
        res = await s.gather(keys=["x"])
    assert res["status"] == "OK"


@gen_cluster(client=True)
async def test_gather_failing_cnn_error(c, s, a, b):
    orig_rpc = s.rpc
    x = await c.scatter({"x": 1}, workers=a.address)

    s.rpc = await FlakyConnectionPool(failing_connections=10)
    res = await s.gather(keys=["x"])
    assert res["status"] == "error"
    assert list(res["keys"]) == ["x"]


@gen_cluster(client=True)
async def test_gather_no_workers(c, s, a, b):
    await asyncio.sleep(1)
    x = await c.scatter({"x": 1}, workers=a.address)

    await a.close()
    await b.close()

    res = await s.gather(keys=["x"])
    assert res["status"] == "error"
    assert list(res["keys"]) == ["x"]


@pytest.mark.slow
@pytest.mark.parametrize("reschedule_different_worker", [True, False])
@pytest.mark.parametrize("swap_data_insert_order", [True, False])
@gen_cluster(client=True, client_kwargs={"direct_to_workers": False})
async def test_gather_allow_worker_reconnect(
    c, s, a, b, reschedule_different_worker, swap_data_insert_order
):
    """
    Test that client resubmissions allow failed workers to reconnect and re-use
    their results. Failure scenario would be a connection issue during result
    gathering.
    Upon connection failure, the worker is flagged as suspicious and removed
    from the scheduler. If the worker is healthy and reconnencts we want to use
    its results instead of recomputing them.

    See also distributed.tests.test_worker.py::test_worker_reconnects_mid_compute
    """
    # GH3246
    if reschedule_different_worker:
        from distributed.diagnostics.plugin import SchedulerPlugin

        class SwitchRestrictions(SchedulerPlugin):
            def __init__(self, scheduler):
                self.scheduler = scheduler

            def transition(self, key, start, finish, **kwargs):
                if key in ("reducer", "final") and finish == "memory":
                    self.scheduler.tasks[key]._worker_restrictions = {b.address}

        plugin = SwitchRestrictions(s)
        s.add_plugin(plugin)

    from distributed import Lock

    b_address = b.address

    def inc_slow(x, lock):
        w = get_worker()
        if w.address == b_address:
            with lock:
                return x + 1
        return x + 1

    lock = Lock()

    await lock.acquire()

    x = c.submit(inc_slow, 1, lock, workers=[a.address], allow_other_workers=True)

    def reducer(*args):
        return get_worker().address

    def finalizer(addr):
        if swap_data_insert_order:
            w = get_worker()
            new_data = {k: w.data[k] for k in list(w.data.keys())[::-1]}
            w.data = new_data
        return addr

    z = c.submit(reducer, x, key="reducer", workers=[a.address])
    fin = c.submit(finalizer, z, key="final", workers=[a.address])

    s.rpc = await FlakyConnectionPool(failing_connections=1)

    # This behaviour is independent of retries. Remove them to reduce complexity
    # of this setup
    with dask.config.set({"distributed.comm.retry.count": 0}):
        with captured_logger(
            logging.getLogger("distributed.scheduler")
        ) as sched_logger, captured_logger(
            logging.getLogger("distributed.client")
        ) as client_logger:
            # Gather using the client (as an ordinary user would)
            # Upon a missing key, the client will reschedule the computations
            res = None
            while not res:
                try:
                    # This reduces test runtime by about a second since we're
                    # depending on a worker heartbeat for a reconnect.
                    res = await asyncio.wait_for(fin, 0.1)
                except asyncio.TimeoutError:
                    await a.heartbeat()

    # Ensure that we're actually reusing the result
    assert res == a.address
    await lock.release()

    while not all(all(ts.state == "memory" for ts in w.tasks.values()) for w in [a, b]):
        await asyncio.sleep(0.01)

    assert z.key in a.tasks
    assert z.key not in b.tasks
    assert b.executed_count == 1
    for w in [a, b]:
        assert x.key in w.tasks
        assert w.tasks[x.key].state == "memory"
    while not len(s.tasks[x.key].who_has) == 2:
        await asyncio.sleep(0.01)
    assert len(s.tasks[z.key].who_has) == 1

    sched_logger = sched_logger.getvalue()
    client_logger = client_logger.getvalue()

    # Ensure that the communication was done via the scheduler, i.e. we actually hit a
    # bad connection
    assert s.rpc.cnn_count > 0

    # The reducer task was actually not found upon first collection. The client will
    # reschedule the graph
    assert "Couldn't gather 1 keys, rescheduling" in client_logger
    # There will also be a `Unexpected worker completed task` message but this
    # is rather an artifact and not the intention
    assert "Workers don't have promised key" in sched_logger


@gen_cluster(client=True)
async def test_too_many_groups(c, s, a, b):
    x = dask.delayed(inc)(1)
    y = dask.delayed(dec)(2)
    z = dask.delayed(operator.add)(x, y)

    await c.compute(z)

    while s.tasks:
        await asyncio.sleep(0.01)

    assert len(s.task_groups) < 3


@gen_test()
async def test_multiple_listeners():
    with captured_logger(logging.getLogger("distributed.scheduler")) as log:
        async with Scheduler(dashboard_address=":0", protocol=["inproc", "tcp"]) as s:
            async with Worker(s.listeners[0].contact_address) as a:
                async with Worker(s.listeners[1].contact_address) as b:
                    assert a.address.startswith("inproc")
                    assert a.scheduler.address.startswith("inproc")
                    assert b.address.startswith("tcp")
                    assert b.scheduler.address.startswith("tcp")

                    async with Client(s.address, asynchronous=True) as c:
                        futures = c.map(inc, range(20))
                        await wait(futures)

                        # Force inter-worker communication both ways
                        await c.submit(sum, futures, workers=[a.address])
                        await c.submit(len, futures, workers=[b.address])

    log = log.getvalue()
    assert re.search(r"Scheduler at:\s*tcp://", log)
    assert re.search(r"Scheduler at:\s*inproc://", log)


@gen_cluster(nthreads=[("127.0.0.1", 1)])
async def test_worker_name_collision(s, a):
    # test that a name collision for workers produces the expected response
    # and leaves the data structures of Scheduler in a good state
    # is not updated by the second worker
    with captured_logger(logging.getLogger("distributed.scheduler")) as log:
        with pytest.raises(ValueError, match=f"name taken, {a.name!r}"):
            await Worker(s.address, name=a.name, loop=s.loop, host="127.0.0.1")

    s.validate_state()
    assert set(s.workers) == {a.address}
    assert s.aliases == {a.name: a.address}

    log = log.getvalue()
    assert "duplicate" in log
    assert str(a.name) in log


@gen_cluster(client=True, config={"distributed.scheduler.unknown-task-duration": "1h"})
async def test_unknown_task_duration_config(client, s, a, b):
    future = client.submit(slowinc, 1)
    while not s.tasks:
        await asyncio.sleep(0.001)
    assert sum(s.get_task_duration(ts) for ts in s.tasks.values()) == 3600
    assert len(s.unknown_durations) == 1
    await wait(future)
    assert len(s.unknown_durations) == 0


@gen_cluster()
async def test_unknown_task_duration_config_2(s, a, b):
    assert s.idle_since == s.time_started


@gen_cluster(client=True)
async def test_retire_state_change(c, s, a, b):
    np = pytest.importorskip("numpy")
    y = c.map(lambda x: x ** 2, range(10))
    await c.scatter(y)
    coros = []
    for x in range(2):
        v = c.map(lambda i: i * np.random.randint(1000), y)
        k = c.map(lambda i: i * np.random.randint(1000), v)
        foo = c.map(lambda j: j * 6, k)
        step = c.compute(foo)
        coros.append(c.gather(step))
    await c.retire_workers(workers=[a.address])
    await asyncio.gather(*coros)


@gen_cluster(client=True, config={"distributed.scheduler.events-log-length": 3})
async def test_configurable_events_log_length(c, s, a, b):
    s.log_event("test", "dummy message 1")
    assert len(s.events["test"]) == 1
    s.log_event("test", "dummy message 2")
    s.log_event("test", "dummy message 3")
    assert len(s.events["test"]) == 3

    # adding a forth message will drop the first one and length stays at 3
    s.log_event("test", "dummy message 4")
    assert len(s.events["test"]) == 3
    assert s.events["test"][0][1] == "dummy message 2"
    assert s.events["test"][1][1] == "dummy message 3"
    assert s.events["test"][2][1] == "dummy message 4"


@gen_cluster()
async def test_get_worker_monitor_info(s, a, b):
    res = await s.get_worker_monitor_info()
    ms = ["cpu", "time", "read_bytes", "write_bytes"]
    if not WINDOWS:
        ms += ["num_fds"]
    for w in (a, b):
        assert all(res[w.address]["range_query"][m] is not None for m in ms)
        assert res[w.address]["count"] is not None
        assert res[w.address]["last_time"] is not None


@gen_cluster(client=True)
async def test_quiet_cluster_round_robin(c, s, a, b):
    await c.submit(inc, 1)
    await c.submit(inc, 2)
    await c.submit(inc, 3)
    assert a.log and b.log


def test_memorystate():
    m = MemoryState(process=100, unmanaged_old=15, managed=80, managed_spilled=12)
    assert m.process == 100
    assert m.managed == 80
    assert m.managed_in_memory == 68
    assert m.managed_spilled == 12
    assert m.unmanaged == 32
    assert m.unmanaged_old == 15
    assert m.unmanaged_recent == 17
    assert m.optimistic == 83

    assert (
        repr(m)
        == dedent(
            """
            Process memory (RSS)  : 100 B
              - managed by Dask   : 68 B
              - unmanaged (old)   : 15 B
              - unmanaged (recent): 17 B
            Spilled to disk       : 12 B
            """
        ).lstrip()
    )


def test_memorystate_sum():
    m1 = MemoryState(process=100, unmanaged_old=15, managed=80, managed_spilled=12)
    m2 = MemoryState(process=80, unmanaged_old=10, managed=60, managed_spilled=2)
    m3 = MemoryState.sum(m1, m2)
    assert m3.process == 180
    assert m3.unmanaged_old == 25
    assert m3.managed == 140
    assert m3.managed_spilled == 14


@pytest.mark.parametrize(
    "process,unmanaged_old,managed,managed_spilled", list(product(*[[0, 1, 2, 3]] * 4))
)
def test_memorystate_adds_up(process, unmanaged_old, managed, managed_spilled):
    """Input data is massaged by __init__ so that everything adds up by construction"""
    m = MemoryState(
        process=process,
        unmanaged_old=unmanaged_old,
        managed=managed,
        managed_spilled=managed_spilled,
    )
    assert m.managed_in_memory + m.unmanaged == m.process
    assert m.managed_in_memory + m.managed_spilled == m.managed
    assert m.unmanaged_old + m.unmanaged_recent == m.unmanaged
    assert m.optimistic + m.unmanaged_recent == m.process


def leaking(out_mib, leak_mib, sleep_time):
    if leak_mib:
        global __test_leak
        __test_leak = "x" * (leak_mib * 2 ** 20)
    out = "x" * (out_mib * 2 ** 20)
    sleep(sleep_time)
    return out


def clear_leak():
    global __test_leak
    del __test_leak
    gc.collect()


async def assert_memory(scheduler_or_workerstate, attr: str, min_, max_, timeout=10):
    t0 = time()
    while True:
        minfo = scheduler_or_workerstate.memory
        nmib = getattr(minfo, attr) / 2 ** 20
        if min_ <= nmib <= max_:
            return
        if time() - t0 > timeout:
            raise TimeoutError(
                f"Expected {min_} MiB <= {attr} <= {max_} MiB; got:\n{minfo!r}"
            )
        await asyncio.sleep(0.1)


# ~31s runtime, or distributed.worker.memory.recent-to-old-time + 1s.
# On Windows, it can take ~65s due to worker memory needing to stabilize first.
@pytest.mark.slow
@pytest.mark.flaky(condition=LINUX, reason="see comments", reruns=10, reruns_delay=5)
@gen_cluster(
    client=True, Worker=Nanny, worker_kwargs={"memory_limit": "500 MiB"}, timeout=120
)
async def test_memory(c, s, *_):
    # WorkerState objects, as opposed to the Nanny objects passed by gen_cluster
    a, b = s.workers.values()

    s_m0 = s.memory
    assert s_m0.process == a.memory.process + b.memory.process
    assert s_m0.managed == 0
    assert a.memory.managed == 0
    assert b.memory.managed == 0

    # When a worker first goes online, its RAM is immediately counted as unmanaged_old.
    # On Windows, however, there is somehow enough time between the worker start and
    # this line for 2 heartbeats and the memory keeps growing substantially for a while.
    # Sometimes there is a single heartbeat but on the consecutive test we observe
    # a large unexplained increase in unmanaged_recent memory.
    # Wait for the situation to stabilize.
    if WINDOWS:
        await asyncio.sleep(10)
        initial_timeout = 40
    else:
        initial_timeout = 0

    await assert_memory(s, "unmanaged_recent", 0, 40, timeout=initial_timeout)
    await assert_memory(a, "unmanaged_recent", 0, 20, timeout=initial_timeout)
    await assert_memory(b, "unmanaged_recent", 0, 20, timeout=initial_timeout)

    f1 = c.submit(leaking, 100, 50, 10, pure=False, workers=[a.name])
    f2 = c.submit(leaking, 100, 50, 10, pure=False, workers=[b.name])
    await assert_memory(s, "unmanaged_recent", 300, 380)
    await assert_memory(a, "unmanaged_recent", 150, 190)
    await assert_memory(b, "unmanaged_recent", 150, 190)
    await wait([f1, f2])

    # On each worker, we now have 100 MiB managed + 50 MiB fresh leak
    await assert_memory(s, "managed_in_memory", 200, 201)
    await assert_memory(a, "managed_in_memory", 100, 101)
    await assert_memory(b, "managed_in_memory", 100, 101)
    await assert_memory(s, "unmanaged_recent", 100, 180)
    await assert_memory(a, "unmanaged_recent", 50, 90)
    await assert_memory(b, "unmanaged_recent", 50, 90)

    # Force the output of f1 and f2 to spill to disk.
    # With spill=0.7 and memory_limit=500 MiB, we'll start spilling at 350 MiB process
    # memory per worker, or up to 20 iterations of the below depending on how much RAM
    # the interpreter is using.
    more_futs = []
    while not s.memory.managed_spilled:
        if a.memory.process < 0.7 * 500 * 2 ** 20:
            more_futs.append(c.submit(leaking, 10, 0, 0, pure=False, workers=[a.name]))
        if b.memory.process < 0.7 * 500 * 2 ** 20:
            more_futs.append(c.submit(leaking, 10, 0, 0, pure=False, workers=[b.name]))
        await wait(more_futs)
        await asyncio.sleep(1)

    # Wait for the spilling to finish. Note that this does not make the test take
    # longer as we're waiting for recent-to-old-time anyway.
    await asyncio.sleep(10)

    # Delete spilled keys
    prev = s.memory
    del f1
    del f2
    await assert_memory(s, "managed_spilled", 0, prev.managed_spilled / 2 ** 20 - 19)

    # Empty the cluster, with the exception of leaked memory
    del more_futs
    await assert_memory(s, "managed", 0, 0)

    orig_unmanaged = s_m0.unmanaged / 2 ** 20
    orig_old = s_m0.unmanaged_old / 2 ** 20

    # Wait until 30s have passed since the spill to observe unmanaged_recent
    # transition into unmanaged_old
    await c.run(gc.collect)
    await assert_memory(s, "unmanaged_recent", 0, 90, timeout=40)
    await assert_memory(s, "unmanaged_old", orig_old + 90, 9999, timeout=40)

    # When the leaked memory is cleared, unmanaged and unmanaged_old drop.
    # On MacOS and Windows, the process memory of the Python interpreter does not shrink
    # as fast as on Linux. Note that this behaviour is heavily impacted by OS tweaks,
    # meaning that what you observe on your local host may behave differently on CI.
    # Even on Linux, this occasionally glitches - hence why there is a flaky marker on
    # this test.
    if not LINUX:
        return

    orig_unmanaged = s.memory.unmanaged / 2 ** 20
    orig_old = s.memory.unmanaged_old / 2 ** 20
    await c.run(clear_leak)
    await assert_memory(s, "unmanaged", 0, orig_unmanaged - 60)
    await assert_memory(s, "unmanaged_old", 0, orig_old - 60)
    await assert_memory(s, "unmanaged_recent", 0, 90)


@gen_cluster(client=True, worker_kwargs={"memory_limit": 0})
async def test_memory_no_zict(c, s, a, b):
    """When Worker.data is not a SpillBuffer, test that querying managed_spilled
    defaults to 0 and doesn't raise KeyError
    """
    await c.wait_for_workers(2)
    assert isinstance(a.data, dict)
    assert isinstance(b.data, dict)
    f = c.submit(leaking, 10, 0, 0)
    await f
    assert 10 * 2 ** 20 < s.memory.managed_in_memory < 11 * 2 ** 20
    assert s.memory.managed_spilled == 0


@gen_cluster(nthreads=[])
async def test_memory_no_workers(s):
    assert s.memory.process == 0
    assert s.memory.managed == 0


@gen_cluster(client=True, nthreads=[])
async def test_memory_is_none(c, s):
    """If Worker.heartbeat() runs before Worker.monitor.update(), then
    Worker.metrics["memory"] will be None and will need special handling in
    Worker.memory and Scheduler.heartbeat_worker().
    """
    with mock.patch("distributed.system_monitor.SystemMonitor.update"):
        async with Worker(s.address, nthreads=1) as w:
            await c.wait_for_workers(1)
            f = await c.scatter(123)
            await w.heartbeat()
            assert s.memory.process == 0  # Forced from None
            assert s.memory.managed == 0  # Capped by process even if we do have keys
            assert s.memory.managed_in_memory == 0
            assert s.memory.managed_spilled == 0
            assert s.memory.unmanaged == 0
            assert s.memory.unmanaged_old == 0
            assert s.memory.unmanaged_recent == 0


@gen_cluster()
async def test_close_scheduler__close_workers_Worker(s, a, b):
    with captured_logger("distributed.comm", level=logging.DEBUG) as log:
        await s.close(close_workers=True)
        while not a.status == Status.closed:
            await asyncio.sleep(0.05)
    log = log.getvalue()
    assert "retry" not in log


@gen_cluster(Worker=Nanny)
async def test_close_scheduler__close_workers_Nanny(s, a, b):
    with captured_logger("distributed.comm", level=logging.DEBUG) as log:
        await s.close(close_workers=True)
        while not a.status == Status.closed:
            await asyncio.sleep(0.05)
    log = log.getvalue()
    assert "retry" not in log


async def assert_ndata(client, by_addr, total=None):
    """Test that the number of elements in Worker.data is as expected.
    To be used when the worker is wrapped by a nanny.

    by_addr: dict of either exact numbers or (min, max) tuples
    total: optional exact match on the total number of keys (with duplicates) across all
    workers
    """
    out = await client.run(lambda dask_worker: len(dask_worker.data))
    try:
        for k, v in by_addr.items():
            if isinstance(v, tuple):
                assert v[0] <= out[k] <= v[1]
            else:
                assert out[k] == v
        if total is not None:
            assert sum(out.values()) == total
    except AssertionError:
        raise AssertionError(f"Expected {by_addr}, total={total}; got {out}")


@gen_cluster(
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1 GiB"},
    config={"distributed.worker.memory.rebalance.sender-min": 0.3},
)
async def test_rebalance(c, s, *_):
    # We used nannies to have separate processes for each worker
    a, b = s.workers

    # Generate 10 buffers worth 512 MiB total on worker a. This sends its memory
    # utilisation slightly above 50% (after counting unmanaged) which is above the
    # distributed.worker.memory.rebalance.sender-min threshold.
    futures = c.map(lambda _: "x" * (2 ** 29 // 10), range(10), workers=[a])
    await wait(futures)
    # Wait for heartbeats
    await assert_memory(s, "process", 512, 1024)
    await assert_ndata(c, {a: 10, b: 0})
    await s.rebalance()
    # Allow for some uncertainty as the unmanaged memory is not stable
    await assert_ndata(c, {a: (3, 7), b: (3, 7)}, total=10)

    # rebalance() when there is nothing to do
    await s.rebalance()
    await assert_ndata(c, {a: (3, 7), b: (3, 7)}, total=10)


@gen_cluster(
    nthreads=[("127.0.0.1", 1)] * 3,
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1 GiB"},
)
async def test_rebalance_workers_and_keys(client, s, *_):
    a, b, c = s.workers
    futures = client.map(lambda _: "x" * (2 ** 29 // 10), range(10), workers=[a])
    await wait(futures)
    # Wait for heartbeats
    await assert_memory(s, "process", 512, 1024)

    # Passing empty iterables is not the same as omitting the arguments
    await s.rebalance(keys=[])
    await assert_ndata(client, {a: 10, b: 0, c: 0})
    await s.rebalance(workers=[])
    await assert_ndata(client, {a: 10, b: 0, c: 0})
    # Limit operation to workers that have nothing to do
    await s.rebalance(workers=[b, c])
    await assert_ndata(client, {a: 10, b: 0, c: 0})

    # Limit rebalancing to two arbitrary keys and two arbitrary workers
    await s.rebalance(keys=[futures[3].key, futures[7].key], workers=[a, b])
    await assert_ndata(client, {a: 8, b: 2, c: 0}, total=10)

    with pytest.raises(KeyError):
        await s.rebalance(workers=["notexist"])


@gen_cluster()
async def test_rebalance_missing_data1(s, a, b):
    """key never existed"""
    out = await s.rebalance(keys=["notexist"])
    assert out == {"status": "partial-fail", "keys": ["notexist"]}


@gen_cluster(client=True)
async def test_rebalance_missing_data2(c, s, a, b):
    """keys exist but belong to unfinished futures. Unlike Client.rebalance(),
    Scheduler.rebalance() does not wait for unfinished futures.
    """
    futures = c.map(slowinc, range(10), delay=0.05, workers=a.address)
    await asyncio.sleep(0.1)
    out = await s.rebalance(keys=[f.key for f in futures])
    assert out["status"] == "partial-fail"
    assert 8 <= len(out["keys"]) <= 10


@pytest.mark.parametrize("explicit", [False, True])
@gen_cluster(client=True, Worker=Nanny, worker_kwargs={"memory_limit": "1 GiB"})
async def test_rebalance_raises_missing_data3(c, s, *_, explicit):
    """keys exist when the sync part of rebalance runs, but are gone by the time the
    actual data movement runs.
    There is an error message only if the keys are explicitly listed in the API call.
    """
    a, _ = s.workers
    futures = c.map(lambda _: "x" * (2 ** 29 // 10), range(10), workers=[a])
    await wait(futures)
    # Wait for heartbeats
    await assert_memory(s, "process", 512, 1024)

    if explicit:
        keys = [f.key for f in futures]
        del futures
        out = await s.rebalance(keys=keys)
        assert out["status"] == "partial-fail"
        assert 1 <= len(out["keys"]) <= 10
    else:
        del futures
        out = await s.rebalance()
        assert out == {"status": "OK"}


@gen_cluster(nthreads=[])
async def test_rebalance_no_workers(s):
    await s.rebalance()


@gen_cluster(
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1000 MiB"},
    config={
        "distributed.worker.memory.rebalance.measure": "managed",
        "distributed.worker.memory.rebalance.sender-min": 0.3,
    },
)
async def test_rebalance_managed_memory(c, s, *_):
    a, b = s.workers
    # Generate 100 buffers worth 400 MiB total on worker a. This sends its memory
    # utilisation to exactly 40%, ignoring unmanaged, which is above the
    # distributed.worker.memory.rebalance.sender-min threshold.
    futures = c.map(lambda _: "x" * (2 ** 22), range(100), workers=[a])
    await wait(futures)
    # Even if we're just using managed memory, which is instantaneously accounted for as
    # soon as the tasks finish, MemoryState.managed is still capped by the process
    # memory, so we need to wait for the heartbeat.
    await assert_memory(s, "managed", 400, 401)
    await assert_ndata(c, {a: 100, b: 0})
    await s.rebalance()
    # We can expect an exact, stable result because we are completely bypassing the
    # unpredictability of unmanaged memory.
    await assert_ndata(c, {a: 62, b: 38})


@gen_cluster(
    client=True,
    worker_kwargs={"memory_limit": 0},
    config={"distributed.worker.memory.rebalance.measure": "managed"},
)
async def test_rebalance_no_limit(c, s, a, b):
    # See notes in test_rebalance_managed_memory
    futures = c.map(lambda _: "x", range(100), workers=[a.address])
    await wait(futures)
    # No reason to wait for memory here as we're allocating hundreds of bytes, so
    # there's plenty of unmanaged process memory to pad it out
    await assert_ndata(c, {a.address: 100, b.address: 0})
    await s.rebalance()
    # Disabling memory_limit made us ignore all % thresholds set in the config
    await assert_ndata(c, {a.address: 50, b.address: 50})


@gen_cluster(
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1000 MiB"},
    config={
        "distributed.worker.memory.rebalance.measure": "managed",
        "distributed.worker.memory.rebalance.sender-min": 0.2,
        "distributed.worker.memory.rebalance.recipient-max": 0.1,
    },
)
async def test_rebalance_no_recipients(c, s, *_):
    """There are sender workers, but no recipient workers"""
    a, b = s.workers
    fut_a = c.map(lambda _: "x" * (2 ** 20), range(250), workers=[a])  # 25%
    fut_b = c.map(lambda _: "x" * (2 ** 20), range(100), workers=[b])  # 10%
    await wait(fut_a + fut_b)
    await assert_memory(s, "managed", 350, 351)
    await assert_ndata(c, {a: 250, b: 100})
    await s.rebalance()
    await assert_ndata(c, {a: 250, b: 100})


@gen_cluster(
    nthreads=[("127.0.0.1", 1)] * 3,
    client=True,
    worker_kwargs={"memory_limit": 0},
    config={"distributed.worker.memory.rebalance.measure": "managed"},
)
async def test_rebalance_skip_recipient(client, s, a, b, c):
    """A recipient is skipped because it already holds a copy of the key to be sent"""
    futures = client.map(lambda _: "x", range(10), workers=[a.address])
    await wait(futures)
    await client.replicate(futures[0:2], workers=[a.address, b.address])
    await client.replicate(futures[2:4], workers=[a.address, c.address])
    await assert_ndata(client, {a.address: 10, b.address: 2, c.address: 2})
    await client.rebalance(futures[:2])
    await assert_ndata(client, {a.address: 8, b.address: 2, c.address: 4})


@gen_cluster(
    client=True,
    worker_kwargs={"memory_limit": 0},
    config={"distributed.worker.memory.rebalance.measure": "managed"},
)
async def test_rebalance_skip_all_recipients(c, s, a, b):
    """All recipients are skipped because they already hold copies"""
    futures = c.map(lambda _: "x", range(10), workers=[a.address])
    await wait(futures)
    await c.replicate([futures[0]])
    await assert_ndata(c, {a.address: 10, b.address: 1})
    await c.rebalance(futures[:2])
    await assert_ndata(c, {a.address: 9, b.address: 2})


@gen_cluster(
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1000 MiB"},
    config={"distributed.worker.memory.rebalance.measure": "managed"},
)
async def test_rebalance_sender_below_mean(c, s, *_):
    """A task remains on the sender because moving it would send it below the mean"""
    a, b = s.workers
    f1 = c.submit(lambda: "x" * (400 * 2 ** 20), workers=[a])
    await wait([f1])
    f2 = c.submit(lambda: "x" * (10 * 2 ** 20), workers=[a])
    await wait([f2])
    await assert_memory(s, "managed", 410, 411)
    await assert_ndata(c, {a: 2, b: 0})
    await s.rebalance()
    assert await c.has_what() == {a: (f1.key,), b: (f2.key,)}


@gen_cluster(
    client=True,
    Worker=Nanny,
    worker_kwargs={"memory_limit": "1000 MiB"},
    config={
        "distributed.worker.memory.rebalance.measure": "managed",
        "distributed.worker.memory.rebalance.sender-min": 0.3,
    },
)
async def test_rebalance_least_recently_inserted_sender_min(c, s, *_):
    """
    1. keys are picked using a least recently inserted policy
    2. workers below sender-min are never senders
    """
    a, b = s.workers
    small_futures = c.map(lambda _: "x", range(10), workers=[a])
    await wait(small_futures)
    await assert_ndata(c, {a: 10, b: 0})
    await s.rebalance()
    await assert_ndata(c, {a: 10, b: 0})

    large_future = c.submit(lambda: "x" * (300 * 2 ** 20), workers=[a])
    await wait([large_future])
    await assert_memory(s, "managed", 300, 301)
    await assert_ndata(c, {a: 11, b: 0})
    await s.rebalance()
    await assert_ndata(c, {a: 1, b: 10})
    has_what = await c.has_what()
    assert has_what[a] == (large_future.key,)
    assert sorted(has_what[b]) == sorted(f.key for f in small_futures)


@gen_cluster(client=True)
async def test_gather_on_worker(c, s, a, b):
    x = await c.scatter("x", workers=[a.address])
    x_ts = s.tasks[x.key]
    a_ws = s.workers[a.address]
    b_ws = s.workers[b.address]

    assert a_ws.nbytes > 0
    assert b_ws.nbytes == 0
    assert x_ts in a_ws.has_what
    assert x_ts not in b_ws.has_what
    assert x_ts.who_has == {a_ws}

    out = await s.gather_on_worker(b.address, {x.key: [a.address]})
    assert out == set()
    assert a.data[x.key] == "x"
    assert b.data[x.key] == "x"

    assert b_ws.nbytes == a_ws.nbytes
    assert x_ts in b_ws.has_what
    assert x_ts.who_has == {a_ws, b_ws}


@gen_cluster(client=True, scheduler_kwargs={"timeout": "100ms"})
async def test_gather_on_worker_bad_recipient(c, s, a, b):
    """The recipient is missing"""
    x = await c.scatter("x")
    await b.close()
    assert s.workers.keys() == {a.address}
    out = await s.gather_on_worker(b.address, {x.key: [a.address]})
    assert out == {x.key}


@gen_cluster(client=True, worker_kwargs={"timeout": "100ms"})
async def test_gather_on_worker_bad_sender(c, s, a, b):
    """The only sender for a key is missing"""
    out = await s.gather_on_worker(a.address, {"x": ["tcp://127.0.0.1:12345"]})
    assert out == {"x"}


@pytest.mark.parametrize("missing_first", [False, True])
@gen_cluster(client=True, worker_kwargs={"timeout": "100ms"})
async def test_gather_on_worker_bad_sender_replicated(c, s, a, b, missing_first):
    """One of the senders for a key is missing, but the key is available somewhere else"""
    x = await c.scatter("x", workers=[a.address])
    bad_addr = "tcp://127.0.0.1:12345"
    # Order matters; test both
    addrs = [bad_addr, a.address] if missing_first else [a.address, bad_addr]
    out = await s.gather_on_worker(b.address, {x.key: addrs})
    assert out == set()
    assert a.data[x.key] == "x"
    assert b.data[x.key] == "x"


@gen_cluster(client=True)
async def test_gather_on_worker_key_not_on_sender(c, s, a, b):
    """The only sender for a key does not actually hold it"""
    out = await s.gather_on_worker(a.address, {"x": [b.address]})
    assert out == {"x"}


@pytest.mark.parametrize("missing_first", [False, True])
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_gather_on_worker_key_not_on_sender_replicated(
    client, s, a, b, c, missing_first
):
    """One of the senders for a key does not actually hold it, but the key is available
    somewhere else
    """
    x = await client.scatter("x", workers=[a.address])
    # Order matters; test both
    addrs = [b.address, a.address] if missing_first else [a.address, b.address]
    out = await s.gather_on_worker(c.address, {x.key: addrs})
    assert out == set()
    assert a.data[x.key] == "x"
    assert c.data[x.key] == "x"


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_gather_on_worker_duplicate_task(client, s, a, b, c):
    """Race condition where the recipient worker receives the same task twice.
    Test that the task nbytes are not double-counted on the recipient.
    """
    x = await client.scatter("x", workers=[a.address, b.address], broadcast=True)
    assert a.data[x.key] == "x"
    assert b.data[x.key] == "x"
    assert x.key not in c.data

    out = await asyncio.gather(
        s.gather_on_worker(c.address, {x.key: [a.address]}),
        s.gather_on_worker(c.address, {x.key: [b.address]}),
    )
    assert out == [set(), set()]
    assert c.data[x.key] == "x"

    a_ws = s.workers[a.address]
    b_ws = s.workers[b.address]
    c_ws = s.workers[c.address]
    assert a_ws.nbytes > 0
    assert c_ws.nbytes == b_ws.nbytes == a_ws.nbytes


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)] * 3, scheduler_kwargs={"timeout": "100ms"}
)
async def test_rebalance_dead_recipient(client, s, a, b, c):
    """A key fails to be rebalanced due to recipient failure.
    The key is not deleted from the sender.
    Unrelated, successful keys are deleted from the senders.
    """
    x, y = await client.scatter(["x", "y"], workers=[a.address])
    a_ws = s.workers[a.address]
    b_ws = s.workers[b.address]
    c_ws = s.workers[c.address]
    x_ts = s.tasks[x.key]
    y_ts = s.tasks[y.key]
    await c.close()
    assert s.workers.keys() == {a.address, b.address}

    out = await s._rebalance_move_data([(a_ws, b_ws, x_ts), (a_ws, c_ws, y_ts)])
    assert out == {"status": "partial-fail", "keys": [y.key]}
    assert a.data == {y.key: "y"}
    assert b.data == {x.key: "x"}
    assert await client.has_what() == {a.address: (y.key,), b.address: (x.key,)}


@gen_cluster(client=True)
async def test_delete_worker_data(c, s, a, b):
    # delete only copy of x
    # delete one of the copies of y
    # don't touch z
    x, y, z = await c.scatter(["x", "y", "z"], workers=[a.address])
    await c.replicate(y)

    assert a.data == {x.key: "x", y.key: "y", z.key: "z"}
    assert b.data == {y.key: "y"}
    assert s.tasks.keys() == {x.key, y.key, z.key}

    await s.delete_worker_data(a.address, [x.key, y.key])
    assert a.data == {z.key: "z"}
    assert b.data == {y.key: "y"}
    assert s.tasks.keys() == {y.key, z.key}
    assert s.workers[a.address].nbytes == s.tasks[z.key].nbytes


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_delete_worker_data_double_delete(c, s, a):
    """_delete_worker_data race condition where the same key is deleted twice.
    WorkerState.nbytes is not double-decreased.
    """
    x, y = await c.scatter(["x", "y"])
    await asyncio.gather(
        s.delete_worker_data(a.address, [x.key]),
        s.delete_worker_data(a.address, [x.key]),
    )
    assert a.data == {y.key: "y"}
    a_ws = s.workers[a.address]
    y_ts = s.tasks[y.key]
    assert a_ws.nbytes == y_ts.nbytes


@gen_cluster(scheduler_kwargs={"timeout": "100ms"})
async def test_delete_worker_data_bad_worker(s, a, b):
    """_delete_worker_data gracefully handles a non-existing worker;
    e.g. a sender died in the middle of rebalance()
    """
    await a.close()
    assert s.workers.keys() == {b.address}
    await s.delete_worker_data(a.address, ["x"])


@pytest.mark.parametrize("bad_first", [False, True])
@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_delete_worker_data_bad_task(c, s, a, bad_first):
    """_delete_worker_data gracefully handles a non-existing key;
    e.g. a task was stolen by work stealing in the middle of a rebalance().
    Other tasks on the same worker are deleted.
    """
    x, y = await c.scatter(["x", "y"])
    assert a.data == {x.key: "x", y.key: "y"}
    assert s.tasks.keys() == {x.key, y.key}

    keys = ["notexist", x.key] if bad_first else [x.key, "notexist"]
    await s.delete_worker_data(a.address, keys)
    assert a.data == {y.key: "y"}
    assert s.tasks.keys() == {y.key}
    assert s.workers[a.address].nbytes == s.tasks[y.key].nbytes


@gen_cluster(client=True)
async def test_computations(c, s, a, b):
    da = pytest.importorskip("dask.array")

    x = da.ones(100, chunks=(10,))
    y = (x + 1).persist()
    await y

    z = (x - 2).persist()
    await z

    assert len(s.computations) == 2
    assert "add" in str(s.computations[0].groups)
    assert "sub" in str(s.computations[1].groups)
    assert "sub" not in str(s.computations[0].groups)

    assert isinstance(repr(s.computations[1]), str)

    assert s.computations[1].stop == max(tg.stop for tg in s.task_groups.values())

    assert s.computations[0].states["memory"] == y.npartitions


@gen_cluster(client=True)
async def test_computations_futures(c, s, a, b):
    futures = [c.submit(inc, i) for i in range(10)]
    total = c.submit(sum, futures)
    await total

    [computation] = s.computations
    assert "sum" in str(computation.groups)
    assert "inc" in str(computation.groups)


@gen_cluster(client=True)
async def test_transition_counter(c, s, a, b):
    assert s.transition_counter == 0
    await c.submit(inc, 1)
    assert s.transition_counter > 1


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1) for _ in range(10)],
)
async def test_worker_heartbeat_after_cancel(c, s, *workers):
    """This test is intended to ensure that after cancellation of a graph, the
    worker heartbeat is always successful. The heartbeat may not be successful if
    the worker and scheduler state drift and the scheduler doesn't handle
    unknown information gracefully. One example would be a released/cancelled
    computation where the worker returns metrics about duration, type, etc. and
    the scheduler doesn't handle the forgotten task gracefully.

    See also https://github.com/dask/distributed/issues/4587
    """
    for w in workers:
        w.periodic_callbacks["heartbeat"].stop()

    futs = c.map(slowinc, range(100), delay=0.1)

    while sum(w.executing_count for w in workers) < len(workers):
        await asyncio.sleep(0.001)

    await c.cancel(futs)

    while any(w.tasks for w in workers):
        await asyncio.gather(*(w.heartbeat() for w in workers))


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_worker_reconnect_task_memory_with_resources(c, s, a):
    async with Worker(s.address, resources={"A": 1}) as b:
        b.periodic_callbacks["heartbeat"].stop()

        futs = c.map(inc, range(10), resources={"A": 1})
        res = c.submit(sum, futs)

        while not b.executing_count and not b.data:
            await asyncio.sleep(0.001)

        await s.remove_worker(address=b.address, close=False)
        while not res.done():
            await b.heartbeat()

        await res
        assert ("no-worker", "memory") in {
            (start, finish) for (_, start, finish, _, _) in s.transition_log
        }


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_set_restrictions(c, s, a, b):

    f = c.submit(inc, 1, workers=[b.address])
    await f
    s.set_restrictions(worker={f.key: a.address})
    assert s.tasks[f.key].worker_restrictions == {a.address}
    s.reschedule(f)
    await f
