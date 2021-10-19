import asyncio
import importlib
import logging
import os
import sys
import threading
import traceback
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from numbers import Number
from operator import add
from time import sleep
from unittest import mock

import psutil
import pytest
from tlz import first, pluck, sliding_window

import dask
from dask import delayed
from dask.system import CPU_COUNT
from dask.utils import tmpfile

import distributed
from distributed import (
    Client,
    Nanny,
    Reschedule,
    default_client,
    get_client,
    get_worker,
    wait,
)
from distributed.comm.registry import backends
from distributed.comm.tcp import TCPBackend
from distributed.compatibility import LINUX, WINDOWS
from distributed.core import CommClosedError, Status, rpc
from distributed.diagnostics import nvml
from distributed.diagnostics.plugin import PipInstall
from distributed.metrics import time
from distributed.scheduler import Scheduler
from distributed.utils import TimeoutError
from distributed.utils_test import (
    TaskStateMetadataPlugin,
    _LockedCommPool,
    captured_logger,
    dec,
    div,
    gen_cluster,
    gen_test,
    inc,
    mul,
    nodebug,
    slowinc,
    slowsum,
)
from distributed.worker import Worker, error_message, logger, parse_memory_limit

pytestmark = pytest.mark.ci1


@gen_cluster(nthreads=[])
async def test_worker_nthreads(s):
    async with Worker(s.address) as w:
        assert w.executor._max_workers == CPU_COUNT


@gen_cluster()
async def test_str(s, a, b):
    assert a.address in str(a)
    assert a.address in repr(a)
    assert str(a.nthreads) in str(a)
    assert str(a.nthreads) in repr(a)
    assert str(a.executing_count) in repr(a)


@gen_cluster(nthreads=[])
async def test_identity(s):
    async with Worker(s.address) as w:
        ident = w.identity(None)
        assert "Worker" in ident["type"]
        assert ident["scheduler"] == s.address
        assert isinstance(ident["nthreads"], int)
        assert isinstance(ident["memory_limit"], Number)


@gen_cluster(client=True)
async def test_worker_bad_args(c, s, a, b):
    class NoReprObj:
        """This object cannot be properly represented as a string."""

        def __str__(self):
            raise ValueError("I have no str representation.")

        def __repr__(self):
            raise ValueError("I have no repr representation.")

    x = c.submit(NoReprObj, workers=a.address)
    await wait(x)
    assert not a.executing_count
    assert a.data

    def bad_func(*args, **kwargs):
        1 / 0

    class MockLoggingHandler(logging.Handler):
        """Mock logging handler to check for expected logs."""

        def __init__(self, *args, **kwargs):
            self.reset()
            super().__init__(*args, **kwargs)

        def emit(self, record):
            self.messages[record.levelname.lower()].append(record.getMessage())

        def reset(self):
            self.messages = {
                "debug": [],
                "info": [],
                "warning": [],
                "error": [],
                "critical": [],
            }

    hdlr = MockLoggingHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(hdlr)
    y = c.submit(bad_func, x, k=x, workers=b.address)
    await wait(y)

    assert not b.executing_count
    assert y.status == "error"
    # Make sure job died because of bad func and not because of bad
    # argument.
    with pytest.raises(ZeroDivisionError):
        await y

    tb = await y._traceback()
    assert any("1 / 0" in line for line in pluck(3, traceback.extract_tb(tb)) if line)
    assert "Compute Failed" in hdlr.messages["warning"][0]
    logger.setLevel(old_level)

    # Now we check that both workers are still alive.

    xx = c.submit(add, 1, 2, workers=a.address)
    yy = c.submit(add, 3, 4, workers=b.address)

    results = await c._gather([xx, yy])

    assert tuple(results) == (3, 7)


@gen_cluster(client=True)
async def test_upload_file(c, s, a, b):
    assert not os.path.exists(os.path.join(a.local_directory, "foobar.py"))
    assert not os.path.exists(os.path.join(b.local_directory, "foobar.py"))
    assert a.local_directory != b.local_directory

    with rpc(a.address) as aa, rpc(b.address) as bb:
        await asyncio.gather(
            aa.upload_file(filename="foobar.py", data=b"x = 123"),
            bb.upload_file(filename="foobar.py", data="x = 123"),
        )

    assert os.path.exists(os.path.join(a.local_directory, "foobar.py"))
    assert os.path.exists(os.path.join(b.local_directory, "foobar.py"))

    def g():
        import foobar

        return foobar.x

    future = c.submit(g, workers=a.address)
    result = await future
    assert result == 123

    await c.close()
    await s.close(close_workers=True)
    assert not os.path.exists(os.path.join(a.local_directory, "foobar.py"))


@pytest.mark.skip(reason="don't yet support uploading pyc files")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_upload_file_pyc(c, s, w):
    with tmpfile() as dirname:
        os.mkdir(dirname)
        with open(os.path.join(dirname, "foo.py"), mode="w") as f:
            f.write("def f():\n    return 123")

        sys.path.append(dirname)
        try:
            import foo

            assert foo.f() == 123
            pyc = importlib.util.cache_from_source(os.path.join(dirname, "foo.py"))
            assert os.path.exists(pyc)
            await c.upload_file(pyc)

            def g():
                import foo

                return foo.x

            future = c.submit(g)
            result = await future
            assert result == 123
        finally:
            sys.path.remove(dirname)


@gen_cluster(client=True)
async def test_upload_egg(c, s, a, b):
    eggname = "testegg-1.0.0-py3.4.egg"
    local_file = __file__.replace("test_worker.py", eggname)
    assert not os.path.exists(os.path.join(a.local_directory, eggname))
    assert not os.path.exists(os.path.join(b.local_directory, eggname))
    assert a.local_directory != b.local_directory

    await c.upload_file(filename=local_file)

    assert os.path.exists(os.path.join(a.local_directory, eggname))
    assert os.path.exists(os.path.join(b.local_directory, eggname))

    def g(x):
        import testegg

        return testegg.inc(x)

    future = c.submit(g, 10, workers=a.address)
    result = await future
    assert result == 10 + 1

    await c.close()
    await s.close()
    await a.close()
    await b.close()
    assert not os.path.exists(os.path.join(a.local_directory, eggname))


@gen_cluster(client=True)
async def test_upload_pyz(c, s, a, b):
    pyzname = "mytest.pyz"
    local_file = __file__.replace("test_worker.py", pyzname)
    assert not os.path.exists(os.path.join(a.local_directory, pyzname))
    assert not os.path.exists(os.path.join(b.local_directory, pyzname))
    assert a.local_directory != b.local_directory

    await c.upload_file(filename=local_file)

    assert os.path.exists(os.path.join(a.local_directory, pyzname))
    assert os.path.exists(os.path.join(b.local_directory, pyzname))

    def g(x):
        from mytest import mytest

        return mytest.inc(x)

    future = c.submit(g, 10, workers=a.address)
    result = await future
    assert result == 10 + 1

    await c.close()
    await s.close()
    await a.close()
    await b.close()
    assert not os.path.exists(os.path.join(a.local_directory, pyzname))


@pytest.mark.xfail(reason="Still lose time to network I/O")
@gen_cluster(client=True)
async def test_upload_large_file(c, s, a, b):
    pytest.importorskip("crick")
    await asyncio.sleep(0.05)
    with rpc(a.address) as aa:
        await aa.upload_file(filename="myfile.dat", data=b"0" * 100000000)
        await asyncio.sleep(0.05)
        assert a.digests["tick-duration"].components[0].max() < 0.050


@gen_cluster()
async def test_broadcast(s, a, b):
    with rpc(s.address) as cc:
        results = await cc.broadcast(msg={"op": "ping"})
        assert results == {a.address: b"pong", b.address: b"pong"}


@gen_cluster(nthreads=[])
async def test_worker_with_port_zero(s):
    async with Worker(s.address) as w:
        assert isinstance(w.port, int)
        assert w.port > 1024


@gen_cluster(nthreads=[])
async def test_worker_port_range(s):
    port = "9867:9868"
    async with Worker(s.address, port=port) as w1:
        assert w1.port == 9867  # Selects first port in range
        async with Worker(s.address, port=port) as w2:
            assert w2.port == 9868  # Selects next port in range
            with pytest.raises(
                ValueError, match="Could not start Worker"
            ):  # No more ports left
                async with Worker(s.address, port=port):
                    pass


@pytest.mark.slow
@gen_test(timeout=60)
async def test_worker_waits_for_scheduler():
    w = Worker("127.0.0.1:8724")
    try:
        await asyncio.wait_for(w, 3)
    except TimeoutError:
        pass
    else:
        assert False
    assert w.status not in (Status.closed, Status.running, Status.paused)
    await w.close(timeout=0.1)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_worker_task_data(c, s, w):
    x = delayed(2)
    xx = c.persist(x)
    await wait(xx)
    assert w.data[x.key] == 2


def test_error_message():
    class MyException(Exception):
        def __init__(self, a, b):
            self.args = (a + b,)

        def __str__(self):
            return "MyException(%s)" % self.args

    msg = error_message(MyException("Hello", "World!"))
    assert "Hello" in str(msg["exception"])

    max_error_len = 100
    with dask.config.set({"distributed.admin.max-error-length": max_error_len}):
        msg = error_message(RuntimeError("-" * max_error_len))
        assert len(msg["exception_text"]) <= max_error_len + 30
        assert len(msg["exception_text"]) < max_error_len * 2
        msg = error_message(RuntimeError("-" * max_error_len * 20))

    max_error_len = 1000000
    with dask.config.set({"distributed.admin.max-error-length": max_error_len}):
        msg = error_message(RuntimeError("-" * max_error_len * 2))
        assert len(msg["exception_text"]) > 10100  # default + 100


@gen_cluster(client=True)
async def test_chained_error_message(c, s, a, b):
    def chained_exception_fn():
        class MyException(Exception):
            def __init__(self, msg):
                self.msg = msg

            def __str__(self):
                return "MyException(%s)" % self.msg

        exception = MyException("Foo")
        inner_exception = MyException("Bar")

        try:
            raise inner_exception
        except Exception as e:
            raise exception from e

    f = c.submit(chained_exception_fn)
    try:
        await f
    except Exception as e:
        assert e.__cause__ is not None
        assert "Bar" in str(e.__cause__)


@gen_cluster(client=True)
async def test_gather(c, s, a, b):
    x, y = await c.scatter(["x", "y"], workers=[b.address])
    with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], y.key: [b.address]})

    assert resp == {"status": "OK"}
    assert a.data[x.key] == b.data[x.key] == "x"
    assert a.data[y.key] == b.data[y.key] == "y"


@gen_cluster(client=True)
async def test_gather_missing_keys(c, s, a, b):
    """A key is missing. Other keys are gathered successfully."""
    x = await c.scatter("x", workers=[b.address])
    with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], "y": [b.address]})

    assert resp == {"status": "partial-fail", "keys": {"y": (b.address,)}}
    assert a.data[x.key] == b.data[x.key] == "x"


@gen_cluster(client=True, worker_kwargs={"timeout": "100ms"})
async def test_gather_missing_workers(c, s, a, b):
    """A worker owning the only copy of a key is missing.
    Keys from other workers are gathered successfully.
    """
    assert b.address.startswith("tcp://127.0.0.1:")
    bad_addr = "tcp://127.0.0.1:12345"
    x = await c.scatter("x", workers=[b.address])

    with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], "y": [bad_addr]})

    assert resp == {"status": "partial-fail", "keys": {"y": (bad_addr,)}}
    assert a.data[x.key] == b.data[x.key] == "x"


@pytest.mark.parametrize("missing_first", [False, True])
@gen_cluster(client=True, worker_kwargs={"timeout": "100ms"})
async def test_gather_missing_workers_replicated(c, s, a, b, missing_first):
    """A worker owning a redundant copy of a key is missing.
    The key is successfully gathered from other workers.
    """
    assert b.address.startswith("tcp://127.0.0.1:")
    x = await c.scatter("x", workers=[b.address])
    bad_addr = "tcp://127.0.0.1:12345"
    # Order matters! Test both
    addrs = [bad_addr, b.address] if missing_first else [b.address, bad_addr]
    with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: addrs})
    assert resp == {"status": "OK"}
    assert a.data[x.key] == b.data[x.key] == "x"


@gen_cluster(nthreads=[])
async def test_io_loop(s):
    async with Worker(s.address, loop=s.loop) as w:
        assert w.io_loop is s.loop


@gen_cluster(client=True, nthreads=[])
async def test_spill_to_disk(c, s):
    np = pytest.importorskip("numpy")
    w = await Worker(
        s.address,
        loop=s.loop,
        memory_limit=1200 / 0.6,
        memory_pause_fraction=None,
        memory_spill_fraction=None,
    )

    x = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="x")
    await wait(x)
    y = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="y")
    await wait(y)

    assert set(w.data) == {x.key, y.key}
    assert set(w.data.memory) == {x.key, y.key}

    z = c.submit(np.random.randint, 0, 255, size=500, dtype="u1", key="z")
    await wait(z)
    assert set(w.data) == {x.key, y.key, z.key}
    assert set(w.data.memory) == {y.key, z.key}
    assert set(w.data.disk) == {x.key}

    await x
    assert set(w.data.memory) == {x.key, z.key}
    assert set(w.data.disk) == {y.key}
    await w.close()


@gen_cluster(client=True)
async def test_access_key(c, s, a, b):
    def f(i):
        from distributed.worker import thread_state

        return thread_state.key

    futures = [c.submit(f, i, key="x-%d" % i) for i in range(20)]
    results = await c._gather(futures)
    assert list(results) == ["x-%d" % i for i in range(20)]


@gen_cluster(client=True)
async def test_run_dask_worker(c, s, a, b):
    def f(dask_worker=None):
        return dask_worker.id

    response = await c._run(f)
    assert response == {a.address: a.id, b.address: b.id}


@gen_cluster(client=True)
async def test_run_coroutine_dask_worker(c, s, a, b):
    async def f(dask_worker=None):
        await asyncio.sleep(0.001)
        return dask_worker.id

    response = await c.run(f)
    assert response == {a.address: a.id, b.address: b.id}


@gen_cluster(client=True, nthreads=[])
async def test_Executor(c, s):
    with ThreadPoolExecutor(2) as e:
        async with Worker(s.address, executor=e) as w:
            assert w.executor is e

            future = c.submit(inc, 1)
            result = await future
            assert result == 2

            assert e._threads  # had to do some work


@pytest.mark.skip(
    reason="Other tests leak memory, so process-level checks trigger immediately"
)
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    timeout=30,
    worker_kwargs={"memory_limit": 10e6},
)
async def test_spill_by_default(c, s, w):
    da = pytest.importorskip("dask.array")
    x = da.ones(int(10e6 * 0.7), chunks=1e6, dtype="u1")
    y = c.persist(x)
    await wait(y)
    assert len(w.data.disk)  # something is on disk


@gen_cluster(nthreads=[("127.0.0.1", 1)], worker_kwargs={"reconnect": False})
async def test_close_on_disconnect(s, w):
    await s.close()

    start = time()
    while w.status != Status.closed:
        await asyncio.sleep(0.01)
        assert time() < start + 5


@gen_cluster(nthreads=[])
async def test_memory_limit_auto(s):
    async with Worker(s.address, nthreads=1) as a, Worker(
        s.address, nthreads=2
    ) as b, Worker(s.address, nthreads=100) as c, Worker(s.address, nthreads=200) as d:
        assert isinstance(a.memory_limit, Number)
        assert isinstance(b.memory_limit, Number)

        if CPU_COUNT > 1:
            assert a.memory_limit < b.memory_limit

        assert c.memory_limit == d.memory_limit


@gen_cluster(client=True)
async def test_inter_worker_communication(c, s, a, b):
    [x, y] = await c._scatter([1, 2], workers=a.address)

    future = c.submit(add, x, y, workers=b.address)
    result = await future
    assert result == 3


@gen_cluster(client=True)
async def test_clean(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)

    await y

    collections = [
        a.tasks,
        a.data,
        a.threads,
    ]
    for c in collections:
        assert c

    x.release()
    y.release()

    while x.key in a.tasks:
        await asyncio.sleep(0.01)

    for c in collections:
        assert not c


@gen_cluster(client=True)
async def test_message_breakup(c, s, a, b):
    n = 100000
    a.target_message_size = 10 * n
    b.target_message_size = 10 * n
    xs = [c.submit(mul, b"%d" % i, n, workers=a.address) for i in range(30)]
    y = c.submit(lambda *args: None, xs, workers=b.address)
    await y

    assert 2 <= len(b.incoming_transfer_log) <= 20
    assert 2 <= len(a.outgoing_transfer_log) <= 20

    assert all(msg["who"] == b.address for msg in a.outgoing_transfer_log)
    assert all(msg["who"] == a.address for msg in a.incoming_transfer_log)


@gen_cluster(client=True)
async def test_types(c, s, a, b):
    assert all(ts.type is None for ts in a.tasks.values())
    assert all(ts.type is None for ts in b.tasks.values())
    x = c.submit(inc, 1, workers=a.address)
    await wait(x)
    assert a.tasks[x.key].type == int

    y = c.submit(inc, x, workers=b.address)
    await wait(y)
    assert b.tasks[x.key].type == int
    assert b.tasks[y.key].type == int

    await c._cancel(y)

    start = time()
    while y.key in b.data:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    assert y.key not in b.tasks


@gen_cluster()
async def test_system_monitor(s, a, b):
    assert b.monitor
    b.monitor.update()


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 2, {"resources": {"A": 1}}), ("127.0.0.1", 1)]
)
async def test_restrictions(c, s, a, b):
    # Worker has resource available
    assert a.available_resources == {"A": 1}
    # Resource restrictions
    x = c.submit(inc, 1, resources={"A": 1})
    await x
    ts = a.tasks[x.key]
    assert ts.resource_restrictions == {"A": 1}
    await c._cancel(x)

    while ts.state != "memory":
        # Resource should be unavailable while task isn't finished
        assert a.available_resources == {"A": 0}
        await asyncio.sleep(0.01)

    # Resource restored after task is in memory
    assert a.available_resources["A"] == 1


@gen_cluster(client=True)
async def test_clean_nbytes(c, s, a, b):
    L = [delayed(inc)(i) for i in range(10)]
    for i in range(5):
        L = [delayed(add)(x, y) for x, y in sliding_window(2, L)]
    total = delayed(sum)(L)

    future = c.compute(total)
    await wait(future)

    await asyncio.sleep(1)
    assert (
        len(list(filter(None, [ts.nbytes for ts in a.tasks.values()])))
        + len(list(filter(None, [ts.nbytes for ts in b.tasks.values()])))
        == 1
    )


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 20)
async def test_gather_many_small(c, s, a, *workers):
    """If the dependencies of a given task are very small, do not limit the
    number of concurrent outgoing connections
    """
    a.total_out_connections = 2
    futures = await c._scatter(list(range(100)))

    assert all(w.data for w in workers)

    def f(*args):
        return 10

    future = c.submit(f, *futures, workers=a.address)
    await wait(future)

    types = list(pluck(0, a.log))
    req = [i for i, t in enumerate(types) if t == "request-dep"]
    recv = [i for i, t in enumerate(types) if t == "receive-dep"]
    assert min(recv) > max(req)

    assert a.comm_nbytes == 0


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_multiple_transfers(c, s, w1, w2, w3):
    x = c.submit(inc, 1, workers=w1.address)
    y = c.submit(inc, 2, workers=w2.address)
    z = c.submit(add, x, y, workers=w3.address)

    await wait(z)

    r = w3.tasks[z.key].startstops
    transfers = [t for t in r if t["action"] == "transfer"]
    assert len(transfers) == 2


@pytest.mark.xfail(reason="very high flakiness")
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_share_communication(c, s, w1, w2, w3):
    x = c.submit(mul, b"1", int(w3.target_message_size + 1), workers=w1.address)
    y = c.submit(mul, b"2", int(w3.target_message_size + 1), workers=w2.address)
    await wait([x, y])
    await c._replicate([x, y], workers=[w1.address, w2.address])
    z = c.submit(add, x, y, workers=w3.address)
    await wait(z)
    assert len(w3.incoming_transfer_log) == 2
    assert w1.outgoing_transfer_log
    assert w2.outgoing_transfer_log


@pytest.mark.xfail(reason="very high flakiness")
@gen_cluster(client=True)
async def test_dont_overlap_communications_to_same_worker(c, s, a, b):
    x = c.submit(mul, b"1", int(b.target_message_size + 1), workers=a.address)
    y = c.submit(mul, b"2", int(b.target_message_size + 1), workers=a.address)
    await wait([x, y])
    z = c.submit(add, x, y, workers=b.address)
    await wait(z)
    assert len(b.incoming_transfer_log) == 2
    l1, l2 = b.incoming_transfer_log

    assert l1["stop"] < l2["start"]


@gen_cluster(client=True)
async def test_log_exception_on_failed_task(c, s, a, b):
    with captured_logger("distributed.worker") as logger:
        future = c.submit(div, 1, 0)
        await wait(future)

        await asyncio.sleep(0.1)

    text = logger.getvalue()
    assert "ZeroDivisionError" in text
    assert "Exception" in text


@gen_cluster(client=True)
async def test_clean_up_dependencies(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(2)
    xx = delayed(inc)(x)
    yy = delayed(inc)(y)
    z = delayed(add)(xx, yy)

    zz = c.persist(z)
    await wait(zz)

    while len(a.data) + len(b.data) > 1:
        await asyncio.sleep(0.01)

    assert set(a.data) | set(b.data) == {zz.key}


@gen_cluster(client=True)
async def test_hold_onto_dependents(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)
    await wait(y)

    assert x.key in b.data

    await c._cancel(y)
    while x.key not in b.data:
        await asyncio.sleep(0.1)


# Normally takes >2s but it has been observed to take >30s occasionally
@pytest.mark.slow
@gen_test(timeout=120)
async def test_worker_death_timeout():
    w = Worker("tcp://127.0.0.1:12345", death_timeout=0.1)
    with pytest.raises(TimeoutError) as info:
        await w

    assert "Worker" in str(info.value)
    assert "timed out" in str(info.value) or "failed to start" in str(info.value)
    assert w.status == Status.closed


@gen_cluster(client=True)
async def test_stop_doing_unnecessary_work(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.01)
    await asyncio.sleep(0.1)

    del futures

    start = time()
    while a.executing_count:
        await asyncio.sleep(0.01)
        assert time() - start < 0.5


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_priorities(c, s, w):
    values = []
    for i in range(10):
        a = delayed(slowinc)(i, dask_key_name="a-%d" % i, delay=0.01)
        a1 = delayed(inc)(a, dask_key_name="a1-%d" % i)
        a2 = delayed(inc)(a1, dask_key_name="a2-%d" % i)
        b1 = delayed(dec)(a, dask_key_name="b1-%d" % i)  # <<-- least favored

        values.append(a2)
        values.append(b1)

    futures = c.compute(values)
    await wait(futures)

    log = [
        t[0]
        for t in w.log
        if t[1] == "executing" and t[2] == "memory" and not t[0].startswith("finalize")
    ]

    assert any(key.startswith("b1") for key in log[: len(log) // 2])


@gen_cluster(client=True)
async def test_heartbeats(c, s, a, b):
    x = s.workers[a.address].last_seen
    start = time()
    await asyncio.sleep(a.periodic_callbacks["heartbeat"].callback_time / 1000 + 0.1)
    while s.workers[a.address].last_seen == x:
        await asyncio.sleep(0.01)
        assert time() < start + 2
    assert a.periodic_callbacks["heartbeat"].callback_time < 1000


@pytest.mark.parametrize("worker", [Worker, Nanny])
def test_worker_dir(worker):
    with tmpfile() as fn:

        @gen_cluster(client=True, worker_kwargs={"local_directory": fn})
        async def test_worker_dir(c, s, a, b):
            directories = [w.local_directory for w in s.workers.values()]
            assert all(d.startswith(fn) for d in directories)
            assert len(set(directories)) == 2  # distinct

        test_worker_dir()


@gen_cluster(nthreads=[])
async def test_false_worker_dir(s):
    async with Worker(s.address, local_directory="") as w:
        local_directory = w.local_directory

    cwd = os.getcwd()
    assert os.path.dirname(local_directory) == os.path.join(cwd, "dask-worker-space")


@gen_cluster(client=True)
async def test_dataframe_attribute_error(c, s, a, b):
    class BadSize:
        def __init__(self, data):
            self.data = data

        def __sizeof__(self):
            raise TypeError("Hello")

    future = c.submit(BadSize, 123)
    result = await future
    assert result.data == 123


@gen_cluster(client=True)
async def test_fail_write_to_disk(c, s, a, b):
    class Bad:
        def __getstate__(self):
            raise TypeError()

        def __sizeof__(self):
            return int(100e9)

    future = c.submit(Bad)
    await wait(future)

    assert future.status == "error"

    with pytest.raises(TypeError):
        await future

    futures = c.map(inc, range(10))
    results = await c._gather(futures)
    assert results == list(map(inc, range(10)))


@pytest.mark.skip(reason="Our logic here is faulty")
@gen_cluster(
    nthreads=[("127.0.0.1", 2)], client=True, worker_kwargs={"memory_limit": 10e9}
)
async def test_fail_write_many_to_disk(c, s, a):
    a.validate = False
    await asyncio.sleep(0.1)
    assert a.status == Status.running

    class Bad:
        def __init__(self, x):
            pass

        def __getstate__(self):
            raise TypeError()

        def __sizeof__(self):
            return int(2e9)

    futures = c.map(Bad, range(11))
    future = c.submit(lambda *args: 123, *futures)

    await wait(future)

    with pytest.raises(Exception) as info:
        await future

    # workers still operational
    result = await c.submit(inc, 1, workers=a.address)
    assert result == 2


@gen_cluster()
async def test_pid(s, a, b):
    assert s.workers[a.address].pid == os.getpid()


@gen_cluster(client=True)
async def test_get_client(c, s, a, b):
    def f(x):
        cc = get_client()
        future = cc.submit(inc, x)
        return future.result()

    assert default_client() is c

    future = c.submit(f, 10, workers=a.address)
    result = await future
    assert result == 11

    assert a._client
    assert not b._client

    assert a._client is c
    assert default_client() is c

    a_client = a._client

    for i in range(10):
        await wait(c.submit(f, i))

    assert a._client is a_client


def test_get_client_sync(client):
    def f(x):
        cc = get_client()
        future = cc.submit(inc, x)
        return future.result()

    future = client.submit(f, 10)
    assert future.result() == 11


@gen_cluster(client=True)
async def test_get_client_coroutine(c, s, a, b):
    async def f():
        client = await get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    results = await c.run(f)
    assert results == {a.address: 11, b.address: 11}


def test_get_client_coroutine_sync(client, s, a, b):
    async def f():
        client = await get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    results = client.run(f)
    assert results == {a["address"]: 11, b["address"]: 11}


@gen_cluster()
async def test_global_workers(s, a, b):
    n = len(Worker._instances)
    w = first(Worker._instances)
    assert w is a or w is b


@pytest.mark.skipif(WINDOWS, reason="num_fds not supported on windows")
@gen_cluster(nthreads=[])
async def test_worker_fds(s):
    proc = psutil.Process()
    before = psutil.Process().num_fds()

    async with Worker(s.address, loop=s.loop):
        assert proc.num_fds() > before

    while proc.num_fds() > before:
        await asyncio.sleep(0.01)


@gen_cluster(nthreads=[])
async def test_service_hosts_match_worker(s):
    async with Worker(s.address, host="tcp://0.0.0.0") as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")

    async with Worker(
        s.address, host="tcp://127.0.0.1", dashboard_address="0.0.0.0:0"
    ) as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")

    async with Worker(s.address, host="tcp://127.0.0.1") as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")

    # See what happens with e.g. `dask-worker --listen-address tcp://:8811`
    async with Worker(s.address, host="") as w:
        sock = first(w.http_server._sockets.values())
        assert sock.getsockname()[0] in ("::", "0.0.0.0")
        # Address must be a connectable address. 0.0.0.0 is not!
        address_all = w.address.rsplit(":", 1)[0]
        assert address_all in ("tcp://[::1]", "tcp://127.0.0.1")

    # Check various malformed IPv6 addresses
    # Since these hostnames get passed to distributed.comm.address_from_user_args,
    # bracketing is mandatory for IPv6.
    with pytest.raises(ValueError) as exc:
        async with Worker(s.address, host="::") as w:
            pass
    assert "bracketed" in str(exc)
    with pytest.raises(ValueError) as exc:
        async with Worker(s.address, host="tcp://::1") as w:
            pass
    assert "bracketed" in str(exc)


@gen_cluster(nthreads=[])
async def test_start_services(s):
    async with Worker(s.address, dashboard_address=1234) as w:
        assert w.http_server.port == 1234


@gen_test()
async def test_scheduler_file():
    with tmpfile() as fn:
        async with Scheduler(scheduler_file=fn, dashboard_address=":0") as s:
            async with Worker(scheduler_file=fn) as w:
                assert set(s.workers) == {w.address}


@gen_cluster(client=True)
async def test_scheduler_delay(c, s, a, b):
    old = a.scheduler_delay
    assert abs(a.scheduler_delay) < 0.6
    assert abs(b.scheduler_delay) < 0.6
    await asyncio.sleep(a.periodic_callbacks["heartbeat"].callback_time / 1000 + 0.6)
    assert a.scheduler_delay != old


@pytest.mark.flaky(reruns=10, reruns_delay=5)
@gen_cluster(client=True)
async def test_statistical_profiling(c, s, a, b):
    futures = c.map(slowinc, range(10), delay=0.1)
    await wait(futures)

    profile = a.profile_keys["slowinc"]
    assert profile["count"]


@pytest.mark.slow
@nodebug
@gen_cluster(
    client=True,
    timeout=30,
    config={
        "distributed.worker.profile.interval": "1ms",
        "distributed.worker.profile.cycle": "100ms",
    },
)
async def test_statistical_profiling_2(c, s, a, b):
    da = pytest.importorskip("dask.array")
    while True:
        x = da.random.random(1000000, chunks=(10000,))
        y = (x + x * 2) - x.sum().persist()
        await wait(y)

        profile = await a.get_profile()
        text = str(profile)
        if profile["count"] and "sum" in text and "random" in text:
            break


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    client=True,
    worker_kwargs={"memory_monitor_interval": 10},
)
async def test_robust_to_bad_sizeof_estimates(c, s, a):
    np = pytest.importorskip("numpy")
    memory = psutil.Process().memory_info().rss
    a.memory_limit = memory / 0.7 + 400e6

    class BadAccounting:
        def __init__(self, data):
            self.data = data

        def __sizeof__(self):
            return 10

    def f(n):
        x = np.ones(int(n), dtype="u1")
        result = BadAccounting(x)
        return result

    futures = c.map(f, [100e6] * 8, pure=False)

    start = time()
    while not a.data.disk:
        await asyncio.sleep(0.1)
        assert time() < start + 5


@pytest.mark.slow
@gen_cluster(
    nthreads=[("127.0.0.1", 2)],
    client=True,
    worker_kwargs={
        "memory_monitor_interval": 10,
        "memory_spill_fraction": False,  # don't spill
        "memory_target_fraction": False,
        "memory_pause_fraction": 0.5,
    },
)
async def test_pause_executor(c, s, a):
    memory = psutil.Process().memory_info().rss
    a.memory_limit = memory / 0.5 + 200e6
    np = pytest.importorskip("numpy")

    def f():
        x = np.ones(int(400e6), dtype="u1")
        sleep(1)

    with captured_logger(logging.getLogger("distributed.worker")) as logger:
        future = c.submit(f)
        futures = c.map(slowinc, range(30), delay=0.1)

        while a.status != Status.paused:
            await asyncio.sleep(0.01)

        out = logger.getvalue()
        assert "memory" in out.lower()
        assert "pausing" in out.lower()

    assert sum(f.status == "finished" for f in futures) < 4

    await wait(futures)


@gen_cluster(client=True, worker_kwargs={"profile_cycle_interval": "50 ms"})
async def test_statistical_profiling_cycle(c, s, a, b):
    futures = c.map(slowinc, range(20), delay=0.05)
    await wait(futures)
    await asyncio.sleep(0.01)
    end = time()
    assert len(a.profile_history) > 3

    x = await a.get_profile(start=time() + 10, stop=time() + 20)
    assert not x["count"]

    x = await a.get_profile(start=0, stop=time() + 10)
    recent = a.profile_recent["count"]
    actual = sum(p["count"] for _, p in a.profile_history) + a.profile_recent["count"]
    x2 = await a.get_profile(start=0, stop=time() + 10)
    assert x["count"] <= actual <= x2["count"]

    y = await a.get_profile(start=end - 0.300, stop=time())
    assert 0 < y["count"] <= x["count"]


@gen_cluster(client=True)
async def test_get_current_task(c, s, a, b):
    def some_name():
        return get_worker().get_current_task()

    result = await c.submit(some_name)
    assert result.startswith("some_name")


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 2)
async def test_reschedule(c, s, a, b):
    s.extensions["stealing"]._pc.stop()
    a_address = a.address

    def f(x):
        sleep(0.1)
        if get_worker().address == a_address:
            raise Reschedule()

    futures = c.map(f, range(4))
    futures2 = c.map(slowinc, range(10), delay=0.1, workers=a.address)
    await wait(futures)

    assert all(f.key in b.data for f in futures)


@gen_cluster(nthreads=[])
async def test_deque_handler(s):
    from distributed.worker import logger

    async with Worker(s.address) as w:
        deque_handler = w._deque_handler
        logger.info("foo456")
        assert deque_handler.deque
        msg = deque_handler.deque[-1]
        assert "distributed.worker" in deque_handler.format(msg)
        assert any(msg.msg == "foo456" for msg in deque_handler.deque)


@gen_cluster(nthreads=[], client=True)
async def test_avoid_memory_monitor_if_zero_limit(c, s):
    worker = await Worker(
        s.address, loop=s.loop, memory_limit=0, memory_monitor_interval=10
    )
    assert type(worker.data) is dict
    assert "memory" not in worker.periodic_callbacks

    future = c.submit(inc, 1)
    assert (await future) == 2
    await asyncio.sleep(worker.memory_monitor_interval / 1000)

    await c.submit(inc, 2)  # worker doesn't pause

    await worker.close()


@gen_cluster(
    nthreads=[("127.0.0.1", 1)],
    config={
        "distributed.worker.memory.spill": False,
        "distributed.worker.memory.target": False,
    },
)
async def test_dict_data_if_no_spill_to_disk(s, w):
    assert type(w.data) is dict


def test_get_worker_name(client):
    def f():
        get_client().submit(inc, 1).result()

    client.run(f)

    def func(dask_scheduler):
        return list(dask_scheduler.clients)

    start = time()
    while not any("worker" in n for n in client.run_on_scheduler(func)):
        sleep(0.1)
        assert time() < start + 10


@gen_cluster(nthreads=[("127.0.0.1", 1)], worker_kwargs={"memory_limit": "2e3 MB"})
async def test_parse_memory_limit(s, w):
    assert w.memory_limit == 2e9


@gen_cluster(nthreads=[], client=True)
async def test_scheduler_address_config(c, s):
    with dask.config.set({"scheduler-address": s.address}):
        worker = await Worker(loop=s.loop)
        assert worker.scheduler.address == s.address
    await worker.close()


@pytest.mark.xfail(reason="very high flakiness")
@pytest.mark.slow
@gen_cluster(client=True)
async def test_wait_for_outgoing(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = np.random.random(10000000)
    future = await c.scatter(x, workers=a.address)

    y = c.submit(inc, future, workers=b.address)
    await wait(y)

    assert len(b.incoming_transfer_log) == len(a.outgoing_transfer_log) == 1
    bb = b.incoming_transfer_log[0]["duration"]
    aa = a.outgoing_transfer_log[0]["duration"]
    ratio = aa / bb

    assert 1 / 3 < ratio < 3


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(
    nthreads=[("127.0.0.1", 1), ("127.0.0.1", 1), ("127.0.0.2", 1)], client=True
)
async def test_prefer_gather_from_local_address(c, s, w1, w2, w3):
    x = await c.scatter(123, workers=[w1.address, w3.address], broadcast=True)

    y = c.submit(inc, x, workers=[w2.address])
    await wait(y)

    assert any(d["who"] == w2.address for d in w1.outgoing_transfer_log)
    assert not any(d["who"] == w2.address for d in w3.outgoing_transfer_log)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 20,
    timeout=30,
    config={"distributed.worker.connections.incoming": 1},
)
async def test_avoid_oversubscription(c, s, *workers):
    np = pytest.importorskip("numpy")
    x = c.submit(np.random.random, 1000000, workers=[workers[0].address])
    await wait(x)

    futures = [c.submit(len, x, pure=False, workers=[w.address]) for w in workers[1:]]

    await wait(futures)

    # Original worker not responsible for all transfers
    assert len(workers[0].outgoing_transfer_log) < len(workers) - 2

    # Some other workers did some work
    assert len([w for w in workers if len(w.outgoing_transfer_log) > 0]) >= 3


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
async def test_custom_metrics(c, s, a, b):
    assert s.workers[a.address].metrics["my_port"] == a.port
    assert s.workers[b.address].metrics["my_port"] == b.port


@gen_cluster(client=True)
async def test_register_worker_callbacks(c, s, a, b):
    # preload function to run
    def mystartup(dask_worker):
        dask_worker.init_variable = 1

    def mystartup2():
        import os

        os.environ["MY_ENV_VALUE"] = "WORKER_ENV_VALUE"
        return "Env set."

    # Check that preload function has been run
    def test_import(dask_worker):
        return hasattr(dask_worker, "init_variable")
        #       and dask_worker.init_variable == 1

    def test_startup2():
        import os

        return os.getenv("MY_ENV_VALUE", None) == "WORKER_ENV_VALUE"

    # Nothing has been run yet
    result = await c.run(test_import)
    assert list(result.values()) == [False] * 2
    result = await c.run(test_startup2)
    assert list(result.values()) == [False] * 2

    # Start a worker and check that startup is not run
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [False]
    await worker.close()

    # Add a preload function
    response = await c.register_worker_callbacks(setup=mystartup)
    assert len(response) == 2

    # Check it has been ran on existing worker
    result = await c.run(test_import)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is ran on it
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [True]
    await worker.close()

    # Register another preload function
    response = await c.register_worker_callbacks(setup=mystartup2)
    assert len(response) == 2

    # Check it has been run
    result = await c.run(test_startup2)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is ran on it
    worker = await Worker(s.address, loop=s.loop)
    result = await c.run(test_import, workers=[worker.address])
    assert list(result.values()) == [True]
    result = await c.run(test_startup2, workers=[worker.address])
    assert list(result.values()) == [True]
    await worker.close()


@gen_cluster(client=True)
async def test_register_worker_callbacks_err(c, s, a, b):
    with pytest.raises(ZeroDivisionError):
        await c.register_worker_callbacks(setup=lambda: 1 / 0)


@gen_cluster(nthreads=[])
async def test_data_types(s):
    w = await Worker(s.address, data=dict)
    assert isinstance(w.data, dict)
    await w.close()

    data = dict()
    w = await Worker(s.address, data=data)
    assert w.data is data
    await w.close()

    class Data(dict):
        def __init__(self, x, y):
            self.x = x
            self.y = y

    w = await Worker(s.address, data=(Data, {"x": 123, "y": 456}))
    assert w.data.x == 123
    assert w.data.y == 456
    await w.close()


@gen_cluster(nthreads=[])
async def test_local_directory(s):
    with tmpfile() as fn:
        with dask.config.set(temporary_directory=fn):
            w = await Worker(s.address)
            assert w.local_directory.startswith(fn)
            assert "dask-worker-space" in w.local_directory


@gen_cluster(nthreads=[])
async def test_local_directory_make_new_directory(s):
    with tmpfile() as fn:
        w = await Worker(s.address, local_directory=os.path.join(fn, "foo", "bar"))
        assert w.local_directory.startswith(fn)
        assert "foo" in w.local_directory
        assert "dask-worker-space" in w.local_directory


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(nthreads=[], client=True)
async def test_host_address(c, s):
    w = await Worker(s.address, host="127.0.0.2")
    assert "127.0.0.2" in w.address
    await w.close()

    n = await Nanny(s.address, host="127.0.0.3")
    assert "127.0.0.3" in n.address
    assert "127.0.0.3" in n.worker_address
    await n.close()


def test_resource_limit(monkeypatch):
    assert parse_memory_limit("250MiB", 1, total_cores=1) == 1024 * 1024 * 250

    new_limit = 1024 * 1024 * 200
    import distributed.worker

    monkeypatch.setattr(distributed.system, "MEMORY_LIMIT", new_limit)
    assert parse_memory_limit("250MiB", 1, total_cores=1) == new_limit


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_interface_async(cleanup, loop, Worker):
    from distributed.utils import get_ip_interface

    psutil = pytest.importorskip("psutil")
    if_names = sorted(psutil.net_if_addrs())
    for if_name in if_names:
        try:
            ipv4_addr = get_ip_interface(if_name)
        except ValueError:
            pass
        else:
            if ipv4_addr == "127.0.0.1":
                break
    else:
        pytest.skip(
            "Could not find loopback interface. "
            "Available interfaces are: %s." % (if_names,)
        )

    async with Scheduler(dashboard_address=":0", interface=if_name) as s:
        assert s.address.startswith("tcp://127.0.0.1")
        async with Worker(s.address, interface=if_name) as w:
            assert w.address.startswith("tcp://127.0.0.1")
            assert w.ip == "127.0.0.1"
            async with Client(s.address, asynchronous=True) as c:
                info = c.scheduler_info()
                assert "tcp://127.0.0.1" in info["address"]
                assert all("127.0.0.1" == d["host"] for d in info["workers"].values())


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_protocol_from_scheduler_address(cleanup, Worker):
    pytest.importorskip("ucp")

    async with Scheduler(protocol="ucx", dashboard_address=":0") as s:
        assert s.address.startswith("ucx://")
        async with Worker(s.address) as w:
            assert w.address.startswith("ucx://")
            async with Client(s.address, asynchronous=True) as c:
                info = c.scheduler_info()
                assert info["address"].startswith("ucx://")


@pytest.mark.asyncio
async def test_host_uses_scheduler_protocol(cleanup, monkeypatch):
    # Ensure worker uses scheduler's protocol to determine host address, not the default scheme
    # See https://github.com/dask/distributed/pull/4883

    class BadBackend(TCPBackend):
        def get_address_host(self, loc):
            raise ValueError("asdf")

    monkeypatch.setitem(backends, "foo", BadBackend())

    with dask.config.set({"distributed.comm.default-scheme": "foo"}):
        async with Scheduler(protocol="tcp", dashboard_address=":0") as s:
            async with Worker(s.address):
                # Ensure that worker is able to properly start up
                # without BadBackend.get_address_host raising a ValueError
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize("Worker", [Worker, Nanny])
async def test_worker_listens_on_same_interface_by_default(cleanup, Worker):
    async with Scheduler(host="localhost", dashboard_address=":0") as s:
        assert s.ip in {"127.0.0.1", "localhost"}
        async with Worker(s.address) as w:
            assert s.ip == w.ip


@gen_cluster(client=True)
async def test_close_gracefully(c, s, a, b):
    futures = c.map(slowinc, range(200), delay=0.1)
    while not b.data:
        await asyncio.sleep(0.1)

    mem = set(b.data)
    proc = [ts for ts in b.tasks.values() if ts.state == "executing"]

    await b.close_gracefully()

    assert b.status == Status.closed
    assert b.address not in s.workers
    assert mem.issubset(set(a.data))
    for ts in proc:
        assert ts.state in ("executing", "memory")


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_lifetime(c, s):
    async with Worker(s.address) as a, Worker(s.address, lifetime="1 seconds") as b:
        futures = c.map(slowinc, range(200), delay=0.1, worker=[b.address])
        await asyncio.sleep(1.5)
        assert b.status not in (Status.running, Status.paused)
        await b.finished()
        assert set(b.data) == set(a.data)  # successfully moved data over


@gen_cluster(worker_kwargs={"lifetime": "10s", "lifetime_stagger": "2s"})
async def test_lifetime_stagger(s, a, b):
    assert a.lifetime != b.lifetime
    assert 8 <= a.lifetime <= 12
    assert 8 <= b.lifetime <= 12


@gen_cluster(nthreads=[])
async def test_bad_metrics(s):
    def bad_metric(w):
        raise Exception("Hello")

    async with Worker(s.address, metrics={"bad": bad_metric}) as w:
        assert "bad" not in s.workers[w.address].metrics


@gen_cluster(nthreads=[])
async def test_bad_startup(s):
    def bad_startup(w):
        raise Exception("Hello")

    try:
        await Worker(s.address, startup_information={"bad": bad_startup})
    except Exception:
        pytest.fail("Startup exception was raised")


@gen_cluster(client=True)
async def test_pip_install(c, s, a, b):
    with mock.patch(
        "distributed.diagnostics.plugin.subprocess.Popen.communicate",
        return_value=(b"", b""),
    ) as p1:
        with mock.patch(
            "distributed.diagnostics.plugin.subprocess.Popen", return_value=p1
        ) as p2:
            p1.communicate.return_value = b"", b""
            p1.wait.return_value = 0
            await c.register_worker_plugin(
                PipInstall(packages=["requests"], pip_options=["--upgrade"])
            )

            args = p2.call_args[0][0]
            assert "python" in args[0]
            assert args[1:] == ["-m", "pip", "install", "--upgrade", "requests"]


@gen_cluster(client=True)
async def test_pip_install_fails(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.ERROR
    ) as logger:
        with mock.patch(
            "distributed.diagnostics.plugin.subprocess.Popen.communicate",
            return_value=(b"", b"error"),
        ) as p1:
            with mock.patch(
                "distributed.diagnostics.plugin.subprocess.Popen", return_value=p1
            ) as p2:
                p1.communicate.return_value = (
                    b"",
                    b"Could not find a version that satisfies the requirement not-a-package",
                )
                p1.wait.return_value = 1
                await c.register_worker_plugin(PipInstall(packages=["not-a-package"]))

                assert "not-a-package" in logger.getvalue()


#             args = p2.call_args[0][0]
#             assert "python" in args[0]
#             assert args[1:] == ["-m", "pip", "--upgrade", "install", "requests"]


@gen_cluster(nthreads=[])
async def test_update_latency(s):
    async with await Worker(s.address) as w:
        original = w.latency
        await w.heartbeat()
        assert original != w.latency

        if w.digests is not None:
            assert w.digests["latency"].size() > 0


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_workerstate_executing(c, s, a):
    ws = s.workers[a.address]
    # Initially there are no active tasks
    assert not ws.executing
    # Submit a task and ensure the WorkerState is updated with the task
    # it's executing
    f = c.submit(slowinc, 1, delay=3)
    while not ws.executing:
        assert f.status == "pending"
        await asyncio.sleep(0.01)
    assert s.tasks[f.key] in ws.executing
    await f


@pytest.mark.parametrize("reconnect", [True, False])
@gen_cluster(nthreads=[])
async def test_heartbeat_comm_closed(s, monkeypatch, reconnect):
    with captured_logger("distributed.worker", level=logging.WARNING) as logger:

        def bad_heartbeat_worker(*args, **kwargs):
            raise CommClosedError()

        async with await Worker(s.address, reconnect=reconnect) as w:
            # Trigger CommClosedError during worker heartbeat
            monkeypatch.setattr(w.scheduler, "heartbeat_worker", bad_heartbeat_worker)

            await w.heartbeat()
            if reconnect:
                assert w.status == Status.running
            else:
                assert w.status == Status.closed
    assert "Heartbeat to scheduler failed" in logger.getvalue()


@gen_cluster(nthreads=[])
async def test_bad_local_directory(s):
    try:
        async with Worker(s.address, local_directory="/not/a/valid-directory"):
            pass
    except OSError:
        # On Linux: [Errno 13] Permission denied: '/not'
        # On MacOSX: [Errno 30] Read-only file system: '/not'
        pass
    else:
        assert WINDOWS

    assert not any("error" in log for log in s.get_logs())


@gen_cluster(client=True, nthreads=[])
async def test_taskstate_metadata(c, s):
    async with await Worker(s.address) as w:
        await c.register_worker_plugin(TaskStateMetadataPlugin())

        f = c.submit(inc, 1)
        await f

        ts = w.tasks[f.key]
        assert "start_time" in ts.metadata
        assert "stop_time" in ts.metadata
        assert ts.metadata["stop_time"] > ts.metadata["start_time"]

        # Check that Scheduler TaskState.metadata was also updated
        assert s.tasks[f.key].metadata == ts.metadata


@gen_cluster(client=True, nthreads=[])
async def test_executor_offload(c, s, monkeypatch):
    class SameThreadClass:
        def __getstate__(self):
            return ()

        def __setstate__(self, state):
            self._thread_ident = threading.get_ident()
            return self

    monkeypatch.setattr("distributed.worker.OFFLOAD_THRESHOLD", 1)

    async with Worker(s.address, executor="offload") as w:
        from distributed.utils import _offload_executor

        assert w.executor is _offload_executor

        x = SameThreadClass()

        def f(x):
            return threading.get_ident() == x._thread_ident

        assert await c.submit(f, x)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_story(c, s, w):
    future = c.submit(inc, 1)
    await future
    ts = w.tasks[future.key]
    assert ts.state in str(w.story(ts))
    assert w.story(ts) == w.story(ts.key)


@gen_cluster(client=True)
async def test_story_with_deps(c, s, a, b):
    """
    Assert that the structure of the story does not change unintentionally and
    expected subfields are actually filled
    """
    dep = c.submit(inc, 1, workers=[a.address])
    res = c.submit(inc, dep, workers=[b.address])
    await res
    key = res.key

    story = a.story(key)
    assert story == []
    story = b.story(key)

    pruned_story = []
    stimulus_ids = set()
    # Story now includes randomized stimulus_ids and timestamps.
    for msg in story:
        assert isinstance(msg, tuple), msg
        assert isinstance(msg[-1], float), msg
        assert msg[-1] > time() - 60, msg
        pruned_msg = list(msg)
        stimulus_ids.add(msg[-2])
        pruned_story.append(tuple(pruned_msg[:-2]))

    assert len(stimulus_ids) == 3
    stimulus_id = pruned_story[0][-1]
    assert isinstance(stimulus_id, str)
    assert stimulus_id.startswith("compute-task")
    # This is a simple transition log
    expected_story = [
        (key, "compute-task"),
        (key, "released", "waiting", {}),
        (key, "waiting", "ready", {}),
        (key, "ready", "executing", {}),
        (key, "put-in-memory"),
        (key, "executing", "memory", {}),
    ]
    assert pruned_story == expected_story

    dep_story = dep.key

    story = b.story(dep_story)
    pruned_story = []
    stimulus_ids = set()
    for msg in story:
        assert isinstance(msg, tuple), msg
        assert isinstance(msg[-1], float), msg
        assert msg[-1] > time() - 60, msg
        pruned_msg = list(msg)
        stimulus_ids.add(msg[-2])
        pruned_story.append(tuple(pruned_msg[:-2]))

    assert len(stimulus_ids) == 3
    stimulus_id = pruned_story[0][-1]
    assert isinstance(stimulus_id, str)
    expected_story = [
        (dep_story, "register-replica", "released"),
        (dep_story, "released", "fetch", {}),
        (
            "gather-dependencies",
            a.address,
            {dep.key},
        ),
        (dep_story, "fetch", "flight", {}),
        (
            "request-dep",
            a.address,
            {dep.key},
        ),
        (
            "receive-dep",
            a.address,
            {dep.key},
        ),
        (dep_story, "put-in-memory"),
        (dep_story, "flight", "memory", {res.key: "ready"}),
    ]
    assert pruned_story == expected_story


@gen_cluster(client=True)
async def test_gather_dep_one_worker_always_busy(c, s, a, b):
    # Ensure that both dependencies for H are on another worker than H itself.
    # The worker where the dependencies are on is then later blocked such that
    # the data cannot be fetched
    # In the past it was important that there is more than one key on the
    # worker. This should be kept to avoid any edge case specific to one
    f = c.submit(inc, 1, workers=[a.address])
    g = c.submit(
        inc,
        2,
        workers=[a.address],
    )

    await f
    await g
    # We will block A for any outgoing communication. This simulates an
    # overloaded worker which will always return "busy" for get_data requests,
    # effectively blocking H indefinitely
    a.outgoing_current_count = 10000000
    assert f.key in a.tasks
    assert g.key in a.tasks
    # Ensure there are actually two distinct tasks and not some pure=True
    # caching
    assert f.key != g.key
    h = c.submit(add, f, g, workers=[b.address])

    fut = asyncio.wait_for(h, 0.1)

    while h.key not in b.tasks:
        await asyncio.sleep(0.01)

    ts_h = b.tasks[h.key]
    ts_f = b.tasks[f.key]
    ts_g = b.tasks[g.key]

    with pytest.raises(asyncio.TimeoutError):
        assert ts_h.state == "waiting"
        assert ts_f.state in ["flight", "fetch"]
        assert ts_g.state in ["flight", "fetch"]
        await fut

    # Ensure B wasn't lazy but tried at least once
    assert b.repetitively_busy

    x = await Worker(s.address, name="x")
    # We "scatter" the data to another worker which is able to serve this data.
    # In reality this could be another worker which fetched this dependency and
    # got through to A or another worker executed the task using work stealing
    # or any other. To avoid cross effects, we'll just put the data onto the
    # worker ourselves
    x.update_data(data={key: a.data[key] for key in [f.key, g.key]})

    assert await h == 5

    # Since we put the data onto the worker ourselves, the gather_dep might
    # still be mid execution and we'll get a dangling task. Let it finish
    # naturally
    while any(["Worker.gather_dep" in str(t) for t in asyncio.all_tasks()]):
        await asyncio.sleep(0.05)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 0)])
async def test_worker_client_uses_default_no_close(c, s, a):
    """
    If a default client is available in the process, the worker will pick this
    one and will not close it if it is closed
    """
    assert not Worker._initialized_clients
    assert default_client() is c
    existing_client = c.id

    def get_worker_client_id():
        def_client = get_client()
        return def_client.id

    worker_client = await c.submit(get_worker_client_id)
    assert worker_client == existing_client

    assert not Worker._initialized_clients

    await a.close()

    assert len(Client._instances) == 1
    assert c.status == "running"
    c_def = default_client()
    assert c is c_def


@gen_cluster(nthreads=[("127.0.0.1", 0)])
async def test_worker_client_closes_if_created_on_worker_one_worker(s, a):
    async with Client(s.address, set_as_default=False, asynchronous=True) as c:

        with pytest.raises(ValueError):
            default_client()

        def get_worker_client_id():
            def_client = get_client()
            return def_client.id

        new_client_id = await c.submit(get_worker_client_id)
        default_client_id = await c.submit(get_worker_client_id)
        assert new_client_id != c.id
        assert new_client_id == default_client_id

        new_client = default_client()
        assert new_client_id == new_client.id
        assert new_client.status == "running"

        # If a worker closes, all clients created on it should close as well
        await a.close()
        assert new_client.status == "closed"

        assert len(Client._instances) == 2

        assert c.status == "running"

        with pytest.raises(ValueError):
            default_client()


@gen_cluster()
async def test_worker_client_closes_if_created_on_worker_last_worker_alive(s, a, b):
    async with Client(s.address, set_as_default=False, asynchronous=True) as c:

        with pytest.raises(ValueError):
            default_client()

        def get_worker_client_id():
            def_client = get_client()
            return def_client.id

        new_client_id = await c.submit(get_worker_client_id, workers=[a.address])
        default_client_id = await c.submit(get_worker_client_id, workers=[a.address])

        default_client_id_b = await c.submit(get_worker_client_id, workers=[b.address])
        assert not b._comms
        assert new_client_id != c.id
        assert new_client_id == default_client_id
        assert new_client_id == default_client_id_b

        new_client = default_client()
        assert new_client_id == new_client.id
        assert new_client.status == "running"

        # We'll close A. This should *not* close the client since the client is also used by B
        await a.close()
        assert new_client.status == "running"

        client_id_b_after = await c.submit(get_worker_client_id, workers=[b.address])
        assert client_id_b_after == default_client_id_b

        assert len(Client._instances) == 2
        await b.close()
        assert new_client.status == "closed"

        assert c.status == "running"

        with pytest.raises(ValueError):
            default_client()


@gen_cluster(client=True, nthreads=[])
async def test_multiple_executors(c, s):
    def get_thread_name():
        return threading.current_thread().name

    async with Worker(
        s.address,
        nthreads=2,
        executor={"foo": ThreadPoolExecutor(1, thread_name_prefix="Dask-Foo-Threads")},
    ):
        futures = []
        with dask.annotate(executor="default"):
            futures.append(c.submit(get_thread_name, pure=False))
        with dask.annotate(executor="foo"):
            futures.append(c.submit(get_thread_name, pure=False))
        default_result, gpu_result = await c.gather(futures)
        assert "Dask-Default-Threads" in default_result
        assert "Dask-Foo-Threads" in gpu_result


@gen_cluster(client=True)
async def test_process_executor(c, s, a, b):
    with ProcessPoolExecutor() as e:
        a.executors["processes"] = e
        b.executors["processes"] = e

        future = c.submit(os.getpid, pure=False)
        assert (await future) == os.getpid()

        with dask.annotate(executor="processes"):
            future = c.submit(os.getpid, pure=False)

        assert (await future) != os.getpid()


def kill_process():
    import os
    import signal

    if WINDOWS:
        # There's no SIGKILL on Windows
        sig = signal.SIGTERM
    else:
        # With SIGTERM there may be several seconds worth of delay before the worker
        # actually shuts down - particularly on slow CI. Use SIGKILL for instant
        # termination.
        sig = signal.SIGKILL

    os.kill(os.getpid(), sig)
    sleep(60)  # Cope with non-instantaneous termination


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_process_executor_kills_process(c, s, a):
    with ProcessPoolExecutor() as e:
        a.executors["processes"] = e
        with dask.annotate(executor="processes", retries=1):
            future = c.submit(kill_process)

        msg = "A child process terminated abruptly, the process pool is not usable anymore"
        with pytest.raises(BrokenProcessPool, match=msg):
            await future

        with dask.annotate(executor="processes", retries=1):
            future = c.submit(inc, 1)

        # The process pool is now unusable and the worker is effectively dead
        with pytest.raises(BrokenProcessPool, match=msg):
            await future


def raise_exc():
    raise RuntimeError("foo")


@gen_cluster(client=True)
async def test_process_executor_raise_exception(c, s, a, b):
    with ProcessPoolExecutor() as e:
        a.executors["processes"] = e
        b.executors["processes"] = e
        with dask.annotate(executor="processes", retries=1):
            future = c.submit(raise_exc)

        with pytest.raises(RuntimeError, match="foo"):
            await future


@pytest.mark.gpu
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_gpu_executor(c, s, w):
    if nvml.device_get_count() > 0:
        e = w.executors["gpu"]
        assert isinstance(e, distributed.threadpoolexecutor.ThreadPoolExecutor)
        assert e._max_workers == 1
    else:
        assert "gpu" not in w.executors


def assert_task_states_on_worker(expected, worker):
    for dep_key, expected_state in expected.items():
        assert dep_key in worker.tasks, (worker.name, dep_key, worker.tasks)
        dep_ts = worker.tasks[dep_key]
        assert dep_ts.state == expected_state, (worker.name, dep_ts, expected_state)
    assert set(expected) == set(worker.tasks)


@gen_cluster(client=True)
async def test_worker_state_error_release_error_last(c, s, a, b):
    """
    Create a chain of tasks and err one of them. Then release tasks in a certain
    order and ensure the tasks are released and/or kept in memory as appropriate

    F -- RES (error)
        /
       /
    G

    Free error last
    """

    def raise_exc(*args):
        raise RuntimeError()

    f = c.submit(inc, 1, workers=[a.address], key="f")
    g = c.submit(inc, 1, workers=[b.address], key="g")
    res = c.submit(raise_exc, f, g, workers=[a.address])

    with pytest.raises(RuntimeError):
        await res

    # Nothing bad happened on B, therefore B should hold on to G
    assert len(b.tasks) == 1
    assert g.key in b.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.tasks
    ts = a.tasks[res.key]
    assert ts.state == "error"

    expected_states = {
        # A was instructed to compute this result and we're still holding a ref via `f`
        f.key: "memory",
        # This was fetched from another worker. While we hold a ref via `g`, the
        # scheduler only instructed to compute this on B
        g.key: "memory",
        res.key: "error",
    }
    assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures
    f.release()
    g.release()

    # We no longer hold any refs to f or g and B didn't have any erros. It
    # releases everything as expected
    while b.tasks:
        await asyncio.sleep(0.01)

    expected_states = {
        f.key: "released",
        g.key: "released",
        res.key: "error",
    }

    assert_task_states_on_worker(expected_states, a)

    res.release()

    # We no longer hold any refs. Cluster should reset completely
    # This is not happening
    for server in [s, a, b]:
        while server.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_worker_state_error_release_error_first(c, s, a, b):
    """
    Create a chain of tasks and err one of them. Then release tasks in a certain
    order and ensure the tasks are released and/or kept in memory as appropriate

    F -- RES (error)
        /
       /
    G

    Free error first
    """

    def raise_exc(*args):
        raise RuntimeError()

    f = c.submit(inc, 1, workers=[a.address], key="f")
    g = c.submit(inc, 1, workers=[b.address], key="g")
    res = c.submit(raise_exc, f, g, workers=[a.address])

    with pytest.raises(RuntimeError):
        await res

    # Nothing bad happened on B, therefore B should hold on to G
    assert len(b.tasks) == 1
    assert g.key in b.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.tasks
    ts = a.tasks[res.key]
    assert ts.state == "error"

    expected_states = {
        # A was instructed to compute this result and we're still holding a ref
        # via `f`
        f.key: "memory",
        # This was fetched from another worker. While we hold a ref via `g`, the
        # scheduler only instructed to compute this on B
        g.key: "memory",
        res.key: "error",
    }
    assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures

    res.release()
    # We no longer hold any refs to f or g and B didn't have any erros. It
    # releases everything as expected
    while res.key in a.tasks:
        await asyncio.sleep(0.01)

    expected_states = {
        f.key: "memory",
        g.key: "memory",
    }

    assert_task_states_on_worker(expected_states, a)

    f.release()
    g.release()

    for server in [s, a, b]:
        while server.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_worker_state_error_release_error_int(c, s, a, b):
    """
    Create a chain of tasks and err one of them. Then release tasks in a certain
    order and ensure the tasks are released and/or kept in memory as appropriate

    F -- RES (error)
        /
       /
    G

    Free one successful task, then error, then last task
    """

    def raise_exc(*args):
        raise RuntimeError()

    f = c.submit(inc, 1, workers=[a.address], key="f")
    g = c.submit(inc, 1, workers=[b.address], key="g")
    res = c.submit(raise_exc, f, g, workers=[a.address])

    with pytest.raises(RuntimeError):
        await res

    # Nothing bad happened on B, therefore B should hold on to G
    assert len(b.tasks) == 1
    assert g.key in b.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.tasks
    ts = a.tasks[res.key]
    assert ts.state == "error"

    expected_states = {
        # A was instructed to compute this result and we're still holding a ref via `f`
        f.key: "memory",
        # This was fetched from another worker. While we hold a ref via `g`, the
        # scheduler only instructed to compute this on B
        g.key: "memory",
        res.key: "error",
    }
    assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures

    f.release()
    res.release()
    # We no longer hold any refs to f or g and B didn't have any erros. It
    # releases everything as expected
    while len(a.tasks) > 1:
        await asyncio.sleep(0.01)

    expected_states = {
        g.key: "memory",
    }

    assert_task_states_on_worker(expected_states, a)
    assert_task_states_on_worker(expected_states, b)

    g.release()

    # We no longer hold any refs. Cluster should reset completely
    for server in [s, a, b]:
        while server.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_worker_state_error_long_chain(c, s, a, b):
    def raise_exc(*args):
        raise RuntimeError()

    # f (A) --------> res (B)
    #                /
    # g (B) -> h (A)

    f = c.submit(inc, 1, workers=[a.address], key="f", allow_other_workers=False)
    g = c.submit(inc, 1, workers=[b.address], key="g", allow_other_workers=False)
    h = c.submit(inc, g, workers=[a.address], key="h", allow_other_workers=False)
    res = c.submit(
        raise_exc, f, h, workers=[b.address], allow_other_workers=False, key="res"
    )

    with pytest.raises(RuntimeError):
        await res

    expected_states_A = {
        f.key: "memory",
        g.key: "memory",
        h.key: "memory",
    }
    await asyncio.sleep(0.05)
    assert_task_states_on_worker(expected_states_A, a)

    expected_states_B = {
        f.key: "memory",
        g.key: "memory",
        h.key: "memory",
        res.key: "error",
    }
    await asyncio.sleep(0.05)
    assert_task_states_on_worker(expected_states_B, b)

    f.release()

    expected_states_A = {
        g.key: "memory",
        h.key: "memory",
    }
    await asyncio.sleep(0.05)
    assert_task_states_on_worker(expected_states_A, a)

    expected_states_B = {
        f.key: "released",
        g.key: "memory",
        h.key: "memory",
        res.key: "error",
    }
    await asyncio.sleep(0.05)
    assert_task_states_on_worker(expected_states_B, b)

    g.release()

    expected_states_A = {
        g.key: "released",
        h.key: "memory",
    }
    await asyncio.sleep(0.05)
    assert_task_states_on_worker(expected_states_A, a)

    # B must not forget a task since all have a still valid dependent
    expected_states_B = {
        f.key: "released",
        h.key: "memory",
        res.key: "error",
    }
    assert_task_states_on_worker(expected_states_B, b)
    h.release()
    await asyncio.sleep(0.05)

    expected_states_A = {}
    assert_task_states_on_worker(expected_states_A, a)
    expected_states_B = {
        f.key: "released",
        h.key: "released",
        res.key: "error",
    }

    assert_task_states_on_worker(expected_states_B, b)
    res.release()

    # We no longer hold any refs. Cluster should reset completely
    for server in [s, a, b]:
        while server.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("127.0.0.1", x) for x in range(4)])
async def test_hold_on_to_replicas(c, s, *workers):
    f1 = c.submit(inc, 1, workers=[workers[0].address], key="f1")
    f2 = c.submit(inc, 2, workers=[workers[1].address], key="f2")

    sum_1 = c.submit(
        slowsum, [f1, f2], delay=0.1, workers=[workers[2].address], key="sum"
    )
    sum_2 = c.submit(
        slowsum, [f1, sum_1], delay=0.2, workers=[workers[3].address], key="sum_2"
    )
    f1.release()
    f2.release()

    while sum_2.key not in workers[3].tasks:
        await asyncio.sleep(0.01)

    while not workers[3].tasks[sum_2.key].state == "memory":
        assert len(s.tasks[f1.key].who_has) >= 2
        assert s.tasks[f2.key].state == "released"
        await asyncio.sleep(0.01)

    while len(workers[2].data) > 1:
        await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_worker_reconnects_mid_compute(c, s, a, b):
    """Ensure that, if a worker disconnects while computing a result, the scheduler will
    still accept the result.

    There is also an edge case tested which ensures that the reconnect is
    successful if a task is currently executing; see
    https://github.com/dask/distributed/issues/5078

    See also distributed.tests.test_scheduler.py::test_gather_allow_worker_reconnect
    """
    with captured_logger("distributed.scheduler") as s_logs:
        # Let's put one task in memory to ensure the reconnect has tasks in
        # different states
        f1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
        await f1
        a_address = a.address
        a.periodic_callbacks["heartbeat"].stop()
        await a.heartbeat()
        a.heartbeat_active = True

        from distributed import Lock

        def fast_on_a(lock):
            w = get_worker()
            import time

            if w.address != a_address:
                lock.acquire()
            else:
                time.sleep(1)

        lock = Lock()
        # We want to be sure that A is the only one computing this result
        async with lock:

            f2 = c.submit(
                fast_on_a, lock, workers=[a.address], allow_other_workers=True
            )

            while f2.key not in a.tasks:
                await asyncio.sleep(0.01)

            await s.stream_comms[a.address].close()

            assert len(s.workers) == 1
            a.heartbeat_active = False
            await a.heartbeat()
            assert len(s.workers) == 2
            # Since B is locked, this is ensured to originate from A
            await f2

    assert "Unexpected worker completed task" in s_logs.getvalue()

    # Ensure that all in-memory tasks on A have been restored on the
    # scheduler after reconnect
    for ts in a.tasks.values():
        if ts.state == "memory":
            assert a.address in {ws.address for ws in s.tasks[ts.key].who_has}

    # Ensure that all keys have been properly registered and will also be
    # cleaned up nicely.
    del f1, f2

    while any(w.tasks for w in [a, b]):
        await asyncio.sleep(0.001)


@gen_cluster(client=True)
async def test_worker_reconnects_mid_compute_multiple_states_on_scheduler(c, s, a, b):
    """
    Ensure that a reconnecting worker does not break the scheduler regardless of
    what state the keys of the worker are in when it connects back

    See also test_worker_reconnects_mid_compute which uses a smaller chain of
    tasks and does not release f1 in between
    """

    with captured_logger("distributed.scheduler") as s_logs:
        # Let's put one task in memory to ensure the reconnect has tasks in
        # different states
        f1 = c.submit(inc, 1, workers=[a.address], allow_other_workers=True)
        f2 = c.submit(inc, f1, workers=[a.address], allow_other_workers=True)
        a_address = a.address

        a.periodic_callbacks["heartbeat"].stop()
        await a.heartbeat()
        a.heartbeat_active = True

        from distributed import Lock

        def fast_on_a(lock):
            w = get_worker()
            import time

            if w.address != a_address:
                lock.acquire()
            else:
                time.sleep(1)

        lock = Lock()
        # We want to be sure that A is the only one computing this result
        async with lock:

            f3 = c.submit(
                fast_on_a, lock, workers=[a.address], allow_other_workers=True
            )

            while f3.key not in a.tasks:
                await asyncio.sleep(0.01)

            await s.stream_comms[a.address].close()
            f1.release()
            assert len(s.workers) == 1
            while s.tasks[f1.key].state != "released":
                await asyncio.sleep(0)
            a.heartbeat_active = False
            await a.heartbeat()
            assert len(s.workers) == 2
            # Since B is locked, this is ensured to originate from A
            await f3

    assert "Unexpected worker completed task" in s_logs.getvalue()

    # Ensure that all in-memory tasks on A have been restored on the
    # scheduler after reconnect
    for ts in a.tasks.values():
        if ts.state == "memory":
            assert a.address in {ws.address for ws in s.tasks[ts.key].who_has}

    del f1, f2, f3
    while any(w.tasks for w in [a, b]):
        await asyncio.sleep(0.001)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_forget_dependents_after_release(c, s, a):

    fut = c.submit(inc, 1, key="f-1")
    fut2 = c.submit(inc, fut, key="f-2")

    await asyncio.wait([fut, fut2])

    assert fut.key in a.tasks
    assert fut2.key in a.tasks
    assert fut2.key in {d.key for d in a.tasks[fut.key].dependents}

    fut2.release()

    while fut2.key in a.tasks:
        await asyncio.sleep(0.001)
    assert fut2.key not in {d.key for d in a.tasks[fut.key].dependents}


@gen_cluster(client=True)
async def test_steal_during_task_deserialization(c, s, a, b, monkeypatch):
    stealing_ext = s.extensions["stealing"]
    stealing_ext._pc.stop()
    from distributed.utils import ThreadPoolExecutor

    class CountingThreadPool(ThreadPoolExecutor):
        counter = 0

        def submit(self, *args, **kwargs):
            CountingThreadPool.counter += 1
            return super().submit(*args, **kwargs)

    # Ensure we're always offloading
    monkeypatch.setattr("distributed.worker.OFFLOAD_THRESHOLD", 1)
    threadpool = CountingThreadPool(
        max_workers=1, thread_name_prefix="Counting-Offload-Threadpool"
    )
    try:
        monkeypatch.setattr("distributed.utils._offload_executor", threadpool)

        class SlowDeserializeCallable:
            def __init__(self, delay=0.1):
                self.delay = delay

            def __getstate__(self):
                return self.delay

            def __setstate__(self, state):
                delay = state
                import time

                time.sleep(delay)
                return SlowDeserializeCallable(delay)

            def __call__(self, *args, **kwargs):
                return 41

        slow_deserialized_func = SlowDeserializeCallable()
        fut = c.submit(
            slow_deserialized_func, 1, workers=[a.address], allow_other_workers=True
        )

        while CountingThreadPool.counter == 0:
            await asyncio.sleep(0)

        ts = s.tasks[fut.key]
        a.handle_steal_request(fut.key, stimulus_id="test")
        stealing_ext.scheduler.send_task_to_worker(b.address, ts)

        fut2 = c.submit(inc, fut, workers=[a.address])
        fut3 = c.submit(inc, fut2, workers=[a.address])

        assert await fut2 == 42
        await fut3

    finally:
        threadpool.shutdown()


@gen_cluster(client=True)
async def test_gather_dep_exception_one_task(c, s, a, b):
    """Ensure an exception in a single task does not tear down an entire batch of gather_dep


    See also https://github.com/dask/distributed/issues/5152
    See also test_gather_dep_exception_one_task_2
    """
    fut = c.submit(inc, 1, workers=[a.address], key="f1")
    fut2 = c.submit(inc, 2, workers=[a.address], key="f2")
    fut3 = c.submit(inc, 3, workers=[a.address], key="f3")

    import asyncio

    event = asyncio.Event()
    write_queue = asyncio.Queue()
    event.clear()
    b.rpc = _LockedCommPool(b.rpc, write_event=event, write_queue=write_queue)
    b.rpc.remove(a.address)

    def sink(a, b, *args):
        return a + b

    res1 = c.submit(sink, fut, fut2, fut3, workers=[b.address])
    res2 = c.submit(sink, fut, fut2, workers=[b.address])

    # Wait until we're sure the worker is attempting to fetch the data
    while True:
        peer_addr, msg = await write_queue.get()
        if peer_addr == a.address and msg["op"] == "get_data":
            break

    # Provoke an "impossible transision exception"
    # By choosing a state which doesn't exist we're not running into validation
    # errors and the state machine should raise if we want to transition from
    # fetch to memory

    b.validate = False
    b.tasks[fut3.key].state = "fetch"
    event.set()

    assert await res1 == 5
    assert await res2 == 5

    del res1, res2, fut, fut2
    fut3.release()

    while a.tasks and b.tasks:
        await asyncio.sleep(0.1)


@gen_cluster(client=True)
async def test_gather_dep_exception_one_task_2(c, s, a, b):
    """Ensure an exception in a single task does not tear down an entire batch of gather_dep

    The below triggers an fetch->memory transition

    See also https://github.com/dask/distributed/issues/5152
    See also test_gather_dep_exception_one_task
    """
    # This test does not trigger the condition reliably but is a very easy case
    # which should function correctly regardles

    fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
    fut2 = c.submit(inc, fut1, workers=[b.address], key="f2")

    while fut1.key not in b.tasks or b.tasks[fut1.key].state == "flight":
        await asyncio.sleep(0)

    s.handle_missing_data(key="f1", errant_worker=a.address)

    await fut2


def _acquire_replicas(scheduler, worker, *futures):
    keys = [f.key for f in futures]

    scheduler.stream_comms[worker.address].send(
        {
            "op": "acquire-replicas",
            "keys": keys,
            "stimulus_id": f"acquire-replicas-{time()}",
            "priorities": {key: scheduler.tasks[key].priority for key in keys},
            "who_has": {
                key: {w.address for w in scheduler.tasks[key].who_has} for key in keys
            },
        },
    )


def _remove_replicas(scheduler, worker, *futures):
    keys = [f.key for f in futures]
    ws = scheduler.workers[worker.address]
    for k in keys:
        ts = scheduler.tasks[k]
        if ws in ts.who_has:
            scheduler.remove_replica(ts, ws)
    scheduler.stream_comms[ws.address].send(
        {
            "op": "remove-replicas",
            "keys": keys,
            "stimulus_id": f"remove-replicas-{time()}",
        }
    )


@gen_cluster(client=True)
async def test_acquire_replicas(c, s, a, b):
    fut = c.submit(inc, 1, workers=[a.address])
    await fut

    _acquire_replicas(s, b, fut)

    while len(s.who_has[fut.key]) != 2:
        await asyncio.sleep(0.005)

    for w in (a, b):
        assert w.data[fut.key] == 2
        assert w.tasks[fut.key].state == "memory"

    fut.release()

    while b.tasks or a.tasks:
        await asyncio.sleep(0.005)


@gen_cluster(client=True)
async def test_acquire_replicas_same_channel(c, s, a, b):
    fut = c.submit(inc, 1, workers=[a.address], key="f-replica")
    futB = c.submit(inc, 2, workers=[a.address], key="f-B")
    futC = c.submit(inc, futB, workers=[b.address], key="f-C")
    await fut

    _acquire_replicas(s, b, fut)

    await futC
    while fut.key not in b.tasks:
        await asyncio.sleep(0.005)
    assert len(s.who_has[fut.key]) == 2

    # Ensure that both the replica and an ordinary dependency pass through the
    # same communication channel

    for f in [fut, futB]:
        assert any("request-dep" in msg for msg in b.story(f.key))
        assert any("gather-dependencies" in msg for msg in b.story(f.key))
        assert any(f.key in msg["keys"] for msg in b.incoming_transfer_log)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_acquire_replicas_many(c, s, *workers):
    futs = c.map(inc, range(10), workers=[workers[0].address])
    res = c.submit(sum, futs, workers=[workers[1].address])
    final = c.submit(slowinc, res, delay=0.5, workers=[workers[1].address])

    await wait(futs)

    _acquire_replicas(s, workers[2], *futs)

    # Worker 2 should normally not even be involved if there was no replication
    while not all(
        f.key in workers[2].tasks and workers[2].tasks[f.key].state == "memory"
        for f in futs
    ):
        await asyncio.sleep(0.01)

    assert all(ts.state == "memory" for ts in workers[2].tasks.values())

    assert await final == sum(map(inc, range(10))) + 1
    # All workers have a replica
    assert all(len(s.tasks[f.key].who_has) == 3 for f in futs)
    del futs, res, final

    while any(w.tasks for w in workers):
        await asyncio.sleep(0.001)


@gen_cluster(client=True)
async def test_remove_replica_simple(c, s, a, b):
    futs = c.map(inc, range(10), workers=[a.address])
    await wait(futs)
    _acquire_replicas(s, b, *futs)

    while not all(len(s.tasks[f.key].who_has) == 2 for f in futs):
        await asyncio.sleep(0.01)

    _remove_replicas(s, b, *futs)

    assert all(len(s.tasks[f.key].who_has) == 1 for f in futs)

    while b.tasks:
        await asyncio.sleep(0.01)

    # Ensure there is no delayed reply to re-register the key
    await asyncio.sleep(0.01)
    assert all(s.tasks[f.key].who_has == {s.workers[a.address]} for f in futs)


@gen_cluster(
    client=True,
    config={"distributed.comm.recent-messages-log-length": 1_000},
)
async def test_remove_replica_while_computing(c, s, *workers):
    futs = c.map(inc, range(10), workers=[workers[0].address])

    # All interesting things will happen on that worker
    w = workers[1]
    intermediate = c.map(slowinc, futs, delay=0.05, workers=[w.address])

    def reduce(*args, **kwargs):
        import time

        time.sleep(0.5)
        return

    final = c.submit(reduce, intermediate, workers=[w.address], key="final")

    while not any(f.key in w.tasks for f in intermediate):
        await asyncio.sleep(0.001)

    # The scheduler removes keys from who_has/has_what immediately
    # Make sure the worker responds to the rejection and the scheduler corrects
    # the state
    ws = s.workers[w.address]
    while not any(s.tasks[fut.key] in ws.has_what for fut in futs):
        await asyncio.sleep(0.001)

    _remove_replicas(s, w, *futs)
    # Scheduler removed keys immediately...
    assert not any(s.tasks[fut.key] in ws.has_what for fut in futs)
    # ... but the state is properly restored
    while not any(s.tasks[fut.key] in ws.has_what for fut in futs):
        await asyncio.sleep(0.01)

    # The worker should reject all of these since they are required
    while not all(fut.done() for fut in intermediate):
        _remove_replicas(s, w, *futs)
        await asyncio.sleep(0.01)

    await wait(intermediate)

    # If a request is rejected, the worker responds with an add-keys message to
    # reenlist the key in the schedulers state system to avoid race conditions,
    # see also https://github.com/dask/distributed/issues/5265
    rejections = set()
    for msg in w.log:
        if msg[0] == "remove-replica-rejected":
            rejections.update(msg[1])
    for rejected_key in rejections:

        def answer_sent(key):
            for batch in w.batched_stream.recent_message_log:
                for msg in batch:
                    if "op" in msg and msg["op"] == "add-keys" and key in msg["keys"]:
                        return True
            return False

        assert answer_sent(rejected_key)

    # Since intermediate is done, futs replicas may be removed.
    # They might be already gone due to the above remove replica calls
    _remove_replicas(s, w, *futs)

    while any(w.tasks[f.key].state != "released" for f in futs if f.key in w.tasks):
        await asyncio.sleep(0.001)

    # The scheduler actually gets notified about the removed replica
    while not all(len(s.tasks[f.key].who_has) == 1 for f in futs):
        await asyncio.sleep(0.001)

    await final
    del final, intermediate, futs

    while any(w.tasks for w in workers):
        await asyncio.sleep(0.001)


@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_who_has_consistent_remove_replica(c, s, *workers):
    a = workers[0]
    other_workers = {w for w in workers if w != a}
    f1 = c.submit(inc, 1, key="f1", workers=[w.address for w in other_workers])
    await wait(f1)
    for w in other_workers:
        _acquire_replicas(s, w, f1)

    while not len(s.tasks[f1.key].who_has) == len(other_workers):
        await asyncio.sleep(0)

    f2 = c.submit(inc, f1, workers=[a.address])

    # Wait just until the moment the worker received the task and scheduled the
    # task to be fetched, then remove the replica from the worker this one is
    # trying to get the data from. Ensure this is handled gracefully and no
    # suspicious counters are raised since this is expected behaviour when
    # removing replicas

    while f1.key not in a.tasks or a.tasks[f1.key].state != "flight":
        await asyncio.sleep(0)

    coming_from = None
    for w in other_workers:
        coming_from = w
        if w.address == a.tasks[f1.key].coming_from:
            break

    coming_from.handle_remove_replicas([f1.key], "test")

    await f2

    assert (f1.key, "missing-dep") in a.story(f1.key)
    assert a.tasks[f1.key].suspicious_count == 0
    assert s.tasks[f1.key].suspicious == 0


@gen_cluster(client=True)
async def test_missing_released_zombie_tasks(c, s, a, b):
    """
    Ensure that no fetch/flight tasks are left in the task dict of a
    worker after everything was released
    """
    a.total_in_connections = 0
    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[b.address])
    key = f1.key

    while key not in b.tasks or b.tasks[key].state != "fetch":
        await asyncio.sleep(0.01)

    await a.close(report=False)

    del f1, f2

    while b.tasks:
        await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_missing_released_zombie_tasks_2(c, s, a, b):
    a.total_in_connections = 0
    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[b.address])

    while f1.key not in b.tasks:
        await asyncio.sleep(0)

    ts = b.tasks[f1.key]
    assert ts.state == "fetch"

    # A few things can happen to clear who_has. The dominant process is upon
    # connection failure to a worker. Regardless of how the set was cleared, the
    # task will be transitioned to missing where the worker is trying to
    # reaquire this information from the scheduler. While this is happening on
    # worker side, the tasks are released and we want to ensure that no dangling
    # zombie tasks are left on the worker
    ts.who_has.clear()

    del f1, f2

    while b.tasks:
        await asyncio.sleep(0.01)

    story = b.story(ts)
    assert any("missing" in msg for msg in story)


@pytest.mark.slow
@gen_cluster(
    client=True,
    Worker=Nanny,
    nthreads=[("", 1)],
    config={"distributed.worker.memory.pause": 0.5},
    worker_kwargs={"memory_limit": 2 ** 29},  # 500 MiB
)
async def test_worker_status_sync(c, s, a):
    (ws,) = s.workers.values()

    while ws.status != Status.running:
        await asyncio.sleep(0.01)

    def leak():
        distributed._test_leak = "x" * 2 ** 28  # 250 MiB

    def clear_leak():
        del distributed._test_leak

    await c.run(leak)

    while ws.status != Status.paused:
        await asyncio.sleep(0.01)

    await c.run(clear_leak)

    while ws.status != Status.running:
        await asyncio.sleep(0.01)

    await s.retire_workers()

    while ws.status != Status.closed:
        await asyncio.sleep(0.01)

    events = [ev for _, ev in s.events[ws.address] if ev["action"] != "heartbeat"]
    assert events == [
        {"action": "add-worker"},
        {
            "action": "worker-status-change",
            "prev-status": "undefined",
            "status": "running",
        },
        {
            "action": "worker-status-change",
            "prev-status": "running",
            "status": "paused",
        },
        {
            "action": "worker-status-change",
            "prev-status": "paused",
            "status": "running",
        },
        {"action": "remove-worker", "processing-tasks": {}},
        {"action": "retired"},
    ]


async def _wait_for_flight(key, worker):
    while key not in worker.tasks or worker.tasks[key].state != "flight":
        await asyncio.sleep(0)


@pytest.mark.xfail(reason="#5406")
@gen_cluster(client=True)
async def test_gather_dep_do_not_handle_response_of_not_requested_tasks(c, s, a, b):
    """At time of writing, the gather_dep implementation filtered tasks again
    for in-flight state. The response parser, however, did not distinguish
    resulting in unwanted missing-data signals to the scheduler, causing
    potential rescheduling or data leaks.
    This test may become obsolete if the implementation changes significantly.
    """
    import distributed

    with mock.patch.object(distributed.worker.Worker, "gather_dep") as mocked_gather:
        fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
        fut2 = c.submit(inc, fut1, workers=[a.address], key="f2")
        await fut2
        fut4 = c.submit(sum, fut1, fut2, workers=[b.address], key="f4")
        fut3 = c.submit(inc, fut1, workers=[b.address], key="f3")

        fut2_key = fut2.key

        await _wait_for_flight(fut2_key, b)

        fut4.release()
        while fut4.key in b.tasks:
            await asyncio.sleep(0)

    story_before = b.story(fut2.key)
    assert fut2.key in mocked_gather.call_args.kwargs["to_gather"]
    await Worker.gather_dep(b, **mocked_gather.call_args.kwargs)
    story_after = b.story(fut2.key)
    assert story_before == story_after
    await fut3


@pytest.mark.xfail(reason="#5406")
@gen_cluster(
    client=True,
    config={
        "distributed.comm.recent-messages-log-length": 1000,
    },
)
async def test_gather_dep_no_longer_in_flight_tasks(c, s, a, b):
    import distributed

    with mock.patch.object(distributed.worker.Worker, "gather_dep") as mocked_gather:
        fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
        fut2 = c.submit(sum, fut1, fut1, workers=[b.address], key="f2")

        fut1_key = fut1.key

        await _wait_for_flight(fut1_key, b)

        fut2.release()
        while fut2.key in b.tasks:
            await asyncio.sleep(0)

    assert b.tasks[fut1.key] != "flight"
    log_before = list(b.log)
    await Worker.gather_dep(b, **mocked_gather.call_args.kwargs)
    assert log_before == list(b.log)
