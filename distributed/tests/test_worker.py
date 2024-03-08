from __future__ import annotations

import asyncio
import gc
import importlib
import itertools
import logging
import os
import random
import sys
import tempfile
import threading
import traceback
import warnings
import weakref
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from numbers import Number
from operator import add
from time import sleep
from unittest import mock

import psutil
import pytest
from tlz import first, pluck, sliding_window
from tornado.ioloop import IOLoop

import dask
from dask import delayed
from dask.system import CPU_COUNT
from dask.utils import tmpfile

from distributed import (
    Client,
    Event,
    Nanny,
    WorkerPlugin,
    default_client,
    get_client,
    get_worker,
    profile,
    wait,
)
from distributed.comm.registry import backends
from distributed.comm.utils import OFFLOAD_THRESHOLD
from distributed.compatibility import LINUX, WINDOWS
from distributed.core import CommClosedError, Status, rpc
from distributed.diagnostics.plugin import ForwardOutput
from distributed.metrics import time
from distributed.protocol import pickle
from distributed.scheduler import KilledWorker, Scheduler
from distributed.utils import wait_for
from distributed.utils_test import (
    NO_AMM,
    BlockedExecute,
    BlockedGatherDep,
    BlockedGetData,
    TaskStateMetadataPlugin,
    _LockedCommPool,
    assert_story,
    async_poll_for,
    captured_logger,
    dec,
    div,
    freeze_batched_send,
    freeze_data_fetching,
    gen_cluster,
    gen_test,
    inc,
    mul,
    nodebug,
    raises_with_cause,
    slowinc,
    slowsum,
    wait_for_state,
)
from distributed.worker import (
    Worker,
    benchmark_disk,
    benchmark_memory,
    benchmark_network,
    error_message,
    logger,
)
from distributed.worker_state_machine import (
    AcquireReplicasEvent,
    ComputeTaskEvent,
    ExecuteFailureEvent,
    ExecuteSuccessEvent,
    RemoveReplicasEvent,
)

pytestmark = pytest.mark.ci1


@gen_cluster(nthreads=[])
async def test_worker_nthreads(s):
    async with Worker(s.address) as w:
        assert w.executor._max_workers == CPU_COUNT


@gen_cluster()
async def test_str(s, a, b):
    assert a.address in str(a)
    assert a.address in repr(a)
    assert str(a.state.nthreads) in str(a)
    assert str(a.state.nthreads) in repr(a)
    assert str(a.state.executing_count) in repr(a)


@gen_cluster(nthreads=[])
async def test_identity(s):
    async with Worker(s.address) as w:
        ident = w.identity()
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
    assert not a.state.executing_count
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

    assert not b.state.executing_count
    assert y.status == "error"
    # Make sure job died because of bad func and not because of bad
    # argument.
    with pytest.raises(ZeroDivisionError):
        await y

    tb = await y._traceback()
    assert any("1 / 0" in line for line in pluck(3, traceback.extract_tb(tb)) if line)
    assert "Compute Failed" in hdlr.messages["warning"][0]
    assert y.key in hdlr.messages["warning"][0]
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

    async with rpc(a.address) as aa, rpc(b.address) as bb:
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

    await s.close()
    await c.close()

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
    async with rpc(a.address) as aa:
        await aa.upload_file(filename="myfile.dat", data=b"0" * 100000000)
        await asyncio.sleep(0.05)
        assert a.digests["tick-duration"].components[0].max() < 0.050


@gen_cluster()
async def test_broadcast(s, a, b):
    async with rpc(s.address) as cc:
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
            with raises_with_cause(
                RuntimeError, None, ValueError, match_cause="Could not start Worker"
            ):  # No more ports left
                async with Worker(s.address, port=port):
                    pass


@pytest.mark.slow
@gen_test(timeout=60)
async def test_worker_waits_for_scheduler():
    w = Worker("127.0.0.1:8724")

    async def f():
        await w

    task = asyncio.create_task(f())
    await asyncio.sleep(3)
    assert not task.done()
    task.cancel()

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


@pytest.mark.slow
@pytest.mark.parametrize("sync", [True, False])
@pytest.mark.parametrize(
    "exc_type", [BaseException, SystemExit, KeyboardInterrupt, asyncio.CancelledError]
)
@gen_cluster(
    nthreads=[("", 1)],
    client=True,
    Worker=Nanny,
    config={"distributed.scheduler.allowed-failures": 0},
)
async def test_base_exception_in_task(c, s, a, sync, exc_type):
    if sync:

        def raiser():
            raise exc_type(f"this is a {exc_type}")

    else:

        async def raiser():
            raise exc_type(f"this is a {exc_type}")

    f = c.submit(raiser)

    try:
        with pytest.raises(
            KilledWorker if exc_type in (SystemExit, KeyboardInterrupt) else exc_type
        ):
            await f
    except BaseException as e:
        # Prevent test failure from killing the whole pytest process
        traceback.print_exc()
        pytest.fail(f"BaseException propagated back to test: {e!r}. See stdout.")

    # Nanny restarts it
    await c.wait_for_workers(1)


@gen_test()
async def test_plugin_exception():
    class MyPlugin(WorkerPlugin):
        def setup(self, worker=None):
            raise ValueError("Setup failed")

    async with Scheduler(port=0, dashboard_address=":0") as s:
        with raises_with_cause(
            RuntimeError, "Worker failed to start", ValueError, "Setup failed"
        ):
            async with Worker(s.address, plugins={MyPlugin()}):
                pass


@gen_test()
async def test_plugin_multiple_exceptions():
    class MyPlugin1(WorkerPlugin):
        def setup(self, worker=None):
            raise ValueError("MyPlugin1 Error")

    class MyPlugin2(WorkerPlugin):
        def setup(self, worker=None):
            raise RuntimeError("MyPlugin2 Error")

    async with Scheduler(port=0, dashboard_address=":0") as s:
        # There's no guarantee on the order of which exception is raised first
        with raises_with_cause(
            RuntimeError,
            None,
            (ValueError, RuntimeError),
            match_cause="MyPlugin.* Error",
        ):
            with captured_logger("distributed.worker") as logger:
                async with Worker(
                    s.address,
                    plugins={
                        MyPlugin1(),
                        MyPlugin2(),
                    },
                ) as w:
                    pass

            text = logger.getvalue()
            assert "MyPlugin1 Error" in text
            assert "MyPlugin2 Error" in text


@gen_test()
async def test_plugin_internal_exception():
    async with Scheduler(port=0) as s:
        with raises_with_cause(
            RuntimeError,
            "Worker failed to start",
            UnicodeDecodeError,
            match_cause="codec can't decode",
        ):
            async with Worker(
                s.address,
                plugins={
                    b"corrupting pickle" + pickle.dumps(lambda: None),
                },
            ) as w:
                pass


@gen_cluster(client=True)
async def test_gather(c, s, a, b):
    x, y = await c.scatter(["x", "y"], workers=[b.address])
    async with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], y.key: [b.address]})

    assert resp == {"status": "OK"}
    assert a.data[x.key] == b.data[x.key] == "x"
    assert a.data[y.key] == b.data[y.key] == "y"


@gen_cluster(client=True)
async def test_gather_missing_keys(c, s, a, b):
    """A key is missing. Other keys are gathered successfully."""
    x = await c.scatter("x", workers=[b.address])
    async with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], "y": [b.address]})

    assert resp == {"status": "partial-fail", "keys": ("y",)}
    assert a.data[x.key] == b.data[x.key] == "x"


@gen_cluster(client=True, worker_kwargs={"timeout": "100ms"})
async def test_gather_missing_workers(c, s, a, b):
    """A worker owning the only copy of a key is missing.
    Keys from other workers are gathered successfully.
    """
    assert b.address.startswith("tcp://127.0.0.1:")
    bad_addr = "tcp://127.0.0.1:12345"
    x = await c.scatter("x", workers=[b.address])

    async with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: [b.address], "y": [bad_addr]})

    assert resp == {"status": "partial-fail", "keys": ("y",)}
    assert a.data[x.key] == b.data[x.key] == "x"


@pytest.mark.slow
@pytest.mark.parametrize("know_real", [False, True, True, True, True])  # Read below
@gen_cluster(client=True, worker_kwargs={"timeout": "1s"}, config=NO_AMM)
async def test_gather_missing_workers_replicated(c, s, a, b, know_real):
    """A worker owning a redundant copy of a key is missing.
    The key is successfully gathered from other workers.

    know_real=False
        gather() will try to connect to the bad address, fail, and then query the
        scheduler who will respond with the good address. Then gather will successfully
        retrieve the key from the good address.
    know_real=True
        50% of the times, gather() will try to connect to the bad address, fail, and
        immediately connect to the good address.
        The other 50% of the times it will directly connect to the good address,
        hence why this test is repeated.
    """
    assert b.address.startswith("tcp://127.0.0.1:")
    x = await c.scatter("x", workers=[b.address])
    bad_addr = "tcp://127.0.0.1:12345"
    addrs = [bad_addr, b.address] if know_real else [bad_addr]
    async with rpc(a.address) as aa:
        resp = await aa.gather(who_has={x.key: addrs})
    assert resp == {"status": "OK"}
    assert a.data[x.key] == b.data[x.key] == "x"


@gen_cluster(nthreads=[])
async def test_io_loop(s):
    async with Worker(s.address) as w:
        assert w.io_loop is w.loop is s.loop


@gen_cluster(nthreads=[])
async def test_io_loop_alternate_loop(s, loop):
    with pytest.warns(
        DeprecationWarning,
        match=r"The `loop` argument to `Worker` is ignored, and will be "
        r"removed in a future release. The Worker always binds to the current loop",
    ):
        async with Worker(s.address, loop=loop) as w:
            assert w.io_loop is w.loop is IOLoop.current()


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
async def test_run_dask_worker_kwonlyarg(c, s, a, b):
    def f(*, dask_worker=None):
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


@gen_cluster(nthreads=[("127.0.0.1", 1)])
async def test_close_on_disconnect(s, w):
    await s.close()
    await async_poll_for(lambda: w.status == Status.closed, timeout=5)


@gen_cluster(nthreads=[])
async def test_memory_limit_auto(s):
    async with Worker(s.address, nthreads=1) as a, Worker(
        s.address, nthreads=2
    ) as b, Worker(s.address, nthreads=100) as c, Worker(s.address, nthreads=200) as d:
        assert isinstance(a.memory_manager.memory_limit, Number)
        assert isinstance(b.memory_manager.memory_limit, Number)

        if CPU_COUNT > 1:
            assert a.memory_manager.memory_limit < b.memory_manager.memory_limit

        assert c.memory_manager.memory_limit == d.memory_manager.memory_limit


@gen_cluster(client=True)
async def test_inter_worker_communication(c, s, a, b):
    [x, y] = await c._scatter([1, 2], workers=a.address)

    future = c.submit(add, x, y, workers=b.address)
    result = await future
    assert result == 3


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_clean(c, s, a):
    x = c.submit(inc, 1)
    await x

    collections = [a.state.tasks, a.data, a.threads]
    assert all(collections)

    x.release()

    while x.key in a.state.tasks:
        await asyncio.sleep(0.01)

    assert not any(collections)


@gen_cluster(client=True)
async def test_message_breakup(c, s, a, b):
    n = 100_000
    a.state.transfer_message_bytes_limit = 10 * n
    b.state.transfer_message_bytes_limit = 10 * n
    xs = [
        c.submit(mul, b"%d" % i, n, key=f"x{i}", workers=[a.address]) for i in range(30)
    ]
    y = c.submit(lambda _: None, xs, key="y", workers=[b.address])
    await y

    assert 2 <= len(b.transfer_incoming_log) <= 20
    assert 2 <= len(a.transfer_outgoing_log) <= 20

    assert all(msg["who"] == b.address for msg in a.transfer_outgoing_log)
    assert all(msg["who"] == a.address for msg in a.transfer_incoming_log)


@gen_cluster(client=True)
async def test_types(c, s, a, b):
    assert all(ts.type is None for ts in a.state.tasks.values())
    assert all(ts.type is None for ts in b.state.tasks.values())
    x = c.submit(inc, 1, workers=a.address)
    await wait(x)
    assert a.state.tasks[x.key].type == int

    y = c.submit(inc, x, workers=b.address)
    await wait(y)
    assert b.state.tasks[x.key].type == int
    assert b.state.tasks[y.key].type == int

    await c._cancel(y)

    await async_poll_for(lambda: y.key not in b.data, timeout=5)
    assert y.key not in b.state.tasks


@gen_cluster()
async def test_system_monitor(s, a, b):
    assert b.monitor
    b.monitor.update()


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 2, {"resources": {"A": 1}}), ("127.0.0.1", 1)]
)
async def test_restrictions(c, s, a, b):
    # Worker has resource available
    assert a.state.available_resources == {"A": 1}
    # Resource restrictions
    x = c.submit(inc, 1, resources={"A": 1})
    await x
    ts = a.state.tasks[x.key]
    assert ts.resource_restrictions == {"A": 1}
    await c.cancel([x])

    while ts.state == "executing":
        # Resource should be unavailable while task isn't finished
        assert a.state.available_resources == {"A": 0}
        await asyncio.sleep(0.01)

    # Resource restored after task is in memory
    assert a.state.available_resources["A"] == 1


@gen_cluster(client=True)
async def test_clean_nbytes(c, s, a, b):
    L = [delayed(inc)(i) for i in range(10)]
    for _ in range(5):
        L = [delayed(add)(x, y) for x, y in sliding_window(2, L)]
    total = delayed(sum)(L)

    future = c.compute(total)
    await wait(future)

    await asyncio.sleep(1)
    assert (
        len(list(filter(None, [ts.nbytes for ts in a.state.tasks.values()])))
        + len(list(filter(None, [ts.nbytes for ts in b.state.tasks.values()])))
        == 1
    )


@pytest.mark.parametrize("as_deps", [True, False])
@gen_cluster(client=True, nthreads=[("", 1)] * 21)
async def test_gather_many_small(c, s, a, *snd_workers, as_deps):
    """If the dependencies of a given task are very small, do not limit the number of
    concurrent outgoing connections. If multiple small fetches from the same worker are
    scheduled all at once, they will result in a single call to gather_dep.
    """
    a.state.transfer_incoming_count_limit = 2
    futures = await c.scatter(
        {f"x{i}": i for i in range(100)},
        workers=[w.address for w in snd_workers],
    )
    assert all(w.data for w in snd_workers)

    if as_deps:
        future = c.submit(lambda _: None, futures, key="y", workers=[a.address])
        await wait(future)
    else:
        s.request_acquire_replicas(a.address, list(futures), stimulus_id="test")
        while len(a.data) < 100:
            await asyncio.sleep(0.01)

    assert a.state.transfer_incoming_bytes == 0

    story = a.state.story("request-dep", "receive-dep")
    assert len(story) == 40  # 1 request-dep + 1 receive-dep per sender worker
    # All GatherDep instructions are fired at the same time; each fetches all keys
    # available on the sender worker
    for ev in story[:20]:
        assert ev[0] == "request-dep"
        assert len(ev[2]) > 1
    for ev in story[20:]:
        assert ev[0] == "receive-dep"


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3)
async def test_multiple_transfers(c, s, w1, w2, w3):
    x = c.submit(inc, 1, workers=w1.address)
    y = c.submit(inc, 2, workers=w2.address)
    z = c.submit(add, x, y, workers=w3.address)

    await wait(z)

    r = w3.state.tasks[z.key].startstops
    transfers = [t for t in r if t["action"] == "transfer"]
    assert len(transfers) == 2


@pytest.mark.xfail(reason="very high flakiness")
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 3,
    config=NO_AMM,
)
async def test_share_communication(c, s, w1, w2, w3):
    x = c.submit(
        mul, b"1", int(w3.transfer_message_bytes_limit + 1), workers=w1.address
    )
    y = c.submit(
        mul, b"2", int(w3.transfer_message_bytes_limit + 1), workers=w2.address
    )
    await wait([x, y])
    await c.replicate([x, y], workers=[w1.address, w2.address])
    z = c.submit(add, x, y, workers=w3.address)
    await wait(z)
    assert len(w3.transfer_incoming_log) == 2
    assert w1.transfer_outgoing_log
    assert w2.transfer_outgoing_log


@pytest.mark.xfail(reason="very high flakiness")
@gen_cluster(client=True)
async def test_dont_overlap_communications_to_same_worker(c, s, a, b):
    x = c.submit(mul, b"1", int(b.transfer_message_bytes_limit + 1), workers=a.address)
    y = c.submit(mul, b"2", int(b.transfer_message_bytes_limit + 1), workers=a.address)
    await wait([x, y])
    z = c.submit(add, x, y, workers=b.address)
    await wait(z)
    assert len(b.transfer_incoming_log) == 2
    l1, l2 = b.transfer_incoming_log

    assert l1["stop"] < l2["start"]


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_log_event(c, s, a):
    def log_event(msg):
        w = get_worker()
        w.log_event("test-topic", msg)

    await c.submit(log_event, "foo")

    class C:
        pass

    with pytest.raises(TypeError, match="msgpack"):
        await c.submit(log_event, C())

    # Worker still works
    await c.submit(log_event, "bar")
    await c.submit(log_event, error_message(Exception()))

    # assertion reversed for mock.ANY.__eq__(Serialized())
    assert [
        "foo",
        "bar",
        {
            "status": "error",
            "exception": mock.ANY,
            "traceback": mock.ANY,
            "exception_text": "Exception()",
            "traceback_text": "",
            "worker": a.address,
        },
    ] == [msg[1] for msg in s.get_events("test-topic")]


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


@gen_cluster(client=True, config=NO_AMM)
async def test_hold_onto_dependents(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)
    await wait(y)

    assert x.key in b.data

    await c._cancel(y)
    while x.key not in b.data:
        await asyncio.sleep(0.1)


@pytest.mark.xfail(reason="asyncio.wait_for bug")
@gen_test()
async def test_worker_death_timeout():
    w = Worker("tcp://127.0.0.1:12345", death_timeout=0.1)
    with pytest.raises(asyncio.TimeoutError) as info:
        await w

    assert "Worker" in str(info.value)
    assert "timed out" in str(info.value)
    assert w.status == Status.failed


@gen_cluster(client=True)
async def test_stop_doing_unnecessary_work(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.01)
    await asyncio.sleep(0.1)

    del futures
    await async_poll_for(lambda: a.state.executing_count == 0, timeout=0.5)


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
        for t in w.state.log
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
def test_worker_dir(worker, tmp_path):
    @gen_cluster(client=True, worker_kwargs={"local_directory": str(tmp_path)})
    async def test_worker_dir(c, s, a, b):
        directories = [w.local_directory for w in s.workers.values()]
        assert all(d.startswith(str(tmp_path)) for d in directories)
        assert len(set(directories)) == 2  # distinct

    test_worker_dir()


@gen_cluster(client=True, nthreads=[], config={"temporary-directory": None})
async def test_default_worker_dir(c, s):
    expect = os.path.join(tempfile.gettempdir(), "dask-scratch-space")

    async with Worker(s.address) as w:
        assert os.path.dirname(w.local_directory) == expect

    async with Nanny(s.address) as n:
        assert n.local_directory == expect
        results = await c.run(lambda dask_worker: dask_worker.local_directory)
        assert os.path.dirname(results[n.worker_address]) == expect


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
        # TODO: the existence of `await get_client()` implies the possibility
        # of `async with get_client()` and that will kill the workers' client
        # if you do that. We really don't want users to do that.
        # https://github.com/dask/distributed/pull/6921/
        client = get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    results = await c.run(f)
    assert results == {a.address: 11, b.address: 11}


@pytest.mark.xfail(
    reason="Async tasks do not provide worker context. See https://github.com/dask/distributed/pull/7339"
)
def test_get_client_coroutine_sync(client, s, a, b):
    async def f():
        client = await get_client()
        future = client.submit(inc, 10)
        result = await future
        return result

    for w in [a, b]:
        assert client.submit(f, workers=[w["address"]]).result() == 1


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

    async with Worker(s.address):
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

    # See what happens with e.g. `dask worker --listen-address tcp://:8811`
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
@gen_cluster(
    client=True,
    config={
        "distributed.worker.profile.enabled": True,
    },
)
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
        "distributed.worker.profile.enabled": True,
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
    client=True,
    config={
        "distributed.worker.profile.enabled": True,
        "distributed.worker.profile.cycle": "10ms",
    },
)
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
async def test_scheduler_address_config(c, s):
    with dask.config.set({"scheduler-address": s.address}):
        async with Worker() as w:
            assert w.scheduler.address == s.address


@pytest.mark.xfail(reason="very high flakiness")
@pytest.mark.slow
@gen_cluster(client=True)
async def test_wait_for_outgoing(c, s, a, b):
    np = pytest.importorskip("numpy")
    x = np.random.random(10000000)
    future = await c.scatter(x, workers=a.address)

    y = c.submit(inc, future, workers=b.address)
    await wait(y)

    assert len(b.transfer_incoming_log) == len(a.transfer_outgoing_log) == 1
    bb = b.transfer_incoming_log[0]["duration"]
    aa = a.outgoing_transfer_log[0]["duration"]
    ratio = aa / bb

    assert 1 / 3 < ratio < 3


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(
    nthreads=[("127.0.0.1", 1), ("127.0.0.1", 1), ("127.0.0.2", 1)],
    client=True,
    config=NO_AMM,
)
async def test_prefer_gather_from_local_address(c, s, w1, w2, w3):
    x = await c.scatter(123, workers=[w1.address, w3.address], broadcast=True)

    y = c.submit(inc, x, workers=[w2.address])
    await wait(y)

    assert any(d["who"] == w2.address for d in w1.transfer_outgoing_log)
    assert not any(d["who"] == w2.address for d in w3.transfer_outgoing_log)


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 20,
    config={"distributed.worker.connections.incoming": 1},
)
async def test_avoid_oversubscription(c, s, *workers):
    np = pytest.importorskip("numpy")
    x = c.submit(np.random.random, 1000000, workers=[workers[0].address])
    await wait(x)

    futures = [c.submit(len, x, pure=False, workers=[w.address]) for w in workers[1:]]

    await wait(futures)

    # Original worker not responsible for all transfers
    assert len(workers[0].transfer_outgoing_log) < len(workers) - 2

    # Some other workers did some work
    assert len([w for w in workers if len(w.transfer_outgoing_log) > 0]) >= 3


@gen_cluster(client=True, worker_kwargs={"metrics": {"my_port": lambda w: w.port}})
async def test_custom_metrics(c, s, a, b):
    assert s.workers[a.address].metrics["my_port"] == a.port
    assert s.workers[b.address].metrics["my_port"] == b.port


@gen_cluster(client=True)
async def test_register_worker_callbacks(c, s, a, b):
    # plugin function to run
    def mystartup(dask_worker):
        dask_worker.init_variable = 1

    def mystartup2():
        import os

        os.environ["MY_ENV_VALUE"] = "WORKER_ENV_VALUE"
        return "Env set."

    # Check that plugin function has been run
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
    async with Worker(s.address) as worker:
        result = await c.run(test_import, workers=[worker.address])
        assert list(result.values()) == [False]

    # Add a plugin function
    response = await c.register_worker_callbacks(setup=mystartup)
    assert len(response) == 2

    # Check it has been ran on existing worker
    result = await c.run(test_import)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is run on it
    async with Worker(s.address) as worker:
        result = await c.run(test_import, workers=[worker.address])
        assert list(result.values()) == [True]

    # Register another plugin function
    response = await c.register_worker_callbacks(setup=mystartup2)
    assert len(response) == 2

    # Check it has been run
    result = await c.run(test_startup2)
    assert list(result.values()) == [True] * 2

    # Start a worker and check it is run on it
    async with Worker(s.address) as worker:
        result = await c.run(test_import, workers=[worker.address])
        assert list(result.values()) == [True]
        result = await c.run(test_startup2, workers=[worker.address])
        assert list(result.values()) == [True]


@gen_cluster(client=True)
async def test_register_worker_callbacks_err(c, s, a, b):
    with pytest.raises(ZeroDivisionError):
        await c.register_worker_callbacks(setup=lambda: 1 / 0)


@gen_cluster(nthreads=[])
async def test_local_directory(s, tmp_path):
    with dask.config.set(temporary_directory=str(tmp_path)):
        async with Worker(s.address) as w:
            assert w.local_directory.startswith(str(tmp_path))
            assert "dask-scratch-space" in w.local_directory


@gen_cluster(nthreads=[])
async def test_local_directory_make_new_directory(s, tmp_path):
    async with Worker(s.address, local_directory=str(tmp_path / "foo" / "bar")) as w:
        assert w.local_directory.startswith(str(tmp_path))
        assert "foo" in w.local_directory
        assert "dask-scratch-space" in w.local_directory


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(nthreads=[], client=True)
async def test_host_address(c, s):
    async with Worker(s.address, host="127.0.0.2") as w:
        assert "127.0.0.2" in w.address

    async with Nanny(s.address, host="127.0.0.3") as n:
        assert "127.0.0.3" in n.address
        assert "127.0.0.3" in n.worker_address


@pytest.mark.parametrize("Worker", [Worker, Nanny])
@gen_test()
async def test_interface_async(Worker):
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


@pytest.mark.gpu
@pytest.mark.parametrize("Worker", [Worker, Nanny])
@gen_test()
async def test_protocol_from_scheduler_address(ucx_loop, Worker):
    pytest.importorskip("ucp")

    async with Scheduler(protocol="ucx", dashboard_address=":0") as s:
        assert s.address.startswith("ucx://")
        async with Worker(s.address) as w:
            assert w.address.startswith("ucx://")
            async with Client(s.address, asynchronous=True) as c:
                info = c.scheduler_info()
                assert info["address"].startswith("ucx://")


@gen_test()
async def test_host_uses_scheduler_protocol(monkeypatch):
    # Ensure worker uses scheduler's protocol to determine host address, not the default scheme
    # See https://github.com/dask/distributed/pull/4883
    from distributed.comm.tcp import TCPBackend

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


@pytest.mark.parametrize("Worker", [Worker, Nanny])
@gen_test()
async def test_worker_listens_on_same_interface_by_default(Worker):
    async with Scheduler(host="localhost", dashboard_address=":0") as s:
        assert s.ip in {"127.0.0.1", "localhost"}
        async with Worker(s.address) as w:
            assert s.ip == w.ip


def assert_amm_transfer_story(key: str, w_from: Worker, w_to: Worker) -> None:
    """Test that an in-memory key was transferred from worker w_from to worker w_to by
    the Active Memory Manager and it was not recalculated on w_to
    """
    assert_story(
        w_to.state.story(key),
        [
            (key, "fetch", "flight", "flight", {}),
            ("request-dep", w_from.address, lambda set_: key in set_),
            ("receive-dep", w_from.address, lambda set_: key in set_),
            (key, "put-in-memory"),
            (key, "flight", "memory", "memory", {}),
        ],
        strict=False,
    )
    assert key in w_to.data
    # The key may or may not still be in w_from.data, depending if the AMM had the
    # chance to run a second time after the copy was successful.


@pytest.mark.slow
@gen_cluster(client=True)
async def test_close_gracefully(c, s, a, b):
    futures = c.map(slowinc, range(200), delay=0.1, workers=[b.address])

    # Note: keys will appear in b.data several milliseconds before they switch to
    # status=memory in s.tasks. It's important to sample the in-memory keys from the
    # scheduler side, because those that the scheduler thinks are still processing won't
    # be replicated by retire_workers().
    while True:
        mem = {k for k, ts in s.tasks.items() if ts.state == "memory"}
        if len(mem) >= 8 and any(
            ts.state == "executing" for ts in b.state.tasks.values()
        ):
            break
        await asyncio.sleep(0.01)

    assert any(ts for ts in b.state.tasks.values() if ts.state == "executing")

    with captured_logger("distributed.worker") as logger:
        await b.close_gracefully(reason="foo")
    assert "Reason: foo" in logger.getvalue()

    assert b.status == Status.closed
    assert b.address not in s.workers

    # All tasks that were in memory in b have been copied over to a;
    # they have not been recomputed
    for key in mem:
        assert_amm_transfer_story(key, b, a)


@pytest.mark.slow
@gen_cluster(client=True, Worker=Nanny)
async def test_close_gracefully_no_suspicious_tasks(c, s, a, b):
    workers = {a.worker_address: a, b.worker_address: b}
    started = Event()
    block_task = Event()

    def blocking_task():
        started.set()
        block_task.wait()

    fut = c.submit(blocking_task)
    await started.wait()

    to_close = s.tasks[fut.key].processing_on.address

    async def close_gracefully(dask_worker):
        await asyncio.shield(dask_worker.close_gracefully())

    try:
        await c.run(close_gracefully, workers=[to_close])
    except CommClosedError:
        pass
    await async_poll_for(lambda: to_close not in s.workers, 5)

    assert b.address not in s.workers
    assert s.tasks[fut.key].suspicious == 0
    await block_task.set()
    await fut


@pytest.mark.parametrize("sync", [False, pytest.param(True, marks=[pytest.mark.slow])])
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_close_while_executing(c, s, a, sync):
    ev = Event()

    if sync:

        def f(ev):
            ev.set()
            sleep(2)

    else:

        async def f(ev):
            await ev.set()
            await asyncio.Event().wait()  # Block indefinitely

    f1 = c.submit(f, ev, key="f1")
    await ev.wait()
    task = next(
        task for task in asyncio.all_tasks() if "execute('f1')" in task.get_name()
    )
    await a.close()
    assert task.done()
    assert s.tasks["f1"].state in ("queued", "no-worker")


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_close_async_task_handles_cancellation(c, s, a):
    ev = Event()

    async def f(ev):
        await ev.set()
        try:
            await asyncio.Event().wait()  # Block indefinitely
        except asyncio.CancelledError:
            await asyncio.Event().wait()  # Ignore the first cancel()

    f1 = c.submit(f, ev, key="f1")
    await ev.wait()
    task = next(
        task for task in asyncio.all_tasks() if "execute('f1')" in task.get_name()
    )
    with captured_logger(
        "distributed.worker.state_machine", level=logging.ERROR
    ) as logger:
        await wait_for(a.close(timeout=1), timeout=5)
    assert "Failed to cancel asyncio task" in logger.getvalue()
    assert not task.cancelled()
    assert s.tasks["f1"].state in ("queued", "no-worker")
    task.cancel()
    await asyncio.wait({task})


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)], timeout=10)
async def test_lifetime(c, s, a):
    # Note: test was occasionally failing with lifetime="1 seconds"
    async with Worker(s.address, lifetime="2 seconds") as b:
        futures = c.map(slowinc, range(200), delay=0.1, workers=[b.address])

        # Note: keys will appear in b.data several milliseconds before they switch to
        # status=memory in s.tasks. It's important to sample the in-memory keys from the
        # scheduler side, because those that the scheduler thinks are still processing
        # won't be replicated by retire_workers().
        while True:
            mem = {k for k, ts in s.tasks.items() if ts.state == "memory"}
            if len(mem) >= 8:
                break
            await asyncio.sleep(0.01)

        assert b.status == Status.running
        assert not a.data

        while b.status != Status.closed:
            await asyncio.sleep(0.01)

    # All tasks that were in memory in b have been copied over to a;
    # they have not been recomputed
    for key in mem:
        assert_amm_transfer_story(key, b, a)


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
    """Bad startup functions do not cause the Worker to fail"""

    def bad_startup(w):
        raise Exception("Hello")

    async with Worker(s.address, startup_information={"bad": bad_startup}):
        pass


@gen_cluster(nthreads=[])
async def test_update_latency(s):
    async with Worker(s.address) as w:
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


@gen_cluster(nthreads=[("", 1)])
async def test_shutdown_on_scheduler_comm_closed(s, a):
    with captured_logger("distributed.core", level=logging.INFO) as logger:
        # Temporary network disconnect
        s.stream_comms[a.address].abort()

        await a.finished()
        assert a.status == Status.closed
        assert not s.workers
        assert not s.stream_comms
        assert f"Connection to {s.address} has been closed" in logger.getvalue()


@gen_cluster(nthreads=[])
async def test_heartbeat_comm_closed(s, monkeypatch):
    with captured_logger("distributed.worker", level=logging.WARNING) as logger:

        def bad_heartbeat_worker(*args, **kwargs):
            raise CommClosedError()

        async with Worker(s.address) as w:
            # Trigger CommClosedError during worker heartbeat
            monkeypatch.setattr(w.scheduler, "heartbeat_worker", bad_heartbeat_worker)

            await w.heartbeat()
            assert w.status == Status.running
    logs = logger.getvalue()
    assert "Failed to communicate with scheduler during heartbeat" in logs
    assert "Traceback" in logs


@gen_cluster(nthreads=[("", 1)], worker_kwargs={"heartbeat_interval": "100s"})
async def test_heartbeat_missing(s, a, monkeypatch):
    async def missing_heartbeat_worker(*args, **kwargs):
        return {"status": "missing"}

    with captured_logger("distributed.worker", level=logging.WARNING) as wlogger:
        monkeypatch.setattr(a.scheduler, "heartbeat_worker", missing_heartbeat_worker)
        await a.heartbeat()
        assert a.status == Status.closed
        assert "Scheduler was unaware of this worker" in wlogger.getvalue()

        while s.workers:
            await asyncio.sleep(0.01)


@gen_cluster(nthreads=[("", 1)], worker_kwargs={"heartbeat_interval": "100s"})
async def test_heartbeat_missing_real_cluster(s, a):
    # The idea here is to create a situation where `s.workers[a.address]`,
    # doesn't exist, but the worker is not yet closed and can still heartbeat.
    # Ideally, this state would be impossible, and this test would be removed,
    # and the `status: missing` handling would be replaced with an assertion error.
    # However, `Scheduler.remove_worker` and `Worker.close` both currently leave things
    # in degenerate, half-closed states while they're running (and yielding control
    # via `await`).
    # When https://github.com/dask/distributed/issues/6390 is fixed, this should no
    # longer be possible.

    assumption_msg = "Test assumptions have changed. Race condition may have been fixed; this test may be removable."

    with captured_logger(
        "distributed.worker", level=logging.WARNING
    ) as wlogger, captured_logger(
        "distributed.scheduler", level=logging.WARNING
    ) as slogger:
        with freeze_batched_send(s.stream_comms[a.address]):
            await s.remove_worker(a.address, stimulus_id="foo")
            assert not s.workers

            # The scheduler has removed the worker state, but the close message has
            # not reached the worker yet.
            assert a.status == Status.running, assumption_msg
            assert a.periodic_callbacks["heartbeat"].is_running(), assumption_msg

            # The heartbeat PeriodicCallback is still running, so one _could_ fire
            # before the `op: close` message reaches the worker. We simulate that explicitly.
            await a.heartbeat()

            # The heartbeat receives a `status: missing` from the scheduler, so it
            # closes the worker. Heartbeats aren't sent over batched comms, so
            # `freeze_batched_send` doesn't affect them.
            assert a.status == Status.closed

            assert "Scheduler was unaware of this worker" in wlogger.getvalue()
            assert "Received heartbeat from unregistered worker" in slogger.getvalue()

        assert not s.workers


@pytest.mark.slow
@gen_cluster(
    client=True,
    nthreads=[("", 1)],
    Worker=Nanny,
    worker_kwargs={"heartbeat_interval": "1ms"},
)
async def test_heartbeat_missing_doesnt_restart(c, s, n):
    """Read: https://github.com/dask/distributed/pull/8522"""
    old_heartbeat_handler = s.handlers["heartbeat_worker"]
    s.handlers["heartbeat_worker"] = lambda *args, **kwargs: {"status": "missing"}

    assert n.process
    await n.process.stopped.wait()

    assert not s.workers
    s.handlers["heartbeat_worker"] = old_heartbeat_handler
    assert n.status == Status.closing_gracefully
    assert not n.process.is_alive()


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
    async with Worker(s.address) as a:
        await c.register_plugin(TaskStateMetadataPlugin())

        f = c.submit(inc, 1)
        await f

        ts = a.state.tasks[f.key]
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

    monkeypatch.setattr("distributed.comm.utils.OFFLOAD_THRESHOLD", 1)

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
    ts = w.state.tasks[future.key]
    assert ts.state in str(w.state.story(ts))
    assert w.state.story(ts) == w.state.story(ts.key)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_stimulus_story(c, s, a):
    # Test that substrings aren't matched by stimulus_story()
    f = c.submit(inc, 1, key="f")
    f1 = c.submit(lambda: "foo", key="f1")
    f2 = c.submit(inc, f1, key="f2")  # This will fail
    await wait([f, f1, f2])

    story = a.state.stimulus_story("f1", "f2")
    assert len(story) == 4

    assert isinstance(story[0], ComputeTaskEvent)
    assert story[0].key == "f1"
    assert story[0].run_spec is None  # Not logged

    assert isinstance(story[1], ExecuteSuccessEvent)
    assert story[1].key == "f1"
    assert story[1].value is None  # Not logged
    assert story[1].handled >= story[0].handled

    assert isinstance(story[2], ComputeTaskEvent)
    assert story[2].key == "f2"
    assert story[2].who_has == {"f1": (a.address,)}
    assert story[2].run_spec is None  # Not logged
    assert story[2].handled >= story[1].handled

    assert isinstance(story[3], ExecuteFailureEvent)
    assert story[3].key == "f2"
    assert story[3].handled >= story[2].handled


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_worker_descopes_data(c, s, a):
    """Test that data is released on the worker:
    1. when it's the output of a successful task
    2. when it's the input of a failed task
    3. when it's a local variable in the frame of a failed task
    4. when it's embedded in the exception of a failed task
    """

    class C:
        instances = weakref.WeakSet()

        def __init__(self):
            C.instances.add(self)

    def f(x):
        y = C()
        raise Exception(x, y)

    f1 = c.submit(C, key="f1")
    f2 = c.submit(f, f1, key="f2")
    await wait([f2])

    assert type(a.data["f1"]) is C

    del f1
    del f2
    while a.data:
        await asyncio.sleep(0.01)
    with profile.lock:
        gc.collect()
    assert not C.instances


@gen_cluster(client=True, config=NO_AMM)
async def test_gather_dep_one_worker_always_busy(c, s, a, b):
    # Ensure that both dependencies for H are on another worker than H itself.
    # The worker where the dependencies are on is then later blocked such that
    # the data cannot be fetched
    # In the past it was important that there is more than one key on the
    # worker. This should be kept to avoid any edge case specific to one
    f = c.submit(inc, 1, key="f", workers=[a.address])
    g = c.submit(inc, 2, key="g", workers=[a.address])
    await wait([f, g])
    assert set(a.state.tasks) == {"f", "g"}

    # We will block A for any outgoing communication. This simulates an
    # overloaded worker which will always return "busy" for get_data requests,
    # effectively blocking H indefinitely
    a.transfer_outgoing_count = 10000000

    h = c.submit(add, f, g, key="h", workers=[b.address])

    await wait_for_state(h.key, "waiting", b)
    assert b.state.tasks[f.key].state in ("flight", "fetch")
    assert b.state.tasks[g.key].state in ("flight", "fetch")

    with pytest.raises(asyncio.TimeoutError):
        await h.result(timeout=0.8)

    story = b.state.story("gather-dep-busy")
    # 1 busy response straight away, followed by 1 retry every 150ms for 800ms.
    # The requests for b and g are clustered together in single messages.
    # We need to be very lax in measuring as PeriodicCallback+network comms have been
    # observed on CI to occasionally lag behind by several hundreds of ms.
    assert 2 <= len(story) <= 8

    async with Worker(s.address, name="x") as x:
        # We "scatter" the data to another worker which is able to serve this data.
        # In reality this could be another worker which fetched this dependency and got
        # through to A or another worker executed the task using work stealing or any
        # other. To avoid cross effects, we'll just put the data onto the worker
        # ourselves. Note that doing so means that B won't know about the existence of
        # the extra replicas until it takes the initiative to invoke scheduler.who_has.
        x.update_data({"f": 2, "g": 3})
        s.add_keys(worker=x.address, keys=("f", "g"))
        assert await h == 5


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)] * 2 + [("127.0.0.2", 2)] * 10,  # type: ignore
    config=NO_AMM,
)
async def test_gather_dep_local_workers_first(c, s, a, lw, *rws):
    f = (
        await c.scatter(
            {"f": 1}, workers=[w.address for w in (lw,) + rws], broadcast=True
        )
    )["f"]
    g = c.submit(inc, f, key="g", workers=[a.address])
    assert await g == 2
    assert_story(a.state.story("f"), [("receive-dep", lw.address, {"f"})])


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.2", 1)] + [("127.0.0.1", 1)] * 10,  # type: ignore
    config=NO_AMM,
)
async def test_gather_dep_from_remote_workers_if_all_local_workers_are_busy(
    c, s, rw, a, *lws
):
    f = (
        await c.scatter(
            {"f": 1}, workers=[w.address for w in (rw,) + lws], broadcast=True
        )
    )["f"]
    for w in lws:
        w.transfer_outgoing_count = 10000000

    g = c.submit(inc, f, key="g", workers=[a.address])
    assert await g == 2

    # Tried fetching from each local worker exactly once before falling back to the
    # remote worker
    assert sorted(ev[1] for ev in a.state.story("gather-dep-busy")) == sorted(
        w.address for w in lws
    )
    assert_story(a.state.story("receive-dep"), [("receive-dep", rw.address, {"f"})])


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
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

    wclient = await c.submit(get_worker_client_id)
    assert wclient == existing_client

    assert not Worker._initialized_clients

    await a.close()

    assert len(Client._instances) == 1
    assert c.status == "running"
    c_def = default_client()
    assert c is c_def


@gen_cluster(nthreads=[("127.0.0.1", 1)])
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
async def test_bad_executor_annotation(c, s, a, b):
    with dask.annotate(executor="bad"):
        future = c.submit(inc, 1)
    with pytest.raises(ValueError, match="Invalid executor 'bad'; expected one of: "):
        await future
    assert future.status == "error"


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


async def assert_task_states_on_worker(
    expected: dict[str, str], worker: Worker
) -> None:
    active_exc = None
    for _ in range(10):
        try:
            for dep_key, expected_state in expected.items():
                assert dep_key in worker.state.tasks, (
                    worker.name,
                    dep_key,
                    worker.state.tasks,
                )
                dep_ts = worker.state.tasks[dep_key]
                assert dep_ts.state == expected_state, (
                    worker.name,
                    dep_ts,
                    expected_state,
                )
            assert set(expected) == set(worker.state.tasks)
            return
        except AssertionError as exc:
            active_exc = exc
            await asyncio.sleep(0.1)
    # If after a second the workers are not in equilibrium, they are broken
    assert active_exc
    raise active_exc


@gen_cluster(client=True, config=NO_AMM)
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
    assert len(b.state.tasks) == 1
    assert g.key in b.state.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.state.tasks
    ts = a.state.tasks[res.key]
    assert ts.state == "error"

    expected_states = {
        # A was instructed to compute this result and we're still holding a ref via `f`
        f.key: "memory",
        # This was fetched from another worker. While we hold a ref via `g`, the
        # scheduler only instructed to compute this on B
        g.key: "memory",
        res.key: "error",
    }
    await assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures
    f.release()
    g.release()

    # We no longer hold any refs to f or g and B didn't have any errors. It
    # releases everything as expected
    while b.state.tasks:
        await asyncio.sleep(0.01)

    expected_states = {
        f.key: "released",
        g.key: "released",
        res.key: "error",
    }

    await assert_task_states_on_worker(expected_states, a)

    res.release()

    # We no longer hold any refs. Cluster should reset completely
    # This is not happening
    while any((s.tasks, a.state.tasks, b.state.tasks)):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=NO_AMM)
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
    assert len(b.state.tasks) == 1
    assert g.key in b.state.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.state.tasks
    ts = a.state.tasks[res.key]
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
    await assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures

    res.release()
    # We no longer hold any refs to f or g and B didn't have any errors. It
    # releases everything as expected
    while res.key in a.state.tasks:
        await asyncio.sleep(0.01)

    expected_states = {
        f.key: "memory",
        g.key: "memory",
    }

    await assert_task_states_on_worker(expected_states, a)

    f.release()
    g.release()

    while any((s.tasks, a.state.tasks, b.state.tasks)):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=NO_AMM)
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
    assert len(b.state.tasks) == 1
    assert g.key in b.state.tasks

    # A raised the exception therefore we should hold on to the erroneous task
    assert res.key in a.state.tasks
    ts = a.state.tasks[res.key]
    assert ts.state == "error"

    expected_states = {
        # A was instructed to compute this result and we're still holding a ref via `f`
        f.key: "memory",
        # This was fetched from another worker. While we hold a ref via `g`, the
        # scheduler only instructed to compute this on B
        g.key: "memory",
        res.key: "error",
    }
    await assert_task_states_on_worker(expected_states, a)
    # Expected states after we release references to the futures

    f.release()
    res.release()
    # We no longer hold any refs to f or g and B didn't have any errors. It
    # releases everything as expected
    while len(a.state.tasks) > 1:
        await asyncio.sleep(0.01)

    expected_states = {
        g.key: "memory",
    }

    await assert_task_states_on_worker(expected_states, a)
    await assert_task_states_on_worker(expected_states, b)

    g.release()

    # We no longer hold any refs. Cluster should reset completely
    while any((s.tasks, a.state.tasks, b.state.tasks)):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, config=NO_AMM)
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
    await assert_task_states_on_worker(expected_states_A, a)

    expected_states_B = {
        f.key: "memory",
        g.key: "memory",
        h.key: "memory",
        res.key: "error",
    }
    await assert_task_states_on_worker(expected_states_B, b)

    f.release()

    expected_states_A = {
        g.key: "memory",
        h.key: "memory",
    }
    await assert_task_states_on_worker(expected_states_A, a)

    expected_states_B = {
        f.key: "released",
        g.key: "memory",
        h.key: "memory",
        res.key: "error",
    }
    await assert_task_states_on_worker(expected_states_B, b)

    g.release()

    expected_states_A = {
        g.key: "released",
        h.key: "memory",
    }
    await assert_task_states_on_worker(expected_states_A, a)

    # B must not forget a task since all have a still valid dependent
    expected_states_B = {
        f.key: "released",
        h.key: "memory",
        res.key: "error",
    }
    await assert_task_states_on_worker(expected_states_B, b)
    h.release()

    expected_states_A = {}
    await assert_task_states_on_worker(expected_states_A, a)
    expected_states_B = {
        f.key: "released",
        h.key: "released",
        res.key: "error",
    }

    await assert_task_states_on_worker(expected_states_B, b)
    res.release()

    # We no longer hold any refs. Cluster should reset completely
    while any((s.tasks, a.state.tasks, b.state.tasks)):
        await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("", x) for x in (1, 2, 3, 4)], config=NO_AMM)
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

    while sum_2.key not in workers[3].state.tasks:
        await asyncio.sleep(0.01)

    while not workers[3].state.tasks[sum_2.key].state == "memory":
        assert len(s.tasks[f1.key].who_has) >= 2
        assert s.tasks[f2.key].state == "released"
        await asyncio.sleep(0.01)

    while len(workers[2].data) > 1:
        await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_forget_dependents_after_release(c, s, a):
    fut = c.submit(inc, 1, key="f-1")
    fut2 = c.submit(inc, fut, key="f-2")

    await asyncio.gather(fut, fut2)

    assert fut.key in a.state.tasks
    assert fut2.key in a.state.tasks
    assert fut2.key in {d.key for d in a.state.tasks[fut.key].dependents}

    fut2.release()

    while fut2.key in a.state.tasks:
        await asyncio.sleep(0.001)
    assert fut2.key not in {d.key for d in a.state.tasks[fut.key].dependents}


@gen_cluster(client=True, config=NO_AMM)
async def test_acquire_replicas(c, s, a, b):
    fut = c.submit(inc, 1, workers=[a.address])
    await fut

    s.request_acquire_replicas(b.address, [fut.key], stimulus_id=f"test-{time()}")

    while len(s.tasks[fut.key].who_has) != 2:
        await asyncio.sleep(0.005)

    for w in (a, b):
        assert w.data[fut.key] == 2
        assert w.state.tasks[fut.key].state == "memory"

    fut.release()

    while b.state.tasks or a.state.tasks:
        await asyncio.sleep(0.005)


@gen_cluster(client=True, config=NO_AMM)
async def test_acquire_replicas_same_channel(c, s, a, b):
    futA = c.submit(inc, 1, workers=[a.address], key="f-A")
    futB = c.submit(inc, 2, workers=[a.address], key="f-B")
    futC = c.submit(inc, futB, workers=[b.address], key="f-C")
    await futA

    s.request_acquire_replicas(b.address, [futA.key], stimulus_id=f"test-{time()}")

    await futC
    while (
        futA.key not in b.state.tasks or not b.state.tasks[futA.key].state == "memory"
    ):
        await asyncio.sleep(0.005)

    while len(s.tasks[futA.key].who_has) != 2:
        await asyncio.sleep(0.005)

    # Ensure that both the replica and an ordinary dependency pass through the
    # same communication channel

    for fut in (futA, futB):
        assert_story(
            b.state.story(fut.key),
            [
                ("gather-dependencies", a.address, {fut.key}),
                ("request-dep", a.address, {fut.key}),
            ],
        )
        assert any(fut.key in msg["keys"] for msg in b.transfer_incoming_log)


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 3, config=NO_AMM)
async def test_acquire_replicas_many(c, s, w1, w2, w3):
    futs = c.map(inc, range(10), workers=[w1.address])
    res = c.submit(sum, futs, workers=[w2.address])
    final = c.submit(slowinc, res, delay=0.5, workers=[w2.address])

    await wait(futs)

    s.request_acquire_replicas(
        w3.address, [fut.key for fut in futs], stimulus_id=f"test-{time()}"
    )

    # Worker 2 should normally not even be involved if there was no replication
    while not all(
        f.key in w3.state.tasks and w3.state.tasks[f.key].state == "memory"
        for f in futs
    ):
        await asyncio.sleep(0.01)

    assert all(ts.state == "memory" for ts in w3.state.tasks.values())

    assert await final == sum(map(inc, range(10))) + 1
    # All workers have a replica
    assert all(len(s.tasks[f.key].who_has) == 3 for f in futs)
    del futs, res, final

    while any(w.state.tasks for w in (w1, w2, w3)):
        await asyncio.sleep(0.001)


@gen_cluster(client=True, nthreads=[("", 1)], config=NO_AMM)
async def test_acquire_replicas_already_in_flight(c, s, a):
    """Trying to acquire a replica that is already in flight is a no-op"""
    async with BlockedGatherDep(s.address) as b:
        x = c.submit(inc, 1, workers=[a.address], key="x")
        y = c.submit(inc, x, workers=[b.address], key="y")
        await b.in_gather_dep.wait()
        assert b.state.tasks["x"].state == "flight"

        b.handle_stimulus(
            AcquireReplicasEvent(
                who_has={"x": a.address}, nbytes={"x": 1}, stimulus_id="test"
            )
        )
        assert b.state.tasks["x"].state == "flight"
        b.block_gather_dep.set()
        assert await y == 3

        assert_story(
            b.state.story("x"),
            [
                ("x", "fetch", "flight", "flight", {}),
                ("x", "flight", "fetch", "flight", {}),
            ],
        )


@pytest.mark.slow
@gen_cluster(client=True, config=NO_AMM)
async def test_forget_acquire_replicas(c, s, a, b):
    """
    1. The scheduler sends acquire-replicas to the worker
    2. Before the task can reach the worker, the task is forgotten on the scheduler
       and on the peer workers holding the replicas.
       This is unlike in a compute-task command, where the workers that are fetching
       the key are tracked on the scheduler side and receive a release-key command too.
    3. The task eventually transitions to missing
    4. At the next run of find_missing, the worker sends a request-refresh-who-has
       message to the scheduler
    5. The scheduler responds with free-keys
    6. The task is forgotten everywhere.
    """
    x = c.submit(inc, 2, key="x", workers=[a.address])
    await x
    with freeze_data_fetching(b, jump_start=True):
        s.request_acquire_replicas(b.address, ["x"], stimulus_id="test")
        await wait_for_state("x", "fetch", b)
        x.release()
        while "x" in s.tasks or "x" in a.state.tasks:
            await asyncio.sleep(0.01)

    while "x" in b.state.tasks:
        await asyncio.sleep(0.01)
    assert "x" not in s.tasks


@gen_cluster(client=True, config=NO_AMM)
async def test_remove_replicas_simple(c, s, a, b):
    futs = c.map(inc, range(10), workers=[a.address])
    await wait(futs)
    s.request_acquire_replicas(
        b.address, [fut.key for fut in futs], stimulus_id=f"test-{time()}"
    )

    while not all(len(s.tasks[f.key].who_has) == 2 for f in futs):
        await asyncio.sleep(0.01)

    s.request_remove_replicas(
        b.address, [fut.key for fut in futs], stimulus_id=f"test-{time()}"
    )

    assert all(len(s.tasks[f.key].who_has) == 1 for f in futs)

    while b.state.tasks:
        await asyncio.sleep(0.01)

    # Ensure there is no delayed reply to re-register the key
    await asyncio.sleep(0.01)
    assert all(s.tasks[f.key].who_has == {s.workers[a.address]} for f in futs)


@gen_cluster(client=True, nthreads=[("", 1)] * 3, config=NO_AMM)
async def test_who_has_consistent_remove_replicas(c, s, *workers):
    a = workers[0]
    other_workers = {w for w in workers if w != a}
    f1 = c.submit(inc, 1, key="f1", workers=[w.address for w in other_workers])
    await wait(f1)
    for w in other_workers:
        s.request_acquire_replicas(w.address, [f1.key], stimulus_id=f"test-{time()}")

    while not len(s.tasks[f1.key].who_has) == len(other_workers):
        await asyncio.sleep(0)

    f2 = c.submit(inc, f1, workers=[a.address])

    # Wait just until the moment the worker received the task and scheduled the
    # task to be fetched, then remove the replica from the worker this one is
    # trying to get the data from. Ensure this is handled gracefully and no
    # suspicious counters are raised since this is expected behaviour when
    # removing replicas

    while f1.key not in a.state.tasks or a.state.tasks[f1.key].state != "flight":
        await asyncio.sleep(0)

    coming_from = None
    for w in other_workers:
        coming_from = w
        if w.address == a.state.tasks[f1.key].coming_from:
            break

    coming_from.handle_stimulus(RemoveReplicasEvent(keys=[f1.key], stimulus_id="test"))
    await f2

    assert a.state.tasks[f1.key].suspicious_count == 0
    assert s.tasks[f1.key].suspicious == 0


@gen_cluster(client=True, config=NO_AMM)
async def test_acquire_replicas_with_no_priority(c, s, a, b):
    """Scattered tasks have no priority. When they transit to another worker through
    acquire-replicas, they end up in the Worker.data_needed heap together with tasks
    with a priority; they must gain a priority to become sortable.
    """
    x = (await c.scatter({"x": 1}, workers=[a.address]))["x"]
    y = c.submit(lambda: 2, key="y", workers=[a.address])
    await y
    assert s.tasks["x"].priority is None
    assert a.state.tasks["x"].priority is None
    assert s.tasks["y"].priority is not None
    assert a.state.tasks["y"].priority is not None

    s.request_acquire_replicas(b.address, [x.key, y.key], stimulus_id=f"test-{time()}")
    while b.data != {"x": 1, "y": 2}:
        await asyncio.sleep(0.01)

    assert s.tasks["x"].priority is None
    assert a.state.tasks["x"].priority is None
    assert b.state.tasks["x"].priority is not None


@gen_cluster(client=True, nthreads=[("", 1)], config=NO_AMM)
async def test_acquire_replicas_large_data(c, s, a):
    """When acquire-replicas is used to acquire multiple sizeable tasks, it respects
    transfer_message_bytes_limit and acquires them over multiple iterations.
    """
    size = a.state.transfer_message_bytes_limit // 5 - 10_000

    class C:
        def __sizeof__(self):
            return size

    futs = [c.submit(C, key=f"x{i}", workers=[a.address]) for i in range(10)]
    await wait(futs)

    async with BlockedGatherDep(s.address) as b:
        s.request_acquire_replicas(
            b.address, [f"x{i}" for i in range(10)], stimulus_id="test"
        )
        await b.in_gather_dep.wait()
        assert len(b.state.in_flight_tasks) == 5
        assert len(b.state.data_needed[a.address]) == 5
        b.block_gather_dep.set()
        while len(b.data) < 10:
            await asyncio.sleep(0.01)


@gen_cluster(client=True)
async def test_missing_released_zombie_tasks(c, s, a, b):
    """
    Ensure that no fetch/flight tasks are left in the task dict of a
    worker after everything was released
    """
    a.transfer_outgoing_count_limit = 0
    f1 = c.submit(inc, 1, key="f1", workers=[a.address])
    f2 = c.submit(inc, f1, key="f2", workers=[b.address])
    key = f1.key

    while key not in b.state.tasks or b.state.tasks[key].state != "fetch":
        await asyncio.sleep(0.01)

    await a.close()

    del f1, f2

    while b.state.tasks:
        await asyncio.sleep(0.01)


class BrokenWorker(Worker):
    async def get_data(self, comm, *args, **kwargs):
        comm.abort()


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_missing_released_zombie_tasks_2(c, s, b):
    # If get_data_from_worker raises this will suggest a dead worker to B and it
    # will transition the task to missing. We want to make sure that a missing
    # task is properly released and not left as a zombie
    async with BrokenWorker(s.address) as a:
        f1 = c.submit(inc, 1, key="f1", workers=[a.address])
        f2 = c.submit(inc, f1, key="f2", workers=[b.address])

        while f1.key not in b.state.tasks:
            await asyncio.sleep(0)

        ts = b.state.tasks[f1.key]
        assert ts.state == "flight"

        while ts.state != "missing":
            # If we sleep for a longer time, the worker will spin into an
            # endless loop of asking the scheduler who_has and trying to connect
            # to A
            await asyncio.sleep(0)

        del f1, f2

        while b.state.tasks:
            await asyncio.sleep(0.01)

        assert_story(
            b.state.story(ts),
            [("f1", "missing", "released", "released", {"f1": "forgotten"})],
        )


@gen_cluster(nthreads=[("", 1)], config={"distributed.worker.memory.pause": False})
async def test_worker_status_sync(s, a):
    ws = s.workers[a.address]
    a.status = Status.paused
    while ws.status != Status.paused:
        await asyncio.sleep(0.01)

    a.status = Status.running
    while ws.status != Status.running:
        await asyncio.sleep(0.01)

    await s.retire_workers()
    while ws.status != Status.closed:
        await asyncio.sleep(0.01)

    events = [ev for _, ev in s.events[ws.address] if ev["action"] != "heartbeat"]
    for ev in events:
        if "stimulus_id" in ev:  # Strip timestamp
            ev["stimulus_id"] = ev["stimulus_id"].rsplit("-", 1)[0]

    assert events == [
        {"action": "add-worker"},
        {
            "action": "worker-status-change",
            "prev-status": "init",
            "status": "running",
            "stimulus_id": "worker-status-change",
        },
        {
            "action": "worker-status-change",
            "prev-status": "running",
            "status": "paused",
            "stimulus_id": "worker-status-change",
        },
        {
            "action": "worker-status-change",
            "prev-status": "paused",
            "status": "running",
            "stimulus_id": "worker-status-change",
        },
        {
            "action": "worker-status-change",
            "prev-status": "running",
            "status": "closing_gracefully",
            "stimulus_id": "retire-workers",
        },
        {
            "action": "remove-worker",
            "lost-computed-tasks": set(),
            "lost-scattered-tasks": set(),
            "processing-tasks": set(),
            "stimulus_id": "retire-workers",
        },
        {"action": "retired", "stimulus_id": "retire-workers"},
    ]


@gen_cluster(client=True)
async def test_log_remove_worker(c, s, a, b):
    # Computed task
    x = c.submit(inc, 1, key="x", workers=a.address)
    await x
    ev = Event()
    # Processing task
    y = c.submit(
        lambda ev: ev.wait(), ev, key="y", workers=a.address, allow_other_workers=True
    )
    await wait_for_state("y", "processing", s)
    # Scattered task
    z = await c.scatter({"z": 3}, workers=a.address)

    s.events.clear()

    with captured_logger("distributed.scheduler", level=logging.INFO) as log:
        # Successful graceful shutdown
        await s.retire_workers([a.address], stimulus_id="graceful")
        # Refuse to retire gracefully as there's nowhere to put x and z
        await s.retire_workers([b.address], stimulus_id="graceful_abort")
        await asyncio.sleep(0.2)
        # Ungraceful shutdown
        await s.remove_worker(b.address, stimulus_id="ungraceful")
        await asyncio.sleep(0.2)
    await ev.set()

    assert log.getvalue().splitlines() == [
        # Successful graceful
        f"Retire worker addresses ['{a.address}']",
        f"Retiring worker '{a.address}' (stimulus_id='graceful')",
        f"Remove worker <WorkerState '{a.address}', name: 0, status: "
        "closing_gracefully, memory: 2, processing: 1> (stimulus_id='graceful')",
        f"Retired worker '{a.address}' (stimulus_id='graceful')",
        # Aborted graceful
        f"Retire worker addresses ['{b.address}']",
        f"Retiring worker '{b.address}' (stimulus_id='graceful_abort')",
        f"Could not retire worker '{b.address}': unique data could not be "
        "moved to any other worker (stimulus_id='graceful_abort')",
        # Ungraceful
        f"Remove worker <WorkerState '{b.address}', name: 1, status: "
        "running, memory: 2, processing: 1> (stimulus_id='ungraceful')",
        f"Removing worker '{b.address}' caused the cluster to lose already "
        "computed task(s), which will be recomputed elsewhere: {'x'} "
        "(stimulus_id='ungraceful')",
        f"Removing worker '{b.address}' caused the cluster to lose scattered "
        "data, which can't be recovered: {'z'} (stimulus_id='ungraceful')",
        "Lost all workers",
    ]

    events = {topic: [ev for _, ev in evs] for topic, evs in s.events.items()}
    for evs in events.values():
        for ev in evs:
            if ev["action"] == "retire-workers":
                for k in ("retired", "could-not-retire"):
                    ev[k] = {addr: "snip" for addr in ev[k]}
            if "stimulus_id" in ev:  # Strip timestamp
                ev["stimulus_id"] = ev["stimulus_id"].rsplit("-", 1)[0]

    assert events == {
        a.address: [
            {
                "action": "worker-status-change",
                "prev-status": "running",
                "status": "closing_gracefully",
                "stimulus_id": "graceful",
            },
            {
                "action": "remove-worker",
                "lost-computed-tasks": set(),
                "lost-scattered-tasks": set(),
                "processing-tasks": {"y"},
                "stimulus_id": "graceful",
            },
            {"action": "retired", "stimulus_id": "graceful"},
        ],
        b.address: [
            {
                "action": "worker-status-change",
                "prev-status": "running",
                "status": "closing_gracefully",
                "stimulus_id": "graceful_abort",
            },
            {"action": "could-not-retire", "stimulus_id": "graceful_abort"},
            {
                "action": "worker-status-change",
                "prev-status": "closing_gracefully",
                "status": "running",
                "stimulus_id": "worker-status-change",
            },
            {
                "action": "remove-worker",
                "lost-computed-tasks": {"x"},
                "lost-scattered-tasks": {"z"},
                "processing-tasks": {"y"},
                "stimulus_id": "ungraceful",
            },
            {"action": "closing-worker", "reason": "scheduler-remove-worker"},
        ],
        "all": [
            {
                "action": "remove-worker",
                "lost-computed-tasks": set(),
                "lost-scattered-tasks": set(),
                "processing-tasks": {"y"},
                "stimulus_id": "graceful",
                "worker": a.address,
            },
            {
                "action": "retire-workers",
                "stimulus_id": "graceful",
                "retired": {a.address: "snip"},
                "could-not-retire": {},
            },
            {
                "action": "retire-workers",
                "stimulus_id": "graceful_abort",
                "retired": {},
                "could-not-retire": {b.address: "snip"},
            },
            {
                "action": "remove-worker",
                "lost-computed-tasks": {"x"},
                "lost-scattered-tasks": {"z"},
                "processing-tasks": {"y"},
                "stimulus_id": "ungraceful",
                "worker": b.address,
            },
        ],
    }


@gen_cluster(client=True)
async def test_task_flight_compute_oserror(c, s, a, b):
    """If the remote worker dies while a task is in flight, the task may be
    rescheduled to be computed on the worker trying to fetch the data.
    However, the OSError caused by the dead remote would try to transition the
    task to missing which is not what we want. This test ensures that the task
    is properly transitioned to executing and the scheduler doesn't reschedule
    anything and rejects the "false missing" signal from the worker, if there is
    any.
    """

    write_queue = asyncio.Queue()
    write_event = asyncio.Event()
    b.rpc = _LockedCommPool(
        b.rpc,
        write_queue=write_queue,
        write_event=write_event,
    )
    futs = c.submit(map, inc, range(10), workers=[a.address], allow_other_workers=True)
    await wait(futs)
    assert a.data
    assert write_queue.empty()
    f1 = c.submit(sum, futs, workers=[b.address], key="f1")
    peer, msg = await write_queue.get()
    assert peer == a.address
    assert msg["op"] == "get_data"
    in_flight_tasks = [ts for ts in b.state.tasks.values() if ts.key != "f1"]
    assert all(ts.state == "flight" for ts in in_flight_tasks)
    await a.close()
    write_event.set()

    await f1

    # If the above doesn't deadlock the behavior should be OK. We're still
    # asserting a few internals to make sure that if things change this is done
    # deliberately

    sum_story = b.state.story("f1")
    expected_sum_story = [
        ("f1", "compute-task", "released"),
        (
            "f1",
            "released",
            "waiting",
            "waiting",
            {ts.key: "fetch" for ts in in_flight_tasks},
        ),
        # inc is lost and needs to be recomputed. Therefore, sum is released
        ("free-keys", ("f1",)),
        # The recommendations here are hard to predict. Whatever key is
        # currently scheduled to be fetched, if any, will be recommended to be
        # released.
        ("f1", "waiting", "released", "released", lambda msg: msg["f1"] == "forgotten"),
        ("f1", "released", "forgotten", "forgotten", {}),
        # Now, we actually compute the task *once*. This must not cycle back
        ("f1", "compute-task", "released"),
        ("f1", "released", "waiting", "waiting", {"f1": "ready"}),
        ("f1", "waiting", "ready", "ready", {"f1": "executing"}),
        ("f1", "ready", "executing", "executing", {}),
        ("f1", "put-in-memory"),
        ("f1", "executing", "memory", "memory", {}),
    ]
    assert_story(sum_story, expected_sum_story, strict=True)


@gen_cluster(client=True, nthreads=[])
async def test_gather_dep_cancelled_rescheduled(c, s):
    """At time of writing, the gather_dep implementation filtered tasks again
    for in-flight state. The response parser, however, did not distinguish
    resulting in unwanted missing-data signals to the scheduler, causing
    potential rescheduling or data leaks.
    (Note: missing-data was removed in #6445).

    If a cancelled key is rescheduled for fetching while gather_dep waits
    internally for get_data, the response parser would misclassify this key and
    cause the key to be recommended for a release causing deadlocks and/or lost
    keys.
    At time of writing, this transition was implemented wrongly and caused a
    flight->cancelled transition which should be recoverable but the cancelled
    state was corrupted by this transition since ts.done==True. This attribute
    setting would cause a cancelled->fetch transition to actually drop the key
    instead, causing https://github.com/dask/distributed/issues/5366

    See also test_gather_dep_do_not_handle_response_of_not_requested_tasks
    """
    async with BlockedGetData(s.address) as a:
        async with BlockedGatherDep(s.address) as b:
            fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
            fut2 = c.submit(inc, fut1, workers=[a.address], key="f2")
            await wait(fut2)
            fut4 = c.submit(sum, fut1, fut2, workers=[b.address], key="f4")
            fut3 = c.submit(inc, fut1, workers=[b.address], key="f3")

            await wait_for_state(fut2.key, "flight", b)
            await b.in_gather_dep.wait()

            fut4.release()
            while fut4.key in b.state.tasks:
                await asyncio.sleep(0)

            assert b.state.tasks[fut2.key].state == "cancelled"

            b.block_gather_dep.set()
            await a.in_get_data.wait()

            fut4 = c.submit(sum, [fut1, fut2], workers=[b.address], key="f4")
            await wait_for_state(fut2.key, "flight", b)

            a.block_get_data.set()
            await wait([fut3, fut4])
            f2_story = b.state.story(fut2.key)
            assert f2_story


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_gather_dep_do_not_handle_response_of_not_requested_tasks(c, s, a):
    """At time of writing, the gather_dep implementation filtered tasks again
    for in-flight state. The response parser, however, did not distinguish
    resulting in unwanted missing-data signals to the scheduler, causing
    potential rescheduling or data leaks.
    (Note: missing-data was removed in #6445).

    This test may become obsolete if the implementation changes significantly.
    """
    async with BlockedGatherDep(s.address) as b:
        fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
        fut2 = c.submit(inc, fut1, workers=[a.address], key="f2")
        await fut2
        fut4 = c.submit(sum, fut1, fut2, workers=[b.address], key="f4")
        fut3 = c.submit(inc, fut1, workers=[b.address], key="f3")

        await b.in_gather_dep.wait()
        assert b.state.tasks[fut2.key].state == "flight"

        fut4.release()
        while fut4.key in b.state.tasks:
            await asyncio.sleep(0.01)

        assert b.state.tasks[fut2.key].state == "cancelled"

        b.block_gather_dep.set()

        await fut3
        assert fut2.key not in b.state.tasks
        f2_story = b.state.story(fut2.key)
        assert f2_story
        assert not any("missing-dep" in msg for msg in f2_story)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_gather_dep_no_longer_in_flight_tasks(c, s, a):
    async with BlockedGatherDep(s.address) as b:
        fut1 = c.submit(inc, 1, workers=[a.address], key="f1")
        fut2 = c.submit(sum, fut1, fut1, workers=[b.address], key="f2")

        await wait_for_state(fut1.key, "flight", b)
        await b.in_gather_dep.wait()

        fut2.release()
        while fut2.key in b.state.tasks:
            await asyncio.sleep(0.01)

        assert b.state.tasks[fut1.key].state == "cancelled"

        b.block_gather_dep.set()
        while fut2.key in b.state.tasks:
            await asyncio.sleep(0.01)

        f1_story = b.state.story(fut1.key)
        f2_story = b.state.story(fut2.key)
        assert f1_story
        assert f2_story
        assert not any("missing-dep" in msg for msg in f2_story)


@gen_cluster(client=True, nthreads=[("", 1)], timeout=5)
async def test_get_data_cancelled_error(c, s, a):
    """Something somewhere in the networking stack raises CancelledError while
    get_data is running

    See Also
    --------
    https://github.com/dask/distributed/issues/8006
    """

    class FlakyInboundRPC(Worker):
        flake = 0

        def handle_comm(self, comm):
            if self.flake:
                self.flake -= 1

                async def write(*args, **kwargs):
                    raise asyncio.CancelledError()

                comm.write = write

            return super().handle_comm(comm)

    async with FlakyInboundRPC(s.address) as b:
        x = c.submit(inc, 1, key="x", workers=[b.address])
        await wait(x)
        b.flake = 2
        y = c.submit(inc, x, key="y", workers=[a.address])
        assert await y == 3
        assert b.flake == 0


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_Worker__to_dict(c, s, a):
    x = c.submit(inc, 1, key="x")
    await wait(x)
    d = a._to_dict()
    assert set(d) == {
        "type",
        "id",
        "scheduler",
        "address",
        "status",
        "thread_id",
        "logs",
        "config",
        "transfer_incoming_log",
        "transfer_outgoing_log",
        # Attributes of WorkerMemoryManager
        "data",
        "max_spill",
        "memory_limit",
        "memory_monitor_interval",
        "memory_pause_fraction",
        "memory_spill_fraction",
        "memory_target_fraction",
        # Attributes of WorkerState
        "nthreads",
        "running",
        "ready",
        "has_what",
        "constrained",
        "executing",
        "long_running",
        "missing_dep_flight",
        "in_flight_tasks",
        "in_flight_workers",
        "busy_workers",
        "log",
        "stimulus_log",
        "transition_counter",
        "tasks",
        "data_needed",
        "task_counts",
        "task_cumulative_elapsed",
    }
    assert d["tasks"]["x"]["key"] == "x"
    assert d["data"] == {"x": None}


@gen_cluster(nthreads=[])
async def test_extension_methods(s):
    flag = False
    shutdown = False

    class WorkerExtension:
        def __init__(self, worker):
            pass

        def heartbeat(self):
            return {"data": 123}

        async def close(self):
            nonlocal shutdown
            shutdown = True

    class SchedulerExtension:
        def __init__(self, scheduler):
            self.scheduler = scheduler
            pass

        def heartbeat(self, ws, data):
            nonlocal flag
            assert ws in self.scheduler.workers.values()
            assert data == {"data": 123}
            flag = True

    s.extensions["test"] = SchedulerExtension(s)

    async with Worker(s.address, extensions={"test": WorkerExtension}) as w:
        assert not shutdown
        await w.heartbeat()
        assert flag

    assert shutdown


@gen_cluster()
async def test_benchmark_hardware(s, a, b):
    sizes = ["1 kiB", "10 kiB"]
    disk = benchmark_disk(sizes=sizes, duration="1 ms")
    memory = benchmark_memory(sizes=sizes, duration="1 ms")
    network = await benchmark_network(
        address=a.address, rpc=b.rpc, sizes=sizes, duration="1 ms"
    )

    assert set(disk) == set(memory) == set(network) == set(sizes)


@pytest.mark.slow
@gen_cluster(
    client=True,
    config={
        "distributed.admin.tick.interval": "5ms",
        "distributed.admin.tick.cycle": "100ms",
    },
)
async def test_tick_interval(c, s, a, b):
    import time

    await a.heartbeat()
    x = s.workers[a.address].metrics["event_loop_interval"]
    while s.workers[a.address].metrics["event_loop_interval"] > 0.050:
        await asyncio.sleep(0.01)
    while s.workers[a.address].metrics["event_loop_interval"] < 0.100:
        await asyncio.sleep(0.01)
        time.sleep(0.200)


class BreakingWorker(Worker):
    broke_once = False

    def get_data(self, comm, **kwargs):
        if not self.broke_once:
            self.broke_once = True
            raise OSError("fake error")
        return super().get_data(comm, **kwargs)


@pytest.mark.slow
@gen_cluster(client=True, Worker=BreakingWorker)
async def test_broken_comm(c, s, a, b):
    pytest.importorskip("dask.dataframe")

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
    )
    with dask.config.set({"dataframe.shuffle.method": "tasks"}):
        s = df.shuffle("id")
    await c.compute(s.size)


@gen_cluster(nthreads=[])
async def test_do_not_block_event_loop_during_shutdown(s):
    loop = asyncio.get_running_loop()
    called_handler = threading.Event()
    block_handler = threading.Event()

    w = await Worker(s.address)
    executor = w.executors["default"]

    # The block wait must be smaller than the test timeout and smaller than the
    # default value for timeout in `Worker.close``
    async def block():
        def fn():
            called_handler.set()
            assert block_handler.wait(20)

        await loop.run_in_executor(executor, fn)

    async def set_future():
        while True:
            try:
                await loop.run_in_executor(executor, sleep, 0.1)
            except RuntimeError:  # executor has started shutting down
                block_handler.set()
                return

    async def close():
        called_handler.wait()
        # executor_wait is True by default but we want to be explicit here
        await w.close(executor_wait=True)

    await asyncio.gather(block(), close(), set_future())


@gen_cluster(nthreads=[])
async def test_reconnect_argument_deprecated(s):
    with pytest.deprecated_call(match="`reconnect` argument"):
        async with Worker(s.address, reconnect=False):
            pass
    with pytest.raises(ValueError, match="reconnect=True"):
        async with Worker(s.address, reconnect=True):
            pass

    with warnings.catch_warnings():
        # No argument should not warn or raise
        warnings.simplefilter("error")
        async with Worker(s.address):
            pass


@gen_cluster(client=True, nthreads=[])
async def test_worker_running_before_running_plugins(c, s, caplog):
    class InitWorkerNewThread(WorkerPlugin):
        name = "init_worker_new_thread"
        setup_status = None

        def setup(self, worker):
            self.setup_status = worker.status

        def teardown(self, worker):
            pass

    await c.register_plugin(InitWorkerNewThread())
    async with Worker(s.address) as worker:
        assert await c.submit(inc, 1) == 2
        assert worker.plugins[InitWorkerNewThread.name].setup_status is Status.running


@gen_cluster(
    client=True,
    nthreads=[],
    config={
        # This is just to make Scheduler.retire_worker more reactive to changes
        "distributed.scheduler.active-memory-manager.start": True,
        "distributed.scheduler.active-memory-manager.interval": "50ms",
    },
)
async def test_execute_preamble_abort_retirement(c, s):
    """Test race condition in the preamble of Worker.execute(), which used to cause a
    task to remain permanently in executing state in case of very tight timings when
    exiting the closing_gracefully status.

    See also
    --------
    https://github.com/dask/distributed/issues/6867
    test_cancelled_state.py::test_execute_preamble_early_cancel
    """
    async with BlockedExecute(s.address) as a:
        await c.wait_for_workers(1)

        x = await c.scatter({"x": 1}, workers=[a.address])
        y = c.submit(inc, 1, key="y", workers=[a.address])
        await a.in_execute.wait()

        async with BlockedGatherDep(s.address) as b:
            await c.wait_for_workers(2)
            retire_fut = asyncio.create_task(c.retire_workers([a.address]))
            while a.status != Status.closing_gracefully:
                await asyncio.sleep(0.01)
            # The Active Memory Manager will send to b the message
            # {op: acquire-replicas, who_has: {x: [a.address]}}
            await b.in_gather_dep.wait()

            # Run Worker.execute. At the moment of writing this test, the method would
            # detect the closing_gracefully status and perform an early exit.
            a.block_execute.set()
            await a.in_execute_exit.wait()

        # b has shut down. There's nowhere to replicate x to anymore, so retire_workers
        # will give up and reinstate a to running status.
        assert await retire_fut == {}
        while a.status != Status.running:
            await asyncio.sleep(0.01)

        # Finally let the done_callback of Worker.execute run
        a.block_execute_exit.set()

        # Test that y does not get stuck.
        assert await y == 2


@gen_cluster()
async def test_deprecation_of_renamed_worker_attributes(s, a, b):
    msg = (
        "The `Worker.outgoing_count` attribute has been renamed to "
        "`Worker.transfer_outgoing_count_total`"
    )
    with pytest.warns(DeprecationWarning, match=msg):
        assert a.outgoing_count == a.transfer_outgoing_count_total

    msg = (
        "The `Worker.outgoing_current_count` attribute has been renamed to "
        "`Worker.transfer_outgoing_count`"
    )
    with pytest.warns(DeprecationWarning, match=msg):
        assert a.outgoing_current_count == a.transfer_outgoing_count


@gen_cluster(client=True, Worker=Nanny)
async def test_forward_output(c, s, a, b, capsys):
    def print_stdout(*args, **kwargs):
        print(*args, file=sys.stdout, **kwargs)

    def print_stderr(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

    plugin = ForwardOutput()
    out, err = capsys.readouterr()

    counter = itertools.count()

    # Without the plugin installed, we should not see any output
    # Note that we use nannies so workers run in subprocesses
    await c.submit(print_stdout, "foo", key=next(counter))
    await c.submit(print_stdout, "bar\n", key=next(counter))
    await c.submit(print_stdout, "baz", end="", flush=True, key=next(counter))
    await c.submit(print_stdout, 1, 2, end="\n", sep="\n", key=next(counter))
    out, err = capsys.readouterr()

    assert "" == out
    assert "" == err

    await c.submit(print_stderr, "foo", key=next(counter))
    await c.submit(print_stderr, "bar\n", key=next(counter))
    await c.submit(print_stderr, "baz", flush=True, key=next(counter))
    await c.submit(print_stderr, 1, 2, end="\n", sep="\n", key=next(counter))
    out, err = capsys.readouterr()

    assert "" == out
    assert "" == err

    # After installing, output should be forwarded
    await c.register_plugin(plugin, "forward")
    await asyncio.sleep(0.1)  # Let setup messages come in
    capsys.readouterr()

    await c.submit(print_stdout, "foo", key=next(counter))
    out, err = capsys.readouterr()

    assert "foo\n" == out
    assert "" == err

    await c.submit(print_stdout, "bar\n", key=next(counter))
    out, err = capsys.readouterr()

    assert "bar\n\n" == out
    assert "" == err

    await c.submit(print_stdout, "baz", end="", flush=True, key=next(counter))
    out, err = capsys.readouterr()

    assert "baz" == out
    assert "" == err

    await c.submit(print_stdout, "first\nsecond", end="", key=next(counter))
    out, err = capsys.readouterr()

    assert "first\nsecond" == out
    assert "" == err

    await c.submit(print_stdout, 1, 2, sep=":", key=next(counter))
    out, err = capsys.readouterr()

    assert "1:2\n" == out
    assert err == ""

    await c.submit(print_stderr, "fatal", key=next(counter))
    out, err = capsys.readouterr()

    assert "" == out
    assert "fatal\n" == err

    # Registering the plugin is idempotent
    other_plugin = ForwardOutput()
    await c.register_plugin(other_plugin, "forward")
    await asyncio.sleep(0.1)  # Let teardown/setup messages come in
    out, err = capsys.readouterr()

    await c.submit(print_stdout, "foo", key=next(counter))
    out, err = capsys.readouterr()

    assert "foo\n" == out
    assert "" == err

    # After unregistering the plugin, we should once again not see any output
    await c.unregister_worker_plugin("forward")
    await asyncio.sleep(0.1)  # Let teardown messages come in
    capsys.readouterr()

    await c.submit(print_stdout, "foo", key=next(counter))
    out, err = capsys.readouterr()

    assert "" == out
    assert "" == err

    await c.submit(print_stderr, "fatal", key=next(counter))
    out, err = capsys.readouterr()

    assert "" == out
    assert "" == err


class EnsureOffloaded:
    def __init__(self, main_thread_id):
        self.main_thread_id = main_thread_id
        self.data = random.randbytes(OFFLOAD_THRESHOLD + 1)

    def __sizeof__(self):
        return len(self.data)

    def __getstate__(self):
        assert self.main_thread_id
        assert self.main_thread_id != threading.get_ident()
        return (self.data, self.main_thread_id, threading.get_ident())

    def __setstate__(self, state):
        _, main_thread, serialize_thread = state
        assert main_thread != threading.get_ident()
        return EnsureOffloaded(main_thread)


@gen_cluster(client=True)
async def test_offload_getdata(c, s, a, b):
    """Test that functions wrapped by offload() are metered"""
    x = c.submit(EnsureOffloaded, threading.get_ident(), key="x", workers=[a.address])
    y = c.submit(lambda x: None, x, key="y", workers=[b.address])
    await y


@gen_cluster(client=True)
async def test_startstops(c, s, a, b):
    t0 = time()
    x = c.submit(inc, 1, key="x", workers=[a.address])
    y = c.submit(inc, x, key="y", workers=[b.address])
    await wait(y)
    t1 = time()
    ss = b.state.tasks["y"].startstops
    assert len(ss) == 2
    assert ss[0]["action"] == "transfer"
    assert ss[0]["source"] == a.address
    assert ss[1]["action"] == "compute"
    assert (
        t0 + b.scheduler_delay
        < ss[0]["start"]
        < ss[0]["stop"]
        < ss[1]["start"]
        < ss[1]["stop"]
        < t1 + b.scheduler_delay
    )


@gen_cluster(client=True, nthreads=[("", 1)])
@pytest.mark.parametrize("state", ["cancelled", "resumed"])
async def test_suppress_keyerror_for_cancelled_tasks(c, s, a, state):
    async with BlockedExecute(s.address) as b:
        with captured_logger("distributed.worker", level=logging.ERROR) as log:
            x = (await c.scatter({"x": 1}))["x"]
            y = c.submit(inc, x, key="y", workers=[b.address])
            await b.in_execute.wait()
            del x, y
            await async_poll_for(lambda: "x" not in b.data, timeout=5)

            if state == "resumed":
                y = c.submit(inc, 1, key="y", workers=[a.address])
                z = c.submit(inc, y, key="z", workers=[b.address])
                await wait_for_state("y", "resumed", b)

            b.block_execute.set()
            b.block_execute_exit.set()

            if state == "resumed":
                assert await z == 3
                del y, z

            await async_poll_for(lambda: not b.state.tasks, timeout=5)

    assert not log.getvalue()
