import asyncio
import os
import pathlib
import socket
import threading
from contextlib import contextmanager
from time import sleep

import pytest
import yaml
from tornado import gen

import dask.config

from distributed import Client, Nanny, Scheduler, Worker, config, default_client
from distributed.core import Server, rpc
from distributed.metrics import time
from distributed.utils import get_ip
from distributed.utils_test import (
    _LockedCommPool,
    _UnhashableCallable,
    assert_worker_story,
    cluster,
    dump_cluster_state,
    gen_cluster,
    gen_test,
    inc,
    new_config,
    tls_only_security,
    wait_for_port,
)


def test_bare_cluster(loop):
    with cluster(nworkers=10) as (s, _):
        pass


def test_cluster(loop):
    with cluster() as (s, [a, b]):
        with rpc(s["address"]) as s:
            ident = loop.run_sync(s.identity)
            assert ident["type"] == "Scheduler"
            assert len(ident["workers"]) == 2


@gen_cluster(client=True)
async def test_gen_cluster(c, s, a, b):
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.nthreads == {w.address: w.nthreads for w in [a, b]}
    assert await c.submit(lambda: 123) == 123


@gen_cluster(client=True)
async def test_gen_cluster_pytest_fixture(c, s, a, b, tmp_path):
    assert isinstance(tmp_path, pathlib.Path)
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)


@pytest.mark.parametrize("foo", [True])
@gen_cluster(client=True)
async def test_gen_cluster_parametrized(c, s, a, b, foo):
    assert foo is True
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)


@pytest.mark.parametrize("foo", [True])
@pytest.mark.parametrize("bar", ["a", "b"])
@gen_cluster(client=True)
async def test_gen_cluster_multi_parametrized(c, s, a, b, foo, bar):
    assert foo is True
    assert bar in ("a", "b")
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)


@pytest.mark.parametrize("foo", [True])
@gen_cluster(client=True)
async def test_gen_cluster_parametrized_variadic_workers(c, s, *workers, foo):
    assert foo is True
    assert isinstance(c, Client)
    assert isinstance(s, Scheduler)
    for w in workers:
        assert isinstance(w, Worker)


@gen_cluster(
    client=True,
    Worker=Nanny,
    config={"distributed.comm.timeouts.connect": "1s", "new.config.value": "foo"},
)
async def test_gen_cluster_set_config_nanny(c, s, a, b):
    def assert_config():
        assert dask.config.get("distributed.comm.timeouts.connect") == "1s"
        assert dask.config.get("new.config.value") == "foo"

    await c.run(assert_config)
    await c.run_on_scheduler(assert_config)


@pytest.mark.skip(reason="This hangs on travis")
def test_gen_cluster_cleans_up_client(loop):
    import dask.context

    assert not dask.config.get("get", None)

    @gen_cluster(client=True)
    async def f(c, s, a, b):
        assert dask.config.get("get", None)
        await c.submit(inc, 1)

    f()

    assert not dask.config.get("get", None)


@gen_cluster()
async def test_gen_cluster_without_client(s, a, b):
    assert isinstance(s, Scheduler)
    for w in [a, b]:
        assert isinstance(w, Worker)
    assert s.nthreads == {w.address: w.nthreads for w in [a, b]}

    async with Client(s.address, asynchronous=True) as c:
        future = c.submit(lambda x: x + 1, 1)
        result = await future
        assert result == 2


@gen_cluster(
    client=True,
    scheduler="tls://127.0.0.1",
    nthreads=[("tls://127.0.0.1", 1), ("tls://127.0.0.1", 2)],
    security=tls_only_security(),
)
async def test_gen_cluster_tls(e, s, a, b):
    assert isinstance(e, Client)
    assert isinstance(s, Scheduler)
    assert s.address.startswith("tls://")
    for w in [a, b]:
        assert isinstance(w, Worker)
        assert w.address.startswith("tls://")
    assert s.nthreads == {w.address: w.nthreads for w in [a, b]}


@pytest.mark.xfail(
    reason="Test should always fail to ensure the body of the test function was run",
    strict=True,
)
@gen_test()
async def test_gen_test():
    await asyncio.sleep(0.01)
    assert False


@pytest.mark.xfail(
    reason="Test should always fail to ensure the body of the test function was run",
    strict=True,
)
@gen_test()
def test_gen_test_legacy_implicit():
    yield asyncio.sleep(0.01)
    assert False


@pytest.mark.xfail(
    reason="Test should always fail to ensure the body of the test function was run",
    strict=True,
)
@gen_test()
@gen.coroutine
def test_gen_test_legacy_explicit():
    yield asyncio.sleep(0.01)
    assert False


@pytest.mark.parametrize("foo", [True])
@gen_test()
async def test_gen_test_parametrized(foo):
    assert foo is True


@pytest.mark.parametrize("foo", [True])
@pytest.mark.parametrize("bar", [False])
@gen_test()
async def test_gen_test_double_parametrized(foo, bar):
    assert foo is True
    assert bar is False


@gen_test()
async def test_gen_test_pytest_fixture(tmp_path, c):
    assert isinstance(tmp_path, pathlib.Path)
    assert isinstance(c, Client)


@contextmanager
def _listen(delay=0):
    serv = socket.socket()
    serv.bind(("127.0.0.1", 0))
    e = threading.Event()

    def do_listen():
        e.set()
        sleep(delay)
        serv.listen(5)
        ret = serv.accept()
        if ret is not None:
            cli, _ = ret
            cli.close()
        serv.close()

    t = threading.Thread(target=do_listen)
    t.daemon = True
    t.start()
    try:
        e.wait()
        sleep(0.01)
        yield serv
    finally:
        t.join(5.0)


def test_wait_for_port():
    t1 = time()
    with pytest.raises(RuntimeError):
        wait_for_port((get_ip(), 9999), 0.5)
    t2 = time()
    assert t2 - t1 >= 0.5

    with _listen(0) as s1:
        t1 = time()
        wait_for_port(s1.getsockname())
        t2 = time()
        assert t2 - t1 <= 1.0

    with _listen(1) as s1:
        t1 = time()
        wait_for_port(s1.getsockname())
        t2 = time()
        assert t2 - t1 <= 2.0


def test_new_config():
    c = config.copy()
    with new_config({"xyzzy": 5}):
        config["xyzzy"] == 5

    assert config == c
    assert "xyzzy" not in config


def test_lingering_client():
    @gen_cluster()
    async def f(s, a, b):
        await Client(s.address, asynchronous=True)

    f()

    with pytest.raises(ValueError):
        default_client()


def test_lingering_client_2(loop):
    with cluster() as (s, [a, b]):
        client = Client(s["address"], loop=loop)


def test_tls_cluster(tls_client):
    tls_client.submit(lambda x: x + 1, 10).result() == 11
    assert tls_client.security


@pytest.mark.asyncio
async def test_tls_scheduler(security, cleanup):
    async with Scheduler(
        security=security, host="localhost", dashboard_address=":0"
    ) as s:
        assert s.address.startswith("tls")


def test__UnhashableCallable():
    func = _UnhashableCallable()
    assert func(1) == 2
    with pytest.raises(TypeError, match="unhashable"):
        hash(func)


class MyServer(Server):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handlers["ping"] = self.pong
        self.counter = 0

    def pong(self, comm):
        self.counter += 1
        return "pong"


@pytest.mark.asyncio
async def test_locked_comm_drop_in_replacement(loop):

    a = await MyServer({})
    await a.listen(0)

    read_event = asyncio.Event()
    read_event.set()
    read_queue = asyncio.Queue()
    original_pool = a.rpc
    a.rpc = _LockedCommPool(original_pool, read_event=read_event, read_queue=read_queue)

    b = await MyServer({})
    await b.listen(0)
    # Event is set, the pool works like an ordinary pool
    res = await a.rpc(b.address).ping()
    assert await read_queue.get() == (b.address, "pong")
    assert res == "pong"
    assert b.counter == 1

    read_event.clear()
    # Can also be used without a lock to intercept network traffic
    a.rpc = _LockedCommPool(original_pool, read_queue=read_queue)
    a.rpc.remove(b.address)
    res = await a.rpc(b.address).ping()
    assert await read_queue.get() == (b.address, "pong")


@pytest.mark.asyncio
async def test_locked_comm_intercept_read(loop):

    a = await MyServer({})
    await a.listen(0)
    b = await MyServer({})
    await b.listen(0)

    read_event = asyncio.Event()
    read_queue = asyncio.Queue()
    a.rpc = _LockedCommPool(a.rpc, read_event=read_event, read_queue=read_queue)

    async def ping_pong():
        return await a.rpc(b.address).ping()

    fut = asyncio.create_task(ping_pong())

    # We didn't block the write but merely the read. The remove should have
    # received the message and responded already
    while not b.counter:
        await asyncio.sleep(0.001)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(fut), 0.01)

    assert await read_queue.get() == (b.address, "pong")
    read_event.set()
    assert await fut == "pong"


@pytest.mark.asyncio
async def test_locked_comm_intercept_write(loop):

    a = await MyServer({})
    await a.listen(0)
    b = await MyServer({})
    await b.listen(0)

    write_event = asyncio.Event()
    write_queue = asyncio.Queue()
    a.rpc = _LockedCommPool(a.rpc, write_event=write_event, write_queue=write_queue)

    async def ping_pong():
        return await a.rpc(b.address).ping()

    fut = asyncio.create_task(ping_pong())

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(fut), 0.01)
    # Write was blocked. The remote hasn't received the message, yet
    assert b.counter == 0
    assert await write_queue.get() == (b.address, {"op": "ping", "reply": True})
    write_event.set()
    assert await fut == "pong"


@pytest.mark.slow()
def test_dump_cluster_state_timeout(tmp_path):
    sleep_time = 30

    async def inner_test(c, s, a, b):
        await asyncio.sleep(sleep_time)

    # This timeout includes cluster startup and teardown which sometimes can
    # take a significant amount of time. For this particular test we would like
    # to keep the _test timeout_ small because we intend to trigger it but the
    # overall timeout large.
    test = gen_cluster(client=True, timeout=5, cluster_dump_directory=tmp_path)(
        inner_test
    )
    try:
        with pytest.raises(asyncio.TimeoutError) as exc:
            test()
        assert "inner_test" in str(exc)
        assert "await asyncio.sleep(sleep_time)" in str(exc)
    except gen.TimeoutError:
        pytest.xfail("Cluster startup or teardown took too long")

    _, dirs, files = next(os.walk(tmp_path))
    assert not dirs
    assert files == [inner_test.__name__ + ".yaml"]
    import yaml

    with open(tmp_path / files[0], "rb") as fd:
        state = yaml.load(fd, Loader=yaml.Loader)

    assert "scheduler" in state
    assert "workers" in state


def test_assert_worker_story():
    now = time()
    story = [
        ("foo", "id1", now - 600),
        ("bar", "id2", now),
        ("baz", {1: 2}, "id2", now),
    ]
    # strict=False
    assert_worker_story(story, [("foo",), ("bar",), ("baz", {1: 2})])
    assert_worker_story(story, [])
    assert_worker_story(story, [("foo",)])
    assert_worker_story(story, [("foo",), ("bar",)])
    assert_worker_story(story, [("baz", lambda d: d[1] == 2)])
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("foo", "nomatch")])
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("baz",)])
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("baz", {1: 3})])
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("foo",), ("bar",), ("baz", "extra"), ("+1",)])
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("baz", lambda d: d[1] == 3)])
    with pytest.raises(KeyError):  # Faulty lambda
        assert_worker_story(story, [("baz", lambda d: d[2] == 1)])
    assert_worker_story([], [])
    assert_worker_story([("foo", "id1", now)], [("foo",)])
    with pytest.raises(AssertionError):
        assert_worker_story([], [("foo",)])

    # strict=True
    assert_worker_story([], [], strict=True)
    assert_worker_story([("foo", "id1", now)], [("foo",)])
    assert_worker_story(story, [("foo",), ("bar",), ("baz", {1: 2})], strict=True)
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("foo",), ("bar",)], strict=True)
    with pytest.raises(AssertionError):
        assert_worker_story(story, [("foo",), ("baz", {1: 2})], strict=True)
    with pytest.raises(AssertionError):
        assert_worker_story(story, [], strict=True)


@pytest.mark.parametrize(
    "story",
    [
        [()],  # Missing payload, stimulus_id, ts
        [("foo",)],  # Missing (stimulus_id, ts)
        [("foo", "bar")],  # Missing ts
        [("foo", "bar", "baz")],  # ts is not a float
        [("foo", "bar", time() + 3600)],  # ts is in the future
        [("foo", "bar", time() - 7200)],  # ts is too old
        [("foo", 123, time())],  # stimulus_id is not a string
        [("foo", "", time())],  # stimulus_id is an empty string
        [("", time())],  # no payload
        [("foo", "id", time()), ("foo", "id", time() - 10)],  # timestamps out of order
    ],
)
def test_assert_worker_story_malformed_story(story):
    with pytest.raises(AssertionError, match="Malformed story event"):
        assert_worker_story(story, [])


@gen_cluster()
async def test_dump_cluster_state(s, a, b, tmpdir):
    await dump_cluster_state(s, [a, b], str(tmpdir), "dump")
    with open(f"{tmpdir}/dump.yaml") as fh:
        out = yaml.safe_load(fh)

    assert out.keys() == {"scheduler", "workers", "versions"}
    assert out["workers"].keys() == {a.address, b.address}


@gen_cluster(nthreads=[])
async def test_dump_cluster_state_no_workers(s, tmpdir):
    await dump_cluster_state(s, [], str(tmpdir), "dump")
    with open(f"{tmpdir}/dump.yaml") as fh:
        out = yaml.safe_load(fh)

    assert out.keys() == {"scheduler", "workers", "versions"}
    assert out["workers"] == {}


@gen_cluster(Worker=Nanny)
async def test_dump_cluster_state_nannies(s, a, b, tmpdir):
    await dump_cluster_state(s, [a, b], str(tmpdir), "dump")
    with open(f"{tmpdir}/dump.yaml") as fh:
        out = yaml.safe_load(fh)

    assert out.keys() == {"scheduler", "workers", "versions"}
    assert out["workers"].keys() == s.workers.keys()


@gen_cluster()
async def test_dump_cluster_state_unresponsive_local_worker(s, a, b, tmpdir):
    a.stop()
    await dump_cluster_state(s, [a, b], str(tmpdir), "dump")
    with open(f"{tmpdir}/dump.yaml") as fh:
        out = yaml.safe_load(fh)

    assert out.keys() == {"scheduler", "workers", "versions"}
    assert isinstance(out["workers"][a.address], dict)
    assert isinstance(out["workers"][b.address], dict)


@pytest.mark.slow
@gen_cluster(
    client=True,
    Worker=Nanny,
    config={"distributed.comm.timeouts.connect": "600ms"},
)
async def test_dump_cluster_unresponsive_remote_worker(c, s, a, b, tmpdir):
    clog_fut = asyncio.create_task(
        c.run(lambda dask_scheduler: dask_scheduler.stop(), workers=[a.worker_address])
    )
    await asyncio.sleep(0.2)

    await dump_cluster_state(s, [a, b], str(tmpdir), "dump")
    with open(f"{tmpdir}/dump.yaml") as fh:
        out = yaml.safe_load(fh)

    assert out.keys() == {"scheduler", "workers", "versions"}
    assert isinstance(out["workers"][b.worker_address], dict)
    assert out["workers"][a.worker_address].startswith(
        "OSError('Timed out trying to connect to"
    )

    clog_fut.cancel()
