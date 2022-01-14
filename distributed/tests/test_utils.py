import array
import asyncio
import contextvars
import functools
import io
import os
import queue
import socket
import traceback
from collections import deque
from time import sleep

import pytest
from tornado.ioloop import IOLoop

import dask

from distributed.compatibility import MACOS, WINDOWS
from distributed.metrics import time
from distributed.utils import (
    LRU,
    All,
    Log,
    Logs,
    LoopRunner,
    TimeoutError,
    _maybe_complex,
    ensure_bytes,
    ensure_ip,
    format_dashboard_link,
    get_ip_interface,
    get_traceback,
    is_kernel,
    is_valid_xml,
    iscoroutinefunction,
    nbytes,
    offload,
    open_port,
    parse_ports,
    read_block,
    recursive_to_dict,
    seek_delimiter,
    set_thread_state,
    sync,
    thread_state,
    truncate_exception,
    warn_on_duration,
)
from distributed.utils_test import (
    _UnhashableCallable,
    captured_logger,
    div,
    gen_test,
    has_ipv6,
    inc,
    throws,
)


def test_All(loop):
    async def throws():
        1 / 0

    async def slow():
        await asyncio.sleep(10)

    async def inc(x):
        return x + 1

    async def f():
        results = await All([inc(i) for i in range(10)])
        assert results == list(range(1, 11))

        start = time()
        for tasks in [[throws(), slow()], [slow(), throws()]]:
            try:
                await All(tasks)
                assert False
            except ZeroDivisionError:
                pass
            end = time()
            assert end - start < 10

    loop.run_sync(f)


def test_sync_error(loop_in_thread):
    loop = loop_in_thread
    try:
        result = sync(loop, throws, 1)
    except Exception as exc:
        f = exc
        assert "hello" in str(exc)
        tb = get_traceback()
        L = traceback.format_tb(tb)
        assert any("throws" in line for line in L)

    def function1(x):
        return function2(x)

    def function2(x):
        return throws(x)

    try:
        result = sync(loop, function1, 1)
    except Exception as exc:
        assert "hello" in str(exc)
        tb = get_traceback()
        L = traceback.format_tb(tb)
        assert any("function1" in line for line in L)
        assert any("function2" in line for line in L)


def test_sync_timeout(loop_in_thread):
    loop = loop_in_thread
    with pytest.raises(TimeoutError):
        sync(loop_in_thread, asyncio.sleep, 0.5, callback_timeout=0.05)

    with pytest.raises(TimeoutError):
        sync(loop_in_thread, asyncio.sleep, 0.5, callback_timeout="50ms")


def test_sync_closed_loop():
    loop = IOLoop.current()
    loop.close()
    IOLoop.clear_current()
    IOLoop.clear_instance()

    with pytest.raises(RuntimeError) as exc_info:
        sync(loop, inc, 1)
    exc_info.match("IOLoop is clos(ed|ing)")


def test_is_kernel():
    pytest.importorskip("IPython")
    assert is_kernel() is False


# @pytest.mark.leaking('fds')
# def test_zzz_leaks(l=[]):
# import os, subprocess
# l.append(b"x" * (17 * 1024**2))
# os.open(__file__, os.O_RDONLY)
# subprocess.Popen('sleep 100', shell=True, stdin=subprocess.DEVNULL)


def test_ensure_ip():
    assert ensure_ip("localhost") in ("127.0.0.1", "::1")
    assert ensure_ip("123.123.123.123") == "123.123.123.123"
    assert ensure_ip("8.8.8.8") == "8.8.8.8"
    if has_ipv6():
        assert ensure_ip("2001:4860:4860::8888") == "2001:4860:4860::8888"
        assert ensure_ip("::1") == "::1"


@pytest.mark.skipif(WINDOWS, reason="TODO")
def test_get_ip_interface():
    iface = "lo0" if MACOS else "lo"
    assert get_ip_interface(iface) == "127.0.0.1"
    with pytest.raises(ValueError, match=f"'__notexist'.+network interface.+'{iface}'"):
        get_ip_interface("__notexist")


def test_truncate_exception():
    e = ValueError("a" * 1000)
    assert len(str(e)) >= 1000
    f = truncate_exception(e, 100)
    assert type(f) == type(e)
    assert len(str(f)) < 200
    assert "aaaa" in str(f)

    e = ValueError("a")
    assert truncate_exception(e) is e


def test_get_traceback():
    def a(x):
        return div(x, 0)

    def b(x):
        return a(x)

    def c(x):
        return b(x)

    try:
        c(1)
    except Exception as e:
        tb = get_traceback()
        assert type(tb).__name__ == "traceback"


def test_maybe_complex():
    assert not _maybe_complex(1)
    assert not _maybe_complex("x")
    assert _maybe_complex((inc, 1))
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex({"x": (inc, 1)})


def test_read_block():
    delimiter = b"\n"
    data = delimiter.join([b"123", b"456", b"789"])
    f = io.BytesIO(data)

    assert read_block(f, 1, 2) == b"23"
    assert read_block(f, 0, 1, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 2, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 3, delimiter=b"\n") == b"123\n"
    assert read_block(f, 0, 5, delimiter=b"\n") == b"123\n456\n"
    assert read_block(f, 0, 8, delimiter=b"\n") == b"123\n456\n789"
    assert read_block(f, 0, 100, delimiter=b"\n") == b"123\n456\n789"
    assert read_block(f, 1, 1, delimiter=b"\n") == b""
    assert read_block(f, 1, 5, delimiter=b"\n") == b"456\n"
    assert read_block(f, 1, 8, delimiter=b"\n") == b"456\n789"

    for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)], [(0, 4), (4, 4), (8, 4)]]:
        out = [read_block(f, o, l, b"\n") for o, l in ols]
        assert b"".join(filter(None, out)) == data


def test_seek_delimiter_endline():
    f = io.BytesIO(b"123\n456\n789")

    # if at zero, stay at zero
    seek_delimiter(f, b"\n", 5)
    assert f.tell() == 0

    # choose the first block
    for bs in [1, 5, 100]:
        f.seek(1)
        seek_delimiter(f, b"\n", blocksize=bs)
        assert f.tell() == 4

    # handle long delimiters well, even with short blocksizes
    f = io.BytesIO(b"123abc456abc789")
    for bs in [1, 2, 3, 4, 5, 6, 10]:
        f.seek(1)
        seek_delimiter(f, b"abc", blocksize=bs)
        assert f.tell() == 6

    # End at the end
    f = io.BytesIO(b"123\n456")
    f.seek(5)
    seek_delimiter(f, b"\n", 5)
    assert f.tell() == 7


def test_ensure_bytes():
    data = [b"1", "1", memoryview(b"1"), bytearray(b"1"), array.array("b", [49])]
    for d in data:
        result = ensure_bytes(d)
        assert isinstance(result, bytes)
        assert result == b"1"


def test_ensure_bytes_ndarray():
    np = pytest.importorskip("numpy")
    result = ensure_bytes(np.arange(12))
    assert isinstance(result, bytes)


def test_ensure_bytes_pyarrow_buffer():
    pa = pytest.importorskip("pyarrow")
    buf = pa.py_buffer(b"123")
    result = ensure_bytes(buf)
    assert isinstance(result, bytes)


def test_nbytes():
    np = pytest.importorskip("numpy")

    def check(obj, expected):
        assert nbytes(obj) == expected
        assert nbytes(memoryview(obj)) == expected

    check(b"123", 3)
    check(bytearray(b"4567"), 4)

    multi_dim = np.ones(shape=(10, 10))
    scalar = np.array(1)

    check(multi_dim, multi_dim.nbytes)
    check(scalar, scalar.nbytes)


def test_open_port():
    port = open_port()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", port))
    s.close()


def test_set_thread_state():
    with set_thread_state(x=1):
        assert thread_state.x == 1

    assert not hasattr(thread_state, "x")


def assert_running(loop):
    """
    Raise if the given IOLoop is not running.
    """
    q = queue.Queue()
    loop.add_callback(q.put, 42)
    assert q.get(timeout=1) == 42


def assert_not_running(loop):
    """
    Raise if the given IOLoop is running.
    """
    q = queue.Queue()
    try:
        loop.add_callback(q.put, 42)
    except RuntimeError:
        # On AsyncIOLoop, can't add_callback() after the loop is closed
        pass
    else:
        with pytest.raises(queue.Empty):
            q.get(timeout=0.02)


def test_loop_runner(loop_in_thread):
    # Implicit loop
    loop = IOLoop()
    loop.make_current()
    runner = LoopRunner()
    assert runner.loop not in (loop, loop_in_thread)
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)

    # Explicit loop
    loop = IOLoop()
    runner = LoopRunner(loop=loop)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(loop)
    runner.start()
    assert runner.is_started()
    assert_running(loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(loop)

    # Explicit loop, already started
    runner = LoopRunner(loop=loop_in_thread)
    assert not runner.is_started()
    assert_running(loop_in_thread)
    runner.start()
    assert runner.is_started()
    assert_running(loop_in_thread)
    runner.stop()
    assert not runner.is_started()
    assert_running(loop_in_thread)

    # Implicit loop, asynchronous=True
    loop = IOLoop()
    loop.make_current()
    runner = LoopRunner(asynchronous=True)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_not_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)

    # Explicit loop, asynchronous=True
    loop = IOLoop()
    runner = LoopRunner(loop=loop, asynchronous=True)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_not_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)


def test_two_loop_runners(loop_in_thread):
    # Loop runners tied to the same loop should cooperate

    # ABCCBA
    loop = IOLoop()
    a = LoopRunner(loop=loop)
    b = LoopRunner(loop=loop)
    assert_not_running(loop)
    a.start()
    assert_running(loop)
    c = LoopRunner(loop=loop)
    b.start()
    assert_running(loop)
    c.start()
    assert_running(loop)
    c.stop()
    assert_running(loop)
    b.stop()
    assert_running(loop)
    a.stop()
    assert_not_running(loop)

    # ABCABC
    loop = IOLoop()
    a = LoopRunner(loop=loop)
    b = LoopRunner(loop=loop)
    assert_not_running(loop)
    a.start()
    assert_running(loop)
    b.start()
    assert_running(loop)
    c = LoopRunner(loop=loop)
    c.start()
    assert_running(loop)
    a.stop()
    assert_running(loop)
    b.stop()
    assert_running(loop)
    c.stop()
    assert_not_running(loop)

    # Explicit loop, already started
    a = LoopRunner(loop=loop_in_thread)
    b = LoopRunner(loop=loop_in_thread)
    assert_running(loop_in_thread)
    a.start()
    assert_running(loop_in_thread)
    b.start()
    assert_running(loop_in_thread)
    a.stop()
    assert_running(loop_in_thread)
    b.stop()
    assert_running(loop_in_thread)


@gen_test()
async def test_loop_runner_gen():
    runner = LoopRunner(asynchronous=True)
    assert runner.loop is IOLoop.current()
    assert not runner.is_started()
    await asyncio.sleep(0.01)
    runner.start()
    assert runner.is_started()
    await asyncio.sleep(0.01)
    runner.stop()
    assert not runner.is_started()
    await asyncio.sleep(0.01)


@gen_test()
async def test_all_exceptions_logging():
    async def throws():
        raise Exception("foo1234")

    with captured_logger("") as sio:
        try:
            await All([throws() for _ in range(5)], quiet_exceptions=Exception)
        except Exception:
            pass

        import gc

        gc.collect()
        await asyncio.sleep(0.1)

    assert "foo1234" not in sio.getvalue()


def test_warn_on_duration():
    with pytest.warns(None) as record:
        with warn_on_duration("10s", "foo"):
            pass
    assert not record

    with pytest.warns(None) as record:
        with warn_on_duration("1ms", "foo"):
            sleep(0.100)

    assert record
    assert any("foo" in str(rec.message) for rec in record)


def test_logs():
    log = Log("Hello")
    assert isinstance(log, str)
    d = Logs({"123": log, "456": Log("World!")})
    assert isinstance(d, dict)
    text = d._repr_html_()
    assert is_valid_xml("<div>" + text + "</div>")
    assert "Hello" in text
    assert "456" in text


def test_is_valid_xml():
    assert is_valid_xml("<a>foo</a>")
    with pytest.raises(Exception):
        assert is_valid_xml("<a>foo")


def test_format_dashboard_link():
    with dask.config.set({"distributed.dashboard.link": "foo"}):
        assert format_dashboard_link("host", 1234) == "foo"

    assert "host" in format_dashboard_link("host", 1234)
    assert "1234" in format_dashboard_link("host", 1234)

    try:
        os.environ["host"] = "hello"
        assert "hello" not in format_dashboard_link("host", 1234)
    finally:
        del os.environ["host"]


def test_parse_ports():
    assert parse_ports(None) == [None]
    assert parse_ports(23) == [23]
    assert parse_ports("45") == [45]
    assert parse_ports("100:103") == [100, 101, 102, 103]

    with pytest.raises(ValueError, match="port_stop must be greater than port_start"):
        parse_ports("103:100")


def test_lru():

    l = LRU(maxsize=3)
    l["a"] = 1
    l["b"] = 2
    l["c"] = 3
    assert list(l.keys()) == ["a", "b", "c"]

    # Use "a" and ensure it becomes the most recently used item
    l["a"]
    assert list(l.keys()) == ["b", "c", "a"]

    # Ensure maxsize is respected
    l["d"] = 4
    assert len(l) == 3
    assert list(l.keys()) == ["c", "a", "d"]


@pytest.mark.asyncio
async def test_offload():
    assert (await offload(inc, 1)) == 2
    assert (await offload(lambda x, y: x + y, 1, y=2)) == 3


@pytest.mark.asyncio
async def test_offload_preserves_contextvars():
    var = contextvars.ContextVar("var")

    async def set_var(v: str):
        var.set(v)
        r = await offload(var.get)
        assert r == v

    await asyncio.gather(set_var("foo"), set_var("bar"))


def test_serialize_for_cli_deprecated():
    with pytest.warns(FutureWarning, match="serialize_for_cli is deprecated"):
        from distributed.utils import serialize_for_cli
    assert serialize_for_cli is dask.config.serialize


def test_deserialize_for_cli_deprecated():
    with pytest.warns(FutureWarning, match="deserialize_for_cli is deprecated"):
        from distributed.utils import deserialize_for_cli
    assert deserialize_for_cli is dask.config.deserialize


def test_parse_bytes_deprecated():
    with pytest.warns(FutureWarning, match="parse_bytes is deprecated"):
        from distributed.utils import parse_bytes
    assert parse_bytes is dask.utils.parse_bytes


def test_format_bytes_deprecated():
    with pytest.warns(FutureWarning, match="format_bytes is deprecated"):
        from distributed.utils import format_bytes
    assert format_bytes is dask.utils.format_bytes


def test_format_time_deprecated():
    with pytest.warns(FutureWarning, match="format_time is deprecated"):
        from distributed.utils import format_time
    assert format_time is dask.utils.format_time


def test_funcname_deprecated():
    with pytest.warns(FutureWarning, match="funcname is deprecated"):
        from distributed.utils import funcname
    assert funcname is dask.utils.funcname


def test_parse_timedelta_deprecated():
    with pytest.warns(FutureWarning, match="parse_timedelta is deprecated"):
        from distributed.utils import parse_timedelta
    assert parse_timedelta is dask.utils.parse_timedelta


def test_typename_deprecated():
    with pytest.warns(FutureWarning, match="typename is deprecated"):
        from distributed.utils import typename
    assert typename is dask.utils.typename


def test_tmpfile_deprecated():
    with pytest.warns(FutureWarning, match="tmpfile is deprecated"):
        from distributed.utils import tmpfile
    assert tmpfile is dask.utils.tmpfile


def test_iscoroutinefunction_unhashable_input():
    # Ensure iscoroutinefunction can handle unhashable callables
    assert not iscoroutinefunction(_UnhashableCallable())


def test_iscoroutinefunction_nested_partial():
    async def my_async_callable(x, y, z):
        pass

    assert iscoroutinefunction(
        functools.partial(functools.partial(my_async_callable, 1), 2)
    )


def test_recursive_to_dict():
    class C:
        def __init__(self, x):
            self.x = x

        def __repr__(self):
            return "<C>"

        def _to_dict(self, *, exclude):
            assert exclude == ["foo"]
            return ["C:", recursive_to_dict(self.x, exclude=exclude)]

    class D:
        def __repr__(self):
            return "<D>"

    inp = [
        1,
        1.1,
        True,
        False,
        None,
        "foo",
        b"bar",
        C,
        C(1),
        D(),
        (1, 2),
        [3, 4],
        {5, 6},
        frozenset([7, 8]),
        deque([9, 10]),
    ]
    expect = [
        1,
        1.1,
        True,
        False,
        None,
        "foo",
        "b'bar'",
        "<class 'test_utils.test_recursive_to_dict.<locals>.C'>",
        ["C:", 1],
        "<D>",
        [1, 2],
        [3, 4],
        list({5, 6}),
        list(frozenset([7, 8])),
        [9, 10],
    ]
    assert recursive_to_dict(inp, exclude=["foo"]) == expect

    # Test recursion
    a = []
    c = C(a)
    a += [c, c]
    # The blocklist of already-seen objects is reentrant: a is converted to string when
    # found inside itself; c must *not* be converted to string the second time it's
    # found, because it's outside of itself.
    assert recursive_to_dict(a, exclude=["foo"]) == [
        ["C:", "[<C>, <C>]"],
        ["C:", "[<C>, <C>]"],
    ]
