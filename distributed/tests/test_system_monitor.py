import multiprocessing
import subprocess
import tempfile
import time
import sys
import pytest

from distributed.system_monitor import SystemMonitor


def write_script(folder):
    ret = tempfile.NamedTemporaryFile(dir=folder).name
    with open(ret, mode="w") as stream:
        stream.write(
            """
def fib(n):
    if n < 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)

"""
        )
    return ret


def fib(n):
    if n < 2:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def test_zombie():
    with tempfile.TemporaryDirectory() as folder:
        sm = SystemMonitor()
        sm.update()
        process = subprocess.Popen(
            (sys.executable, write_script(folder)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        sm.update()
        process.terminate()
        try:
            sm.update()
        except Exception as exc:
            pytest.fail("unexpected exception: {0}".format(exc))


def test_missing_subprocess():
    sm = SystemMonitor()
    p = multiprocessing.Process(target=fib, args=(20,))
    p.start()
    sm.update()
    p.join()
    try:
        sm.update()
    except Exception as exc:
        pytest.fail("unexpected exception: {0}".format(exc))


def test_subprocess():
    sm = SystemMonitor()
    a = sm.update()
    p = multiprocessing.Process(target=fib, args=(20,))
    p.start()

    # On the first iteration CPU usage of the subprocess is 0
    sm.update()
    b = sm.update()
    p.join()

    assert sm.cpu
    assert sm.memory
    assert a["memory"] < b["memory"]


def test_SystemMonitor():
    sm = SystemMonitor()
    a = sm.update()
    time.sleep(0.01)
    b = sm.update()

    assert sm.cpu
    assert sm.memory
    assert set(a) == set(b)
    assert all(rb >= 0 for rb in sm.read_bytes)
    assert all(wb >= 0 for wb in sm.write_bytes)
    assert all(len(q) == 3 for q in sm.quantities.values())

    assert "cpu" in repr(sm)


def test_count():
    sm = SystemMonitor(n=5)
    assert sm.count == 1
    sm.update()
    assert sm.count == 2

    for i in range(10):
        sm.update()

    assert sm.count == 12
    for v in sm.quantities.values():
        assert len(v) == 5


def test_range_query():
    sm = SystemMonitor(n=5)

    assert all(len(v) == 1 for v in sm.range_query(0).values())
    assert all(len(v) == 0 for v in sm.range_query(123).values())

    sm.update()
    sm.update()
    sm.update()

    assert all(len(v) == 4 for v in sm.range_query(0).values())
    assert all(len(v) == 3 for v in sm.range_query(1).values())

    for i in range(10):
        sm.update()

    assert all(len(v) == 4 for v in sm.range_query(10).values())
    assert all(len(v) == 5 for v in sm.range_query(0).values())
