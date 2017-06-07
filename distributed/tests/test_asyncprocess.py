from __future__ import print_function, division, absolute_import

from datetime import datetime
import gc
import os
import signal
import sys
from time import sleep
import weakref

import pytest
from tornado import gen

try:
    import psutil
except ImportError:
    psutil = None

from distributed.metrics import time
from distributed.process import AsyncProcess
from distributed.utils import ignoring, mp_context
from distributed.utils_test import gen_test


def feed(in_q, out_q):
    obj = in_q.get(timeout=5)
    out_q.put(obj)

def exit(q):
    sys.exit(q.get())

def exit_with_signal(signum):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    while True:
        os.kill(os.getpid(), signum)
        sleep(0.01)

def wait():
    while True:
        sleep(0.01)


@gen_test()
def test_simple():
    to_child = mp_context.Queue()
    from_child = mp_context.Queue()

    proc = AsyncProcess(target=feed, args=(to_child, from_child))
    assert not proc.is_alive()
    assert proc.pid is None
    assert proc.exitcode is None
    assert proc.daemon

    with pytest.raises(AssertionError):
        yield proc.join()

    yield proc.start()
    assert proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode is None
    if psutil is not None:
        p = psutil.Process(proc.pid)
        assert p.is_running()
        assert os.path.realpath(p.exe()) == os.path.realpath(sys.executable)

    t1 = time()
    yield proc.join(timeout=0.02)
    dt = time() - t1
    assert 0.2 >= dt >= 0.01
    assert proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode is None

    to_child.put(5)
    assert from_child.get() == 5

    t1 = time()
    yield proc.join(timeout=10)
    dt = time() - t1
    assert dt <= 1.0
    assert not proc.is_alive()
    assert proc.pid is not None
    assert proc.exitcode is 0

    # join() again
    t1 = time()
    yield proc.join()
    dt = time() - t1
    assert dt <= 0.2

    wr1 = weakref.ref(proc)
    wr2 = weakref.ref(proc._process)
    del proc
    gc.collect()
    t1 = time()
    assert wr1() is None
    while wr2() is not None:
        yield gen.sleep(0.01)
        dt = time() - t1
        assert dt < 1.0


@gen_test()
def test_exitcode():
    q = mp_context.Queue()

    proc = AsyncProcess(target=exit, kwargs={'q': q})
    assert not proc.is_alive()
    assert proc.exitcode is None

    yield proc.start()
    assert proc.is_alive()
    assert proc.exitcode is None

    q.put(5)
    yield proc.join(timeout=1.0)
    assert not proc.is_alive()
    assert proc.exitcode == 5


@pytest.mark.skipif(os.name == 'nt', reason="POSIX only")
@gen_test()
def test_signal():
    proc = AsyncProcess(target=exit_with_signal, args=(signal.SIGINT,))
    assert not proc.is_alive()
    assert proc.exitcode is None

    yield proc.start()
    yield proc.join(timeout=2.0)

    assert not proc.is_alive()
    # Can be 255 with forkserver, see https://bugs.python.org/issue30589
    assert proc.exitcode in (-signal.SIGINT, 255)

    proc = AsyncProcess(target=wait)
    yield proc.start()
    os.kill(proc.pid, signal.SIGTERM)
    yield proc.join(timeout=2.0)

    assert not proc.is_alive()
    assert proc.exitcode in (-signal.SIGTERM, 255)


@gen_test()
def test_terminate():
    proc = AsyncProcess(target=wait)
    yield proc.start()
    yield proc.terminate()

    yield proc.join(timeout=2.0)
    assert not proc.is_alive()
    assert proc.exitcode in (-signal.SIGTERM, 255)
