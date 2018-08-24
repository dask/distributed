from __future__ import print_function, division, absolute_import

from time import sleep

import pytest
from toolz import frequencies, pluck
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client, LocalCluster
from distributed.utils_test import gen_test, slowinc
from distributed.utils_test import loop, nodebug  # noqa: F401
from distributed.metrics import time


def test_adaptive_local_cluster_adapt(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostics_port=None, loop=loop) as cluster:
        cluster.adapt(interval=100)
        with Client(cluster, loop=loop) as c:
            assert not c.ncores()
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.ncores()

            sleep(0.1)
            assert c.ncores()  # still there after some time

            del future

            start = time()
            while cluster.scheduler.ncores:
                sleep(0.01)
                assert time() < start + 5

            assert not c.ncores()


@gen_test(timeout=30)
def test_min_max():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True,
                                 threads_per_worker=2)
    yield cluster._start()
    try:
        adapt = cluster.adapt(minimum=1, maximum=2, interval='20 ms', wait_count=10)
        c = yield Client(cluster, asynchronous=True, loop=loop)

        start = time()
        while not cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 1

        yield gen.sleep(0.2)
        assert len(cluster.scheduler.workers) == 1
        assert frequencies(pluck(1, adapt.log)) == {'up': 1}

        futures = c.map(slowinc, range(100), delay=0.1)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            yield gen.sleep(0.01)
            assert time() < start + 1

        assert len(cluster.scheduler.workers) == 2
        yield gen.sleep(0.5)
        assert len(cluster.scheduler.workers) == 2
        assert len(cluster.workers) == 2
        assert frequencies(pluck(1, adapt.log)) == {'up': 2}

        del futures

        start = time()
        while len(cluster.scheduler.workers) != 1:
            yield gen.sleep(0.01)
            assert time() < start + 2
        assert frequencies(pluck(1, adapt.log)) == {'up': 2, 'down': 1}
    finally:
        yield c._close()
        yield cluster._close()


@gen_test(timeout=30)
def test_min_max_cores():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True,
                                 threads_per_worker=2)
    cluster.worker_info = {'cores': 2, 'memory': '256 MB'}
    yield cluster._start()
    try:
        adapt = cluster.adapt(minimum_cores=2, maximum_cores=4, interval='20 ms', wait_count=10)
        c = yield Client(cluster, asynchronous=True, loop=loop)

        start = time()
        while not cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 1

        yield gen.sleep(0.2)
        assert len(cluster.scheduler.workers) == 1
        assert frequencies(pluck(1, adapt.log)) == {'up': 1}

        futures = c.map(slowinc, range(100), delay=0.1)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            yield gen.sleep(0.01)
            assert time() < start + 1

        assert len(cluster.scheduler.workers) == 2
        yield gen.sleep(0.5)
        assert len(cluster.scheduler.workers) == 2
        assert len(cluster.workers) == 2
        assert frequencies(pluck(1, adapt.log)) == {'up': 2}

        del futures

        start = time()
        while len(cluster.scheduler.workers) != 1:
            yield gen.sleep(0.01)
            assert time() < start + 2
        assert frequencies(pluck(1, adapt.log)) == {'up': 2, 'down': 1}
    finally:
        yield c._close()
        yield cluster._close()


@gen_test(timeout=30)
def test_scale_cores_and_memory():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True)
    cluster.worker_info = {'cores': 2, 'memory': '256 MB'}
    yield cluster._start()
    try:
        cluster.scale(cores=4)
        c = yield Client(cluster, asynchronous=True, loop=loop)

        start = time()
        while not cluster.scheduler.workers and len(cluster.scheduler.workers) < 2:
            yield gen.sleep(0.01)
            assert time() < start + 1

        assert len(cluster.scheduler.workers) == 2
        yield gen.sleep(0.5)
        assert len(cluster.scheduler.workers) == 2
        assert len(cluster.workers) == 2

        cluster.scale(memory='0 B')
        start = time()
        while len(cluster.scheduler.workers) != 0:
            yield gen.sleep(0.01)
            assert time() < start + 2

    finally:
        yield c._close()
        yield cluster._close()


@gen_test(timeout=30)
def test_scale_cores_error():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True)
    yield cluster._start()
    with pytest.raises(NotImplementedError):
        cluster.scale(cores=10)

    with pytest.raises(NotImplementedError):
        cluster.scale(memory='10TB')

    with pytest.raises(NotImplementedError):
        cluster.adapt(minimum_cores=4)

    with pytest.raises(ValueError):
        cluster.scale(10, cores=20)
