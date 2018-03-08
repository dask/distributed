from __future__ import print_function, division, absolute_import

from time import sleep

import pytest
from toolz import frequencies, pluck
from tornado import gen
from tornado.ioloop import IOLoop

from distributed import Client, wait, Adaptive, LocalCluster
from distributed.utils_test import gen_cluster, gen_test, slowinc
from distributed.utils_test import loop, nodebug  # noqa: F401
from distributed.metrics import time


def test_get_scale_up_kwargs(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostics_port=None, loop=loop) as cluster:

        alc = Adaptive(cluster.scheduler, cluster, interval=100,
                       scale_factor=3)
        assert alc.get_scale_up_kwargs() == {'n': 1}

        with Client(cluster, loop=loop) as c:
            future = c.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            assert c.ncores()
            assert alc.get_scale_up_kwargs() == {'n': 3}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 4)
def test_simultaneous_scale_up_and_down(c, s, *workers):
    class TestAdaptive(Adaptive):
        def get_scale_up_kwargs(self):
            assert False

        def _retire_workers(self):
            assert False

    class TestCluster(object):
        def scale_up(self, n, **kwargs):
            assert False

        def scale_down(self, workers):
            assert False

    cluster = TestCluster()

    s.task_duration['a'] = 4
    s.task_duration['b'] = 4
    s.task_duration['c'] = 1

    future = c.map(slowinc, [1, 1, 1], key=['a-4', 'b-4', 'c-1'])

    while len(s.rprocessing) < 3:
        yield gen.sleep(0.001)

    ta = TestAdaptive(s, cluster, interval=100, scale_factor=2)

    yield gen.sleep(0.3)


def test_adaptive_local_cluster(loop):
    with LocalCluster(0, scheduler_port=0, silence_logs=False,
                      diagnostics_port=None, loop=loop) as cluster:
        alc = Adaptive(cluster.scheduler, cluster, interval=100)
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


@nodebug
@gen_test(timeout=30)
def test_adaptive_local_cluster_multi_workers():
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 asynchronous=True)
    try:
        cluster.scheduler.allowed_failures = 1000
        alc = Adaptive(cluster.scheduler, cluster, interval=100)
        c = yield Client(cluster, asynchronous=True)

        futures = c.map(slowinc, range(100), delay=0.01)

        start = time()
        while not cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 15, alc.log

        yield c.gather(futures)
        del futures

        start = time()
        # while cluster.workers:
        while cluster.scheduler.workers:
            yield gen.sleep(0.01)
            assert time() < start + 15, alc.log

        # assert not cluster.workers
        assert not cluster.scheduler.workers
        yield gen.sleep(0.2)
        # assert not cluster.workers
        assert not cluster.scheduler.workers

        futures = c.map(slowinc, range(100), delay=0.01)
        yield c.gather(futures)

    finally:
        yield c._close()
        yield cluster._close()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 10)
def test_adaptive_scale_down_override(c, s, *workers):
    class TestAdaptive(Adaptive):
        def __init__(self, *args, **kwargs):
            self.min_size = kwargs.pop("min_size", 0)
            Adaptive.__init__(self, *args, **kwargs)

        def workers_to_close(self):
            num_workers = len(self.scheduler.workers)
            to_close = self.scheduler.workers_to_close()
            if num_workers - len(to_close) < self.min_size:
                to_close = to_close[:num_workers - self.min_size]

            return to_close

    class TestCluster(object):
        def scale_up(self, n, **kwargs):
            assert False

        def scale_down(self, workers):
            assert False

    assert len(s.workers) == 10

    # Assert that adaptive cycle does not reduce cluster below minimum size
    # as determined via override.
    cluster = TestCluster()
    ta = TestAdaptive(s, cluster, min_size=2, interval=.1, scale_factor=2)
    yield gen.sleep(0.3)

    assert len(s.workers) == 2


@gen_test(timeout=30)
def test_min_max():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True)
    yield cluster._start()
    try:
        adapt = Adaptive(cluster.scheduler, cluster, minimum=1, maximum=2,
                         interval='20 ms')
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
            assert time() < start + 1
        assert frequencies(pluck(1, adapt.log)) == {'up': 2, 'down': 1}
    finally:
        yield c._close()
        yield cluster._close()


@gen_test()
def test_avoid_churn():
    """ We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    cluster = yield LocalCluster(0, asynchronous=True, processes=False,
                                 scheduler_port=0, silence_logs=False,
                                 diagnostics_port=None)
    client = yield Client(cluster, asynchronous=True)
    try:
        adapt = Adaptive(cluster.scheduler, cluster, interval=20, wait_count=5)

        for i in range(10):
            yield client.submit(slowinc, i, delay=0.040)
            yield gen.sleep(0.040)

        assert frequencies(pluck(1, adapt.log)) == {'up': 1}
    finally:
        yield client._close()
        yield cluster._close()


@gen_test(timeout=None)
def test_adapt_quickly():
    """ We want to avoid creating and deleting workers frequently

    Instead we want to wait a few beats before removing a worker in case the
    user is taking a brief pause between work
    """
    cluster = yield LocalCluster(0, asynchronous=True, processes=False,
                                 scheduler_port=0, silence_logs=False,
                                 diagnostics_port=None)
    client = yield Client(cluster, asynchronous=True)
    adapt = Adaptive(cluster.scheduler, cluster, interval=20, wait_count=5,
                     maximum=10)
    try:
        future = client.submit(slowinc, 1, delay=0.100)
        yield wait(future)
        assert len(adapt.log) == 1

        # Scale up when there is plenty of available work
        futures = client.map(slowinc, range(1000), delay=0.100)
        while frequencies(pluck(1, adapt.log)) == {'up': 1}:
            yield gen.sleep(0.01)
        assert len(adapt.log) == 2
        assert 'up' in adapt.log[-1]
        d = [x for x in adapt.log[-1] if isinstance(x, dict)][0]
        assert 2 < d['n'] <= adapt.maximum

        while len(cluster.scheduler.workers) < adapt.maximum:
            yield gen.sleep(0.01)

        del futures

        while len(cluster.scheduler.workers) > 1:
            yield gen.sleep(0.01)

        # Don't scale up for large sequential computations
        x = yield client.scatter(1)
        for i in range(100):
            x = client.submit(slowinc, x)

        yield gen.sleep(0.1)
        assert len(cluster.scheduler.workers) == 1
    finally:
        yield client._close()
        yield cluster._close()


@gen_test(timeout=None)
def test_adapt_down():
    """ Ensure that redefining adapt with a lower maximum removes workers """
    cluster = yield LocalCluster(0, asynchronous=True, processes=False,
                                 scheduler_port=0, silence_logs=False,
                                 diagnostics_port=None)
    client = yield Client(cluster, asynchronous=True)
    cluster.adapt(interval='20ms', maximum=5)

    try:
        futures = client.map(slowinc, range(1000), delay=0.1)
        while len(cluster.scheduler.workers) < 5:
            yield gen.sleep(0.1)

        cluster.adapt(maximum=2)

        start = time()
        while len(cluster.scheduler.workers) != 2:
            yield gen.sleep(0.1)
            assert time() < start + 1
    finally:
        yield client._close()
        yield cluster._close()


@pytest.mark.xfail(reason="we currently only judge occupancy, not ntasks")
@gen_test(timeout=30)
def test_no_more_workers_than_tasks():
    loop = IOLoop.current()
    cluster = yield LocalCluster(0, scheduler_port=0, silence_logs=False,
                                 processes=False, diagnostics_port=None,
                                 loop=loop, asynchronous=True)
    yield cluster._start()
    try:
        adapt = Adaptive(cluster.scheduler, cluster, minimum=0, maximum=4,
                         interval='10 ms')
        client = yield Client(cluster, asynchronous=True, loop=loop)
        cluster.scheduler.task_duration['slowinc'] = 1000

        yield client.submit(slowinc, 1, delay=0.100)

        assert len(cluster.scheduler.workers) <= 1
    finally:
        yield client._close()
        yield cluster._close()
