from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep

import pytest
from toolz import take
from tornado import gen

from distributed import Client, Queue
from distributed import worker_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, loop, cluster, slowinc


@gen_cluster(client=True)
def test_queue(c, s, a, b):
    x = yield Queue('x')
    y = yield Queue('y')
    xx = yield Queue('x')
    assert x.client is c

    future = c.submit(inc, 1)

    yield x._put(future)
    yield y._put(future)
    future2 = yield xx._get()
    assert future.key == future2.key

    with pytest.raises(gen.TimeoutError):
        yield x._get(timeout=0.1)

    del future, future2

    yield gen.sleep(0.1)
    assert s.task_state  # future still present in y's queue
    yield y._get()  # burn future

    start = time()
    while s.task_state:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True)
def test_queue_with_data(c, s, a, b):
    x = yield Queue('x')
    xx = yield Queue('x')
    assert x.client is c

    yield x._put([1, 'hello'])
    data = yield xx._get()

    assert data == [1, 'hello']

    with pytest.raises(gen.TimeoutError):
        yield x._get(timeout=0.1)


def test_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            future = c.submit(lambda x: x + 1, 10)
            x = Queue('x')
            xx = Queue('x')
            x.put(future)
            assert x.qsize() == 1
            assert xx.qsize() == 1
            future2 = xx.get()

            assert future2.result() == 11
