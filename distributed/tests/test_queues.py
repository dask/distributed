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
    xx = yield Queue('x')
    assert x.client is c

    future = c.submit(inc, 1)

    yield x._put(future)
    future2 = yield xx._get()
    assert future.key == future2.key

    with pytest.raises(gen.TimeoutError):
        yield x._get(timeout=0.1)


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
