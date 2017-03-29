from __future__ import print_function, division, absolute_import

from operator import add
from time import sleep

import pytest
from toolz import take
from tornado import gen

from distributed import Client
from distributed import worker_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, loop, cluster, slowinc


@gen_cluster(client=True)
def test_channel(c, s, a, b):
    x = c.channel('x')
    y = c.channel('y')

    assert len(x) == 0

    while set(c.extensions['channels'].channels) != {'x', 'y'}:
        yield gen.sleep(0.01)

    xx = c.channel('x')
    yy = c.channel('y')

    assert len(x) == 0

    yield gen.sleep(0.1)
    assert set(c.extensions['channels'].channels) == {'x', 'y'}

    future = c.submit(inc, 1)

    x.append(future)

    while not x.data:
        yield gen.sleep(0.01)

    assert len(x) == 1

    assert xx.data[0].key == future.key

    xxx = c.channel('x')
    while not xxx.data:
        yield gen.sleep(0.01)

    assert xxx.data[0].key == future.key

    assert 'x' in repr(x)
    assert '1' in repr(x)


def test_worker_client(loop):
    def produce(n):
        with worker_client() as c:
            x = c.channel('x')
            for i in range(n):
                future = c.submit(slowinc, i, delay=0.01, key='f-%d' % i)
                x.append(future)

            x.flush()

    def consume():
        with worker_client() as c:
            x = c.channel('x')
            y = c.channel('y')
            last = 0
            for i, future in enumerate(x):
                last = c.submit(add, future, last, key='add-' + future.key)
                y.append(last)

            y.flush()

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            x = c.channel('x')
            y = c.channel('y')

            producers = (c.submit(produce, 5), c.submit(produce, 10))
            consumer = c.submit(consume)

            results = []
            for i, future in enumerate(take(15, y)):
                result = future.result()
                results.append(result)

            assert len(results) == 15
            assert all(0 < r < 100 for r in results)


@gen_cluster(client=True)
def test_channel_scheduler(c, s, a, b):
    chan = c.channel('chan', maxlen=5)

    x = c.submit(inc, 1)
    key = x.key
    chan.append(x)
    del x

    while not len(chan):
        yield gen.sleep(0.01)

    assert 'streaming-chan' in s.who_wants[key]
    assert s.wants_what['streaming-chan'] == {key}

    while len(s.who_wants[key]) < 2:
        yield gen.sleep(0.01)

    assert s.wants_what[c.id] == {key}

    for i in range(10):
        chan.append(c.submit(inc, i))

    start = time()
    while True:
        if len(chan) == len(s.task_state) == 5:
            break
        else:
            assert time() < start + 2
            yield gen.sleep(0.01)

    results = yield c._gather(list(chan.data))
    assert results == [6, 7, 8, 9, 10]


@gen_cluster(client=True)
def test_multiple_maxlen(c, s, a, b):
    c2 = Client((s.ip, s.port), start=False)
    yield c2._start()

    x = c.channel('x', maxlen=10)
    assert x.data.maxlen == 10
    x2 = c2.channel('x', maxlen=20)
    assert x2.data.maxlen == 20

    for i in range(10):
        x.append(c.submit(inc, i))

    while len(s.wants_what[c2.id]) < 10:
        yield gen.sleep(0.01)

    for i in range(10, 20):
        x.append(c.submit(inc, i))

    while len(x2) < 20:
        yield gen.sleep(0.01)

    yield gen.sleep(0.1)

    assert len(x2) == 20  # They stay this long after a delay
    assert len(s.task_state) == 20

    yield c2._shutdown()


def test_stop(loop):
    def produce(n):
        with worker_client() as c:
            x = c.channel('x')
            for i in range(n):
                future = c.submit(slowinc, i, delay=0.01, key='f-%d' % i)
                x.append(future)

            x.stop()
            x.flush()

    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            x = c.channel('x')

            producer = c.submit(produce, 5)

            futures = list(x)
            assert len(futures) == 5

            with pytest.raises(StopIteration):
                x.append(c.submit(inc, 1))

            with Client(s['address']) as c2:
                xx = c2.channel('x')
                futures = list(xx)
                assert len(futures) == 5


@gen_cluster(client=True)
def test_values(c, s, a, b):
    c2 = Client((s.ip, s.port), start=False)
    yield c2._start()

    x = c.channel('x')
    x2 = c2.channel('x')

    data = [123, 'Hello!', {'x': [1, 2, 3]}]
    for item in data:
        x.append(item)

    while len(x2.data) < 3:
        yield gen.sleep(0.01)

    assert list(x2.data) == data

    yield c2._shutdown()


def test_channel_gets_updates_immediately(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            x = c.channel('x')
            future = c.submit(inc, 1)
            x.append(future)
            x.flush()

        with Client(s['address']) as c:
            x = c.channel('x')
            future = next(iter(x))
            assert future.result() == 2


def test_channel_gets_updates_immediately_2(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address']) as c:
            x = c.channel('x')

            with Client(s['address']) as c2:
                x2 = c.channel('x')
                future = c2.submit(inc, 1)
                x2.append(future)
                x2.flush()

            future = next(iter(x))
            assert future.result() == 2
