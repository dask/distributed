from operator import add

from time import sleep
from toolz import take
from tornado import gen

from distributed import Client
from distributed import local_client
from distributed.metrics import time
from distributed.utils_test import gen_cluster, inc, loop, cluster, slowinc


@gen_cluster(client=True)
def test_channel(c, s, a, b):
    x = c.channel('x')
    y = c.channel('y')

    assert len(x) == 0

    while set(c._channel_handler.channels) != {'x', 'y'}:
        yield gen.sleep(0.01)

    xx = c.channel('x')
    yy = c.channel('y')

    assert len(x) == 0

    yield gen.sleep(0.1)
    assert set(c._channel_handler.channels) == {'x', 'y'}

    future = c.submit(inc, 1)

    x.append(future)

    while not x.futures:
        yield gen.sleep(0.01)

    assert len(x) == 1

    assert xx.futures[0].key == future.key

    xxx = c.channel('x')
    while not xxx.futures:
        yield gen.sleep(0.01)

    assert xxx.futures[0].key == future.key

    assert 'x' in repr(x)
    assert '1' in repr(x)


def test_local_client(loop):
    def produce(n):
        with local_client() as c:
            x = c.channel('x')
            for i in range(n):
                future = c.submit(slowinc, i, delay=0.01)
                x.append(future)

            x.flush()

    def consume():
        with local_client() as c:
            x = c.channel('x')
            y = c.channel('y')
            last = 0
            for i, future in enumerate(x):
                last = c.submit(add, future, last)
                y.append(last)

            y.flush()

    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            x = c.channel('x')
            y = c.channel('y')

            producers = (c.submit(produce, 5), c.submit(produce, 10))
            consumer = c.submit(consume)

            results = []
            for future in take(15, y):
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

    results = yield c._gather(list(chan.futures))
    assert results == [6, 7, 8, 9, 10]
