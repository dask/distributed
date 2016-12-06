from operator import add

from time import sleep
from toolz import take
from tornado import gen

from distributed import Client
from distributed import local_client
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

            sleep(5)  # TODO: this avoids a race condition

    def consume():
        with local_client() as c:
            x = c.channel('x')
            y = c.channel('y')
            last = 0
            for i, future in enumerate(x):
                last = c.submit(add, future, last)
                y.append(last)

            sleep(5)  # TODO: this avoids a race condition

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

