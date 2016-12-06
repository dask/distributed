from tornado import gen

from distributed.utils_test import gen_cluster, inc


@gen_cluster(client=True)
def test_Buffer(c, s, a, b):
    x = c.channel('x')
    y = c.channel('y')

    while set(c._channel_handler.channels) != {'x', 'y'}:
        yield gen.sleep(0.01)

    xx = c.channel('x')
    yy = c.channel('y')

    yield gen.sleep(0.1)
    assert set(c._channel_handler.channels) == {'x', 'y'}

    future = c.submit(inc, 1)

    x.append(future)

    while not xx.futures:
        yield gen.sleep(0.01)

    assert xx.futures[0].key == future.key

