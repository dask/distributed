from tornado import gen

from distributed.utils_test import gen_cluster, inc
from distributed.streaming_buffer import BufferScheduler, BufferClient, Buffer


@gen_cluster(client=True)
def test_Buffer(c, s, a, b):
    bs = BufferScheduler(s)
    bc = BufferClient(c)

    x = Buffer(c, 'x')
    y = Buffer(c, 'y')

    while set(bs.deques) != {'x', 'y'}:
        yield gen.sleep(0.01)

    xx = Buffer(c, 'x')
    yy = Buffer(c, 'y')

    yield gen.sleep(0.1)
    assert set(bs.deques) == {'x', 'y'}

    future = c.submit(inc, 1)

    x.append(future)

    while not xx.futures:
        yield gen.sleep(0.01)

    assert xx.futures[0].key == future.key

