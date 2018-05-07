from distributed import Pub, Sub, wait
from distributed.utils_test import gen_cluster
from distributed.metrics import time
import toolz

from tornado import gen


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    async def publish():
        pub = Pub('a')

        i = 0
        while True:
            await gen.sleep(0.01)
            pub.put(i)
            i += 1

    def f(_):
        sub = Sub('a')
        return list(toolz.take(5, sub))

    c.run_coroutine(publish, workers=[a.address])

    tasks = [c.submit(f, i) for i in range(4)]
    results = yield c.gather(tasks)

    for r in results:
        x = r[0]
        assert r == [x, x + 1, x + 2, x + 3, x + 4]


@gen_cluster(client=True, timeout=None)
def test_speed(c, s, a, b):
    """
    This tests how quickly we can move messages back and forth

    This is mostly a test of latency
    """
    def pingpong(a, b, start=False, n=1000, msg=1):
        sub = Sub(a)
        pub = Pub(b)

        if start:
            pub.put(msg)

        for i in range(n):
            msg = next(sub)
            pub.put(msg)
            if i % 100 == 0:
                print(a, b, i)

    import numpy as np
    x = np.random.random(1000)

    x = c.submit(pingpong, 'a', 'b', start=True, msg=x)
    y = c.submit(pingpong, 'b', 'a')

    start = time()
    yield wait([x, y])
    stop = time()
    print('duration', stop - start)  # I get around 3ms/roundtrip on my laptop
