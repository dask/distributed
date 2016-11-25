
from operator import add

from distributed.utils_test import gen_cluster, inc
from distributed.worker2 import Worker2


@gen_cluster(client=True, ncores=[])
def test_submit(c, s):
    w = Worker2(s.ip, s.port, ncores=2, loop=s.loop)
    yield w._start()

    future = c.submit(inc, 1)
    result = yield future._result()
    assert result == 2

    yield w._close()


@gen_cluster(client=True)
def test_inter_worker_communication(c, s, a, b):
    [x, y] = yield c._scatter([1, 2])

    w = Worker2(s.ip, s.port, ncores=2, loop=s.loop)
    yield w._start()

    future = c.submit(add, x, y, workers=w.address)
    result = yield future._result()
    assert result == 3

    yield w._close()
