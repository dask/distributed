from distributed.utils_test import gen_cluster, inc, slowinc
from distributed import wait


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    low = c.submit(inc, 1, priority=-1)
    futures = c.map(slowinc, range(10), delay=0.1)
    high = c.submit(inc, 2, priority=1)
    yield wait(high)
    assert all(s.processing.values())
    assert s.tasks[low.key].state == 'processing'
