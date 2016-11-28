
from operator import add, mul
import sys

import pytest
from tornado import gen

from distributed.client import _wait
from distributed.utils_test import gen_cluster, inc, slowinc


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_submit(c, s, a):
    future = c.submit(inc, 1)
    result = yield future._result()
    assert result == 2


@gen_cluster(client=True)
def test_inter_worker_communication(c, s, a, b):
    [x, y] = yield c._scatter([1, 2], workers=a.address)

    future = c.submit(add, x, y, workers=b.address)
    result = yield future._result()
    assert result == 3


@gen_cluster(client=True)
def test_map(c, s, a, b):
    futures = c.map(slowinc, range(20), delay=0.01)
    result = yield c._gather(futures)
    assert result == list(range(1, 21))


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 8,
        timeout=None)
def test_dataframes(c, s, *workers):
    import dask.dataframe as dd
    import numpy as np
    df = dd.demo.make_timeseries('2000-01-01', '2000-02-01',
                                 {'value': float, 'name': str, 'id': int},
                                 freq='60s', partition_freq='1D', seed=1)
    df = c.persist(df)
    yield _wait(df)

    future = c.compute(df.index.quantile(np.linspace(0, 1, 100)))
    result = yield future._result()


@gen_cluster(client=True)
def test_clean(c, s, a, b):
    x = c.submit(inc, 1, workers=a.address)
    y = c.submit(inc, x, workers=b.address)

    yield y._result()

    collections = [a.tasks, a.task_state, a.response, a.data, a.nbytes,
                   a.durations, a.priorities]
    for c in collections:
        assert c

    x.release()
    y.release()

    while x.key in a.task_state:
        yield gen.sleep(0.01)

    for c in collections:
        assert not c


@pytest.mark.skipif(sys.version_info[:2] == (3, 4), reason="mul bytes fails")
@gen_cluster(client=True)
def test_message_breakup(c, s, a, b):
    xs = [c.submit(mul, b'%d' % i, 1000000, workers=a.address) for i in range(30)]
    y = c.submit(lambda *args: None, xs, workers=b.address)
    yield y._result()

    assert 2 <= len(b.incoming_transfer_log) <= 20
    assert 2 <= len(a.outgoing_transfer_log) <= 20

    assert all(msg['who'] == b.address for msg in a.outgoing_transfer_log)
    assert all(msg['who'] == a.address for msg in a.incoming_transfer_log)
