
from operator import add

from distributed.utils_test import gen_cluster, inc, slowinc
from distributed.worker2 import Worker2

from distributed.client import _wait


@gen_cluster(client=True, Worker=Worker2, ncores=[('127.0.0.1', 1)])
def test_submit(c, s, a):
    assert isinstance(a, Worker2)
    future = c.submit(inc, 1)
    result = yield future._result()
    assert result == 2


@gen_cluster(client=True, Worker=Worker2)
def test_inter_worker_communication(c, s, a, b):
    [x, y] = yield c._scatter([1, 2], workers=a.address)

    future = c.submit(add, x, y, workers=b.address)
    result = yield future._result()
    assert result == 3


@gen_cluster(client=True, Worker=Worker2)
def test_map(c, s, a, b):
    futures = c.map(slowinc, range(20), delay=0.01)
    result = yield c._gather(futures)
    assert result == list(range(1, 21))


@gen_cluster(client=True, Worker=Worker2, ncores=[('127.0.0.1', 1)] * 8,
        timeout=None)
def test_dataframes(c, s, *workers):
    import dask.dataframe as dd
    import numpy as np
    df = dd.demo.make_timeseries('2000', '2001',
                                 {'value': float, 'name': str, 'id': int},
                                 freq='60s', partition_freq='1D', seed=1)
    df = c.persist(df)
    yield _wait(df)

    future = c.compute(df.index.quantile(np.linspace(0, 1, 100)))
    result = yield future._result()
