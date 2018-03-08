from dask.core import flatten
from dask import delayed, persist

from distributed.utils_test import gen_cluster, inc, slowinc
from distributed import wait
from distributed.utils import tokey

import pytest


@gen_cluster(client=True)
def test_submit(c, s, a, b):
    low = c.submit(inc, 1, priority=-1)
    futures = c.map(slowinc, range(10), delay=0.1)
    high = c.submit(inc, 2, priority=1)
    yield wait(high)
    assert all(s.processing.values())
    assert s.tasks[low.key].state == 'processing'


@gen_cluster(client=True)
def test_map(c, s, a, b):
    low = c.map(inc, [1, 2, 3], priority=-1)
    futures = c.map(slowinc, range(10), delay=0.1)
    high = c.map(inc, [4, 5, 6], priority=1)
    yield wait(high)
    assert all(s.processing.values())
    assert s.tasks[low[0].key].state == 'processing'


@gen_cluster(client=True)
def test_compute(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.random.random((10, 10), chunks=(5, 5))
    y = da.random.random((10, 10), chunks=(5, 5))

    low = c.compute(x, priority=-1)
    futures = c.map(slowinc, range(10), delay=0.1)
    high = c.compute(y, priority=1)
    yield wait(high)
    assert all(s.processing.values())
    assert s.tasks[tokey(low.key)].state in ('processing', 'waiting')


@gen_cluster(client=True)
def test_persist(c, s, a, b):
    da = pytest.importorskip('dask.array')
    x = da.random.random((10, 10), chunks=(5, 5))
    y = da.random.random((10, 10), chunks=(5, 5))

    low = x.persist(priority=-1)
    futures = c.map(slowinc, range(10), delay=0.1)
    high = y.persist(priority=1)
    yield wait(high)
    assert all(s.processing.values())
    assert all(s.tasks[tokey(k)].state in ('processing', 'waiting')
               for k in flatten(low.__dask_keys__()))


@gen_cluster(client=True)
def test_expand_compute(c, s, a, b):
    low = delayed(inc)(1)
    many = [delayed(slowinc)(i, delay=0.1) for i in range(10)]
    high = delayed(inc)(2)

    low, many, high = c.compute([low, many, high], priority={low: -1, high: 1})
    yield wait(high)
    assert s.tasks[low.key].state == 'processing'


@gen_cluster(client=True)
def test_expand_persist(c, s, a, b):
    low = delayed(inc)(1, dask_key_name='low')
    many = [delayed(slowinc)(i, delay=0.1) for i in range(4)]
    high = delayed(inc)(2, dask_key_name='high')

    low, high, x, y, z, w = persist(low, high, *many, priority={low: -1, high: 1})
    yield wait(high)
    assert s.tasks[low.key].state == 'processing'
