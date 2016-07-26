from __future__ import print_function, division, absolute_import

from datetime import timedelta
import sys

import pytest
pytest.importorskip('numpy')
pytest.importorskip('pandas')

import dask
import dask.dataframe as dd
import dask.bag as db
from distributed import Executor
from distributed.executor import _wait
from distributed.utils_test import cluster, loop, gen_cluster
from distributed.collections import (_futures_to_dask_dataframe,
        futures_to_dask_dataframe, _futures_to_dask_array,
        futures_to_dask_array, _futures_to_collection,
        _futures_to_dask_bag, futures_to_dask_bag, _future_to_dask_array,
         _futures_to_dask_arrays, futures_to_collection, future_to_dask_array,
         futures_to_dask_arrays)
import numpy as np
import pandas as pd
import pandas.util.testing as tm
from toolz import identity

from tornado import gen
from tornado.ioloop import IOLoop


dfs = [pd.DataFrame({'x': [1, 2, 3]}, index=[0, 10, 20]),
       pd.DataFrame({'x': [4, 5, 6]}, index=[30, 40, 50]),
       pd.DataFrame({'x': [7, 8, 9]}, index=[60, 70, 80])]


def assert_equal(a, b):
    assert type(a) == type(b)
    if isinstance(a, pd.DataFrame):
        tm.assert_frame_equal(a, b)
    elif isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
    elif isinstance(a, pd.Index):
        tm.assert_index_equal(a, b)
    else:
        assert a == b


@gen_cluster(executor=True)
def test__futures_to_dask_dataframe(e, s, a, b):
    remote_dfs = e.map(identity, dfs)
    ddf = yield _futures_to_dask_dataframe(remote_dfs, divisions=True,
            executor=e)

    assert isinstance(ddf, dd.DataFrame)
    assert ddf.divisions == (0, 30, 60, 80)
    assert ddf._known_dtype
    expr = ddf.x.sum()
    result = yield e._get(expr.dask, expr._keys())
    assert result == [sum([df.x.sum() for df in dfs])]


@gen_cluster(executor=True)
def test_no_divisions(e, s, a, b):
    dfs = e.map(tm.makeTimeDataFrame, range(5, 10))

    df = yield _futures_to_dask_dataframe(dfs)
    assert not df.known_divisions
    assert list(df.columns) == list(tm.makeTimeDataFrame(5).columns)


def test_futures_to_dask_dataframe(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_dfs = e.map(lambda x: x, dfs)
            ddf = futures_to_dask_dataframe(remote_dfs, divisions=True)

            assert isinstance(ddf, dd.DataFrame)
            assert ddf.x.sum().compute(get=e.get) == sum([df.x.sum() for df in dfs])

            ddf2 = futures_to_collection(remote_dfs, divisions=True)
            assert type(ddf) == type(ddf2)
            assert ddf.dask == ddf2.dask


@gen_cluster(timeout=120, executor=True)
def test_dataframes(e, s, a, b):
    dfs = [pd.DataFrame({'x': np.random.random(100),
                         'y': np.random.random(100)},
                        index=list(range(i, i + 100)))
           for i in range(0, 100*10, 100)]

    remote_dfs = e.map(lambda x: x, dfs)
    rdf = yield _futures_to_dask_dataframe(remote_dfs, divisions=True)
    name = 'foo'
    ldf = dd.DataFrame({(name, i): df for i, df in enumerate(dfs)},
                       name, dfs[0].columns,
                       list(range(0, 1000, 100)) + [999])

    assert rdf.divisions == ldf.divisions

    remote = e.compute(rdf)
    result = yield remote._result()

    tm.assert_frame_equal(result,
                          ldf.compute(get=dask.get))

    exprs = [lambda df: df.x.mean(),
             lambda df: df.y.std(),
             lambda df: df.assign(z=df.x + df.y).drop_duplicates(),
             lambda df: df.index,
             lambda df: df.x,
             lambda df: df.x.cumsum(),
             lambda df: df.groupby(['x', 'y']).count(),
             lambda df: df.loc[50:75]]
    for f in exprs:
        local = f(ldf).compute(get=dask.get)
        remote = e.compute(f(rdf))
        remote = yield gen.with_timeout(timedelta(seconds=5), remote._result())
        assert_equal(local, remote)


@gen_cluster(timeout=60, executor=True)
def test__futures_to_dask_array(e, s, a, b):
    import dask.array as da
    remote_arrays = [[[e.submit(np.full, (2, 3, 4), i + j + k)
                        for i in range(2)]
                        for j in range(2)]
                        for k in range(4)]

    x = yield _futures_to_dask_array(remote_arrays, executor=e)
    assert x.chunks == ((2, 2, 2, 2), (3, 3), (4, 4))
    assert x.dtype == np.full((), 0).dtype

    assert isinstance(x, da.Array)
    expr = x.sum()
    result = yield e._get(expr.dask, expr._keys())
    assert isinstance(result[0], np.number)


@gen_cluster(executor=True)
def test__future_to_dask_array(e, s, a, b):
    import dask.array as da
    f = e.submit(np.ones, (5, 5))
    a = yield _future_to_dask_array(f)

    assert a.shape == (5, 5)
    assert a.dtype == np.ones(1).dtype
    assert isinstance(a, da.Array)

    aa = yield e.compute(a)._result()
    assert (aa == 1).all()


@gen_cluster(executor=True)
def test__futures_to_dask_arrays(e, s, a, b):
    import dask.array as da

    fs = e.map(np.ones, [(5, i) for i in range(1, 6)], pure=False)
    arrs = yield _futures_to_dask_arrays(fs)
    assert len(fs) == len(arrs) == 5

    assert all(a.shape == (5, i + 1) for i, a in enumerate(arrs))
    assert all(a.dtype == np.ones(1).dtype for a in arrs)
    assert all(isinstance(a, da.Array) for a in arrs)

    xs = yield e._gather(e.compute(arrs))
    assert all((x == 1).all() for x in xs)


def test_futures_to_dask_arrays(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            futures = e.map(np.ones, [(5, i) for i in range(1, 6)])
            x = future_to_dask_array(futures[0])
            assert x.shape == (5, 1)
            assert (x.compute(get=e.get) == 1).all()

            xs = futures_to_dask_arrays(futures)
            assert [x.shape for x in xs] == [(5, i) for i in range(1, 6)]



@gen_cluster(executor=True)
def test__dask_array_collections(e, s, a, b):
    import dask.array as da

    x_dsk = {('x', i, j): np.random.random((3, 3)) for i in range(3)
                                                   for j in range(2)}
    y_dsk = {('y', i, j): np.random.random((3, 3)) for i in range(2)
                                                   for j in range(3)}
    x_futures = yield e._scatter(x_dsk)
    y_futures = yield e._scatter(y_dsk)

    dt = np.random.random(0).dtype
    x_local = da.Array(x_dsk, 'x', ((3, 3, 3), (3, 3)), dt)
    y_local = da.Array(y_dsk, 'y', ((3, 3), (3, 3, 3)), dt)

    x_remote = da.Array(x_futures, 'x', ((3, 3, 3), (3, 3)), dt)
    y_remote = da.Array(y_futures, 'y', ((3, 3), (3, 3, 3)), dt)

    exprs = [lambda x, y: x.T + y,
             lambda x, y: x.mean() + y.mean(),
             lambda x, y: x.dot(y).std(axis=0),
             lambda x, y: x - x.mean(axis=1)[:, None]]

    for expr in exprs:
        local = expr(x_local, y_local).compute(get=dask.get)

        remote = e.compute(expr(x_remote, y_remote))
        remote = yield remote._result()

        assert np.all(local == remote)


@gen_cluster(executor=True)
def test__futures_to_dask_bag(e, s, a, b):
    import dask.bag as db

    L = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    futures = yield e._scatter(L)

    rb = yield _futures_to_dask_bag(futures)
    assert isinstance(rb, db.Bag)
    assert rb.npartitions == len(L)

    lb = db.from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9], npartitions=3)

    exprs = [lambda x: x.map(lambda x: x + 1).sum(),
             lambda x: x.filter(lambda x: x % 2)]

    for expr in exprs:
        local = expr(lb).compute(get=dask.get)
        remote = e.compute(expr(rb))
        remote = yield remote._result()

        assert local == remote


def test_futures_to_dask_bag(loop):
    import dask.bag as db
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
            futures = e.scatter(data)
            b = futures_to_dask_bag(futures)

            assert isinstance(b, db.Bag)
            assert b.map(lambda x: x + 1).sum().compute(get=e.get) == sum(range(2, 11))


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason='KQueue error - uncertain cause')
def test_futures_to_dask_array(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            remote_arrays = [[e.submit(np.full, (3, 3), i + j)
                                for i in range(3)]
                                for j in range(3)]

            x = futures_to_dask_array(remote_arrays, executor=e)
            assert x.chunks == ((3, 3, 3), (3, 3, 3))
            assert x.dtype == np.full((), 0).dtype

            assert x.sum().compute(get=e.get) == 162
            assert (x + x.T).sum().compute(get=e.get) == 162 * 2

            y = futures_to_collection(remote_arrays, executor=e)
            assert x.dask == y.dask


@gen_cluster(executor=True)
def test__futures_to_collection(e, s, a, b):
    remote_dfs = e.map(identity, dfs)
    ddf = yield _futures_to_collection(remote_dfs, divisions=True)
    ddf2 = yield _futures_to_dask_dataframe(remote_dfs, divisions=True)
    assert isinstance(ddf, dd.DataFrame)

    assert ddf.dask == ddf2.dask

    remote_arrays = e.map(np.arange, range(3, 5))
    x = yield _futures_to_collection(remote_arrays)
    y = yield _futures_to_dask_array(remote_arrays)

    assert type(x) == type(y)
    assert x.dask == y.dask

    remote_lists = yield e._scatter([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    b = yield _futures_to_collection(remote_lists)
    c = yield _futures_to_dask_bag(remote_lists)

    assert type(b) == type(c)
    assert b.dask == b.dask


@gen_cluster(executor=True)
def test_bag_groupby_tasks_default(e, s, a, b):
    with dask.set_options(get=e.get):
        b = db.range(100, npartitions=10)
        b2 = b.groupby(lambda x: x % 13)
        assert not any('partd' in k[0] for k in b2.dask)


def test_dataframe_set_index_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            with dask.set_options(get=e.get):
                df = dd.demo.make_timeseries('2000', '2001',
                        {'value': float, 'name': str, 'id': int},
                        freq='2H', partition_freq='1M', seed=1)
                df = e.persist(df)

                df2 = df.set_index('name', shuffle='tasks')
                df2 = e.persist(df2)

                df2.head()


def test_loc_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            df = pd.util.testing.makeTimeDataFrame()
            ddf = dd.from_pandas(df, npartitions=10)
            ddf.loc['2000-01-17':'2000-01-24'].compute(get=e.get)


def test_rolling_sync(loop):
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            df = pd.util.testing.makeTimeDataFrame()
            ddf = dd.from_pandas(df, npartitions=10)
            dd.rolling_mean(ddf.A, 2).compute(get=e.get)


@gen_cluster(executor=True)
def test_loc(e, s, a, b):
    df = pd.util.testing.makeTimeDataFrame()
    ddf = dd.from_pandas(df, npartitions=10)
    future = e.compute(ddf.loc['2000-01-17':'2000-01-24'])
    yield future._result()


def test_dataframe_groupby_tasks(loop):
    df = pd.util.testing.makeTimeDataFrame()
    df['A'] = df.A // 0.1
    df['B'] = df.B // 0.1
    ddf = dd.from_pandas(df, npartitions=10)
    with cluster() as (c, [a, b]):
        with Executor(('127.0.0.1', c['port']), loop=loop) as e:
            with dask.set_options(get=e.get):
                for ind in [lambda x: 'A', lambda x: x.A]:
                    a = df.groupby(ind(df)).apply(len)
                    b = ddf.groupby(ind(ddf)).apply(len)
                    assert_equal(a, b.compute(get=dask.get).sort_index())
                    assert not any('partd' in k[0] for k in b.dask)

                    a = df.groupby(ind(df)).B.apply(len)
                    b = ddf.groupby(ind(ddf)).B.apply(len)
                    assert_equal(a, b.compute(get=dask.get).sort_index())
                    assert not any('partd' in k[0] for k in b.dask)

                with pytest.raises(NotImplementedError):
                    ddf.groupby(ddf[['A', 'B']]).apply(len)

                a = df.groupby(['A', 'B']).apply(len)
                b = ddf.groupby(['A', 'B']).apply(len)

                assert_equal(a, b.compute(get=dask.get).sort_index())
