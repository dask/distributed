import pytest
import json

import boto3
from tornado import gen

from dask.imperative import Value
from distributed import Executor
from distributed.executor import _wait, Future
from distributed.s3 import (read_bytes, get_list_of_summary_objects,
        read_content_from_keys, get_s3, read_text, read_block,
        seek_delimiter)
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster, loop, cluster


ip = get_ip()


# These get mirrored on s3://distributed-test/
test_bucket_name = 'distributed-test'
files = {'test/accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                   b'{"amount": 200, "name": "Bob"}\n'
                                   b'{"amount": 300, "name": "Charlie"}\n'
                                   b'{"amount": 400, "name": "Dennis"}\n'),
         'test/accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                                   b'{"amount": 600, "name": "Bob"}\n'
                                   b'{"amount": 700, "name": "Charlie"}\n'
                                   b'{"amount": 800, "name": "Dennis"}\n')}



def test_get_list_of_summary_objects():
    L = get_list_of_summary_objects(test_bucket_name, prefix='test/accounts',
                                    anon=True)

    assert len(L) == 2
    assert list(map(lambda o: o.key, L)) == sorted(list(files))

    L2 = get_list_of_summary_objects('s3://' + test_bucket_name, prefix='/test/accounts',
                                    anon=True)

    assert L == L2


def test_read_keys_from_bucket():
    for k, data in files.items():
        file_contents = read_content_from_keys('distributed-test', k, anon=True)

        assert file_contents == data

    assert (read_content_from_keys('s3://distributed-test', k, anon=True) ==
            read_content_from_keys('distributed-test', k, anon=True))


def test_seek_delimiter():
    fn = 'test/accounts.1.json'
    data = files[fn]
    s3 = get_s3(True)
    ob = s3.Object(test_bucket_name, fn)
    i = seek_delimiter(ob, 0, b'}')
    assert i == 0
    i = seek_delimiter(ob, 5, b'}')
    assert i == data.index(b'}') + 1
    i = seek_delimiter(ob, 5, b'\n')
    assert i == data.index(b'\n') + 1


def test_read_block():
    import io
    data = files['test/accounts.1.json']
    lines = io.BytesIO(data).readlines()
    buc, fn = 'distributed-test', 'test/accounts.1.json'
    assert read_block(buc, fn, 1, 35, b'\n') == lines[1]
    assert read_block(buc, fn, 0, 30, b'\n') == lines[0]
    assert read_block(buc, fn, 0, 35, b'\n') == lines[0] + lines[1]
    assert read_block(buc, fn, 0, 5000, b'\n') == data
    assert len(read_block(buc, fn, 0, 5)) == 5
    assert len(read_block(buc, fn, 0, 5000)) == len(data)
    assert read_block(buc, fn, 5000, 5010) == b''


def test_list_summary_object_with_prefix_and_delimiter():
    keys = get_list_of_summary_objects(test_bucket_name, 'nested/nested2/',
                                       delimiter='/', anon=True)

    assert len(keys) == 2
    assert [k.key for k in keys] == [u'nested/nested2/file1',
                                     u'nested/nested2/file2']

    keys = get_list_of_summary_objects(test_bucket_name, 'nested/', anon=True)

    assert len(keys) == 4
    assert [k.key for k in keys] == [u'nested/file1',
                                     u'nested/file2',
                                     u'nested/nested2/file1',
                                     u'nested/nested2/file2']


@gen_cluster(timeout=60)
def test_read_bytes(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    futures = read_bytes(test_bucket_name, prefix='test/accounts.', anon=True,
                         lazy=False)
    assert len(futures) >= len(files)
    results = yield e._gather(futures)
    assert set(results).issuperset(set(files.values()))

    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_block(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    bs = 15
    vals = read_bytes(test_bucket_name, prefix='test/accounts', anon=True,
                      lazy=True, blocksize=bs)
    assert len(vals) == sum([(len(v) // bs + 1) for v in files.values()])
    futures = e.compute(vals)
    results = yield e._gather(futures)
    assert sum(len(r) for r in results) == sum(len(v) for v in
               files.values())
    futures = read_bytes(test_bucket_name, prefix='test/accounts', anon=True,
                         lazy=False, blocksize=bs)
    assert len(vals) == len(futures)
    results = yield e._gather(futures)
    assert sum(len(r) for r in results) == sum(len(v) for v in
               files.values())
    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_delimited(s, a, b):
    import io
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    bs = 15
    futures = read_bytes(test_bucket_name, prefix='test/accounts', anon=True,
                         lazy=False, blocksize=bs, delimiter=b'\n')
    results = yield e._gather(futures)
    res = [r for r in results if r]
    data = [io.BytesIO(o).readlines() for o in files.values()]
    assert set(res) == set(data[0] + data[1])

    # delimiter not at the end
    d = b'}'
    futures = read_bytes(test_bucket_name, prefix='test/accounts', anon=True,
                         lazy=False, blocksize=bs, delimiter=d)
    results = yield e._gather(futures)
    res = [r for r in results if r]
    assert len(res) == sum(f.count(d) for f in files.values()) + len(files)
    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_lazy(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    values = read_bytes(test_bucket_name, 'test/', lazy=True, anon=True)
    assert all(isinstance(v, Value) for v in values)

    results = e.compute(values, sync=False)
    results = yield e._gather(results)

    assert set(results).issuperset(set(files.values()))

    yield e._shutdown()


def test_get_s3():
    assert get_s3(True) is get_s3(True)
    assert get_s3(False) is get_s3(False)
    assert get_s3(True) is not get_s3(False)
    assert 'boto3' in type(get_s3(True)).__module__


def test_get_s3_threadsafe():
    from multiprocessing.pool import ThreadPool
    tp = ThreadPool(2)

    s3s = tp.map(get_s3, [True] * 8 + [False * 8])
    assert len(set(map(id, s3s))) <= 4


@gen_cluster(timeout=60)
def test_read_text(s, a, b):
    pytest.importorskip('dask.bag')
    import dask.bag as db
    from dask.imperative import Value
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    b = read_text(test_bucket_name, 'test/accounts', lazy=True,
                  collection=True, anon=True)
    assert isinstance(b, db.Bag)
    yield gen.sleep(0.2)
    assert not s.tasks

    future = e.compute(b.filter(None).map(json.loads).pluck('amount').sum())
    result = yield future._result()

    assert result == (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8) * 100

    text = read_text(test_bucket_name, 'test/accounts', lazy=True,
                     collection=False, anon=True)
    assert all(isinstance(v, Value) for v in text)

    text = read_text(test_bucket_name, 'test/accounts', lazy=False,
                     collection=False, anon=True)
    assert all(isinstance(v, Future) for v in text)

    yield e._shutdown()


def test_read_text_sync(loop):
    pytest.importorskip('dask.bag')
    import dask.bag as db
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            b = read_text(test_bucket_name, 'test/accounts', lazy=True,
                          collection=True)
            assert isinstance(b, db.Bag)
            c = b.filter(None).map(json.loads).pluck('amount').sum()
            result = c.compute(get=e.get)

            assert result == (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8) * 100
