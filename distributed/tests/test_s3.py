import pytest
import json

import boto3
from tornado import gen

from dask.imperative import Value
from distributed import Executor
from distributed.executor import _wait, Future
from distributed.s3 import (read_bytes, read_text,
        read_block, seek_delimiter, S3FileSystem, _read_text)
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

@pytest.yield_fixture
def s3():
    # could do with a bucket with write privileges.
    yield S3FileSystem(anon=True)


def test_s3_file_access(s3):
    fn = 'distributed-test/nested/file1'
    data = b'hello\n'
    assert s3.cat(fn) == data
    assert s3.head(fn, 3) == data[:3]
    assert s3.tail(fn, 3) == data[-3:]


def test_s3_file_info(s3):
    fn = 'distributed-test/nested/file1'
    data = b'hello\n'
    assert fn in s3.walk('distributed-test')
    assert s3.exists(fn)
    assert not s3.exists(fn+'another')
    assert s3.info(fn)['Size'] == len(data)
    with pytest.raises(OSError):
        s3.info(fn+'another')
    assert s3.du(test_bucket_name, deep=True)[fn] == len(data)


def test_s3_ls(s3):
    fn = 'distributed-test/nested/file1'
    assert fn not in s3.ls('distributed-test/')
    assert fn in s3.ls('distributed-test/nested/')
    assert fn in s3.ls('distributed-test/nested')
    assert fn in s3.ls('s3://distributed-test/nested/')


def test_s3_glob(s3):
    fn = 'distributed-test/nested/file1'
    assert fn not in s3.glob('distributed-test/')
    assert fn not in s3.glob('distributed-test/*')
    assert fn in s3.glob('distributed-test/nested')
    assert fn in s3.glob('distributed-test/nested/*')
    assert fn in s3.glob('distributed-test/nested/file*')
    assert fn in s3.glob('distributed-test/*/*')


def test_get_list_of_summary_objects(s3):
    L = s3.ls(test_bucket_name + '/test')

    assert len(L) == 2
    assert [l.lstrip(test_bucket_name).lstrip('/') for l in sorted(L)] == sorted(list(files))

    L2 = s3.ls('s3://' + test_bucket_name + '/test')

    assert L == L2


def test_read_keys_from_bucket(s3):
    for k, data in files.items():
        file_contents = s3.cat('/'.join([test_bucket_name, k])) 
        assert file_contents == data

    assert (s3.cat('/'.join([test_bucket_name, k])) ==
            s3.cat('s3://' + '/'.join([test_bucket_name, k])))


def test_seek_delimiter(s3):
    fn = 'test/accounts.1.json'
    data = files[fn]
    with s3.open('/'.join([test_bucket_name, fn])) as f:
        seek_delimiter(f, b'}', 0)
        assert f.tell() == 0
        f.seek(1)
        seek_delimiter(f, b'}', 5)
        assert f.tell() == data.index(b'}') + 1
        seek_delimiter(f, b'\n', 5)
        assert f.tell() == data.index(b'\n') + 1
        f.seek(1, 1)
        ind = data.index(b'\n') + data[data.index(b'\n')+1:].index(b'\n') + 1
        seek_delimiter(f, b'\n', 5)
        assert f.tell() == ind + 1


def test_read_block(s3):
    import io
    data = files['test/accounts.1.json']
    lines = io.BytesIO(data).readlines()
    path = 'distributed-test/test/accounts.1.json'
    assert s3.read_block(path, 1, 35, b'\n') == lines[1]
    assert s3.read_block(path, 0, 30, b'\n') == lines[0]
    assert s3.read_block(path, 0, 35, b'\n') == lines[0] + lines[1]
    assert s3.read_block(path, 0, 5000, b'\n') == data
    assert len(s3.read_block(path, 0, 5)) == 5
    assert len(s3.read_block(path, 0, 5000)) == len(data)
    assert s3.read_block(path, 5000, 5010) == b''


@gen_cluster(timeout=60)
def test_read_bytes(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    futures = read_bytes(test_bucket_name+'/test/accounts.*', lazy=False)
    assert len(futures) >= len(files)
    results = yield e._gather(futures)
    assert set(results) == set(files.values())

    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_block(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    for bs in [5, 15, 45, 1500]:
        vals = read_bytes(test_bucket_name+'/test/account*', blocksize=bs)
        assert len(vals) == sum([(len(v) // bs + 1) for v in files.values()])
        futures = e.compute(vals)
        results = yield e._gather(futures)
        assert sum(len(r) for r in results) == sum(len(v) for v in
                   files.values())
        futures = read_bytes(test_bucket_name+'/test/accounts*', blocksize=bs,
                             lazy=False)
        assert len(vals) == len(futures)
        results = yield e._gather(futures)
        assert sum(len(r) for r in results) == sum(len(v) for v in
                   files.values())
        ourlines = b"".join(results).split(b'\n')
        testlines = b"".join(files.values()).split(b'\n')
        assert set(ourlines) == set(testlines)
    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_delimited(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()
    for bs in [5, 15, 45, 1500]:
        futures = read_bytes(test_bucket_name+'/test/accounts*',
                             lazy=False, blocksize=bs, delimiter=b'\n')
        results = yield e._gather(futures)
        res = [r for r in results if r]
        assert all(r.endswith(b'\n') for r in res)
        ourlines = b''.join(res).split(b'\n')
        testlines = b"".join(files[k] for k in sorted(files)).split(b'\n')
        assert ourlines == testlines
    
        # delimiter not at the end
        d = b'}'
        futures = read_bytes(test_bucket_name+'/test/accounts*',
                             lazy=False, blocksize=bs, delimiter=d)
        results = yield e._gather(futures)
        res = [r for r in results if r]
        # All should end in } except EOF
        assert sum(r.endswith(b'}') for r in res) == len(res) - 2
        ours = b"".join(res)
        test = b"".join(files[v] for v in sorted(files))
        assert ours == test
    yield e._shutdown()


@gen_cluster(timeout=60)
def test_read_bytes_lazy(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    values = read_bytes(test_bucket_name+'/test/', lazy=True)
    assert all(isinstance(v, Value) for v in values)

    results = e.compute(values, sync=False)
    results = yield e._gather(results)

    assert set(results).issuperset(set(files.values()))


@gen_cluster(timeout=60)
def test_read_text(s, a, b):
    e = Executor((s.ip, s.port), start=False)
    yield e._start()

    pytest.importorskip('dask.bag')
    import dask.bag as db
    from dask.imperative import Value

    b = yield _read_text(test_bucket_name+'/test/accounts*', lazy=True,
                  collection=True)
    assert isinstance(b, db.Bag)
    yield gen.sleep(0.2)
    assert not s.tasks

    future = e.compute(b.filter(None).map(json.loads).pluck('amount').sum())
    result = yield future._result()

    assert result == (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8) * 100

    text = yield _read_text(test_bucket_name+'/test/accounts*', lazy=True,
                     collection=False)
    assert all(isinstance(v, Value) for v in text)

    text = yield _read_text(test_bucket_name+'/test/accounts*', lazy=False,
                     collection=False)
    assert all(isinstance(v, Future) for v in text)


def test_read_text_sync(loop):
    pytest.importorskip('dask.bag')
    import dask.bag as db
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            b = read_text(test_bucket_name+'/test/accounts*', lazy=True,
                          collection=True)
            assert isinstance(b, db.Bag)
            c = b.filter(None).map(json.loads).pluck('amount').sum()
            result = c.compute(get=e.get)

            assert result == (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8) * 100
