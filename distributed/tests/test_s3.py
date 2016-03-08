import pytest
import json
import io

import boto3
from tornado import gen

from dask.imperative import Value
from distributed import Executor
from distributed.executor import _wait, Future
from distributed.s3 import (read_bytes, read_text, seek_delimiter,
        S3FileSystem, _read_text, _read_csv, read_csv, avro_body,
        avro_to_df, _read_avro, read_avro)
from distributed.utils import get_ip
from distributed.utils_test import gen_cluster, loop, cluster, slow


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

records = [
{u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
{u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
{u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
{u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

def make_avro():
    from fastavro import writer
    schema = {'name': 'test', 'namespace': 'test', 'type': 'record', 'fields': [
        {'name': 'station', 'type': 'string'}, {'name': 'temp', 'type': 'int'},
        {'name': 'time', 'type': 'long'}]}
    b = io.BytesIO()
    writer(b, schema, records)
    b.seek(0)
    return b.read()


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
    assert s3.tail(fn, 10000) == data


def test_s3_file_info(s3):
    fn = 'distributed-test/nested/file1'
    data = b'hello\n'
    assert fn in s3.walk('distributed-test')
    assert s3.exists(fn)
    assert not s3.exists(fn+'another')
    assert s3.info(fn)['Size'] == len(data)
    with pytest.raises((OSError, IOError)):
        s3.info(fn+'another')

def test_du(s3):
    d = s3.du(test_bucket_name, deep=True)
    assert all(isinstance(v, int) and v >= 0 for v in d.values())
    assert 'distributed-test/nested/file1' in d

    assert s3.du(test_bucket_name + '/test/', total=True) ==\
           sum(map(len, files.values()))


def test_s3_ls(s3):
    fn = 'distributed-test/nested/file1'
    assert fn not in s3.ls('distributed-test/')
    assert fn in s3.ls('distributed-test/nested/')
    assert fn in s3.ls('distributed-test/nested')
    assert s3.ls('s3://distributed-test/nested/') == s3.ls('distributed-test/nested')


def test_s3_ls_detail(s3):
    L = s3.ls('distributed-test/nested', detail=True)
    assert all(isinstance(item, dict) for item in L)


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


@slow
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


def test_read_s3_block(s3):
    import io
    data = files['test/accounts.1.json']
    lines = io.BytesIO(data).readlines()
    path = 'distributed-test/test/accounts.1.json'
    assert s3.read_block(path, 1, 35, b'\n') == lines[1]
    assert s3.read_block(path, 0, 30, b'\n') == lines[0]
    assert s3.read_block(path, 0, 35, b'\n') == lines[0] + lines[1]
    assert s3.read_block(path, 0, 5000, b'\n') == data
    assert len(s3.read_block(path, 0, 5)) == 5
    assert len(s3.read_block(path, 4, 5000)) == len(data) - 4
    assert s3.read_block(path, 5000, 5010) == b''

    assert s3.read_block(path, 5, None) == s3.read_block(path, 5, 1000)


@gen_cluster(timeout=60, executor=True)
def test_read_bytes(e, s, a, b):
    futures = read_bytes(test_bucket_name+'/test/accounts.*', lazy=False)
    assert len(futures) >= len(files)
    results = yield e._gather(futures)
    assert set(results) == set(files.values())


@gen_cluster(timeout=60, executor=True)
def test_read_bytes_blocksize_none(e, s, a, b):
    futures = read_bytes(test_bucket_name+'/test/accounts.*', lazy=False,
                         blocksize=None)
    assert len(futures) == len(files)


@gen_cluster(timeout=60, executor=True)
def test_read_bytes_blocksize_on_large_data(e, s, a, b):
    L = read_bytes('dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv',
                    lazy=True, blocksize=None)
    assert len(L) == 1

    L = read_bytes('dask-data/nyc-taxi/2014/*.csv', lazy=True, blocksize=None)
    assert len(L) == 12


@gen_cluster(timeout=60, executor=True)
def test_read_bytes_block(e, s, a, b):
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


@gen_cluster(timeout=60, executor=True)
def test_read_bytes_delimited(e, s, a, b):
    for bs in [5, 15, 45, 1500]:
        futures = read_bytes(test_bucket_name+'/test/accounts*',
                             lazy=False, blocksize=bs, delimiter=b'\n')
        futures2 = read_bytes(test_bucket_name+'/test/accounts*',
                             lazy=False, blocksize=bs, delimiter=b'foo')
        assert [a.key for a in futures] != [b.key for b in futures2]
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


@gen_cluster(timeout=60, executor=True)
def test_read_bytes_lazy(e, s, a, b):
    values = read_bytes(test_bucket_name+'/test/', lazy=True)
    assert all(isinstance(v, Value) for v in values)

    results = e.compute(values, sync=False)
    results = yield e._gather(results)

    assert set(results).issuperset(set(files.values()))


@gen_cluster(timeout=60, executor=True)
def test_read_text(e, s, a, b):
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


def test_pickle(s3):
    import pickle
    a = pickle.loads(pickle.dumps(s3))

    assert [a.anon, a.key, a.secret, a.kwargs, a.dirs] == \
           [s3.anon, s3.key, s3.secret, s3.kwargs, s3.dirs]

    assert a.ls('distributed-test/') == s3.ls('distributed-test/')


def test_errors(s3):
    try:
        s3.open('distributed-test/test/accounts.1.json', mode='rt')
    except Exception as e:
        assert "mode='rb'" in str(e)
    try:
        s3.open('distributed-test/test/accounts.1.json', mode='r')
    except Exception as e:
        assert "mode='rb'" in str(e)


def test_seek(s3):
    fn = 'test/accounts.1.json'
    b = io.BytesIO(files[fn])
    with s3.open('/'.join([test_bucket_name, fn])) as f:
        assert f.tell() == b.tell()
        f.seek(10)
        b.seek(10)
        assert f.tell() == b.tell()
        f.seek(10, 1)
        b.seek(10, 1)
        assert f.tell() == b.tell()
        assert f.read(5) == b.read(5)
        assert f.tell() == b.tell()
        f.seek(10, 2)
        b.seek(10, 2)
        assert f.tell() == b.tell()
        assert f.read(5) == b.read(5)
        assert f.tell() == b.tell()
        assert f.read(1000) == b.read(1000)
        assert f.tell() == b.tell()


def test_repr(s3):
    with s3.open('distributed-test/test/accounts.1.json', mode='rb') as f:
        assert 'distributed-test' in repr(f)
        assert 'accounts.1.json' in repr(f)


def test_read_past_location(s3):
    with s3.open('distributed-test/test/accounts.1.json', block_size=20) as f:
        while f.read(10):
            pass
        f.seek(5000)
        out = f.read(10)
        assert out == b''


csv_files = {'2014-01-01.csv': (b'name,amount,id\n',
                                b'Alice,100,1\n',
                                b'Bob,200,2\n',
                                b'Charlie,300,3\n'),
             '2014-01-02.csv': (b'name,amount,id\n'),
             '2014-01-03.csv': (b'name,amount,id\n',
                                b'Dennis,400,4\n',
                                b'Edith,500,5\n',
                                b'Frank,600,6\n')}


@gen_cluster(timeout=60, executor=True)
def test_read_csv(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')
    s3 = S3FileSystem(anon=True)

    df = yield _read_csv('distributed-test/csv/2015/', lazy=True)
    yield gen.sleep(0.1)
    assert not s.tasks
    assert isinstance(df, dd.DataFrame)

    df = yield _read_csv('distributed-test/csv/2015/')
    assert isinstance(df, dd.DataFrame)
    assert list(df.columns) == ['name', 'amount', 'id']

    f = e.compute(df.amount.sum())
    result = yield f._result()
    assert result == (100 + 200 + 300 + 400 + 500 + 600)

    futures = yield _read_csv('distributed-test/csv/2015/',
                              collection=False, lazy=False)
    assert len(futures) == 3
    assert all(isinstance(f, Future) for f in futures)
    results = yield e._gather(futures)
    assert results[0].id.sum() == 1 + 2 + 3
    assert results[1].id.sum() == 0
    assert results[2].id.sum() == 4 + 5 + 6

    values = yield _read_csv('distributed-test/csv/2015/',
                              collection=False, lazy=True)
    assert len(values) == 3
    assert all(isinstance(v, Value) for v in values)

    df2 = yield _read_csv('distributed-test/csv/2015/',
                          collection=True, lazy=True, blocksize=20)
    assert df2.npartitions > df.npartitions
    result = yield e.compute(df2.id.sum())._result()
    assert result == 1 + 2 + 3 + 4 + 5 + 6

    df2 = yield _read_csv('distributed-test/csv/2015/',
                          collection=True, lazy=False, blocksize=20)
    f = e.compute(df2.amount.sum())
    result = yield f._result()
    assert result == (100 + 200 + 300 + 400 + 500 + 600)


@gen_cluster(timeout=60, executor=True)
def test_read_csv_gzip(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')
    s3 = S3FileSystem(anon=True)

    df = yield _read_csv('distributed-test/csv/gzip/', compression='gzip')
    assert isinstance(df, dd.DataFrame)
    assert list(df.columns) == ['name', 'amount', 'id']
    f = e.compute(df.amount.sum())
    result = yield f._result()
    assert result == (100 + 200 + 300 + 400 + 500 + 600)


def test_read_csv_sync(loop):
    dd = pytest.importorskip('dask.dataframe')
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            df = read_csv('distributed-test/csv/2015/', lazy=True)
            assert isinstance(df, dd.DataFrame)
            assert list(df.columns) == ['name', 'amount', 'id']
            f = e.compute(df.amount.sum())
            assert f.result() == (100 + 200 + 300 + 400 + 500 + 600)

def test_avro_body():
    fastavro = pytest.importorskip('fastavro')
    b = make_avro()
    av = fastavro.reader(io.BytesIO(b))
    header = av._header
    
    schema = header['meta']['avro.schema'].decode()
    schema = json.loads(schema)
    assert ['station', 'temp', 'time'] == sorted(f['name'] for f in schema['fields'])
    sync = header['sync']
    assert len(sync) == 16
    bit = b[b.index(sync)+len(sync):]
    assert avro_body(bit, header) == records
    bit = b[b.index(sync)+len(sync):-len(sync)]
    assert avro_body(bit, header) == records
    assert avro_body(b"", header) == []


def test_avro_to_df():
    fastavro = pytest.importorskip('fastavro')
    pytest.importorskip('pandas')
    byte = make_avro()
    av = fastavro.reader(io.BytesIO(byte))
    header = av._header
    sync = header['sync']
    bit = byte[byte.index(sync)+len(sync):]

    df = avro_to_df(bit, header)
    assert ['station', 'temp', 'time'] == sorted(df.columns)
    assert len(df) == 4
    assert 22 - 11 + 111 == df.temp.sum()
    df = avro_to_df(b"", header)
    assert len(df) == 0
    df = avro_to_df(b"", header, cols=['station', 'temp', 'time'])
    assert  ['station', 'temp', 'time']== list(df.columns)


@gen_cluster(timeout=60, executor=True)
def test_read_avro(e, s, a, b):
    dd = pytest.importorskip('dask.dataframe')
    s3 = S3FileSystem(anon=True)
    df = yield _read_avro('distributed-test/test/data/avro', fs=s3)
    assert isinstance(df, dd.DataFrame)

    yield gen.sleep(0.1)
    assert not s.tasks

    df = yield _read_avro('distributed-test/test/data/avro/')
    assert isinstance(df, dd.DataFrame)
    assert list(df.columns) == ['amount', 'name']

    f = e.compute(df.amount.sum())
    result = yield f._result()
    assert result == (100 + 200 + 300 + 400) * 2

    futures = yield _read_avro('distributed-test/test/data/avro/',
                               collection=False, lazy=False)
    assert len(futures) == 2
    assert all(isinstance(f, Future) for f in futures)
    results = yield e._gather(futures)
    assert isinstance(results[0][0], dict)
    assert sum(sum(o['amount'] for o in r) for r in results) ==  (100 + 200 + 300 + 400) * 2

    values = yield _read_avro('distributed-test/test/data/avro/',
                              collection=False, lazy=True)
    assert len(values) == 2
    assert all(isinstance(v, Value) for v in values)

    df2 = yield _read_avro('distributed-test/test/data/avro/',
                           collection=True, lazy=True, blocksize=20)
    assert df2.npartitions > df.npartitions
    result = yield e.compute(df2.amount.sum())._result()
    assert result == (100 + 200 + 300 + 400) * 2


def test_read_avro_sync(loop):
    dd = pytest.importorskip('dask.dataframe')
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            df = read_avro('distributed-test/test/data/avro/')
            assert isinstance(df, dd.DataFrame)
            assert list(df.columns) == ['amount', 'name']
            f = e.compute(df.amount.sum())
            assert f.result() == (100 + 200 + 300 + 400) * 2
            df = read_csv('distributed-test/csv/2015/', lazy=False)
            assert df.amount.sum().compute() == (100 + 200 + 300 + 400 + 500 + 600)
