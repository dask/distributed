# -*- coding: utf-8 -*-
import pytest
from distributed.s3fs import S3FileSystem
from distributed.s3 import seek_delimiter
from distributed.utils_test import slow
import moto

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

csv_files = {'2014-01-01.csv': (b'name,amount,id\n'
                                b'Alice,100,1\n'
                                b'Bob,200,2\n'
                                b'Charlie,300,3\n'),
             '2014-01-02.csv': (b'name,amount,id\n'),
             '2014-01-03.csv': (b'name,amount,id\n'
                                b'Dennis,400,4\n'
                                b'Edith,500,5\n'
                                b'Frank,600,6\n')}
text_files = {'nested/file1': b'hello\n',
              'nested/file2': b'world',
              'nested/nested2/file1': b'hello\n',
              'nested/nested2/file2': b'world'}
a = 'tmp/test/a'
b = 'tmp/test/b'
c = 'tmp/test/c'
d = 'tmp/test/d'


@pytest.yield_fixture
def s3():
    # make writable local S3 system
    m = moto.mock_s3()
    m.start()
    import boto3
    client = boto3.client('s3')
    client.create_bucket(Bucket=test_bucket_name)
    for flist in [files, csv_files, text_files]:
        for f, data in flist.items():
            client.put_object(Bucket=test_bucket_name, Key=f, Body=data)
    yield S3FileSystem(anon=True)
    m.stop()


def test_simple(s3):
    data = b'a' * (10 * 2**20)

    with s3.open(a, 'wb') as f:
        f.write(data)

    with s3.open(a, 'rb') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_idempotent_connect(s3):
    s3.connect()
    s3.connect()


def test_ls_touch(s3):
    assert not s3.ls('tmp/test')
    s3.touch(a)
    s3.touch(b)
    L = s3.ls('tmp/test', True)
    assert set(d['Key'] for d in L) == set([a, b])
    L = s3.ls('tmp/test', False)
    assert set(L) == set([a, b])


def test_rm(s3):
    assert not s3.exists(a)
    s3.touch(a)
    assert s3.exists(a)
    s3.rm(a)
    assert not s3.exists(a)


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


def test_seek(s3):
    with s3.open(a, 'wb') as f:
        f.write(b'123')

    with s3.open(a) as f:
        f.seek(1000)
        with pytest.raises(ValueError):
            f.seek(-1)
        with pytest.raises(ValueError):
            f.seek(-5, 2)
        with pytest.raises(ValueError):
            f.seek(0, 10)
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(3)
        assert f.read(1) == b''
        f.seek(-1, 2)
        assert f.read(1) == b'3'
        f.seek(-1, 1)
        f.seek(-1, 1)
        assert f.read(1) == b'2'
        for i in range(4):
            assert f.seek(i) == i


def test_bad_open(s3):
    with pytest.raises(IOError):
        s3.open('')


def test_errors(s3):
    with pytest.raises((IOError, OSError)):
        s3.open('tmp/test/shfoshf', 'rb')

    ## This is fine, no need for interleving directories on S3
    #with pytest.raises((IOError, OSError)):
    #    s3.touch('tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        s3.rm('tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        s3.mv('tmp/test/shfoshf/x', 'tmp/test/shfoshf/y')

    #with pytest.raises((IOError, OSError)):
    #    s3.open('x', 'wb')

    with pytest.raises((IOError, OSError)):
        s3.open('x', 'rb')

    #with pytest.raises(IOError):
    #    s3.chown('/unknown', 'someone', 'group')

    #with pytest.raises(IOError):
    #    s3.chmod('/unknonwn', 'rb')

    with pytest.raises(IOError):
        s3.rm('unknown')


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
