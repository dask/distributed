from __future__ import print_function, division, absolute_import

import logging
import threading

import boto3
from botocore.handlers import disable_signing
from botocore.exceptions import ClientError
from tornado import gen

from dask.imperative import Value

from .compatibility import get_thread_identity
from .executor import default_executor, ensure_default_get


logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


DEFAULT_PAGE_LENGTH = 1000

_conn = dict()


get_s3_lock = threading.Lock()


def get_s3(anon):
    """ Get S3 connection

    Caches connection for future use
    """
    if anon is None:
        try:
            return get_s3(True)
        except:
            return get_s3(False)

    with get_s3_lock:
        key = anon, get_thread_identity()
        if not _conn.get(key):
            logger.debug("Open S3 connection.  Anonymous: %s.  Thread ID: %d",
                         *key)
            s3 = boto3.resource('s3')
            if anon:
                s3.meta.client.meta.events.register('choose-signer.s3.*',
                        disable_signing)
            _conn[key] = s3
        return _conn[key]


def get_list_of_summary_objects(bucket_name, prefix='', delimiter='',
        page_size=DEFAULT_PAGE_LENGTH, anon=None):
    s3 = get_s3(anon)
    if bucket_name.startswith('s3://'):
        bucket_name = bucket_name[len('s3://'):]
    if prefix.startswith('/'):
        prefix = prefix[1:]

    L = list(s3.Bucket(bucket_name)
               .objects.filter(Prefix=prefix, Delimiter=delimiter)
               .page_size(page_size))
    return [s for s in L if s.key[-1] != '/']


def seek_delimiter(ob, offset, delimiter, seeklength=4096):
    """
    Find the bytes location of the next delimiter after the given offset

    Parameters
    ----------
    ob : boto3 open s3 key
    offset : int
        bytes location to start
    delimiter : bytes
        pattern to seek
    seeklength : int
        If seeking delimited, the blocksize for finding delimiters.
    """
    if offset == 0:
        return 0
    data = b''
    while True:
        try:
            piece = ob.get(Range="bytes=%i-%i" %
                           (offset, offset + seeklength))['Body'].read()
        except ClientError:
            return ob.content_length
        data += piece
        try:
            i = data.index(delimiter)
            return offset + i + len(delimiter)
        except ValueError:
            offset += seeklength


def read_block(buc, fn, offset, length, delimiter=None, anon=None):
    """
    Read one chunk of bytes from the given S3 key, optionally delimited.

    Parameters
    ----------
    ob : boto3 open s3 key
    offset : int
        From which byte location to read
    length : int
        size of block to read
    delimiter : bytes
        If given, seek to next delimiter at the star and end of the block
    """
    s3 = get_s3(anon)
    ob = s3.Object(buc, fn)
    if delimiter is None:
        try:
            return ob.get(Range="bytes=%i-%i" %
                          (offset, offset + length - 1))['Body'].read()
        except ClientError:
            return b''
    start = seek_delimiter(ob, offset, delimiter)
    end = seek_delimiter(ob, offset + length, delimiter)
    if start == end:
        return b''
    return ob.get(Range="bytes=%i-%i" % (start, end - 1))['Body'].read()


def read_content_from_keys(bucket, key, anon=None):
    if bucket.startswith('s3://'):
        bucket = bucket[len('s3://'):]
    s3 = get_s3(anon)
    return s3.Object(bucket, key).get()['Body'].read()


def read_bytes(bucket_name, prefix='', path_delimiter='', executor=None,
               lazy=True, anon=None, blocksize=None, delimiter=None):
    """ Read data on S3 into bytes in distributed memory

    Parameters
    ----------
    bucket_name: string
        Name of S3 bucket like ``'my-bucket'``
    prefix: string
        Prefix of key name to match like ``'/data/2016/``
    path_delimiter: string (optional)
        Delimiter like ``'/'`` to define implicit S3 directory structure
    executor: Executor (optional)
        defaults to most recently created executor
    lazy: boolean (optional)
        If True then return lazily evaluated dask Values
    anon: boolean (optional)
        If True then don't try to authenticate with AWS
    blocksize : int or None
        If not none, produce multiple futures per key of blocksize bytes
    delimiter : bytes
        if using blocksize, split on this delimiter rather than exact
        number of bytes

    Returns
    -------
    list of Futures.  Each future holds bytes for one key within the bucket

    Examples
    --------
    >>> futures = read_bytes('distributed-test', 'test')  # doctest: +SKIP
    >>> futures  # doctest: +SKIP
    [<Future: status: finished, key: read_content_from_keys-00092e8a75141837c1e9b717b289f9d2>,
     <Future: status: finished, key: read_content_from_keys-4f0f2cbcf4573a373cc62467ffbfd30d>]
    >>> futures[0].result()  # doctest: +SKIP
    b'{"amount": 100, "name": "Alice"}\\n{"amount": 200, "name": "Bob"}\\n
      {"amount": 300, "name": "Charlie"}\\n{"amount": 400, "name": "Dennis"}\\n'
    """
    executor = default_executor(executor)
    s3_objects = get_list_of_summary_objects(bucket_name, prefix,
                                             path_delimiter, anon=anon)

    if blocksize is not None:
        values = []
        for ob in s3_objects:
            flen = ob.get()['ContentLength']
            for offset in range(0, flen + 1, blocksize):
                name = 'read-bytes-%s-%d' % (ob.key, offset)
                values.append(Value(name, [{name:
                    (read_block, bucket_name, ob.key, offset, blocksize, delimiter)}]))
        if lazy:
            return values
        else:
            return executor.compute(values)
    else:
        keys = [obj.key for obj in s3_objects]
        names = ['read-bytes-{0}'.format(key) for key in keys]
        if lazy:
            values = [Value(name, [{name: (read_content_from_keys, bucket_name,
                                           key, anon)}])
                      for name, key in zip(names, keys)]
            return values
        else:
            return executor.map(read_content_from_keys, [bucket_name] * len(keys),
                    keys, anon=anon)


def read_text(bucket_name, prefix='', path_delimiter='', encoding='utf-8',
        errors='strict', lineterminator='\n', executor=None, anon=None,
        collection=True, lazy=True, compression=None):
    """
    Read lines of text from S3

    Parameters
    ----------
    bucket_name: string
        Name of S3 bucket like ``'my-bucket'``
    prefix: string
        Prefix of key name to match like ``'/data/2016/``
    path_delimiter: string (optional)
        Delimiter like ``'/'`` to define implicit S3 directory structure
    compression: {None, 'gzip'}

    Returns
    -------
    Dask bag
    """
    from dask import do
    import dask.bag as db
    executor = default_executor(executor)

    blocks = read_bytes(bucket_name, prefix, path_delimiter, executor=executor,
                        lazy=True, anon=anon)

    if compression:
        blocks = map(do(decompress[compression]), blocks)

    lists = [b.decode(encoding, errors).split(lineterminator) for b in blocks]

    if collection:
        ensure_default_get(executor)
        b = db.from_imperative(lists).filter(None)
        if lazy:
            return b
        else:
            return executor.persist(b)[0]
    else:
        if lazy:
            ensure_default_get(executor)
            return lists
        else:
            return executor.compute(lists)


from .compatibility import gzip_decompress
decompress = {'gzip': gzip_decompress}
