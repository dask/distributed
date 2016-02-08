from __future__ import print_function, division, absolute_import

import logging
import io

import boto3
from botocore.handlers import disable_signing
from tornado import gen

from dask.imperative import Value
from .executor import default_executor


logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


DEFAULT_PAGE_LENGTH = 1000

_conn = {True: None, False: None}


def get_s3(anon):
    """ Get S3 connection

    Caches connection for future use
    """
    if not _conn[anon]:
        logger.debug("Open S3 connection.  Anonymous: %s", anon)
        s3 = boto3.resource('s3')
        if anon:
            s3.meta.client.meta.events.register('choose-signer.s3.*',
                                                disable_signing)
        _conn[anon] = s3
    return _conn[anon]


def get_list_of_summary_objects(bucket_name, prefix='', delimiter='',
                                page_size=DEFAULT_PAGE_LENGTH, anon=False):
    s3 = get_s3(anon)
    if bucket_name.startswith('s3://'):
        bucket_name = bucket_name[len('s3://'):]
    if prefix.startswith('/'):
        prefix = prefix[1:]

    L = list(s3.Bucket(bucket_name)
               .objects.filter(Prefix=prefix, Delimiter=delimiter)
               .page_size(page_size))
    return [s for s in L if s.key[-1] != '/']


def read_content_from_keys(bucket, key, anon=False):
    if bucket.startswith('s3://'):
        bucket = bucket[len('s3://'):]
    s3 = get_s3(anon)
    return s3.Object(bucket, key).get()['Body'].read()


def read_bytes(bucket_name, prefix='', path_delimiter='', executor=None, lazy=False,
               anon=False):
    """ Read data on S3 into bytes in distributed memory

    Parameters
    ----------
    bucket_name: string
        Name of S3 bucket like ``'my-bucket'``
    prefix: string
        Prefix of key name to match like ``'/data/2016/``
    lazy: boolean (optional)
        If True then return lazily evaluated dask Values
    anon: boolean (optional)
        If True then don't try to authenticate with AWS

    Returns
    -------
    list of Futures.  Each future holds bytes for one key within the bucket

    Examples
    --------
    >>> futures = read_bytes('distributed-test', 'test', anon=True)  # doctest: +SKIP
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


def avro_bytes(data):
    """ Interpret bytes block data as an avro file.

    Parameters
    ----------
    data : bytes
        data, including avro header and packed records

    Returns
    -------
    List of python objects (e.g., dicts)
    """
    import fastavro
    b = io.BytesIO(data)
    return list(fastavro.reader(b))


@gen.coroutine
def _read_avro(bucket_name, prefix='', executor=None, lazy=False,
               path_delimiter='', anon=False, collection=True):
    """ Distributed read of set of avro files on S3.

    Parameters
    ----------
    bucket_name : string
        Location in S3

    perefix : string
        Set of keys to filter for (can be see as firectory name)

    anon : bool (False)
        Whether to attempt to ignore authentication

    lazy : bool (False)
        if False, returns futures (ie., workers start to evaluate), if
        True, return Values to be submitted later.

    Returns
    -------
    List of Futures or Values
    """

    from dask import do
    bytes = read_bytes(bucket_name, prefix=prefix, executor=executor,
                       lazy=True, path_delimiter=path_delimiter, anon=anon)
    dfs = [do(avro_bytes)(b) for b in bytes]

    if lazy:
        raise gen.Return(dfs)
    else:
        raise gen.Return(executor.compute(*dfs))


def buffer_to_csv(b, **kwargs):
    """Load bytes as a CSV into pandas dataframe.

    Parameters
    ----------
    b : bytes
        Data to load (as if read from a file)

    kwarks : passed to pd.read_csv()

    Returns
    -------
    Pandas dataframe
    """
    from io import BytesIO
    import pandas as pd
    bio = BytesIO(b)
    return pd.read_csv(bio, **kwargs)


@gen.coroutine
def _read_csv(bucket_name, prefix='', executor=None, lazy=False,
              path_delimiter='', anon=False, collection=True):
    """ Distributed read of set of CSV files on S3.

    Parameters
    ----------
    bucket_name : string
        Location in S3

    perefix : string
        Set of keys to filter for (can be see as firectory name)

    anon : bool (False)
        Whether to attempt to ignore authentication

    lazy : bool (False)
        if False, returns futures (ie., workers start to evaluate), if
        True, return Values to be submitted later.

    Returns
    -------
    List of Futures or Values
    """
    from dask import do
    bytes = read_bytes(bucket_name, prefix=prefix, executor=executor,
                       lazy=True, path_delimiter=path_delimiter, anon=anon)

    dfs = [do(buffer_to_csv)(b) for b in bytes]

    if lazy:
        raise gen.Return(dfs)
    else:
        raise gen.Return(executor.compute(*dfs))
