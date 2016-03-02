from __future__ import print_function, division, absolute_import

import logging
import threading
import re
import json
import io
import sys

import boto3
from botocore.exceptions import ClientError
from tornado import gen

from dask.imperative import Value
from dask.base import tokenize
from distributed.utils import read_block, seek_delimiter

from .executor import default_executor, ensure_default_get
from .utils import ignoring, sync
from .compatibility import unicode

logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


DEFAULT_PAGE_LENGTH = 1000

_conn = dict()


get_s3_lock = threading.Lock()

def split_path(path):
    path = path.lstrip('s3://')
    if '/' not in path:
        return path, ""
    else:
        bits = path.split('/')
        return bits[0], '/'.join(bits[1:])


class S3FileSystem(object):
    """
    Access S3 data as if it were a file system.
    """
    _conn = {}

    def __init__(self, anon=True, key=None, secret=None, **kwargs):
        """
        Create connection object to S3

        Will use configured key/secret (typically in ~/.aws, see the
        boto3 documentation) unless specified

        Parameters
        ----------
        anon : bool (True)
            whether to use anonymous connection (public buckets only)
        key : string (None)
            if not anonymouns, use this key, if specified
        secret : string (None)
            if not anonymous, use this password, if specified
        kwargs : other parameters for boto3 session
        """
        self.anon = anon
        self.key = key
        self.secret = secret
        self.kwargs = kwargs
        self.connect(anon, key, secret, kwargs)
        self.dirs = {}
        self.s3 = self.connect(anon, key, secret, kwargs)
    
    def connect(self, anon, key, secret, kwargs):
        tok = tokenize(anon, key, secret, kwargs)
        if tok not in self._conn:
            logger.debug("Open S3 connection.  Anonymous: %s",
                         self.anon)
            if self.anon:
                from botocore import UNSIGNED
                from botocore.client import Config                
                s3 = boto3.Session().client('s3',
                         config=Config(signature_version=UNSIGNED))
            else:
                s3 = boto3.Session(self.key, self.secret,
                                   **self.kwargs).client('s3')
            self._conn[tok] = s3
        return self._conn[tok]

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['s3']
        logger.debug("Serialize with state: %s", d)
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.connect(self.anon, self.key, self.secret, self.kwargs)

    def open(self, path, mode='rb', block_size=4*1024**2):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'rb' or 'wb'
        block_size: int
            Size of data-node blocks if reading
        """
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                    " and manage bytes" % (mode + 'b'))
        return S3File(self, path, mode, block_size=block_size)

    def _ls(self, path, refresh=False):
        """ List files below path

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        refresh : bool (=False)
            if False, look in local cache for file details first
        """
        path = path.lstrip('s3://').lstrip('/')
        bucket, key = split_path(path)
        if bucket not in self.dirs or refresh:
            if bucket == '':
                # list of buckets
                if self.anon:
                    # cannot list buckets if not logged in
                    return []
                files = self.s3.list_buckets()['Buckets']
                for f in files:
                    f['Key'] = f['Name']
                    f['Size'] = 0
                    del f['Name']
            else:
                files = self.s3.list_objects(Bucket=bucket).get('Contents', [])
                for f in files:
                    f['Key'] = "/".join([bucket, f['Key']])
            self.dirs[bucket] = files
        files = self.dirs[bucket]
        return files

    def ls(self, path, detail=False):
        path = path.lstrip('s3://').rstrip('/')
        try:
            files = self._ls(path)
        except ClientError:
            files = []
        if path:
            pattern = re.compile(path + '/[^/]*.$')
            files = [f for f in files if pattern.match(f['Key']) is not None]
            if not files:
                files = [self.info(path)]
        if detail:
            return files
        else:
            return [f['Key'] for f in files]
    
    def info(self, path):
        path = path.lstrip('s3://').rstrip('/')
        files = self._ls(path)
        files = [f for f in files if f['Key'].rstrip('/') == path]
        if len(files) == 1:
            return files[0]
        else:
            raise IOError("File not found: %s" %path)

    def walk(self, path):
        return [f['Key'] for f in self._ls(path) if f['Key'].rstrip('/'
                ).startswith(path.rstrip('/') + '/')]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path0 = path
        path = path.lstrip('s3://').lstrip('/')
        bucket, key = split_path(path)
        if "*" in bucket:
            raise ValueError('Bucket cannot contain a "*"')
        if '*' not in path:
            path = path.rstrip('/') + '/*'
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                                        .rstrip('/')
                                        .replace('*', '[^/]*')
                                        .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        if not out:
            out = self.ls(path0)
        return out

    def du(self, path, total=False, deep=False):
        if deep:
            files = self.walk(path)
            files = [self.info(f) for f in files]
        else:
            files = self.ls(path, detail=True)
        if total:
            return sum(f.get('Size', 0) for f in files)
        else:
            return {p['Key']: p['Size'] for p in files}

    def df(self):
        total = 0
        for files in self.dirs.values():
            total += sum(f['Size'] for f in files)
        return {'capacity': None, 'used': total, 'percent-free': None}

    def get_block_locations(self, path, start=0, length=0, sizes=100*1024**2):
        """ Fetch block offsets """
        info = self.info(path)
        length = length or info['Size']
        boffsets = range(start, length, sizes)
        locs = []
        for i in boffsets:
            locs.append({'hosts': set(), 'length': length, 'offset': i})
        return locs

    def __repr__(self):
        return 'S3 File System'

    def mkdir(self, path):
        self.touch(path)

    def touch(self, path):
        bucket, key = split_path(path)
        if not key:
            out = self.s3.create_bucket(Bucket=bucket)
        else:
            out = self.s3.put_object(Bucket=bucket, Key=key)
        if out['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise IOError('Touch failed on %s', path)
        self._ls(path, refresh=True)

    def mv(self, path1, path2):
        self.copy(path1, path2)
        self.rm(path1)        

    def rm(self, path, recursive=True):
        if recursive:
            for f in self.walk(path):
                self.rm(f, recursive=False)
        bucket, key = split_path(path)
        if key:
            out = self.s3.delete_object(Bucket=bucket, Key=key)
        else:
            out = self.s3.delete_bucket(Bucket=bucket)
        if out['ResponseMetadata']['HTTPStatusCode'] != 204:
            raise IOError('rm failed on %s', path)
        self._ls(path, refresh=True)

    def exists(self, path):
        return bool(self.ls(path))

    def copy(self, path1, path2):
        buc2, key2 = path2.lstrip('s3://').split('/', maxsplit=1)
        out = self.s3.copy_object(Bucket=buc2, Key=key2, CopySource=path1.lstrip('s3://'))
        if out['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise IOError('Copy failed on %s->%s', path1, path2)
        self._ls(path2, refresh=True)

    def get(self, s3_path, local_path, blocksize=2**16):
        """ Copy S3 file to local """
        if not self.exists(s3_path):
            raise IOError(s3_path)
        with self.open(s3_path, 'rb') as f:
            with open(local_path, 'wb') as f2:
                out = 1
                while out:
                    out = f.read(blocksize)
                    f2.write(out)

    def cat(self, path):
        with self.open(path, 'rb') as f:
            return f.read()

    def getmerge(self, path, filename, blocksize=2**27):
        """ Concat all files in path (a directory) to output file """
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath['name'], 'rb') as f:
                    out = 1
                    while out:
                        out = f.read(blocksize)
                        f2.write(out)

    def put(self, filename, path, chunk=2**27):
        """ Copy local file to path in S3 """
        with self.open(path, 'wb') as f:
            with open(filename, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f.write(out)
        self._ls(path, refresh=True)

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.info(path)['Size']
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb', block_size=size) as f:
            return f.read(size)

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an S3 file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned will not include the surrounding delimiter strings.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on S3
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> s3.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> s3.read_block('data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200'

        See Also
        --------
        distributed.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['Size']
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


class S3File(object):
    """
    Cached read-only interface to a key in S3, behaving like a seekable file.

    Optimized for a single continguous block.
    """

    def __init__(self, s3, path, mode='rb', block_size=4*2**20):
        """
        Open S3 as a file. Data is only loaded and cached on demand.

        Parameters
        ----------
        s3 : boto3 connection
        bucket : string
            S3 bucket to access
        key : string
            S3 key to access
        blocksize : int
            read-ahead size for finding delimiters
        """
        self.mode = mode
        if mode not in {'rb', 'wb'}:
            raise ValueError("File mode %s not in {'rb', 'wb'}" % mode)
        self.path = path
        bucket, key = split_path(path)
        self.s3 = s3
        if mode == 'wb':
            self.mpu = s3.s3.create_multipart_upload(Bucket=bucket, Key=key)
            self.part_info = {'Parts': []}
            self.size = 0
        else:
            self.size = self.info()['Size']            
        self.bucket = bucket
        self.key = key
        self.blocksize = block_size
        self.cache = b""
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False

    def info(self):
        return self.s3.info(self.path)

    def tell(self):
        return self.loc

    def seek(self, loc, whence=0):
        if whence == 0:
            self.loc = loc
        elif whence == 1:
            self.loc += loc
        elif whence == 2:
            self.loc = self.size + loc
        else:
            raise ValueError("invalid whence (%s, should be 0, 1 or 2)" % whence)
        if self.loc < 0:
            self.loc = 0
        return self.loc

    def _fetch(self, start, end):
        try:
            if self.start is None and self.end is None:
                # First read
                self.start = start
                self.end = end + self.blocksize
                self.cache = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key,
                                                Range='bytes=%i-%i' % (start, self.end - 1)
                                                )['Body'].read()
            if start < self.start:
                new = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key,
                                         Range='bytes=%i-%i' % (start, self.start - 1)
                                         )['Body'].read()
                self.start = start
                self.cache = new + self.cache
            if end > self.end:
                if end > self.size:
                    return
                new = self.s3.s3.get_object.get(Bucket=self.bucket, Key=self.key,
                                             Range='bytes=%i-%i' % (self.end, end + self.blocksize - 1)
                                             )['Body'].read()
                self.end = end + self.blocksize
                self.cache = self.cache + new
        except ClientError:
            self.start = min([start, self.start or self.size])
            self.end = max(end, self.end or self.size)

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        """
        if self.mode != 'rb':
            raise ValueError('File not in read mode')
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        return out

    def write(self, data):
        if self.mode != 'wb':
            raise ValueError('File not in write mode')
        partno = len(self.part_info['Parts']) + 1
        part = self.s3.s3.upload_part(Bucket=self.bucket, Key=self.key,
                                      PartNumber=partno,
                                      UploadId=self.mpu['UploadId'], Body=data)
        self.part_info['Parts'].append({'PartNumber': partno, 'ETag': part['ETag']})

    def flush(self):
        if self.mode != 'wb':
            return
        self.s3.s3.complete_multipart_upload(Bucket=self.bucket, Key=self.key,
                                             UploadId=self.mpu['UploadId'],
                                             MultipartUpload=self.part_info)

    def close(self):
        self.flush()
        self.cache = None
        self.closed = True

    def __repr__(self):
        return "Cached S3 key %s/%s" % (self.bucket, self.key)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def read_bytes(fn, executor=None, s3=None, lazy=True, delimiter=None,
               not_zero=False, blocksize=2**27, **s3pars):
    """ Convert location in S3 to a list of distributed futures

    Parameters
    ----------
    fn: string
        location in S3
    executor: Executor (optional)
        defaults to most recently created executor
    s3: HDFileSystem (optional)
    lazy: boolean (optional)
        If True then return lazily evaluated dask Values
    delimiter: bytes
        An optional delimiter, like ``b'\n'`` on which to split blocks of bytes
    not_zero: force seek of start-of-file delimiter, discarding header
    blocksize: int (=128MB)
        Chunk size
    **s3pars: keyword arguments
        Extra keywords to send to boto3 session (anon, key, secret...)

    Returns
    -------
    List of ``distributed.Future`` objects if ``lazy=False``
    or ``dask.Value`` objects if ``lazy=True``
    """
    if s3 is None:
        s3 = S3FileSystem(**s3pars)
    executor = default_executor(executor)
    filenames, lengths, offsets = [], [], []
    for afile in s3.glob(fn):
        size = s3.info(afile)['Size']
        offset = list(range(0, size, blocksize))
        if not_zero:
            offset[0] = 1
        offsets.extend(offset)
        filenames.extend([afile]*len(offset))
        lengths.extend([blocksize]*len(offset))
    names = ['read-binary-s3-%s-%s' % (fn, tokenize(offset, length, delimiter, not_zero))
            for fn, offset, length in zip(filenames, offsets, lengths)]

    logger.debug("Read %d blocks of binary bytes from %s", len(names), fn)
    if lazy:
        executor._send_to_scheduler({'op': 'update-graph',
                                     'tasks': {},
                                     'dependencies': set(),
                                     'keys': [],
                                     'client': executor.id})
        values = [Value(name, [{name: (read_block_from_s3, fn, offset, length, s3pars, delimiter)}])
                  for name, fn, offset, length in zip(names, filenames, offsets, lengths)]
        return values
    else:
        return executor.map(read_block_from_s3, filenames, offsets, lengths,
                s3pars=s3pars, delimiter=delimiter)


def read_block_from_s3(filename, offset, length, s3pars={}, delimiter=None):
    s3 = S3FileSystem(**s3pars)
    bytes = s3.read_block(filename, offset, length, delimiter)
    return bytes


def bytes_read_csv(b, **kwargs):
    from io import BytesIO
    import pandas as pd
    bio = BytesIO(b)
    return pd.read_csv(bio, **kwargs)


@gen.coroutine
def _read_csv(path, executor=None, fs=None, lazy=True, lineterminator='\n',
        header=True, names=None, collection=True, **kwargs):
    from dask import do
    import pandas as pd
    fs = fs or S3FileSystem()
    executor = default_executor(executor)
    kwargs['lineterminator'] = lineterminator

    filenames = fs.glob(path)
    blockss = [read_bytes(fn, executor, fs, lazy=True,
                          delimiter=ensure_bytes(lineterminator))
               for fn in filenames]
    if names is None and header:
        with fs.open(filenames[0]) as f:
            head = pd.read_csv(f, nrows=5, **kwargs)
            names = head.columns

    dfs1 = [[do(bytes_read_csv)(blocks[0], names=names, skiprows=1, **kwargs)] +
            [do(bytes_read_csv)(b, names=names, **kwargs) for b in blocks[1:]]
            for blocks in blockss]
    dfs2 = sum(dfs1, [])
    if lazy:
        from dask.dataframe import from_imperative
        if collection:
            ensure_default_get(executor)
            raise gen.Return(from_imperative(dfs2, head))
        else:
            raise gen.Return(dfs2)

    else:
        futures = executor.compute(dfs2)
        from distributed.collections import _futures_to_dask_dataframe
        if collection:
            ensure_default_get(executor)
            df = yield _futures_to_dask_dataframe(futures)
            raise gen.Return(df)
        else:
            raise gen.Return(futures)


def read_csv(fn, executor=None, fs=None, lazy=True, **kwargs):
    """ Read CSV encoded data from bytes on S3

    Parameters
    ----------
    fn: string
        filename or globstring of CSV files on S3
    lazy: boolean, optional
        If True return dask Value objects

    Returns
    -------
    List of futures of Python objects
    """
    executor = default_executor(executor)
    return sync(executor.loop, _read_csv, fn, executor, fs, lazy, **kwargs)


def avro_body(data, header):
    """ Convert bytes and header to Python objects

    Parameters
    ----------
    data: bytestring
        bulk avro data, without header information
    header: bytestring
        Header information collected from ``fastavro.reader(f)._header``

    Returns
    -------
    List of deserialized Python objects, probably dictionaries
    """
    import fastavro
    sync = header['sync']
    if not data.endswith(sync):
        # Read delimited should keep end-of-block delimiter
        data = data + sync
    stream = io.BytesIO(data)
    schema = header['meta']['avro.schema'].decode()
    schema = json.loads(schema)
    codec = header['meta']['avro.codec'].decode()
    return list(fastavro._reader._iter_avro(stream, header, codec,
        schema, schema))


def avro_to_df(b, av):
    """Parse avro binary data with header av into a pandas dataframe"""
    import pandas as pd
    return pd.DataFrame(data=avro_body(b, av))


@gen.coroutine
def _read_avro(path, executor=None, fs=None, lazy=True, **kwargs):
    """ See read_avro for docstring """
    from dask import do
    import fastavro
    fs = fs or S3FileSystem()
    executor = default_executor(executor)

    filenames = fs.glob(path)

    blockss = []
    for fn in filenames:
        with fs.open(fn, 'rb') as f:
            av = fastavro.reader(f)
            header = av._header
        schema = json.loads(header['meta']['avro.schema'].decode())

        blockss.extend([read_bytes(fn, executor, fs, lazy=True,
                                   delimiter=header['sync'], not_zero=True)
                       for fn in filenames])  # TODO: why is filenames used twice?

    lazy_values = [do(avro_body)(b, header) for blocks in blockss
                                            for b in blocks]

    if lazy:
        raise gen.Return(lazy_values)
    else:
        futures = executor.compute(lazy_values)
        raise gen.Return(futures)


def read_avro(fn, executor=None, fs=None, lazy=True, **kwargs):
    """ Read avro encoded data from bytes on S3

    Parameters
    ----------
    fn: string
        filename or globstring of avro files on S3
    lazy: boolean, optional
        If True return dask Value objects

    Returns
    -------
    List of futures of Python objects
    """
    executor = default_executor(executor)
    return sync(executor.loop, _read_avro, fn, executor, fs, lazy, **kwargs)


@gen.coroutine
def _read_text(fn, encoding='utf-8', errors='strict', lineterminator='\n',
               executor=None, fs=None, lazy=True, collection=True):
    from dask import do
    fs = fs or S3FileSystem()
    executor = default_executor(executor)

    filenames = sorted(fs.glob(fn))
    blocks = [block for fn in filenames
                    for block in read_bytes(fn, executor, fs, lazy=True,
                                            delimiter=lineterminator.encode())]
    strings = [do(bytes.decode)(b, encoding, errors) for b in blocks]
    lines = [do(unicode.split)(s, lineterminator) for s in strings]

    if lazy:
        from dask.bag import from_imperative
        if collection:
            ensure_default_get(executor)
            raise gen.Return(from_imperative(lines))
        else:
            raise gen.Return(lines)

    else:
        futures = executor.compute(lines)
        from distributed.collections import _futures_to_dask_bag
        if collection:
            ensure_default_get(executor)
            b = yield _futures_to_dask_bag(futures)
            raise gen.Return(b)
        else:
            raise gen.Return(futures)


def read_text(fn, encoding='utf-8', errors='strict', lineterminator='\n',
              executor=None, fs=None, lazy=False, collection=True, **kwargs):
    """ Read text lines from S3

    Parameters
    ----------
    fn: string
        filename or globstring of files on S3
    collection: boolean, optional
        Whether or not to return a high level collection
    lazy: boolean, optional
        Whether or not to start reading immediately

    Returns
    -------
    Dask bag (if collection=True) or Futures or dask values
    """
    executor = default_executor(executor)
    return sync(executor.loop, _read_text, fn, encoding, errors,
            lineterminator, executor, fs, lazy, collection)


def ensure_bytes(s):
    """ Give strings that ctypes is guaranteed to handle """
    if isinstance(s, dict):
        return {k: ensure_bytes(v) for k, v in s.items()}
    if isinstance(s, str) and sys.version_info < (3,):
        return s
    if hasattr(s, 'encode'):
        return s.encode()
    else:
        return s
