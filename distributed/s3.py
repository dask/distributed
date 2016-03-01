from __future__ import print_function, division, absolute_import

import logging
import threading
import re

import boto3
from botocore.handlers import disable_signing
from botocore.exceptions import ClientError
from tornado import gen

from dask.imperative import Value
from dask.base import tokenize
from distributed.utils import read_block

from .compatibility import get_thread_identity
from .executor import default_executor, ensure_default_get


logger = logging.getLogger(__name__)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)


DEFAULT_PAGE_LENGTH = 1000

_conn = dict()


get_s3_lock = threading.Lock()


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
                s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
            else:
                s3 = boto3.session.Session(self.key, self.secret,
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
            Path of file on HDFS
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
        bucket, *key = path.split('/', maxsplit=1)
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
            raise ValueError("Info must be called on exactly one path")

    def walk(self, path):
        return [f['Key'] for f in self._ls(path) if f['Key'].rstrip('/'
                ).startswith(path.rstrip('/') + '/')]

    def glob(self, path):
        """
        Find files by glob-matching.

        Note that the bucket part of the path must not contain a "*"
        """
        path = path.lstrip('s3://').lstrip('/')
        bucket, *key = path.split('/', maxsplit=1)
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
        bucket, *key = path.lstrip('s3://').split('/', maxsplit=1)
        if not key:
            out = self.s3.create_bucket(Bucket=bucket)
        else:
            out = self.s3.put_object(Bucket=bucket, Key=key[0])
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
        bucket, *key = path.lstrip('s3://').split('/', maxsplit=1)
        if key:
            out = self.s3.delete_object(Bucket=bucket, Key=key[0])
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
        """ Copy HDFS file to local """
        #TODO: _lib.hdfsCopy() may do this more efficiently
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
        """ Copy local file to path in HDFS """
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
        """ Read a block of bytes from an HDFS file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned will not include the surrounding delimiter strings.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on HDFS
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> hdfs.read_block('/data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> hdfs.read_block('/data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200'

        See Also
        --------
        hdfs3.utils.read_block
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
        bucket, key = path.lstrip('s3://').split('/', maxsplit=1)
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

from distributed.hdfs import read_bytes, read_text
