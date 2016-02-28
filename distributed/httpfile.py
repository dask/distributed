# -*- coding: utf-8 -*-
import requests

class HTTPFile(object):
    """
    Cached read-only interface to an HTTP URL, behaving like a seekable file.

    Optimized for a single continguous block.
    """

    def __init__(self, url, blocksize=4*2**20, header={}):
        """
        Open URL as a file. Data is only loaded and cached on demand.

        Parameters
        ----------
        url : string
            location of data
        blocksize : int
            read-ahead size for finding delimiters
        headers : dict
            additional parms to add to requests (e.g., user/password)
        """
        self.url = url
        self.header = header
        self.blocksize = blocksize
        r = requests.head(url, headers=header)
        if r.status_code != 200:
            raise IOError("HTTP lookup failed")
        # The following required to reject compression, which breaks blocks
        self.header['accept-encoding'] = 'identity'
        self.size = int(r.headers['Content-Length'])
        self.cache = None
        self.loc = 0
        self.start = None
        self.end = None
        self.closed = False

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
        if self.start is None and self.end is None:
            # First read
            self.start = start
            self.end = end + self.blocksize
            self.header.update(dict(Range='bytes=%i-%i' % (start, self.end - 1)))
            self.cache = requests.get(self.url, headers=self.header).content
        if start < self.start:
            self.header.update(dict(Range='bytes=%i-%i' % (start, self.start - 1)))
            new = requests.get(self.url, headers=self.header).content
            self.start = start
            self.cache = new + self.cache
        if end > self.end:
            if end > self.size:
                return
            self.header.update(dict(Range='bytes=%i-%i' % (self.end, end +
                               self.blocksize - 1)))
            new = requests.get(self.url, headers=self.header).content
            self.end = end + self.blocksize
            self.cache = self.cache + new

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        """
        if length < 0:
            length = self.size
        if self.closed:
            raise ValueError('I/O operation on closed file.')
        self._fetch(self.loc, self.loc + length)
        out = self.cache[self.loc - self.start:
                         self.loc - self.start + length]
        self.loc += len(out)
        return out

    def close(self):
        self.cache = None
        self.closed = True

    def __repr__(self):
        return "Cached S3 key %s/%s" % (self.bucket, self.key)
