from __future__ import print_function, division, absolute_import

import logging
import sys

import cloudpickle

if sys.version_info.major == 2:
    import cPickle as pickle
else:
    import pickle

logger = logging.getLogger(__name__)


def _always_use_pickle_for(x):
    mod, _, _ = x.__class__.__module__.partition('.')
    if mod == 'numpy':
        import numpy as np
        return isinstance(x, np.ndarray)
    elif mod == 'pandas':
        import pandas as pd
        return isinstance(x, pd.core.generic.NDFrame)
    elif mod == 'builtins':
        return isinstance(x, (str, bytes))
    else:
        return False


class _BytelistFile(object):

    def __init__(self, chunks=None):
        if chunks is None:
            chunks = []
        self._chunks = chunks
        self._pos = sum(len(c) for c in chunks)

    def __len__(self):
        return sum(len(c) for c in self._chunks)

    def write(self, chunk):
        self._chunks.append(chunk)

    def read(self, size=None):
        return b''.join(self._collect_chunks(size=size))

    def readline(self):
        raise NotImplementedError

    def _collect_chunks(self, size=None):
        pos = self._pos
        remainder = (len(self) - pos) if size is None else size
        if remainder <= 0:
            return []
        collected = []
        left_to_skip = pos
        for chunk in self._chunks:
            if remainder <= 0:
                break
            if left_to_skip > 0:
                if left_to_skip > len(chunk):
                    left_to_skip -= len(chunk)
                else:
                    chunk = chunk[left_to_skip:left_to_skip + remainder]
                    left_to_skip = 0
                    collected.append(chunk)
                    remainder -= len(chunk)
            else:
                if len(chunk) <= remainder:
                    collected.append(chunk)
                    remainder -= len(chunk)
                else:
                    chunk = chunk[:remainder]
                    collected.append(chunk)
                    remainder = 0
        self._pos += sum(len(c) for c in collected)
        return collected

    def tell(self):
        return self._pos

    def seek(self, pos):
        if pos < 0:
            raise ValueError("Negative position %d is invalid." % pos)
        elif pos > len(self):
            raise ValueError("Position %d is larger than size %d."
                             % (pos, len(self)))
        self._pos = pos


def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b'__main__' in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if _always_use_pickle_for(x) or b'__main__' not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logger.info("Failed to serialize %s. Exception: %s", x, e)
            raise


def dump_bytelist(x):
    """Serialize the list of chunks using the pickle protocol

    Note that cloudpickle leverages nocopy semantics using memoryviews on
    large contiguous datastructures such as numpy arrays and derivatives.
    """
    # TODO: if Python 3 dump supports nocopy dump we should try use it first
    # and only fallback to cloudpickle
    try:
        writer = _BytelistFile()
        cloudpickle.dump(x, writer, protocol=pickle.HIGHEST_PROTOCOL)
        return writer._chunks
    except Exception as e:
        logger.info("Failed to serialize %s. Exception: %s", x, e)
        raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise


def load_bytelist(bytelist):
    try:
        reader = _BytelistFile(bytelist)
        reader.seek(0)
        return pickle.load(reader)
    except Exception:
        logger.info("Failed to deserialize %s", bytelist[0][:10000],
                    exc_info=True)
        raise
