from __future__ import print_function, division, absolute_import

import inspect
from io import BytesIO
import logging
import pickle

import cloudpickle

from tornado import gen

from ..utils import ignoring

logger = logging.getLogger(__file__)

pickle_types = [str, bytes]
with ignoring(ImportError):
    import numpy as np
    pickle_types.append(np.ndarray)
with ignoring(ImportError):
    import pandas as pd
    pickle_types.append(pd.core.generic.NDFrame)
pickle_types = tuple(pickle_types)


@gen.coroutine
def _tornado_coroutine_sample():
    yield

def is_tornado_coroutine(func):
    """
    Return whether *func* is a Tornado coroutine function.
    Running coroutines are not supported.
    """
    return func.__code__ is _tornado_coroutine_sample.__code__

def _rebuild_tornado_coroutine(func):
    from tornado import gen
    return gen.coroutine(func)

def _get_wrapped_function(func):
    try:
        return func.__wrapped__
    except AttributeError:
        pass
    # On old Pythons, functools.wraps() doesn't set the __wrapped__
    # attribute.  Hack around it by inspecting captured variables.
    functions = []
    for cell in func.__closure__:
        with ignoring(ValueError):
            v = cell.cell_contents
            if inspect.isfunction(v):
                functions.append(v)
    if len(functions) != 1:
        raise RuntimeError("failed to unwrap Tornado coroutine %s: "
                           "%d candidates found" % (func, len(functions)))
    return functions[0]


class ExtendedPickler(cloudpickle.CloudPickler):
    """Extended Pickler class with support for Tornado coroutines.
    """

    def save_function_tuple(self, func):
        if is_tornado_coroutine(func):
            self.save_reduce(_rebuild_tornado_coroutine,
                             (_get_wrapped_function(func),),
                             obj=func)
        else:
            cloudpickle.CloudPickler.save_function_tuple(self, func)


def extended_dumps(obj, protocol=2):
    with BytesIO() as bio:
        ExtendedPickler(bio, protocol).dump(obj)
        return bio.getvalue()


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
                return extended_dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if isinstance(x, pickle_types) or b'__main__' not in result:
                return result
            else:
                return extended_dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        try:
            return extended_dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            logger.info("Failed to serialize %s", x, exc_info=True)
            raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise
