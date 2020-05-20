import logging
import pickle
from pickle import HIGHEST_PROTOCOL

import cloudpickle


logger = logging.getLogger(__name__)


def _always_use_pickle_for(x):
    mod, _, _ = x.__class__.__module__.partition(".")
    if mod == "numpy":
        import numpy as np

        return isinstance(x, np.ndarray)
    elif mod == "pandas":
        import pandas as pd

        return isinstance(x, pd.core.generic.NDFrame)
    elif mod == "builtins":
        return isinstance(x, (str, bytes))
    else:
        return False


def dumps(x, *, buffer_callback=None):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    buffers = []
    dump_kwargs = {"protocol": HIGHEST_PROTOCOL}
    if HIGHEST_PROTOCOL >= 5 and buffer_callback is not None:
        dump_kwargs["buffer_callback"] = buffers.append
    try:
        del buffers[:]
        result = pickle.dumps(x, **dump_kwargs)
        if len(result) < 1000:
            if b"__main__" in result:
                del buffers[:]
                result = cloudpickle.dumps(x, **dump_kwargs)
        elif not (_always_use_pickle_for(x) or b"__main__" not in result):
            del buffers[:]
            result = cloudpickle.dumps(x, **dump_kwargs)
    except Exception:
        try:
            del buffers[:]
            result = cloudpickle.dumps(x, **dump_kwargs)
        except Exception as e:
            logger.info("Failed to serialize %s. Exception: %s", x, e)
            raise
    for b in buffers:
        buffer_callback(b)
    return result


def loads(x, *, buffers=()):
    try:
        if buffers:
            return pickle.loads(x, buffers=buffers)
        else:
            return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise
