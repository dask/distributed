import logging

from dask.sizeof import sizeof

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)


def safe_sizeof(obj, default_size=1e6):
    """ Safe variant of sizeof that captures and logs exceptions

    This returns a default size of 1e6 if the sizeof function fails
    """
    try:
        # Handle zero-strided NumPy arrays
        if np is not None and isinstance(obj, np.ndarray) and 0 in obj.strides:
            from .protocol.numpy import _zero_strided_slice

            obj = _zero_strided_slice(obj)
        return sizeof(obj)
    except Exception:
        logger.warning("Sizeof calculation failed.  Defaulting to 1MB", exc_info=True)
        return int(default_size)
