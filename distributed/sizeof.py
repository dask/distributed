from __future__ import annotations

import logging

from dask.sizeof import sizeof
from dask.utils import format_bytes

logger = logging.getLogger(__name__)


def safe_sizeof(obj: object, default_size: float = 1e6) -> int:
    """Safe variant of sizeof that captures and logs exceptions

    This returns a default size of 1e6 if the sizeof function fails
    """
    try:
        return sizeof(obj)
    except Exception:
        logger.warning(
            f"Sizeof calculation failed. Defaulting to {format_bytes(int(default_size))}",
            exc_info=True,
        )
        return int(default_size)
