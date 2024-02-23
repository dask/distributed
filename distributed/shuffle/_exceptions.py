from __future__ import annotations


class ShuffleClosedError(RuntimeError):
    pass


class DataUnavailable(Exception):
    """Raised when data is not available in the buffer"""
