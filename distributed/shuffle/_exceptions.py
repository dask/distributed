from __future__ import annotations


class P2PIllegalStateError(RuntimeError):
    pass


class P2PConsistencyError(RuntimeError):
    pass


class ShuffleClosedError(P2PConsistencyError):
    pass


class DataUnavailable(Exception):
    """Raised when data is not available in the buffer"""
