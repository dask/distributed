from __future__ import print_function, division, absolute_import

import collections


_empty_namedtuple = collections.namedtuple("_empty_namedtuple", ())


def _run_psutil(method_name, default):
    try:
        import psutil
    except ImportError:
        return default
    try:
        return getattr(psutil, method_name)()
    except RuntimeError:
        # This can happen on some systems (e.g. no physical disk in worker)
        return default


def disk_io_counters():
    return _run_psutil("disk_io_counters", _empty_namedtuple())

def net_io_counters():
    return _run_psutil("net_io_counters", _empty_namedtuple())
