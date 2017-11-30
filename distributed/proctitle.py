from __future__ import print_function, division, absolute_import

import os

try:
    import setproctitle as setproctitle_mod
except ImportError:
    setproctitle_mod = None


_enabled = False


def enable_proctitle_on_children():
    """
    """
    os.environ['DASK_PARENT'] = str(os.getpid())


def enable_proctitle_on_current():
    """
    """
    global _enabled
    _enabled = True


def setproctitle(title):
    if setproctitle_mod is None:
        return
    enabled = _enabled
    if not enabled:
        try:
            enabled = int(os.environ.get('DASK_PARENT', '')) != os.getpid()
        except ValueError:
            pass
    if enabled:
        setproctitle_mod.setproctitle(title)
