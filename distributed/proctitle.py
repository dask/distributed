from __future__ import print_function, division, absolute_import

import os


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
    try:
        import setproctitle
    except ImportError:
        return
    enabled = _enabled
    if not enabled:
        try:
            enabled = int(os.environ.get('DASK_PARENT', '')) != os.getpid()
        except ValueError:
            pass
    if enabled:
        setproctitle.setproctitle(title)

