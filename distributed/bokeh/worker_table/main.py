#!/usr/bin/env python
"""
This app route is used by the dask-labextension.
"""

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh.worker_monitor import (
    worker_table_plot, worker_table_update)
from distributed.utils import log_errors
import distributed.bokeh

WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

worker_source, [_, worker_table] = worker_table_plot(width=WIDTH)

def worker_update():
    with log_errors():
        try:
            msg = messages['workers']['deque'][-1]
        except IndexError:
            return
        worker_table_update(worker_source, msg)

doc.add_periodic_callback(worker_update, messages['workers']['interval'])

doc.add_root(worker_table)
