#!/usr/bin/env python
"""
This app route is used by the dask-labextension.
"""

from __future__ import print_function, division, absolute_import

from bisect import bisect

from bokeh.io import curdoc
from bokeh.layouts import column, row
from toolz import valmap

from distributed.bokeh.status_monitor import progress_plot
from distributed.diagnostics.progress_stream import progress_quads
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

progress_source, progress_plot = progress_plot(sizing_mode=SIZING_MODE,
        width=WIDTH, height=160)

def progress_update():
    with log_errors():
        msg = messages['progress']
        if not msg:
            return

        d = progress_quads(msg)
        progress_source.data.update(d)
        progress_plot.title.text = ("Progress -- total: %(total)s, "
            "in-memory: %(in-memory)s, processing: %(processing)s, "
            "ready: %(ready)s, waiting: %(waiting)s, failed: %(failed)s"
            % messages['tasks']['deque'][-1])

doc.add_periodic_callback(progress_update, 50)

doc.add_root(progress_plot)
