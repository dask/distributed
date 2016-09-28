#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh.status_monitor import nbytes_plot
from distributed.diagnostics.progress_stream import nbytes_bar
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

nbytes_task_source, nbytes_task_plot = nbytes_plot(sizing_mode=SIZING_MODE, width=WIDTH, height=60)

def progress_update():
    with log_errors():
        msg = messages['progress']
        if not msg:
            return

        nb = nbytes_bar(msg['nbytes'])
        nbytes_task_source.data.update(nb)
        nbytes_task_plot.title.text = \
                "Memory Use: %0.2f MB" % (sum(msg['nbytes'].values()) / 1e6)

doc.add_periodic_callback(progress_update, 50)

doc.add_root(nbytes_task_plot)
