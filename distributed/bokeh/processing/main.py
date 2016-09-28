#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh.worker_monitor import (
    processing_plot as pplot, processing_update)
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

processing_source, processing_plot = pplot(sizing_mode=SIZING_MODE,
    width=WIDTH, height=150)

def processing_plot_update():
    with log_errors():
        msg = messages['processing']
        if not msg['ncores']:
            return
        data = processing_update(msg)
        x_range = processing_plot.x_range
        max_right = max(data['right'])
        min_left = min(data['left'][:-1])
        cores = max(data['ncores'])
        if min_left < x_range.start:  # not out there enough, jump ahead
            x_range.start = min_left - 2
        elif x_range.start < 2 * min_left - cores:  # way out there, walk back
            x_range.start = x_range.start * 0.95 + min_left * 0.05
        if x_range.end < max_right:
            x_range.end = max_right + 2
        elif x_range.end > 2 * max_right + cores:  # way out there, walk back
            x_range.end = x_range.end * 0.95 + max_right * 0.05

        processing_source.data.update(data)

doc.add_periodic_callback(processing_plot_update, 200)

doc.add_root(processing_plot)
