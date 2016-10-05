#!/usr/bin/env python
"""
This app route is used by the dask-labextension.
"""

from __future__ import print_function, division, absolute_import

from bisect import bisect
from bokeh.io import curdoc

from distributed.bokeh.status_monitor import task_stream_plot
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

task_stream_source, task_stream_plot = task_stream_plot(
        sizing_mode=SIZING_MODE, width=WIDTH, height=300)

task_stream_index = [0]
def task_stream_update():
    with log_errors():
        index = messages['task-events']['index']
        old = rectangles = messages['task-events']['rectangles']

        if not index or index[-1] == task_stream_index[0]:
            return

        ind = bisect(index, task_stream_index[0])
        rectangles = {k: [v[i] for i in range(ind, len(index))]
                      for k, v in rectangles.items()}
        task_stream_index[0] = index[-1]

        # If there has been a five second delay, clear old rectangles
        if rectangles['start']:
            last_end = old['start'][ind - 1] + old['duration'][ind - 1]
            if min(rectangles['start']) > last_end + 20000:  # long delay
                task_stream_source.data.update(rectangles)
                return

        task_stream_source.stream(rectangles, 1000)

doc.add_periodic_callback(task_stream_update, messages['task-events']['interval'])

doc.add_root(task_stream_plot)
