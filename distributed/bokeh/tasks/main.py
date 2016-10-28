#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from toolz import valmap
from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models.widgets import Button

from distributed.bokeh.status_monitor import task_stream_plot
import distributed.bokeh

messages = distributed.bokeh.messages  # global message store
doc = curdoc()
task_stream_source, task_stream_plot = task_stream_plot(sizing_mode='stretch_both')
task_stream_index = [0]

def update():
    rectangles = messages['task-events']['rectangles']
    rectangles = valmap(list, rectangles)
    task_stream_source.data.update(rectangles)

update()

button = Button(label='Refresh')
button.on_click(update)

layout = column(button, task_stream_plot, sizing_mode='stretch_both')
doc.add_root(layout)
