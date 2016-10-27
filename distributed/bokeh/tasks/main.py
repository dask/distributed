#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh.components import TaskStream
import distributed.bokeh

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

task_stream = TaskStream(sizing_mode='stretch_both')
task_stream.update(messages)

doc.add_root(task_stream.root)
