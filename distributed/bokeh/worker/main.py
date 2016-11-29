#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc
from bokeh.layouts import column, row

from distributed.bokeh.worker_server import (
    ExecutingTable
    )

SIZING_MODE = 'scale_width'
WIDTH = 600

doc = curdoc()
worker = doc.session_context.server_context.application_context.application.worker

table = ExecutingTable(worker)
doc.add_periodic_callback(table.update, 100)

doc.add_root(table.root)
