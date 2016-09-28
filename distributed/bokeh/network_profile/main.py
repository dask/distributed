#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

from bisect import bisect
from bokeh.io import curdoc
from toolz import valmap

from distributed.bokeh.worker_monitor import resource_profile_plot
from distributed.utils import log_errors
import distributed.bokeh

SIZING_MODE = 'stretch_both'
WIDTH = 600

messages = distributed.bokeh.messages  # global message store
doc = curdoc()

resource_source, _, network_plot, _ = resource_profile_plot(sizing_mode=SIZING_MODE, width=WIDTH, height=80)

# because it was hidden for the combo toolbar
network_plot.toolbar_location = 'right'

resource_index = [0]
def resource_update():
    with log_errors():
        index = messages['workers']['index']
        data = messages['workers']['plot-data']

        if not index or index[-1] == resource_index[0]:
            return

        if resource_index == [0]:
            data = valmap(list, data)

        ind = bisect(index, resource_index[0])
        indexes = list(range(ind, len(index)))
        data = {k: [v[i] for i in indexes] for k, v in data.items()}
        resource_index[0] = index[-1]
        resource_source.stream(data, 1000)

doc.add_periodic_callback(resource_update, messages['workers']['interval'])

doc.add_root(network_plot)
