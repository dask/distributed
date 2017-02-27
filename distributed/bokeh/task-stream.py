from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh import messages
from distributed.bokeh.components import TaskStream

component = TaskStream(sizing_mode='stretch_both')

doc = curdoc()
doc.title = "Dask Tasks"
doc.add_periodic_callback(lambda: component.update(messages),
                          messages['task-events']['interval'])
doc.add_root(component.root)
