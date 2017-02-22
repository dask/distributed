from __future__ import print_function, division, absolute_import

from bokeh.io import curdoc

from distributed.bokeh import messages
from distributed.bokeh.components import ProcessingStacks

component = ProcessingStacks(sizing_mode='stretch_both')

doc = curdoc()
doc.title = "Dask Processing Stack"
doc.add_periodic_callback(lambda: component.update(messages), 200)
doc.add_root(component.root)
