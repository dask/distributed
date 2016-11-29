from __future__ import print_function, division, absolute_import

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem, BoxSelectTool,
    Circle
)
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
from bokeh.palettes import Spectral9
from bokeh.plotting import figure
from toolz import frequencies

from .components import DashboardComponent
from ..utils import log_errors, key_split


class ExecutingTable(DashboardComponent):
    """ Currently running tasks """
    def __init__(self, worker):
        self.worker = worker

        names = ['Task', 'Dependencies']
        self.source = ColumnDataSource({name: [] for name in names})

        columns = {name: TableColumn(field=name, title=name)
                   for name in names}

        table = DataTable(
            source=self.source, columns=[columns[n] for n in names],
        )
        self.root = table

    def update(self):
        with log_errors():
            keys = sorted(self.worker.executing)
            deps = [frequencies(map(key_split, self.worker.dependencies[key]))
                    for key in keys]
            deps = [', '.join(key if v == 1 else '%d x %s' % (v, key)
                              for key, v in dep.items())
                    for dep in deps]
            self.source.data['Task'] = keys
            self.source.data['Dependencies'] = deps


from bokeh.server.server import Server
from bokeh.command.util import build_single_handler_applications


class BokehWorkerServer(object):
    scripts = ['distributed/bokeh/worker']
    def __init__(self, worker, io_loop=None):
        self.worker = worker
        self.apps = build_single_handler_applications(self.scripts, {})
        for app in self.apps.values():
            app.worker = worker

        self.loop = io_loop or worker.loop
        self.server = None

    def listen(self, port):
        if self.server:
            return
        try:
            self.server = Server(self.apps, io_loop=self.loop, port=port,
                                 host=['*'])
            self.server.start(start_loop=False)
        except SystemExit:
            self.server = Server(self.apps, io_loop=self.loop, port=0,
                                 host=['*'])
            self.server.start(start_loop=False)

    @property
    def port(self):
        return (self.server.port or
                list(self.server._http._sockets.values())[0].getsockname()[1])

    def stop(self):
        for context in self.server._tornado._applications.values():
            context.run_unload_hook()

        self.server._tornado._stats_job.stop()
        self.server._tornado._cleanup_job.stop()
        if self.server._tornado._ping_job is not None:
            self.server._tornado._ping_job.stop()

        # self.server.stop()
        # https://github.com/bokeh/bokeh/issues/5494
