from __future__ import print_function, division, absolute_import

from functools import partial
from time import time

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem, BoxSelectTool,
    Circle, CategoricalAxis,
)
from bokeh.models.widgets import DataTable, TableColumn, NumberFormatter
from bokeh.palettes import Spectral9, RdBu11
from bokeh.plotting import figure
from toolz import frequencies

from .components import DashboardComponent
from ..diagnostics.progress_stream import color_of
from ..utils import log_errors, key_split


red_blue = sorted(RdBu11)[::-1]


class StateTable(DashboardComponent):
    """ Currently running tasks """
    def __init__(self, worker):
        self.worker = worker

        names = ['Stored', 'Executing', 'Ready', 'Waiting', 'Connections', 'Serving']
        self.source = ColumnDataSource({name: [] for name in names})

        columns = {name: TableColumn(field=name, title=name)
                   for name in names}

        table = DataTable(
            source=self.source, columns=[columns[n] for n in names],
            height=70,
        )
        self.root = table

    def update(self):
        with log_errors():
            w = self.worker
            d = {'Stored': [len(w.data)],
                 'Executing': ['%d / %d' % (len(w.executing), w.ncores)],
                 'Ready': [len(w.heap)],
                 'Waiting': [len(w.waiting_for_data)],
                 'Connections': [len(w.connections)],
                 'Serving': [len(w._listen_streams)]}
            self.source.data.update(d)


class CommunicatingStream(DashboardComponent):
    def __init__(self, worker, height=300, **kwargs):
        with log_errors():
            self.worker = worker
            names = ['start', 'stop', 'middle', 'duration', 'who', 'y',
                    'hover', 'alpha', 'bandwidth', 'total' ]

            self.incoming = ColumnDataSource({name: [] for name in names})
            self.outgoing = ColumnDataSource({name: [] for name in names})

            x_range = DataRange1d(range_padding=0)
            y_range = DataRange1d(range_padding=0)

            fig= figure(title='Outgoing Communications',
                    x_axis_type='datetime', x_range=x_range, y_range=y_range,
                    height=height, tools='', **kwargs)

            fig.rect(source=self.incoming, x='middle', y='y', width='duration',
                     height=0.9, color='blue', alpha='alpha')
            fig.rect(source=self.outgoing, x='middle', y='y', width='duration',
                     height=0.9, color='red', alpha='alpha')

            hover = HoverTool(
                point_policy="follow_mouse",
                tooltips="""@hover"""
            )
            fig.add_tools(
                hover,
                ResetTool(reset_size=False),
                PanTool(dimensions="width"),
                WheelZoomTool(dimensions="width")
            )

            self.root = fig

            self.last_incoming = 0
            self.last_outgoing = 0
            self.who = dict()

    def update(self):
        with log_errors():
            outgoing = self.worker.outgoing_transfer_log
            n = self.worker.outgoing_count - self.last_outgoing
            outgoing = [outgoing[-i].copy() for i in range(0, n)]
            self.last_outgoing = self.worker.outgoing_count

            incoming = self.worker.incoming_transfer_log
            n = self.worker.incoming_count - self.last_incoming
            incoming = [incoming[-i].copy() for i in range(0, n)]
            self.last_incoming = self.worker.incoming_count

            for [msgs, source] in [[incoming, self.incoming],
                                   [outgoing, self.outgoing]]:

                for msg in msgs:
                    if 'compressed' in msg:
                        del msg['compressed']
                    del msg['keys']

                    bandwidth = msg['total'] / msg['duration']
                    bw = max(min(bandwidth / 500e6, 1), 0.3)
                    msg['alpha'] = bw
                    try:
                        msg['y'] = self.who[msg['who']]
                    except KeyError:
                        self.who[msg['who']] = len(self.who)
                        msg['y'] = self.who[msg['who']]

                    msg['hover'] = '%s / %s = %s/s' % (
                                format_bytes(msg['total']),
                                format_time(msg['duration']),
                                format_bytes(msg['total'] / msg['duration']))

                    for k in ['middle', 'duration', 'start', 'stop']:
                        msg[k] = msg[k] * 1000

                if msgs:
                    msgs = transpose(msgs)
                    if (len(source.data['stop']) and
                        min(msgs['start']) > source.data['stop'][-1] + 10000):
                        source.data.update(msgs)
                    else:
                        source.stream(msgs, rollover=1000)


class CommunicatingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'in': [], 'out': []})

        x_range = DataRange1d(follow='end', follow_interval=30000)

        fig = figure(title="Communication History",
                     x_axis_type='datetime',
                     y_range=[-0.1, worker.total_connections + 0.1],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='in', color='red')
        fig.line(source=self.source, x='x', y='out', color='blue')

        fig.add_tools(
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )


        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'in': [len(self.worker._listen_streams)],
                                'out': [len(self.worker.connections)]}, 1000)


class ExecutingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'y': []})

        x_range = DataRange1d(follow='end', follow_interval=30000)

        fig = figure(title="Executing History",
                     x_axis_type='datetime', y_range=[-0.1, worker.ncores + 0.1],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='y')

        fig.add_tools(
            ResetTool(reset_size=False),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'y': [len(self.worker.executing)]}, 1000)


from bokeh.server.server import Server
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application


def modify_doc(worker, doc):
    with log_errors():
        statetable = StateTable(worker)
        executing_ts = ExecutingTimeSeries(worker, sizing_mode='scale_width')
        communicating_ts = CommunicatingTimeSeries(worker,
                sizing_mode='scale_width')
        communicating_stream = CommunicatingStream(worker,
                sizing_mode='scale_width')

        xr = executing_ts.root.x_range
        communicating_ts.root.x_range = xr
        communicating_stream.root.x_range = xr

        doc.add_periodic_callback(statetable.update, 100)
        doc.add_periodic_callback(executing_ts.update, 100)
        doc.add_periodic_callback(communicating_ts.update, 100)
        doc.add_periodic_callback(communicating_stream.update, 100)
        doc.add_root(column(statetable.root,
                            executing_ts.root,
                            communicating_ts.root,
                            communicating_stream.root,
                            sizing_mode='scale_width'))


class BokehWorker(object):
    def __init__(self, worker, io_loop=None):
        self.worker = worker
        app = Application(FunctionHandler(partial(modify_doc, worker)))
        self.apps = {'/': app}

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


def transpose(lod):
    keys = list(lod[0].keys())
    return {k: [d[k] for d in lod] for k in keys}


def format_bytes(n):
    """ Format bytes as text

    >>> format_bytes(1)
    '1 B'
    >>> format_bytes(1234)
    '1.23 kB'
    >>> format_bytes(12345678)
    '12.35 MB'
    >>> format_bytes(1234567890)
    '1.23 GB'
    """
    if n > 1e9:
        return '%0.2f GB' % (n / 1e9)
    if n > 1e6:
        return '%0.2f MB' % (n / 1e6)
    if n > 1e3:
        return '%0.2f kB' % (n / 1000)
    return '%d B' % n


def format_time(n):
    """ format integers as time

    >>> format_time(1)
    '1.00 s'
    >>> format_time(0.001234)
    '1.23 ms'
    >>> format_time(0.00012345)
    '123.45 us'
    >>> format_time(123.456)
    '123.46 s'
    """
    if n >= 1:
        return '%.2f s' % n
    if n >= 1e-3:
        return '%.2f ms' % (n * 1e3)
    return '%.2f us' % (n * 1e6)
