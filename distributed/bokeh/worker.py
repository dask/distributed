from __future__ import print_function, division, absolute_import

from functools import partial
import logging
import math
import os

from bokeh.layouts import row, column, widgetbox
from bokeh.models import (ColumnDataSource, DataRange1d, HoverTool,
                          BoxZoomTool, ResetTool, PanTool, WheelZoomTool, NumeralTickFormatter,
                          Select)

from bokeh.models.widgets import DataTable, TableColumn
from bokeh.plotting import figure
from bokeh.palettes import RdBu
from toolz import merge, partition_all

from .components import DashboardComponent, ProfileTimePlot
from .core import BokehServer
from .utils import transpose
from ..compatibility import WINDOWS
from ..diagnostics.progress_stream import color_of
from ..metrics import time
from ..utils import (log_errors, key_split, format_bytes, format_time)


logger = logging.getLogger(__name__)

import jinja2

with open(os.path.join(os.path.dirname(__file__), 'template.html')) as f:
    template_source = f.read()

template = jinja2.Template(template_source)
template_variables = {'pages': ['main', 'system', 'profile', 'crossfilter']}


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
                 'Ready': [len(w.ready)],
                 'Waiting': [len(w.waiting_for_data)],
                 'Connections': [len(w.in_flight_workers)],
                 'Serving': [len(w._comms)]}
            self.source.data.update(d)


class CommunicatingStream(DashboardComponent):
    def __init__(self, worker, height=300, **kwargs):
        with log_errors():
            self.worker = worker
            names = ['start', 'stop', 'middle', 'duration', 'who', 'y',
                     'hover', 'alpha', 'bandwidth', 'total']

            self.incoming = ColumnDataSource({name: [] for name in names})
            self.outgoing = ColumnDataSource({name: [] for name in names})

            x_range = DataRange1d(range_padding=0)
            y_range = DataRange1d(range_padding=0)

            fig = figure(title='Peer Communications',
                         x_axis_type='datetime', x_range=x_range, y_range=y_range,
                         height=height, tools='', **kwargs)

            fig.rect(source=self.incoming, x='middle', y='y', width='duration',
                     height=0.9, color='red', alpha='alpha')
            fig.rect(source=self.outgoing, x='middle', y='y', width='duration',
                     height=0.9, color='blue', alpha='alpha')

            hover = HoverTool(
                point_policy="follow_mouse",
                tooltips="""@hover"""
            )
            fig.add_tools(
                hover,
                ResetTool(),
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
            outgoing = [outgoing[-i].copy() for i in range(1, n + 1)]
            self.last_outgoing = self.worker.outgoing_count

            incoming = self.worker.incoming_transfer_log
            n = self.worker.incoming_count - self.last_incoming
            incoming = [incoming[-i].copy() for i in range(1, n + 1)]
            self.last_incoming = self.worker.incoming_count

            for [msgs, source] in [[incoming, self.incoming],
                                   [outgoing, self.outgoing]]:

                for msg in msgs:
                    if 'compressed' in msg:
                        del msg['compressed']
                    del msg['keys']

                    bandwidth = msg['total'] / (msg['duration'] or 0.5)
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
                        source.stream(msgs, rollover=10000)


class CommunicatingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'in': [], 'out': []})

        x_range = DataRange1d(follow='end', follow_interval=20000, range_padding=0)

        fig = figure(title="Communication History",
                     x_axis_type='datetime',
                     y_range=[-0.1, worker.total_connections + 0.5],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='in', color='red')
        fig.line(source=self.source, x='x', y='out', color='blue')

        fig.add_tools(
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'out': [len(self.worker._comms)],
                                'in': [len(self.worker.in_flight_workers)]},
                               10000)


class ExecutingTimeSeries(DashboardComponent):
    def __init__(self, worker, **kwargs):
        self.worker = worker
        self.source = ColumnDataSource({'x': [], 'y': []})

        x_range = DataRange1d(follow='end', follow_interval=20000, range_padding=0)

        fig = figure(title="Executing History",
                     x_axis_type='datetime', y_range=[-0.1, worker.ncores + 0.1],
                     height=150, tools='', x_range=x_range, **kwargs)
        fig.line(source=self.source, x='x', y='y')

        fig.add_tools(
            ResetTool(),
            PanTool(dimensions="width"),
            WheelZoomTool(dimensions="width")
        )

        self.root = fig

    def update(self):
        with log_errors():
            self.source.stream({'x': [time() * 1000],
                                'y': [len(self.worker.executing)]}, 1000)


class CrossFilter(DashboardComponent):
    def __init__(self, worker, **kwargs):
        with log_errors():
            self.worker = worker

            quantities = ['nbytes', 'duration', 'bandwidth', 'count',
                          'start', 'stop']
            colors = ['inout-color', 'type-color', 'key-color']

            # self.source = ColumnDataSource({name: [] for name in names})
            self.source = ColumnDataSource({
                'nbytes': [1, 2],
                'duration': [0.01, 0.02],
                'bandwidth': [0.01, 0.02],
                'count': [1, 2],
                'type': ['int', 'str'],
                'inout-color': ['blue', 'red'],
                'type-color': ['blue', 'red'],
                'key': ['add', 'inc'],
                'start': [1, 2],
                'stop': [1, 2]
            })

            self.x = Select(title='X-Axis', value='nbytes', options=quantities)
            self.x.on_change('value', self.update_figure)

            self.y = Select(title='Y-Axis', value='bandwidth', options=quantities)
            self.y.on_change('value', self.update_figure)

            self.size = Select(title='Size', value='None',
                               options=['None'] + quantities)
            self.size.on_change('value', self.update_figure)

            self.color = Select(title='Color', value='inout-color',
                                options=['black'] + colors)
            self.color.on_change('value', self.update_figure)

            if 'sizing_mode' in kwargs:
                kw = {'sizing_mode': kwargs['sizing_mode']}
            else:
                kw = {}

            self.control = widgetbox([self.x, self.y, self.size, self.color],
                                     width=200, **kw)

            self.last_outgoing = 0
            self.last_incoming = 0
            self.kwargs = kwargs

            self.layout = row(self.control, self.create_figure(**self.kwargs),
                              **kw)

            self.root = self.layout

    def update(self):
        with log_errors():
            outgoing = self.worker.outgoing_transfer_log
            n = self.worker.outgoing_count - self.last_outgoing
            n = min(n, 1000)
            outgoing = [outgoing[-i].copy() for i in range(1, n)]
            self.last_outgoing = self.worker.outgoing_count

            incoming = self.worker.incoming_transfer_log
            n = self.worker.incoming_count - self.last_incoming
            n = min(n, 1000)
            incoming = [incoming[-i].copy() for i in range(1, n)]
            self.last_incoming = self.worker.incoming_count

            out = []

            for msg in incoming:
                if msg['keys']:
                    d = self.process_msg(msg)
                    d['inout-color'] = 'red'
                    out.append(d)

            for msg in outgoing:
                if msg['keys']:
                    d = self.process_msg(msg)
                    d['inout-color'] = 'blue'
                    out.append(d)

            if out:
                out = transpose(out)
                if (len(self.source.data['stop']) and
                        min(out['start']) > self.source.data['stop'][-1] + 10):
                    self.source.data.update(out)
                else:
                    self.source.stream(out, rollover=1000)

    def create_figure(self, **kwargs):
        with log_errors():
            fig = figure(title='', tools='', **kwargs)

            size = self.size.value
            if size == 'None':
                size = 1

            fig.circle(source=self.source, x=self.x.value, y=self.y.value,
                       color=self.color.value, size=10, alpha=0.5,
                       hover_alpha=1)
            fig.xaxis.axis_label = self.x.value
            fig.yaxis.axis_label = self.y.value

            fig.add_tools(
                # self.hover,
                ResetTool(),
                PanTool(),
                WheelZoomTool(),
                BoxZoomTool(),
            )
            return fig

    def update_figure(self, attr, old, new):
        with log_errors():
            fig = self.create_figure(**self.kwargs)
            self.layout.children[1] = fig

    def process_msg(self, msg):
        try:
            def func(k):
                return msg['keys'].get(k, 0)
            main_key = max(msg['keys'], key=func)
            typ = self.worker.types.get(main_key, object).__name__
            keyname = key_split(main_key)
            d = {
                'nbytes': msg['total'],
                'duration': msg['duration'],
                'bandwidth': msg['bandwidth'],
                'count': len(msg['keys']),
                'type': typ,
                'type-color': color_of(typ),
                'key': keyname,
                'key-color': color_of(keyname),
                'start': msg['start'],
                'stop': msg['stop']
            }
            return d
        except Exception as e:
            logger.exception(e)
            raise


class SystemMonitor(DashboardComponent):
    def __init__(self, worker, height=150, **kwargs):
        self.worker = worker

        names = worker.monitor.quantities
        self.last = 0
        self.source = ColumnDataSource({name: [] for name in names})
        self.source.data.update(self.get_data())

        x_range = DataRange1d(follow='end', follow_interval=20000,
                              range_padding=0)

        tools = 'reset,xpan,xwheel_zoom'

        self.cpu = figure(title="CPU", x_axis_type='datetime',
                          height=height, tools=tools, x_range=x_range, **kwargs)
        self.cpu.line(source=self.source, x='time', y='cpu')
        self.cpu.yaxis.axis_label = 'Percentage'
        self.mem = figure(title="Memory", x_axis_type='datetime',
                          height=height, tools=tools, x_range=x_range, **kwargs)
        self.mem.line(source=self.source, x='time', y='memory')
        self.mem.yaxis.axis_label = 'Bytes'
        self.bandwidth = figure(title='Bandwidth', x_axis_type='datetime',
                                height=height,
                                x_range=x_range, tools=tools, **kwargs)
        self.bandwidth.line(source=self.source, x='time', y='read_bytes',
                            color='red')
        self.bandwidth.line(source=self.source, x='time', y='write_bytes',
                            color='blue')
        self.bandwidth.yaxis.axis_label = 'Bytes / second'

        # self.cpu.yaxis[0].formatter = NumeralTickFormatter(format='0%')
        self.bandwidth.yaxis[0].formatter = NumeralTickFormatter(format='0.0b')
        self.mem.yaxis[0].formatter = NumeralTickFormatter(format='0.0b')

        plots = [self.cpu, self.mem, self.bandwidth]

        if not WINDOWS:
            self.num_fds = figure(title='Number of File Descriptors',
                                  x_axis_type='datetime', height=height,
                                  x_range=x_range, tools=tools, **kwargs)

            self.num_fds.line(source=self.source, x='time', y='num_fds')
            plots.append(self.num_fds)

        if 'sizing_mode' in kwargs:
            kw = {'sizing_mode': kwargs['sizing_mode']}
        else:
            kw = {}

        if not WINDOWS:
            self.num_fds.y_range.start = 0
        self.mem.y_range.start = 0
        self.cpu.y_range.start = 0
        self.bandwidth.y_range.start = 0

        self.root = column(*plots, **kw)
        self.worker.monitor.update()

    def get_data(self):
        d = self.worker.monitor.range_query(start=self.last)
        d['time'] = [x * 1000 for x in d['time']]
        self.last = self.worker.monitor.count
        return d

    def update(self):
        with log_errors():
            self.source.stream(self.get_data(), 1000)


class Counters(DashboardComponent):
    def __init__(self, server, sizing_mode='stretch_both', **kwargs):
        self.server = server
        self.counter_figures = {}
        self.counter_sources = {}
        self.digest_figures = {}
        self.digest_sources = {}
        self.sizing_mode = sizing_mode

        if self.server.digests:
            for name in self.server.digests:
                self.add_digest_figure(name)
        for name in self.server.counters:
            self.add_counter_figure(name)

        figures = merge(self.digest_figures, self.counter_figures)
        figures = [figures[k] for k in sorted(figures)]

        if len(figures) <= 5:
            self.root = column(figures, sizing_mode=sizing_mode)
        else:
            self.root = column(*[row(*pair, sizing_mode=sizing_mode)
                                 for pair in partition_all(2, figures)],
                               sizing_mode=sizing_mode)

    def add_digest_figure(self, name):
        with log_errors():
            n = len(self.server.digests[name].intervals)
            sources = {i: ColumnDataSource({'x': [], 'y': []})
                       for i in range(n)}

            kwargs = {}
            if name.endswith('duration'):
                kwargs['x_axis_type'] = 'datetime'

            fig = figure(title=name, tools='', height=150,
                         sizing_mode=self.sizing_mode, **kwargs)
            fig.yaxis.visible = False
            fig.ygrid.visible = False
            if name.endswith('bandwidth') or name.endswith('bytes'):
                fig.xaxis[0].formatter = NumeralTickFormatter(format='0.0b')

            for i in range(n):
                alpha = 0.3 + 0.3 * (n - i) / n
                fig.line(source=sources[i], x='x', y='y',
                         alpha=alpha, color=RdBu[max(n, 3)][-i])

            fig.xaxis.major_label_orientation = math.pi / 12
            fig.toolbar.logo = None
            self.digest_sources[name] = sources
            self.digest_figures[name] = fig
            return fig

    def add_counter_figure(self, name):
        with log_errors():
            n = len(self.server.counters[name].intervals)
            sources = {i: ColumnDataSource({'x': [], 'y': [],
                                            'y-center': [], 'counts': []})
                       for i in range(n)}

            fig = figure(title=name, tools='', height=150,
                         sizing_mode=self.sizing_mode,
                         x_range=sorted(map(str, self.server.counters[name].components[0])))
            fig.ygrid.visible = False

            for i in range(n):
                width = 0.5 + 0.4 * i / n
                fig.rect(source=sources[i], x='x', y='y-center', width=width,
                         height='y', alpha=0.3, color=RdBu[max(n, 3)][-i])
                hover = HoverTool(
                    point_policy="follow_mouse",
                    tooltips="""@x : @counts"""
                )
                fig.add_tools(hover)
                fig.xaxis.major_label_orientation = math.pi / 12

            fig.toolbar.logo = None

            self.counter_sources[name] = sources
            self.counter_figures[name] = fig
            return fig

    def update(self):
        with log_errors():
            for name, fig in self.digest_figures.items():
                digest = self.server.digests[name]
                d = {}
                for i, d in enumerate(digest.components):
                    if d.size():
                        ys, xs = d.histogram(100)
                        xs = xs[1:]
                        if name.endswith('duration'):
                            xs *= 1000
                        self.digest_sources[name][i].data.update({'x': xs, 'y': ys})
                fig.title.text = '%s: %d' % (name, digest.size())

            for name, fig in self.counter_figures.items():
                counter = self.server.counters[name]
                d = {}
                for i, d in enumerate(counter.components):
                    if d:
                        xs = sorted(d)
                        factor = counter.intervals[0] / counter.intervals[i]
                        counts = [d[x] for x in xs]
                        ys = [factor * c for c in counts]
                        y_centers = [y / 2 for y in ys]
                        xs = list(map(str, xs))
                        d = {'x': xs, 'y': ys, 'y-center': y_centers,
                             'counts': counts}
                        self.counter_sources[name][i].data.update(d)
                    fig.title.text = '%s: %d' % (name, counter.size())
                    fig.x_range.factors = list(map(str, xs))


from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application


def main_doc(worker, extra, doc):
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

        doc.title = "Dask Worker Internal Monitor"
        doc.add_periodic_callback(statetable.update, 200)
        doc.add_periodic_callback(executing_ts.update, 200)
        doc.add_periodic_callback(communicating_ts.update, 200)
        doc.add_periodic_callback(communicating_stream.update, 200)
        doc.add_root(column(statetable.root,
                            executing_ts.root,
                            communicating_ts.root,
                            communicating_stream.root,
                            sizing_mode='scale_width'))
        doc.template = template
        doc.template_variables['active_page'] = 'main'
        doc.template_variables.update(extra)


def crossfilter_doc(worker, extra, doc):
    with log_errors():
        statetable = StateTable(worker)
        crossfilter = CrossFilter(worker)

        doc.title = "Dask Worker Cross-filter"
        doc.add_periodic_callback(statetable.update, 500)
        doc.add_periodic_callback(crossfilter.update, 500)

        doc.add_root(column(statetable.root, crossfilter.root))
        doc.template = template
        doc.template_variables['active_page'] = 'crossfilter'
        doc.template_variables.update(extra)


def systemmonitor_doc(worker, extra, doc):
    with log_errors():
        sysmon = SystemMonitor(worker, sizing_mode='scale_width')
        doc.title = "Dask Worker Monitor"
        doc.add_periodic_callback(sysmon.update, 500)

        doc.add_root(sysmon.root)
        doc.template = template
        doc.template_variables['active_page'] = 'system'
        doc.template_variables.update(extra)


def counters_doc(server, extra, doc):
    with log_errors():
        doc.title = "Dask Worker Counters"
        counter = Counters(server, sizing_mode='stretch_both')
        doc.add_periodic_callback(counter.update, 500)

        doc.add_root(counter.root)
        doc.template = template
        doc.template_variables['active_page'] = 'counters'
        doc.template_variables.update(extra)


def profile_doc(server, extra, doc):
    with log_errors():
        doc.title = "Dask Worker Profile"
        profile = ProfileTimePlot(server, sizing_mode='scale_width')
        profile.trigger_update()

        doc.add_root(profile.root)
        doc.template = template
        doc.template_variables['active_page'] = 'profile'
        doc.template_variables.update(extra)


class BokehWorker(BokehServer):
    def __init__(self, worker, io_loop=None, prefix='', **kwargs):
        self.worker = worker
        self.server_kwargs = kwargs
        self.server_kwargs['prefix'] = prefix or None
        prefix = prefix or ''
        prefix = prefix.rstrip('/')
        if prefix and not prefix.startswith('/'):
            prefix = '/' + prefix
        self.prefix = prefix

        extra = {'prefix': prefix}

        extra.update(template_variables)

        main = Application(FunctionHandler(partial(main_doc, worker, extra)))
        crossfilter = Application(FunctionHandler(partial(crossfilter_doc, worker, extra)))
        systemmonitor = Application(FunctionHandler(partial(systemmonitor_doc, worker, extra)))
        counters = Application(FunctionHandler(partial(counters_doc, worker, extra)))
        profile = Application(FunctionHandler(partial(profile_doc, worker, extra)))

        self.apps = {'/main': main,
                     '/counters': counters,
                     '/crossfilter': crossfilter,
                     '/system': systemmonitor,
                     '/profile': profile}

        self.loop = io_loop or worker.loop
        self.server = None

    @property
    def my_server(self):
        return self.worker
