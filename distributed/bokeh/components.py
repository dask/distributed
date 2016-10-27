from __future__ import print_function, division, absolute_import

from bisect import bisect
from toolz import valmap

from bokeh.layouts import row, column
from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title, Range1d, Quad, Text, value, Line,
    NumeralTickFormatter, ToolbarBox, Legend, LegendItem
)

from distributed.diagnostics.progress_stream import progress_quads, nbytes_bar
from distributed.utils import log_errors

class DashboardComponent(object):
    """ Base class for Distributed UI dashboard components. """

    def __init__(self):
        """
        Should be overridden by subclasses.

        self.source ColumnDataSource
        self.root should a layout-able object, a Plot, Layout, Widget or WidgetBox
        """
        self.source = None
        self.root = None

    def update(self, messages):
        """
        Callback that reads from bokeh.distributed.messages and updates self.source
        """
        pass

class TaskStream(DashboardComponent):
    """ Task Stream """

    def __init__(self, **kwargs):
        """
        kwargs are applied to bokeh.models.plots.Plot constructor
        """

        self.source = ColumnDataSource(data=dict(
            start=[], duration=[], key=[], name=[], color=[],
            worker=[], y=[], worker_thread=[], alpha=[])
        )

        x_range = DataRange1d()
        y_range = DataRange1d(range_padding=0)

        self.root = Plot(
            title=Title(text="Task Stream"), id='bk-task-stream-plot',
            x_range=x_range, y_range=y_range, toolbar_location="above",
            min_border_right=35, **kwargs
        )

        self.root.add_glyph(
            self.source,
            Rect(x="start", y="y", width="duration", height=0.8, fill_color="color",
                 line_color="color", line_alpha=0.6, fill_alpha="alpha", line_width=3)
        )

        self.root.add_layout(DatetimeAxis(axis_label="Time"), "below")

        ticker = BasicTicker(num_minor_ticks=0)
        self.root.add_layout(LinearAxis(axis_label="Worker Core", ticker=ticker), "left")
        self.root.add_layout(Grid(dimension=1, grid_line_alpha=0.4, ticker=ticker))

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 12px; font-weight: bold;">@name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@duration</span>
                    <span style="font-size: 10px;">ms</span>&nbsp;
                </div>
                """
        )

        self.root.add_tools(
            hover, ResetTool(), PanTool(dimensions="width"), WheelZoomTool(dimensions="width")
        )

        # Required for update callback
        self.task_stream_index = [0]

    def update(self, messages):
        """" Update callback """
        with log_errors():
            index = messages['task-events']['index']
            old = rectangles = messages['task-events']['rectangles']

            if not index or index[-1] == self.task_stream_index[0]:
                return

            ind = bisect(index, self.task_stream_index[0])
            rectangles = {k: [v[i] for i in range(ind, len(index))]
                          for k, v in rectangles.items()}
            self.task_stream_index[0] = index[-1]

            # If there has been a five second delay, clear old rectangles
            if rectangles['start']:
                last_end = old['start'][ind - 1] + old['duration'][ind - 1]
                if min(rectangles['start']) > last_end + 20000:  # long delay
                    self.source.data.update(rectangles)
                    return

            self.source.stream(rectangles, 1000)


class TaskProgress(DashboardComponent):
    """ task completion progress bars """

    def __init__(self, **kwargs):
        """ stuff happens """

        data = progress_quads(dict(all={}, memory={}, erred={}, released={}))
        self.source = ColumnDataSource(data=data)

        x_range = DataRange1d()
        y_range = Range1d(-8, 0)

        self.root = Plot(
            id='bk-progress-plot',
            x_range=x_range, y_range=y_range, toolbar_location=None, **kwargs
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='right',
                 fill_color="#aaaaaa", line_color="#aaaaaa", fill_alpha=0.2)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='left', right='released-loc',
                 fill_color="color", line_color="color", fill_alpha=0.6)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='released-loc',
                 right='memory-loc', fill_color="color", line_color="color",
                 fill_alpha=1.0)
        )
        self.root.add_glyph(
            self.source,
            Quad(top='top', bottom='bottom', left='erred-loc',
                 right='erred-loc', fill_color='#000000', line_color='#000000',
                 fill_alpha=0.3)
        )
        self.root.add_glyph(
            self.source,
            Text(text='show-name', y='bottom', x='left', x_offset=5,
                 text_font_size=value('10pt'))
        )
        self.root.add_glyph(
            self.source,
            Text(text='done', y='bottom', x='right', x_offset=-5,
                 text_align='right', text_font_size=value('10pt'))
        )

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">All:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@all</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Memory:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@memory</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Erred:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@erred</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self, messages):
        """ updates stuff """
        with log_errors():
            msg = messages['progress']
            if not msg:
                return
            d = progress_quads(msg)
            self.source.data.update(d)
            if messages['tasks']['deque']:
                self.root.title.text = ("Progress -- total: %(total)s, "
                    "in-memory: %(in-memory)s, processing: %(processing)s, "
                    "ready: %(ready)s, waiting: %(waiting)s, failed: %(failed)s"
                    % messages['tasks']['deque'][-1])


class MemoryUsage(DashboardComponent):
    """ stuff and things """

    def __init__(self, **kwargs):
        """ stuff and things """

        self.source = ColumnDataSource(data=dict(
            name=[], left=[], right=[], center=[], color=[],
            percent=[], MB=[], text=[])
        )

        self.root = Plot(
            id='bk-nbytes-plot', x_range=DataRange1d(), y_range=DataRange1d(),
            toolbar_location=None, outline_line_color=None, **kwargs
        )

        self.root.add_glyph(
            self.source,
            Quad(top=1, bottom=0, left='left', right='right',
                 fill_color='color', fill_alpha=1)
        )

        self.root.add_layout(LinearAxis(), 'left')
        self.root.add_layout(LinearAxis(), 'below')

        hover = HoverTool(
            point_policy="follow_mouse",
            tooltips="""
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Name:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@name</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">Percent:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@percent</span>
                </div>
                <div>
                    <span style="font-size: 14px; font-weight: bold;">MB:</span>&nbsp;
                    <span style="font-size: 10px; font-family: Monaco, monospace;">@MB</span>
                </div>
                """
        )
        self.root.add_tools(hover)

    def update(self, messages):
        """ do the thing """

        with log_errors():
            msg = messages['progress']
            if not msg:
                return
            nb = nbytes_bar(msg['nbytes'])
            self.source.data.update(nb)
            self.root.title.text = \
                    "Memory Use: %0.2f MB" % (sum(msg['nbytes'].values()) / 1e6)


class ResourceProfiles(DashboardComponent):
    """ things and stuff """

    def __init__(self, **kwargs):

        self.source = ColumnDataSource(data={'time': [], 'cpu': [],
            'memory_percent':[], 'network-send':[], 'network-recv':[]}
        )

        x_range = DataRange1d(follow='end', follow_interval=30000, range_padding=0)

        resource_plot = Plot(
            x_range=x_range, y_range=Range1d(start=0, end=1),
            toolbar_location=None, min_border_bottom=10, **kwargs
        )

        line_opts = dict(line_width=2, line_alpha=0.8)
        g1 = resource_plot.add_glyph(
            self.source,
            Line(x='time', y='memory_percent', line_color="#33a02c", **line_opts)
        )
        g2 = resource_plot.add_glyph(
            self.source,
            Line(x='time', y='cpu', line_color="#1f78b4"    , **line_opts)
        )

        resource_plot.add_layout(
            LinearAxis(formatter=NumeralTickFormatter(format="0 %")),
            'left'
        )

        legend_opts = dict(
            location='top_left', orientation='horizontal', padding=5, margin=5,
            label_height=5)

        resource_plot.add_layout(
            Legend(items=[('Memory', [g1]), ('CPU', [g2])], **legend_opts)
        )

        network_plot = Plot(
            x_range=x_range, y_range=DataRange1d(start=0),
            toolbar_location=None, **kwargs
        )
        g1 = network_plot.add_glyph(
            self.source,
            Line(x='time', y='network-send', line_color="#a6cee3", **line_opts)
        )
        g2 =network_plot.add_glyph(
            self.source,
            Line(x='time', y='network-recv', line_color="#b2df8a", **line_opts)
        )

        network_plot.add_layout(DatetimeAxis(axis_label="Time"), "below")
        network_plot.add_layout(LinearAxis(axis_label="MB/s"), 'left')
        network_plot.add_layout(
            Legend(items=[('Network Send', [g1]), ('Network Recv', [g2])], **legend_opts)
        )

        tools = [
            PanTool(dimensions='width'), WheelZoomTool(dimensions='width'),
            BoxZoomTool(), ResetTool()
        ]

        if 'sizing_mode' in kwargs:
            sizing_mode = {'sizing_mode': kwargs['sizing_mode']}
        else:
            sizing_mode = {}

        combo_toolbar = ToolbarBox(
            tools=tools, logo=None, toolbar_location='right', **sizing_mode
        )

        self.root = row(
            column(resource_plot, network_plot, **sizing_mode),
            column(combo_toolbar, **sizing_mode),
            **sizing_mode
        )

        # Required for update callback
        self.resource_index = [0]

    def update(self, messages):
        """ stuff and things """

        with log_errors():
            index = messages['workers']['index']
            data = messages['workers']['plot-data']

            if not index or index[-1] == self.resource_index[0]:
                return

            if self.resource_index == [0]:
                data = valmap(list, data)

            ind = bisect(index, self.resource_index[0])
            indexes = list(range(ind, len(index)))
            data = {k: [v[i] for i in indexes] for k, v in data.items()}
            self.resource_index[0] = index[-1]
            self.source.stream(data, 1000)
