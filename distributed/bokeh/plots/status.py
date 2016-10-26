from bisect import bisect

from bokeh.models import (
    ColumnDataSource, Plot, Datetime, DataRange1d, Rect, LinearAxis,
    DatetimeAxis, Grid, BasicTicker, HoverTool, BoxZoomTool, ResetTool,
    PanTool, WheelZoomTool, Title
)

from distributed.utils import log_errors

class Element(object):
    """ Base class for Distributed UI Dashboard elements. """

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
        Callback that reads from messages and updates self.source
        """
        pass

class TaskStream(Element):
    """ Task Stream """

    def __init__(self, **kwargs):
        """
        kwargs are applied to bokeh.models.plots.Plot constructor
        """

        self.source = ColumnDataSource(data=dict(
            start=[], duration=[], key=[], name=[], color=[],
            worker=[], y=[], worker_thread=[], alpha=[]
        ))

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
                </div>"""
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
