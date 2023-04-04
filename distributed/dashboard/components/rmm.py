from __future__ import annotations

import math
from textwrap import dedent

from bokeh.core.properties import without_property_validation
from bokeh.models import (
    BasicTicker,
    ColumnDataSource,
    HoverTool,
    NumeralTickFormatter,
    OpenURL,
    TapTool,
)
from bokeh.plotting import figure
from tornado import escape

from dask.utils import format_bytes

from distributed.dashboard.components import DashboardComponent, add_periodic_callback
from distributed.dashboard.components.scheduler import BOKEH_THEME, TICKS_1024
from distributed.dashboard.utils import update
from distributed.utils import log_errors


class RMMMemoryUsage(DashboardComponent):
    """
    GPU memory usage plot that includes information about memory
    managed by RMM. If an RMM pool is being used, shows the amount of
    pool memory utilized.
    """

    def __init__(self, scheduler, width=600, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource(
                {
                    "rmm-used": [1, 2],
                    "rmm-used-half": [0.5, 1],
                    "rmm-total": [2, 4],
                    "rmm-total-half": [1, 2],
                    "external-used": [2, 1],
                    "external-used-x": [3, 4.5],
                    "worker": ["a", "b"],
                    "gpu-index": [0, 0],
                    "y": [1, 2],
                    "escaped_worker": ["a", "b"],
                    "rmm_memory_text": [
                        "RMM memory used: 1B/1B\nTotal GPU memory used: 1B/2B",
                        "RMM memory used: 1B/1B\nTotal GPU memory used: 1B/2B",
                    ],
                }
            )

            memory = figure(
                title="RMM Memory",
                tools="",
                width=int(width / 2),
                name="rmm_memory_histogram",
                **kwargs,
            )

            rect = memory.rect(
                source=self.source,
                x="rmm-used-half",
                y="y",
                width="rmm-used",
                height=1,
                color="#76B900",
                alpha=1.0,
            )
            rect.nonselection_glyph = None

            rect = memory.rect(
                source=self.source,
                x="rmm-total-half",
                y="y",
                width="rmm-total",
                height=1,
                color="#76B900",
                alpha=0.75,
            )
            rect.nonselection_glyph = None

            rect = memory.rect(
                source=self.source,
                x="external-used-x",
                y="y",
                width="external-used",
                height=1,
                color="#76B900",
                alpha=0.5,
            )
            rect.nonselection_glyph = None

            memory.axis[0].ticker = BasicTicker(**TICKS_1024)
            memory.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            memory.xaxis.major_label_orientation = -math.pi / 12
            memory.x_range.start = 0

            for fig in [memory]:
                fig.xaxis.minor_tick_line_alpha = 0
                fig.yaxis.visible = False
                fig.ygrid.visible = False

                tap = TapTool(
                    callback=OpenURL(url="./info/worker/@escaped_worker.html")
                )
                fig.add_tools(tap)

                fig.toolbar_location = None
                fig.yaxis.visible = False

            hover = HoverTool()
            hover.tooltips = "@worker : @rmm_memory_text"
            hover.point_policy = "follow_mouse"
            memory.add_tools(hover)

            self.memory_figure = memory

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())
            rmm_total = []
            rmm_used = []
            external_used = []
            gpu_index = []
            y = []
            worker = []
            external_used_x = []
            memory_max = 0
            gpu_total = []
            rmm_memory_text = []

            for idx, ws in enumerate(workers):
                try:
                    rmm_metrics = ws.metrics["rmm"]
                    gpu_metrics = ws.metrics["gpu"]
                    gpu_info = ws.extra["gpu"]
                except KeyError:
                    continue
                rmm_total_worker = rmm_metrics["rmm-total"]  # RMM memory only
                rmm_used_worker = rmm_metrics["rmm-used"]
                gpu_total_worker = gpu_info["memory-total"]  # All GPU memory
                gpu_used_worker = gpu_metrics["memory-used"]

                external_used_worker = gpu_used_worker - rmm_total_worker

                rmm_total.append(rmm_total_worker)
                rmm_used.append(rmm_used_worker)
                gpu_total.append(gpu_total_worker)
                external_used.append(external_used_worker)
                external_used_x.append(rmm_total_worker + external_used_worker / 2)
                worker.append(ws.address)
                gpu_index.append(idx)
                y.append(idx)

                memory_max = max(memory_max, gpu_total_worker)

                rmm_memory_text.append(
                    "RMM memory used: {}/{}\nTotal GPU memory used: {}/{}".format(
                        format_bytes(rmm_used_worker),
                        format_bytes(rmm_total_worker),
                        format_bytes(gpu_used_worker),
                        format_bytes(gpu_total_worker),
                    )
                )

            self.memory_figure.title.text = dedent(
                """\
                RMM Utilization: {} / {}
                GPU Memory: {} / {}
                """.format(
                    format_bytes(sum(rmm_used)),
                    format_bytes(sum(rmm_total)),
                    format_bytes(sum([*rmm_total, *external_used])),
                    format_bytes(sum(gpu_total)),
                )
            )

            result = {
                "rmm-total": rmm_total,
                "rmm-used": rmm_used,
                "external-used": external_used,
                "rmm-total-half": [m // 2 for m in rmm_total],
                "rmm-used-half": [m // 2 for m in rmm_used],
                "external-used-x": external_used_x,
                "worker": worker,
                "gpu-index": gpu_index,
                "y": y,
                "escaped_worker": [escape.url_escape(w) for w in worker],
                "rmm_memory_text": rmm_memory_text,
            }

            self.memory_figure.x_range.end = memory_max

            update(self.source, result)


def rmm_memory_doc(scheduler, extra, doc):
    with log_errors():
        rmm_load = RMMMemoryUsage(scheduler, sizing_mode="stretch_both")
        rmm_load.update()
        add_periodic_callback(doc, rmm_load, 100)
        doc.add_root(rmm_load.memory_figure)
        doc.theme = BOKEH_THEME
