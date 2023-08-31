from __future__ import annotations

import math

from bokeh.core.properties import without_property_validation
from bokeh.models import BasicTicker, ColumnDataSource, NumeralTickFormatter
from bokeh.plotting import figure

from distributed.dashboard.components import DashboardComponent, add_periodic_callback
from distributed.dashboard.components.scheduler import BOKEH_THEME, TICKS_1024
from distributed.dashboard.utils import update
from distributed.utils import log_errors


class CudfSpillingStatistics(DashboardComponent):
    """
    Plot giving an overview of per-worker GPU spilling statistics, including the number
    of bytes spilled to/from CPU and the time spent spilling.
    """

    def __init__(self, scheduler, width=600, **kwargs):
        with log_errors():
            self.last = 0
            self.scheduler = scheduler
            self.source = ColumnDataSource(
                {
                    "from-gpu": [1, 2],
                    "from-gpu-half": [0.5, 1],
                    "worker": ["a", "b"],
                    "gpu-index": [0, 0],
                    "y": [1, 2],
                }
            )

            bytes_spilled = figure(
                title="Bytes spilled from GPU",
                tools="",
                width=int(width / 2),
                name="bytes_spilled_histogram",
                **kwargs,
            )

            rect = bytes_spilled.rect(
                source=self.source,
                x="from-gpu-half",
                y="y",
                width="from-gpu",
                height=1,
                color="#76B900",
                alpha=1.0,
            )
            rect.nonselection_glyph = None

            bytes_spilled.axis[0].ticker = BasicTicker(**TICKS_1024)
            bytes_spilled.xaxis[0].formatter = NumeralTickFormatter(format="0.0 b")
            bytes_spilled.xaxis.major_label_orientation = -math.pi / 12
            bytes_spilled.x_range.start = 0

            for fig in [bytes_spilled]:
                fig.xaxis.minor_tick_line_alpha = 0
                fig.yaxis.visible = False
                fig.ygrid.visible = False

                fig.toolbar_location = None
                fig.yaxis.visible = False

            self.bytes_spilled_figure = bytes_spilled

    @without_property_validation
    def update(self):
        with log_errors():
            workers = list(self.scheduler.workers.values())
            from_gpu = []
            gpu_index = []
            y = []
            worker = []
            memory_max = 0

            for idx, ws in enumerate(workers):
                try:
                    cudf_metrics = ws.metrics["cudf"]
                    gpu_info = ws.extra["gpu"]
                except KeyError:
                    continue

                from_gpu.append(cudf_metrics["gpu-to-cpu"]["nbytes"])
                worker.append(ws.address)
                gpu_index.append(idx)
                y.append(idx)

                memory_max = max(memory_max, gpu_info["memory-total"])

            result = {
                "from-gpu": from_gpu,
                "from-gpu-half": [m // 2 for m in from_gpu],
                "worker": worker,
                "gpu-index": gpu_index,
                "y": y,
            }

            self.bytes_spilled_figure.x_range.end = memory_max

            update(self.source, result)


def cudf_spilling_doc(scheduler, extra, doc):
    with log_errors():
        cudf_spilling = CudfSpillingStatistics(scheduler, sizing_mode="stretch_both")
        cudf_spilling.update()
        add_periodic_callback(doc, cudf_spilling, 100)
        doc.add_root(cudf_spilling.bytes_spilled_figure)
        doc.theme = BOKEH_THEME
