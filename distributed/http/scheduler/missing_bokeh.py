from __future__ import annotations

from ...utils import log_errors
from ..utils import RequestHandler, redirect


class MissingBokeh(RequestHandler):
    def get(self):
        with log_errors():
            self.write(
                "<p>Dask needs bokeh >= 2.1.1 for the dashboard.</p>"
                "<p>Install with conda: conda install bokeh>=1.0</p>"
                "<p>Install with pip: pip install bokeh>=2.1.1</p>"
            )


routes: list[tuple] = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
