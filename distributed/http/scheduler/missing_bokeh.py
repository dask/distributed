from __future__ import annotations

from distributed.http.utils import RequestHandler, redirect
from distributed.utils import log_errors


class MissingBokeh(RequestHandler):
    @log_errors
    def get(self):
        self.write(
            "<p>Dask needs bokeh >= 2.4.2 for the dashboard.</p>"
            '<p>Install with conda: conda install "bokeh>=2.4.2"</p>'
            '<p>Install with pip: pip install "bokeh>=2.4.2"</p>'
        )


routes: list[tuple] = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
