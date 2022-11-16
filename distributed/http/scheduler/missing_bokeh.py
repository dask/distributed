from __future__ import annotations

from distributed.http.utils import RequestHandler, redirect
from distributed.utils import log_errors
from distributed.versions import MIN_BOKEH_VERSION


class MissingBokeh(RequestHandler):
    @log_errors
    def get(self):
        self.write(
            f"<p>Dask needs bokeh >= {MIN_BOKEH_VERSION} for the dashboard.</p>"
            f"<p>Install with conda: conda install bokeh>={MIN_BOKEH_VERSION}</p>"
            f"<p>Install with pip: pip install bokeh>={MIN_BOKEH_VERSION}</p>"
        )


routes: list[tuple] = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
