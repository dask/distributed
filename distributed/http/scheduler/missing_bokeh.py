from __future__ import annotations

from distributed.dashboard.core import _min_bokeh_version
from distributed.http.utils import RequestHandler, redirect
from distributed.utils import log_errors


class MissingBokeh(RequestHandler):
    @log_errors
    def get(self):
        self.write(
            f"<p>Dask needs bokeh >= {_min_bokeh_version}, < 3 for the dashboard.</p>"
            f"<p>Install with conda: conda install bokeh>={_min_bokeh_version},<3</p>"
            f"<p>Install with pip: pip install bokeh>={_min_bokeh_version},<3</p>"
        )


routes: list[tuple] = [(r"/", redirect("status"), {}), (r"status", MissingBokeh, {})]
