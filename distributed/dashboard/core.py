from __future__ import annotations

import functools
import warnings

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.server.server import BokehTornado
from bokeh.server.util import create_hosts_allowlist
from packaging.version import parse as parse_version

import dask

from distributed.dashboard.utils import BOKEH_VERSION

_min_bokeh_version = "2.1.1"

if BOKEH_VERSION < parse_version(_min_bokeh_version):
    warnings.warn(
        f"\nDask needs bokeh >= {_min_bokeh_version}, < 3 for the dashboard."
        "\nContinuing without the dashboard."
    )
    raise ImportError(f"Dask needs bokeh >= {_min_bokeh_version}, < 3")


def BokehApplication(applications, server, prefix="/", template_variables=None):
    template_variables = template_variables or {}
    prefix = "/" + prefix.strip("/") + "/" if prefix else "/"

    extra = {"prefix": prefix, **template_variables}

    funcs = {k: functools.partial(v, server, extra) for k, v in applications.items()}
    apps = {k: Application(FunctionHandler(v)) for k, v in funcs.items()}

    kwargs = dask.config.get("distributed.scheduler.dashboard.bokeh-application").copy()
    extra_websocket_origins = create_hosts_allowlist(
        kwargs.pop("allow_websocket_origin"), server.http_server.port
    )

    return BokehTornado(
        apps,
        prefix=prefix,
        use_index=False,
        extra_websocket_origins=extra_websocket_origins,
        **kwargs,
    )
