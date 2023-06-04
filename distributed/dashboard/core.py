from __future__ import annotations

import functools
import warnings

from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.server.server import BokehTornado
from bokeh.server.util import create_hosts_allowlist

import dask

from distributed.dashboard.utils import BOKEH_VERSION
from distributed.versions import BOKEH_REQUIREMENT

# Set `prereleases=True` to allow for use with dev versions of `bokeh`
if not BOKEH_REQUIREMENT.specifier.contains(BOKEH_VERSION, prereleases=True):
    warnings.warn(
        f"\nDask needs {BOKEH_REQUIREMENT} for the dashboard."
        f"\nYou have bokeh={BOKEH_VERSION}."
        "\nContinuing without the dashboard."
    )
    raise ImportError(
        f"Dask needs {BOKEH_REQUIREMENT} for the dashboard, not bokeh={BOKEH_VERSION}"
    )


if BOKEH_VERSION.major < 3:
    from bokeh.models import Panel as TabPanel  # noqa: F401
else:
    from bokeh.models import TabPanel  # noqa: F401


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
