import functools
import warnings

import bokeh
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.server.server import BokehTornado
from bokeh.server.util import create_hosts_allowlist
from packaging.version import parse as parse_version

import dask

if parse_version(bokeh.__version__) < parse_version("2.1.1"):
    warnings.warn(
        "\nDask needs bokeh >= 2.1.1 for the dashboard."
        "\nContinuing without the dashboard."
    )
    raise ImportError("Dask needs bokeh >= 2.1.1")


def BokehApplication(applications, server, prefix="/", template_variables={}):
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
