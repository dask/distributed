import functools
import warnings
from distutils.version import LooseVersion

import bokeh
from bokeh.server.server import BokehTornado

try:
    from bokeh.server.util import create_hosts_allowlist
except ImportError:
    from bokeh.server.util import create_hosts_whitelist as create_hosts_allowlist

import toolz
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler

import dask

if LooseVersion(bokeh.__version__) < LooseVersion("0.13.0"):
    warnings.warn(
        "\nDask needs bokeh >= 0.13.0 for the dashboard."
        "\nContinuing without the dashboard."
    )
    raise ImportError("Dask needs bokeh >= 0.13.0")


def BokehApplication(applications, server, prefix="/", template_variables={}):
    prefix = prefix or ""
    prefix = "/" + prefix.strip("/")
    if not prefix.endswith("/"):
        prefix = prefix + "/"

    extra = toolz.merge({"prefix": prefix}, template_variables)

    apps = {k: functools.partial(v, server, extra) for k, v in applications.items()}
    apps = {k: Application(FunctionHandler(v)) for k, v in apps.items()}
    kwargs = dask.config.get("distributed.scheduler.dashboard.bokeh-application").copy()
    extra_websocket_origins = create_hosts_allowlist(
        kwargs.pop("allow_websocket_origin"), server.http_server.port
    )

    application = BokehTornado(
        apps,
        prefix=prefix,
        use_index=False,
        extra_websocket_origins=extra_websocket_origins,
        **kwargs,
    )
    return application
