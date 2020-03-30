from distutils.version import LooseVersion
import functools
import warnings

import bokeh
from bokeh.server.server import BokehTornado
from bokeh.application.handlers.function import FunctionHandler
from bokeh.application import Application
import toolz


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

    apps = {
        prefix + k.lstrip("/"): functools.partial(v, server, extra)
        for k, v in applications.items()
    }
    apps = {k: Application(FunctionHandler(v)) for k, v in apps.items()}

    application = BokehTornado(
        apps,
        extra_websocket_origins=["*"],
        keep_alive_milliseconds=500,
        check_unused_sessions_milliseconds=500,
        use_index=False,
    )
    return application
