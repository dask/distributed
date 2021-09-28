from tornado.ioloop import IOLoop

from .components.worker import (
    counters_doc,
    crossfilter_doc,
    profile_doc,
    profile_server_doc,
    status_doc,
    systemmonitor_doc,
)
from .core import BokehApplication

template_variables = {
    "pages": ["status", "system", "profile", "crossfilter", "profile-server"]
}


def connect(application, http_server, worker, prefix=""):
    bokeh_app = BokehApplication(
        applications, worker, prefix=prefix, template_variables=template_variables
    )
    application.add_application(bokeh_app)
    bokeh_app.initialize(IOLoop.current())


applications = {
    "/status": status_doc,
    "/counters": counters_doc,
    "/crossfilter": crossfilter_doc,
    "/system": systemmonitor_doc,
    "/profile": profile_doc,
    "/profile-server": profile_server_doc,
}
