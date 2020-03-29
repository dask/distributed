import os

from tornado import web


dirname = os.path.dirname(__file__)


class RequestHandler(web.RequestHandler):
    def initialize(self, server=None, extra=None):
        self.server = server
        self.extra = extra or {}

    def get_template_path(self):
        return os.path.join(dirname, "templates")


def redirect(path):
    class Redirect(RequestHandler):
        def get(self):
            self.redirect(path)

    return Redirect
