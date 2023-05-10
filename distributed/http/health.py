from __future__ import annotations

from tornado import web


class HealthHandler(web.RequestHandler):
    def get(self):
        self.write("ok")
        self.set_header("Content-Type", "text/plain")


routes: list[tuple] = [("/health", HealthHandler, {})]
