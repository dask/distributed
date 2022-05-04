from __future__ import annotations

import json
from tornado import web


class APIHandler(web.RequestHandler):
    def get(self):
        self.write("API V1")
        self.set_header("Content-Type", "text/plain")


class RetireWorkersHandler(web.RequestHandler):
    def initialize(self, dask_server):
        self.dask_server = dask_server

    async def post(self):
        self.set_header("Content-Type", "text/json")
        try:
            params = json.loads(self.request.body)
            workers_info = await self.dask_server.retire_workers(**params)
            self.write(json.dumps(workers_info))
        except Exception as e:
            self.set_status(400, str(e))
            self.write(json.dumps({"Error": "Bad request"}))


routes: list[tuple] = [
    ("/api/v1", APIHandler, {}),
    ("/api/v1/retire_workers", RetireWorkersHandler, {}),
]