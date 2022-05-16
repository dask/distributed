from __future__ import annotations

import json

from distributed.http.utils import RequestHandler


class APIHandler(RequestHandler):
    def get(self):
        self.write("API V1")
        self.set_header("Content-Type", "text/plain")


class RetireWorkersHandler(RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "text/json")
        scheduler = self.server
        try:
            params = json.loads(self.request.body)
            workers = params["workers"]
            workers_info = await scheduler.retire_workers(workers)
            self.write(json.dumps(workers_info))
        except Exception as e:
            self.set_status(500, str(e))
            self.write(json.dumps({"Error": "Internal Server Error"}))


class GetWorkersHandler(RequestHandler):
    def get(self):
        self.set_header("Content-Type", "text/json")
        scheduler = self.server
        try:
            response = {
                "num_workers": len(scheduler.workers),
                "workers": [
                    {"name": ws.name, "address": ws.address}
                    for ws in scheduler.workers.values()
                ],
            }
            self.write(json.dumps(response))
        except Exception as e:
            self.set_status(500, str(e))
            self.write(json.dumps({"Error": "Internal Server Error"}))


class AdaptiveTargetHandler(RequestHandler):
    def get(self):
        self.set_header("Content-Type", "text/json")
        scheduler = self.server
        try:
            desired_workers = scheduler.adaptive_target()
            response = {
                "workers": desired_workers,
            }
            self.write(json.dumps(response))
        except Exception as e:
            self.set_status(500, str(e))
            self.write(json.dumps({"Error": "Internal Server Error"}))


routes: list[tuple] = [
    ("/api/v1", APIHandler, {}),
    ("/api/v1/retire_workers", RetireWorkersHandler, {}),
    ("/api/v1/get_workers", GetWorkersHandler, {}),
    ("/api/v1/adaptive_target", AdaptiveTargetHandler, {}),
]
