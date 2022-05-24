from __future__ import annotations

import asyncio
import functools
import json

import dask.config

from distributed.http.utils import RequestHandler


def require_auth(method_func):
    @functools.wraps(method_func)
    def wrapper(self):
        auth = self.request.headers.get("Authorization", None)
        key = dask.config.get("distributed.scheduler.http.api-key")
        if key and (
            not auth
            or not auth.startswith("Bearer ")
            or not key
            or key != auth.split(" ")[-1]
        ):
            self.set_status(403, "Unauthorized")
            return

        if not asyncio.iscoroutinefunction(method_func):
            return method_func(self)
        else:

            async def tmp():
                return await method_func(self)

            return tmp()

    return wrapper


class APIHandler(RequestHandler):
    def get(self):
        self.write("API V1")
        self.set_header("Content-Type", "text/plain")


class RetireWorkersHandler(RequestHandler):
    @require_auth
    async def post(self):
        self.set_header("Content-Type", "application/json")
        scheduler = self.server
        try:
            params = json.loads(self.request.body)
            n_workers = params.get("n", 0)
            if n_workers:
                workers = scheduler.workers_to_close(n=n_workers)
                workers_info = await scheduler.retire_workers(workers=workers)
            else:
                workers = params.get("workers", {})
                workers_info = await scheduler.retire_workers(workers=workers)
            self.write(json.dumps(workers_info))
        except Exception as e:
            self.set_status(500, str(e))
            self.write(json.dumps({"Error": "Internal Server Error"}))


class GetWorkersHandler(RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json")
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
        self.set_header("Content-Type", "application/json")
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
