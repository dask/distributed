import hashlib
import json

import tornado.websocket

from .plugin import SchedulerPlugin


class WebsocketPlugin(SchedulerPlugin):
    def __init__(self, socket: tornado.websocket.WebSocketHandler, scheduler):
        self.socket = socket
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

    def _send(self, name, data):
        data["name"] = name
        for k in list(data):
            # Drop bytes objects for now
            if isinstance(data[k], bytes):
                del data[k]
        self.socket.write_message(json.dumps(data))

    @staticmethod
    def _hash_worker(worker):
        return "worker-" + hashlib.md5(worker.encode()).hexdigest()[:8]

    def restart(self, scheduler, **kwargs):
        """ Run when the scheduler restarts itself """
        self._send("reset", {})

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a new worker enters the cluster """
        self._send("worker_join", {"id": self._hash_worker(worker), "worker": worker})

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a worker leaves the cluster"""
        self._send("remove_worker", {"id": self._hash_worker(worker)})

    def transition(self, key, start, finish, *args, **kwargs):
        """ Run whenever a task changes state

        Parameters
        ----------
        key: string
        start: string
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish: string
            Final state of the transition.
        *args, **kwargs: More options passed when transitioning
            This may include worker ID, compute time, etc.
        """
        if start == "released" and finish == "waiting":
            # Task is queued
            pass
        elif start == "waiting" and finish == "processing":
            # Task has begun on a worker
            worker = self.scheduler.tasks[key].processing_on.name
            self._send(
                "start_task", {"id": self._hash_worker(worker), "task_name": key}
            )
        elif start == "processing" and finish == "memory":
            # Task result is in memory on a worker
            start_worker = self._hash_worker(
                list(self.scheduler.tasks[key].who_has)[0].name
            )
            end_worker = self._hash_worker(kwargs["worker"])
            if start_worker != end_worker:
                self._send(
                    "start_transfer",
                    {
                        "start_worker": start_worker,
                        "end_worker": end_worker,
                        "key": key,
                    },
                )
        elif start == "memory" and finish in ["released", "forgotten"]:
            # Task has been forgotten
            pass
        elif start == "released" and finish == "forgotten":
            # Worker has garbage collected task
            pass
        else:
            data = {
                "key": key,
                "start": start,
                "finish": finish,
                "args": args,
                **kwargs,
            }
            print(data)
            self._send("transition", data)
