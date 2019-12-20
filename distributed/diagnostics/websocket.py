import json

import tornado.websocket

from .plugin import SchedulerPlugin
from ..utils import key_split
from .task_stream import colors


class WebsocketPlugin(SchedulerPlugin):
    def __init__(self, socket: tornado.websocket.WebSocketHandler, scheduler):
        self.socket = socket
        self.scheduler = scheduler

    def _send(self, name, data):
        data["name"] = name
        for k in list(data):
            # Drop bytes objects for now
            if isinstance(data[k], bytes):
                del data[k]
        self.socket.write_message(json.dumps(data))

    def restart(self, scheduler, **kwargs):
        """ Run when the scheduler restarts itself """
        self._send("restart", {})

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a new worker enters the cluster """
        self._send("add_worker", {"worker": worker})

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a worker leaves the cluster"""
        self._send("remove_worker", {"worker": worker})

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
        if start == "processing":
            if key not in self.scheduler.tasks:
                return
            kwargs["key"] = key
            if finish == "memory" or finish == "erred":
                startstops = kwargs.get("startstops", [])
                for startstop in startstops:
                    color = colors[startstop["action"]]
                    if type(color) is not str:
                        color = color(kwargs)
                    data = {
                        "key": key,
                        "name": key_split(key),
                        "color": color,
                        **kwargs,
                        **startstop,
                    }
                    self._send("transition", data)
