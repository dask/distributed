import os

from tornado import web
from tornado import escape

from ..utils import log_errors

dirname = os.path.join(os.path.dirname(__file__), 'templates')


class Workers(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self):
        with log_errors():
            self.render(os.path.join(dirname, 'workers.html'),
                        title='Workers',
                        **self.server.__dict__)


class Worker(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self, worker):
        worker = escape.url_unescape(worker)
        with log_errors():
            self.render(os.path.join(dirname, 'worker.html'),
                        title='Worker: ' + worker, worker=worker,
                        **self.server.__dict__)


class Task(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self, task):
        task = escape.url_unescape(task)
        with log_errors():
            self.render(os.path.join(dirname, 'task.html'),
                        title='Task: ' + task,
                        Task=task,
                        server=self.server,
                        **self.server.__dict__)


def get_handlers(server):
    return [
            (r'/scheduler/workers.html', Workers, {'server': server}),
            (r'/scheduler/worker/(.*).html', Worker, {'server': server}),
            (r'/scheduler/task/(.*).html', Task, {'server': server}),
    ]
