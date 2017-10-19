import os

from tornado import web
from tornado import escape

from ..utils import log_errors, format_bytes, format_time

dirname = os.path.dirname(__file__)

ns = {func.__name__: func for func in [format_bytes, format_time]}


class Workers(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self):
        with log_errors():
            self.render(os.path.join(dirname, 'templates', 'workers.html'),
                        title='Workers',
                        **self.server.__dict__, **ns)


class Worker(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self, worker):
        worker = escape.url_unescape(worker)
        with log_errors():
            self.render(os.path.join(dirname, 'templates', 'worker.html'),
                        title='Worker: ' + worker, worker=worker,
                        **self.server.__dict__, **ns)


class Task(web.RequestHandler):
    def initialize(self, server=None):
        self.server = server

    def get(self, task):
        task = escape.url_unescape(task)
        with log_errors():
            self.render(os.path.join(dirname, 'templates', 'task.html'),
                        title='Task: ' + task,
                        Task=task,
                        server=self.server,
                        **self.server.__dict__, **ns)


def get_handlers(server):
    return [
            (r'/scheduler/workers.html', Workers, {'server': server}),
            (r'/scheduler/worker/(.*).html', Worker, {'server': server}),
            (r'/scheduler/task/(.*).html', Task, {'server': server}),
            (r'/static/(.*)', web.StaticFileHandler, {"path": os.path.join(dirname, 'static')}),
    ]
