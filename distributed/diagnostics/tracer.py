from __future__ import print_function, division, absolute_import

import logging
import json
import time
from tornado import gen

from .plugin import SchedulerPlugin
from ..utils import key_split


logger = logging.getLogger(__name__)


class TraceLoggerPlugin(SchedulerPlugin):
    def __init__(self, scheduler):
        self.log = None
        logger.debug('Set up Scheduler trace logger')
        self.scheduler = scheduler
        self.scheduler.handlers['start_trace'] = self.start_trace
        self.scheduler.handlers['stop_trace'] = self.stop_trace

    def start_trace(self, *args, **kwargs):
        log_file = kwargs.pop('log_file', 'trace.log')
        logger.debug('Start trace to file: ' + log_file)
        self.log_file = open(log_file, 'a')
        self.write({'log': 'start'})
        self.scheduler.add_plugin(self)

    def stop_trace(self, *args, **kwargs):
        logger.debug('Stop trace')
        self.write({'log': 'stop'})
        self.scheduler.extensions.pop('trace', None)
        self.log_file.close()
        self.log = None

    def transition(self, key, start, finish, *args, **kwargs):
        if self.log:
            kwargs.pop('type', None)
            self.write({'key': key, 'start': start, 'finish': finish,
                           'args': args, 'kwargs': kwargs, 'log': 'transition'})

    def update_graph(self, scheduler, dsk=None, keys=None,
                     restrictions=None, **kwargs):
        if self.log and dsk or keys:
            kwargs2 = kwargs.copy()
            kwargs2.pop('tasks', None)
            keys = list(keys or [])
            self.write({'log': 'update_graph', 'dsk': dsk, 'keys': keys,
                       'restrictions': restrictions, 'kwargs': kwargs2})

    def restart(self, scheduler, **kwargs):
        if self.log:
            self.write({'log': 'restart'})

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        if self.log:
            self.write({'log': 'add_worker', 'worker': str(worker)})

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        if self.log:
            self.write({'log': 'remove_worker', 'worker': str(worker)})

    def write(self, data):
        data['time'] = time.time()
        json.dump(data, self.log_file, ensure_ascii=False)
        self.log_file.write('\n')


class TraceLoggerClient(object):
    """
    A plugin for the client allowing replay of remote exceptions locally

    Adds the following methods (and their async variants)to the given client:

    - ``recreate_error_locally``: main user method
    - ``get_futures_error``: gets the task, its details and dependencies,
        responsible for failure of the given future.
    """

    def __init__(self, client):
        self.client = client
        self.client.extensions['trace'] = self
        self.client.start_trace = self.start_trace
        self.client.stop_trace = self.stop_trace

    @property
    def scheduler(self):
        return self.client.scheduler

    @gen.coroutine
    def start_trace(self, log_file):
        yield self.scheduler.start_trace(log_file=log_file)

    @gen.coroutine
    def stop_trace(self):
        yield self.scheduler.stop_trace()
