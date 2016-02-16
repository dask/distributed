from __future__ import print_function, division, absolute_import

import logging
import os

from tornado import web

from .core import RequestHandler, MyApp, Resources


logger = logging.getLogger(__name__)


class Info(RequestHandler):
    """Basic info about the worker """
    def get(self):
        resp = {'ncores': self.server.ncores,
                'nkeys': len(self.server.data),
                'status': self.server.status}
        self.write(resp)


class Data(RequestHandler):
    """List of keys on this worker (scheduler also knows this)"""
    def get(self):
        self.write({'keys': list(self.server.data.keys())})


class Value(RequestHandler):
    """The value associated with a specific key.
    Gives None (NULL) if not found."""
    def get(self, key):
        self.write({key: self.server.data.get(key, None)})


class Active(RequestHandler):
    """Keys being computed"""
    def get(self):
        self.write({'active': list(self.server.active)})


class LocalFiles(RequestHandler):
    """List the local spill directory"""
    def get(self):
        self.write({'files': os.listdir(self.server.local_dir)})


def HTTPWorker(worker):
    application = MyApp(web.Application([
        (r'/info.json', Info, {'server': worker}),
        (r'/resources.json', Resources, {'server': worker}),
        (r'/data.json', Data, {'server': worker}),
        (r'/value/(.*?).json', Value, {'server': worker}),
        (r'/active.json', Active, {'server': worker}),
        (r'/files.json', LocalFiles, {'server': worker})
        ]))
    return application
