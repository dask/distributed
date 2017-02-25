from __future__ import print_function, division, absolute_import

import logging
from tornado import gen
from .client import futures_of
from .utils import sync
from .utils_comm import pack_data
from .worker import _deserialize

logger = logging.getLogger(__name__)


class ReplayExceptionScheduler(object):
    """ A plugin for the scheduler to recreate exceptions locally

    This adds the following routes to the scheduler

    *  cause_of_failure
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers['cause_of_failure'] = self.cause_of_failure
        self.scheduler.extensions['exceptions'] = self

    def cause_of_failure(self, *args, **kwargs):
        """
        Return details of first failed task required by set of keys

        Parameters
        ----------
        keys: list of keys known to the scheduler

        Returns
        -------
        Dictionary with:
        cause: the key that failed
        task: the definition of that key
        deps: keys that the task depends on
        """

        keys = kwargs.pop('keys', [])
        for key in keys:
            if isinstance(key, list):
                key = tuple(key)
                key = tuple(key)
            if key in self.scheduler.exceptions_blame:
                cause = self.scheduler.exceptions_blame[key]
                # cannot serialize sets
                return {'deps': list(self.scheduler.dependencies[cause]),
                        'cause': cause,
                        'task': self.scheduler.tasks[cause]}


class ReplayExceptionClient(object):
    """
    A plugin for the client allowing replay of remote exceptions locally

    Adds the following methods to the given client:
    * [_]recreate_error_locally: main user method
    * [_]get_futures_error: gets the task, its details and dependencies,
        responsible for failure of the given future.
    """

    def __init__(self, client):
        self.client = client
        self.client.extensions['exceptions'] = self
        # monkey patch
        self.client.recreate_error_locally = self.recreate_error_locally
        self.client._recreate_error_locally = self._recreate_error_locally
        self.client._get_futures_error = self._get_futures_error
        self.client.get_futures_error = self.get_futures_error

    @property
    def scheduler(self):
        return self.client.scheduler

    @gen.coroutine
    def _get_futures_error(self, future):
        # only get errors for futures that errored.
        futures = [f for f in futures_of(future) if f.status == 'error']
        out = yield self.scheduler.cause_of_failure(
            keys=[f.key for f in futures])
        deps, cause, task = out['deps'], out['cause'], out['task']
        if isinstance(task, dict):
            function, args, kwargs = _deserialize(**task)
            raise gen.Return((function, args, kwargs, deps))
        else:
            raise gen.Return(task)

    def get_futures_error(self, future):
        """
        Ask the scheduler details of the sub-task of the given failed future

        Parameters
        ----------
        future: future that failed

        Returns
        -------
        The function that failed, its arguments, and the keys that it depends
        on.
        """
        return sync(self.client.loop, self._get_futures_error, future)

    @gen.coroutine
    def _recreate_error_locally(self, future):
        out = yield self._get_futures_error(future)
        if len(out) == 4:
            function, args, kwargs, deps = out
            futures = self.client._graph_to_futures({}, deps)
            data = yield self.client._gather(futures)
            args = pack_data(args, data)
            kwargs = pack_data(kwargs, data)
            raise gen.Return((function, args, kwargs))
        else:
            raise gen.Return(out)

    def recreate_error_locally(self, future):
        """
        For a failed calculation, perform the blamed task locally for debugging.

        Parameters
        ----------
        future: future that failed
            The same thing as was given to ``gather``, but came back with
            an exception/stack-trace. Can also be a (persisted) dask collection
            containing any errored futures.

        Returns
        -------
        Nothing; the function runs and should raise an exception, allowing
        the debugger to run.
        """
        out = sync(self.client.loop, self._recreate_error_locally, future)
        if len(out) == 3 and isinstance(out[2], dict):
            func, args, kwargsfunc(*args, **kwargs)
        else:
            exec_tuple(out)


def exec_tuple(t):
    newt = (exec_tuple(v) if isinstance(v, tuple) else v for v in t[1:])
    return t[0](*newt)
