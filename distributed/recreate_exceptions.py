from __future__ import print_function, division, absolute_import

import logging
from tornado import gen
from .client import futures_of
from.worker import _deserialize

logger = logging.getLogger(__name__)


class ExceptionScheduler(object):
    """ A plugin for the scheduler to recreate exceptions locally

    This adds the following routes to the scheduler

    *  channel-subscribe
    *  channel-unsubsribe
    *  channel-append
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler

        handlers = {'cause_of_failure': self.cause_of_failure}

        self.scheduler.handlers.update(handlers)
        self.scheduler.extensions['exceptions'] = self

    def cause_of_failure(self, *args, keys=None, **kwargs):
        for key in keys:
            if key in self.scheduler.exceptions_blame:
                cause = self.scheduler.exceptions_blame[key]
                # cannot serialize sets
                return {'deps': list(self.scheduler.dependencies[cause]),
                        'cause': cause,
                        'task': self.scheduler.tasks[cause]}


class ExceptionsClient(object):
    def __init__(self, client):
        self.client = client
        self.client.extensions['exceptions'] = self
        # monkey patch
        self.client.recreate_error_locally = self.recreate_error_locally
        self.client._get_futures_error = self._get_futures_error
        self.client.get_futures_error = self.get_futures_error

    @property
    def scheduler(self):
        return self.client.scheduler

    @gen.coroutine
    def _get_futures_error(self, future):
        futures = futures_of(future)
        out = yield self.scheduler.cause_of_failure(
            keys=[f.key for f in futures])
        deps, cause, task = out['deps'], out['cause'], out['task']
        function, args, kwargs = _deserialize(**task)
        raise gen.Return((function, args, kwargs, deps))

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
        return sync(self.loop, self._get_futures_error, future)

    def recreate_error_locally(self, future):
        """
        For a failed calculation, perform the blamed task locally for debugging.

        Parameters
        ----------
        future: future that failed
            The same thing as was given to ``gather``, but came back with
            an exception/stack-trace.

        Returns
        -------
        Nothing; the function runs and should raise an exception, allowing
        the debugger to run.
        """
        function, args, kwargs, deps = get_futures_error(future)
        futures = self._graph_to_futures({}, deps)
        data = self.gather(futures)
        data = dict(zip(deps, data))
        args = pack_data(args, data)
        kwargs = pack_data(kwargs, data)

        # run the function, hopefully trigger exception.
        func(*args, **kwargs)
