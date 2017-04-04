from __future__ import print_function, division, absolute_import

from functools import partial
import weakref

import six

from toolz import merge

from tornado import gen

from concurrent.futures import Future, Executor

from .utils import sync


@gen.coroutine
def _cascade_future(future, cf_future):
    """
    Coroutine that waits on future, then transmits its outcome to cf_future.
    """
    result = yield future._result(raiseit=False)
    status = future.status
    if status == 'finished':
        cf_future.set_running_or_notify_cancel()
        cf_future.set_result(result)
    elif status == 'cancelled':
        cf_future.cancel()
        cf_future.set_running_or_notify_cancel()
    else:
        cf_future.set_running_or_notify_cancel()
        try:
            six.reraise(*result)
        except BaseException as exc:
            cf_future.set_exception(exc)


@gen.coroutine
def _wait_on_futures(futures):
    try:
        yield [futures]
    except Exception:
        # One future errored, ignore
        pass


class ClientExecutor(Executor):
    """
    A concurrent.futures Executor that executes tasks on a distributed Client.
    """

    _allowed_kwargs = frozenset(['pure', 'workers', 'resources', 'allow_other_workers'])

    def __init__(self, client, **kwargs):
        assert set(kwargs) <= self._allowed_kwargs, "unsupported kwargs to ClientExecutor"
        self._client = client
        self._futures = weakref.WeakSet()
        self._shutdown = False
        self._kwargs = kwargs

    def _wrap_future(self, future):
        """
        Wrap a distributed Future in a concurrent.futures Future.
        """
        cf_future = Future()
        # Support cancelling task through .cancel() on c.f.Future
        def cf_callback(cf_future):
            if cf_future.cancelled() and future.status != 'cancelled':
                future.cancel()

        cf_future.add_done_callback(cf_callback)

        self._client.loop.add_callback(_cascade_future, future, cf_future)
        return cf_future

    def submit(self, fn, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        future = self._client.submit(fn, *args, **merge(self._kwargs, kwargs))
        self._futures.add(future)
        return self._wrap_future(future)

    def map(self, fn, *iterables, timeout=None, chunksize=1):
        """Returns an iterator equivalent to map(fn, iter).

        Args:
            fn: A callable that will take as many arguments as there are
                passed iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: The size of the chunks the iterable will be broken into
                before being passed to a child process. This argument is only
                used by ProcessPoolExecutor; it is ignored by
                ThreadPoolExecutor.

        Returns:
            An iterator equivalent to: map(func, *iterables) but the calls may
            be evaluated out-of-order.

        Raises:
            TimeoutError: If the entire result iterator could not be generated
                before the given timeout.
            Exception: If fn(*args) raises for any values.
        """
        if timeout is not None:
            # TODO
            raise NotImplementedError("timeout argument not support on CompatibleExecutor")

        fs = self._client.map(fn, *iterables, **self._kwargs)

        # Yield must be hidden in closure so that the tasks are submitted
        # before the first iterator value is required.
        def result_iterator():
            try:
                for future in fs:
                    self._futures.add(future)
                    yield future.result()
            finally:
                remaining = list(fs)
                for future in remaining:
                    self._futures.add(future)
                self._client.cancel(remaining)

        return result_iterator()

    def shutdown(self, wait=True):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
        """
        if not self._shutdown:
            self._shutdown = True
            fs = list(self._futures)
            if wait:
                sync(self._client.loop, _wait_on_futures, fs)
            else:
                self._client.cancel(fs)
