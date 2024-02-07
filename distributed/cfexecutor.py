from __future__ import annotations

import concurrent.futures as cf
import weakref

from tlz import merge
from tornado import gen

from dask.utils import parse_timedelta

from distributed.metrics import time
from distributed.utils import TimeoutError, sync


@gen.coroutine
def _cascade_task(task, cf_future):
    """
    Coroutine that waits on Dask task, then transmits its outcome to
    cf_future.
    """
    result = yield task._result(raiseit=False)
    status = task.status
    if status == "finished":
        cf_future.set_result(result)
    elif status == "cancelled":
        cf_future.cancel()
        # Necessary for wait() and as_completed() to wake up
        cf_future.set_running_or_notify_cancel()
    else:
        try:
            typ, exc, tb = result
            raise exc.with_traceback(tb)
        except BaseException as exc:
            cf_future.set_exception(exc)


@gen.coroutine
def _wait_on_tasks(tasks):
    for fut in tasks:
        try:
            yield fut
        except Exception:
            pass


class ClientExecutor(cf.Executor):
    """
    A concurrent.futures Executor that executes tasks on a dask.distributed Client.
    """

    _allowed_kwargs = frozenset(
        ["pure", "workers", "resources", "allow_other_workers", "retries"]
    )

    def __init__(self, client, **kwargs):
        sk = set(kwargs)
        if not sk <= self._allowed_kwargs:
            raise TypeError(
                "unsupported arguments to ClientExecutor: %s"
                % sorted(sk - self._allowed_kwargs)
            )
        self._client = client
        self._tasks = weakref.WeakSet()
        self._shutdown = False
        self._kwargs = kwargs

    def _wrap_task(self, task):
        """
        Wrap a distributed Task in a concurrent.futures Future.
        """
        cf_future = cf.Future()

        # Support cancelling task through .cancel() on c.f.Future
        def cf_callback(cf_future):
            if cf_future.cancelled() and task.status != "cancelled":
                task.cancel()

        cf_future.add_done_callback(cf_callback)

        self._client.loop.add_callback(_cascade_task, task, cf_future)
        return cf_future

    def submit(self, fn, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as ``fn(*args, **kwargs)``
        and returns a Task instance representing the execution of the callable.

        Returns
        -------
        A Task representing the given call.
        """
        if self._shutdown:
            raise RuntimeError("cannot schedule new tasks after shutdown")
        task = self._client.submit(fn, *args, **merge(self._kwargs, kwargs))
        self._tasks.add(task)
        return self._wrap_task(task)

    def map(self, fn, *iterables, **kwargs):
        """Returns an iterator equivalent to ``map(fn, *iterables)``.

        Parameters
        ----------
        fn : A callable that will take as many arguments as there are
            passed iterables.
        iterables : One iterable for each parameter to *fn*.
        timeout : The maximum number of seconds to wait. If None, then there
            is no limit on the wait time.
        chunksize : ignored.

        Returns
        -------
        An iterator equivalent to: ``map(fn, *iterables)`` but the calls may
        be evaluated out-of-order.

        Raises
        ------
        concurrent.futures.TimeoutError:
            If the entire result iterator could not be generated before the given
            timeout.
        Exception:
            If ``fn(*args)`` raises for any values.
        """
        timeout = kwargs.pop("timeout", None)
        if timeout is not None:
            timeout = parse_timedelta(timeout)
            end_time = timeout + time()
        if "chunksize" in kwargs:
            del kwargs["chunksize"]
        if kwargs:
            raise TypeError("unexpected arguments to map(): %s" % sorted(kwargs))

        fs = self._client.map(fn, *iterables, **self._kwargs)

        # Below iterator relies on fs being an iterator itself, and not just an iterable
        # (such as a list), in order to cancel remaining tasks
        fs = iter(fs)

        # Yield must be hidden in closure so that the tasks are submitted
        # before the first iterator value is required.
        def result_iterator():
            try:
                for task in fs:
                    self._tasks.add(task)
                    if timeout is not None:
                        try:
                            yield task.result(end_time - time())
                        except TimeoutError:
                            raise cf.TimeoutError
                    else:
                        yield task.result()
            finally:
                remaining = list(fs)
                self._tasks.update(remaining)
                self._client.cancel(remaining)

        return result_iterator()

    def shutdown(self, wait=True):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Parameters
        ----------
        wait : If True then shutdown will not return until all running
            tasks have finished executing.  If False then all running
            tasks are cancelled immediately.
        """
        if not self._shutdown:
            self._shutdown = True
            fs = list(self._tasks)
            if wait:
                sync(self._client.loop, _wait_on_tasks, fs)
            else:
                self._client.cancel(fs)
