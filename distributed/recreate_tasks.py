import logging

from dask.utils import stringify

from .client import futures_of, wait, Future
from .utils import sync
from .utils_comm import pack_data
from .worker import _deserialize

logger = logging.getLogger(__name__)


class ReplayTaskScheduler:
    """A plugin for the scheduler to recreate tasks locally

    This adds the following routes to the scheduler

    *  cause_of_failure
    *  get_runspec
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers["get_runspec"] = self.get_runspec
        self.scheduler.handlers["get_error_cause"] = self.get_error_cause
        self.scheduler.handlers["_process_key"] = self._process_key
        self.scheduler.extensions["replay-tasks"] = self

    def _process_key(self, key):
        if isinstance(key, list):
            key = tuple(key)  # ensure not a list from msgpack
        key = stringify(key)
        return key

    def get_error_cause(self, *args, keys=(), **kwargs):
        for key in keys:
            key = self._process_key(key)
            ts = self.scheduler.tasks.get(key)
            if ts is not None and ts.exception_blame is not None:
                return ts.exception_blame.key

    def get_runspec(self, *args, key=None, **kwargs):
        key = self._process_key(key)
        ts = self.scheduler.tasks.get(key)
        return {
            "task": ts.run_spec,
            "deps": [dts.key for dts in ts.dependencies]
        }


class ReplayTaskClient:
    """
    A plugin for the client allowing replay of remote tasks locally

    Adds the following methods (and their async variants)to the given client:

    - ``recreate_error_locally``: main user method for replaying failed tasks
    - ``recreate_task_locally``: main user method for replaying any task
    - ``get_futures_error``: gets the task, its details and dependencies,
        responsible for failure of the given future.
    - ``get_futures_components``: gets the task, its details and dependencies.
    """

    def __init__(self, client):
        self.client = client
        self.client.extensions["replay-tasks"] = self
        # monkey patch
        self.client._get_raw_components_from_future = self._get_raw_components_from_future
        self.client._prepare_raw_components = self._prepare_raw_components
        self.client._get_components_from_future = self._get_components_from_future
        self.client._get_errored_future = self._get_errored_future
        self.client.recreate_task_locally = self.recreate_task_locally
        self.client.recreate_error_locally = self.recreate_error_locally
        self.client._execute_task_components = self._execute_task_components

    @property
    def scheduler(self):
        return self.client.scheduler

    async def _get_raw_components_from_future(self, future):
        await wait(future)
        # one reason not to pass spec into this function is in case we want to
        # expose a method to get all the components/deps in the future. This
        # way it will be easier to do.
        spec = await self.scheduler.get_runspec(key=future.key)
        deps, task = spec["deps"], spec["task"]
        if isinstance(task, dict):
            function, args, kwargs = _deserialize(**task)
            return (function, args, kwargs, deps)
        else:
            function, args, kwargs = _deserialize(task=task)
            return (function, args, kwargs, deps)

    async def _prepare_raw_components(self, raw_components):
        function, args, kwargs, deps = raw_components
        futures = self.client._graph_to_futures({}, deps)
        data = await self.client._gather(futures)
        args = pack_data(args, data)
        kwargs = pack_data(kwargs, data)
        return (function, args, kwargs)

    async def _get_components_from_future(self, future):
        raw_components = await self._get_raw_components_from_future(future)
        return await self._prepare_raw_components(raw_components)

    def _execute_task_components(self, func, args, kwargs, run=True):
        if run:
            func(*args, **kwargs)
        else:
            return func, args, kwargs

    def recreate_task_locally(self, future, run=True):
        func, args, kwargs = sync(
            self.client.loop, self._get_components_from_future, future
        )
        return self._execute_task_components(func, args, kwargs, run=run)

    async def _get_errored_future(self, future):
        await wait(future)
        futures = [f.key for f in futures_of(future) if f.status == "error"]
        if not futures:
            raise ValueError("No errored futures passed")
        cause = await self.scheduler.get_error_cause(keys=futures)
        return Future(cause)

    def recreate_error_locally(self, future, run=True):
        """
        For a failed calculation, perform the blamed task locally for debugging.

        This operation should be performed after a future (result of ``gather``,
        ``compute``, etc) comes back with a status of "error", if the stack-
        trace is not informative enough to diagnose the problem. The specific
        task (part of the graph pointing to the future) responsible for the
        error will be fetched from the scheduler, together with the values of
        its inputs. The function will then be executed, so that ``pdb`` can
        be used for debugging.

        Examples
        --------
        >>> future = c.submit(div, 1, 0)         # doctest: +SKIP
        >>> future.status                        # doctest: +SKIP
        'error'
        >>> c.recreate_error_locally(future)     # doctest: +SKIP
        ZeroDivisionError: division by zero

        If you're in IPython you might take this opportunity to use pdb

        >>> %pdb                                 # doctest: +SKIP
        Automatic pdb calling has been turned ON

        >>> c.recreate_error_locally(future)     # doctest: +SKIP
        ZeroDivisionError: division by zero
              1 def div(x, y):
        ----> 2     return x / y
        ipdb>

        Parameters
        ----------
        future : future or collection that failed
            The same thing as was given to ``gather``, but came back with
            an exception/stack-trace. Can also be a (persisted) dask collection
            containing any errored futures.

        Returns
        -------
        Nothing; the function runs and should raise an exception, allowing
        the debugger to run.
        """
        errored_future = sync(
            self.client.loop, self._get_errored_future, future
        )
        return self.recreate_task_locally(errored_future, run=run)
