import logging

from dask.utils import stringify

from .client import futures_of, wait
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
        self.scheduler.handlers["cause_of_failure"] = self.cause_of_failure
        self.scheduler.handlers["get_runspec"] = self.get_runspec
        self.scheduler.extensions["exceptions"] = self

    def cause_of_failure(self, *args, keys=(), **kwargs):
        """
        Return details of first failed task required by set of keys

        Parameters
        ----------
        keys : list of keys known to the scheduler

        Returns
        -------
        Dictionary with:
        cause: the key that failed
        task: the definition of that key
        deps: keys that the task depends on
        """
        def error_details_extractor(ts):
            if ts is not None and ts.exception_blame is not None:
                cause = ts.exception_blame
                # NOTE: cannot serialize sets
                return {
                    "deps": [dts.key for dts in cause.dependencies],
                    "cause": cause.key,
                    "task": cause.run_spec,
                }
        return self.get_runspec(keys=keys, details_extractor=error_details_extractor, *args, **kwargs)


    def get_runspec(self, *args, keys=(), details_extractor=None, **kwargs):
        def default_details_extractor(ts):
            return {
                "task": ts.run_spec,
                "deps": [dts.key for dts in ts.dependencies]
            }

        if details_extractor is None:
            details_extractor = default_details_extractor


        for key in keys:
            if isinstance(key, list):
                key = tuple(key)  # ensure not a list from msgpack
            key = stringify(key)
            ts = self.scheduler.tasks.get(key)
            details = details_extractor(ts)
            if details is not None:
                return details


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
        self.client.extensions["exceptions"] = self
        # monkey patch
        self.client.recreate_error_locally = self.recreate_error_locally
        self.client._recreate_error_locally = self._recreate_error_locally
        self.client._get_futures_error = self._get_futures_error
        self.client.get_futures_error = self.get_futures_error

        self.client.recreate_task_locally = self.recreate_task_locally
        self.client._recreate_task_locally = self._recreate_task_locally
        self.client._get_futures_components = self._get_futures_components
        self.client.get_futures_components = self.get_futures_components

    @property
    def scheduler(self):
        return self.client.scheduler

    async def _get_futures_components(self, futures, runspec_getter=None):
        if runspec_getter is None:
            runspec_getter = self.scheduler.get_runspec

        if not isinstance(futures, list):  # TODO: other iterables types?
            futures = [futures]

        out = await runspec_getter(keys=[f.key for f in futures])
        deps, task = out["deps"], out["task"]
        if isinstance(task, dict):
            function, args, kwargs = _deserialize(**task)
            return (function, args, kwargs, deps)
        else:
            function, args, kwargs = _deserialize(task=task)
            return (function, args, kwargs, deps)

    async def _get_futures_error(self, future):
        # only get errors for futures that errored.
        futures = [f for f in futures_of(future) if f.status == "error"]
        if not futures:
            raise ValueError("No errored futures passed")
        
        return await self._get_futures_components(futures, self.scheduler.cause_of_failure)

    def get_futures_components(self, future):
        return self.client.sync(self._get_futures_components, future)

    def get_futures_error(self, future):
        """
        Ask the scheduler details of the sub-task of the given failed future

        When a future evaluates to a status of "error", i.e., an exception
        was raised in a task within its graph, we an get information from
        the scheduler. This function gets the details of the specific task
        that raised the exception and led to the error, but does not fetch
        data from the cluster or execute the function.

        Parameters
        ----------
        future : future that failed, having ``status=="error"``, typically
            after an attempt to ``gather()`` shows a stack-stace.

        Returns
        -------
        Tuple:
        - the function that raised an exception
        - argument list (a tuple), may include values and keys
        - keyword arguments (a dictionary), may include values and keys
        - list of keys that the function requires to be fetched to run

        See Also
        --------
        ReplayTaskClient.recreate_error_locally
        """
        return self.client.sync(self._get_futures_error, future)

    
    async def _recreate_task_locally(self, future, component_getter=None):
        if component_getter is None:
            component_getter = self._get_futures_components

        await wait(future)
        out = await component_getter(future)
        function, args, kwargs, deps = out
        futures = self.client._graph_to_futures({}, deps)
        data = await self.client._gather(futures)
        args = pack_data(args, data)
        kwargs = pack_data(kwargs, data)
        return (function, args, kwargs)

    async def _recreate_error_locally(self, future):
        return await self._recreate_task_locally(
            future,
            component_getter=self._get_futures_error
        )

    def recreate_task_locally(self, future):
        func, args, kwargs = sync(
            self.client.loop, self._recreate_task_locally, future
        )
        func(*args, **kwargs)

    def recreate_error_locally(self, future):
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
        func, args, kwargs = sync(
            self.client.loop, self._recreate_error_locally, future
        )
        func(*args, **kwargs)
