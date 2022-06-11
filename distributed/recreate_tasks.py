import logging

from dask.utils import stringify

from distributed.client import futures_of, wait
from distributed.utils import sync
from distributed.utils_comm import pack_data
from distributed.worker import _deserialize

logger = logging.getLogger(__name__)


class ReplayTaskScheduler:
    """A plugin for the scheduler to recreate tasks locally

    This adds the following routes to the scheduler

    *  get_runspec
    *  get_error_cause
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers["get_runspec"] = self.get_runspec
        self.scheduler.handlers["get_error_cause"] = self.get_error_cause

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
        return {"task": ts.run_spec, "deps": [dts.key for dts in ts.dependencies]}


class ReplayTaskClient:
    """
    A plugin for the client allowing replay of remote tasks locally

    Adds the following methods to the given client:

    - ``recreate_error_locally``: main user method for replaying failed tasks
    - ``recreate_task_locally``: main user method for replaying any task
    """

    def __init__(self, client):
        self.client = client
        self.client.extensions["replay-tasks"] = self
        # monkey patch
        self.client._get_raw_components_from_future = (
            self._get_raw_components_from_future
        )
        self.client._prepare_raw_components = self._prepare_raw_components
        self.client._get_components_from_future = self._get_components_from_future
        self.client._get_errored_future = self._get_errored_future
        self.client.recreate_task_locally = self.recreate_task_locally
        self.client.recreate_error_locally = self.recreate_error_locally

    @property
    def scheduler(self):
        return self.client.scheduler

    async def _get_raw_components_from_future(self, future):
        """
        For a given future return the func, args and kwargs and future
        deps that would be executed remotely.
        """
        if isinstance(future, str):
            key = future
        else:
            await wait(future)
            key = future.key
        spec = await self.scheduler.get_runspec(key=key)
        deps, task = spec["deps"], spec["task"]
        if isinstance(task, dict):
            function, args, kwargs = _deserialize(**task)
            return (function, args, kwargs, deps)
        else:
            function, args, kwargs = _deserialize(task=task)
            return (function, args, kwargs, deps)

    async def _prepare_raw_components(self, raw_components):
        """
        Take raw components and resolve future dependencies.
        """
        function, args, kwargs, deps = raw_components
        futures = self.client._graph_to_futures({}, deps)
        data = await self.client._gather(futures)
        args = pack_data(args, data)
        kwargs = pack_data(kwargs, data)
        return (function, args, kwargs)

    async def _get_components_from_future(self, future):
        """
        For a given future return the func, args and kwargs that would be
        executed remotely. Any args/kwargs that are themselves futures will
        be resolved to the return value of those futures.
        """
        raw_components = await self._get_raw_components_from_future(future)
        return await self._prepare_raw_components(raw_components)

    def recreate_task_locally(self, future):
        """
        For any calculation, whether it succeeded or failed, perform the task
        locally for debugging.

        This operation should be performed after a future (result of ``gather``,
        ``compute``, etc) comes back with a status other than "pending". Cases
        where you might want to debug a successfully completed future could
        include a calculation that returns an unexpected results. A common
        debugging process might include running the task locally in debug mode,
        with `pdb.runcall`.

        Examples
        --------
        >>> import pdb                                    # doctest: +SKIP
        >>> future = c.submit(div, 1, 1)                  # doctest: +SKIP
        >>> future.status                                 # doctest: +SKIP
        'finished'
        >>> pdb.runcall(c.recreate_task_locally, future)  # doctest: +SKIP

        Parameters
        ----------
        future : future
            The same thing as was given to ``gather``.

        Returns
        -------
        Any; will return the result of the task future.
        """
        func, args, kwargs = sync(
            self.client.loop, self._get_components_from_future, future
        )
        return func(*args, **kwargs)

    async def _get_errored_future(self, future):
        """
        For a given future collection, return the first future that raised
        an error.
        """
        await wait(future)
        futures = [f.key for f in futures_of(future) if f.status == "error"]
        if not futures:
            raise ValueError("No errored futures passed")
        cause_key = await self.scheduler.get_error_cause(keys=futures)
        return cause_key

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
        errored_future_key = sync(self.client.loop, self._get_errored_future, future)
        return self.recreate_task_locally(errored_future_key)
