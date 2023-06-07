from __future__ import annotations

import math
from collections import defaultdict
from contextlib import suppress
from typing import Any

from dask.utils import parse_bytes

from distributed.core import Status
from distributed.deploy.adaptive import Adaptive
from distributed.deploy.spec import SpecCluster
from distributed.utils import NoOpAwaitable


class HeterogeneousCluster(SpecCluster):
    """Cluster for heterogeneous workers

    This class inherits the ``SpecCluster`` class, so it expects a full
    specification of the Scheduler and Workers to use.  It removes any handling
    of user inputs (like threads vs processes, number of cores, and so on) and
    any handling of cluster resource managers (like pods, jobs, and so on).
    Instead, it expects this information to be passed in scheduler and worker
    specifications.

    Compared to the ``SpecCluster``, this cluster supports workers with
    non-identical resources connecting to the same dask scheduler.
    For example, one set of workers with high memory availability, another
    set of workers with low memory availability, and a different set of workers
    having access to GPUs can form a single dask cluster. To achieve this, the
    cluster internally organizes such non-identical workers into separate
    groups, called "worker pools". Each worker pool consists of a set of
    identical workers within it, which can be scaled individually. The user
    simply needs to provide a specification of such different types of workers
    in the form of a dictionary from a worker pool to its specification.

    Parameters
    ----------
    *args : Any
        Positional arguments that will be passed to the parent ``SpecCluster``.
    **kwargs : Any
        Keyword arguments that will be passed to the parent ``SpecCluster``.

    Examples
    --------
    To create a HeterogeneousCluster you specify how to set up a scheduler and
    a dict of worker-pools. Below we have an example to create two worker-pools for
    "low memory" and "high memory" type of workers.

    >>> from dask.distributed import Scheduler, Worker, Nanny
    >>> scheduler = {'cls': Scheduler, 'options': {"dashboard_address": ':8787'}}
    >>> worker = {
    ...     'low-memory-pool': {"cls": Worker, "options": {"memory_limit": "1GB"}},
    ...     'high-memory-pool': {"cls": Worker, "options": {"memory_limit": "3GB"}},
    ... }
    >>> cluster = HeterogeneousCluster(scheduler=scheduler, worker=worker)

    The worker spec is stored as the ``.worker_spec`` attribute

    >>> cluster.worker_spec
    {}

    We can individually ``.scale(...)`` each worker pool, which adds new workers
    of a given form.

    >>> cluster.scale(3, pool='low-memory-pool')
    >>> cluster.scale(2, pool='high-memory-pool')
    >>> cluster.worker_spec
    {'low-memory-pool-0': {'cls': distributed.worker.Worker,
    'options': {'memory_limit': '1GB'},
    'pool_name': 'low-memory-pool'},
    'low-memory-pool-1': {'cls': distributed.worker.Worker,
    'options': {'memory_limit': '1GB'},
    'pool_name': 'low-memory-pool'},
    'low-memory-pool-2': {'cls': distributed.worker.Worker,
    'options': {'memory_limit': '1GB'},
    'pool_name': 'low-memory-pool'},
    'high-memory-pool-0': {'cls': distributed.worker.Worker,
    'options': {'memory_limit': '5GB'},
    'pool_name': 'high-memory-pool'},
    'high-memory-pool-1': {'cls': distributed.worker.Worker,
    'options': {'memory_limit': '5GB'},
    'pool_name': 'high-memory-pool'}}

    While the instantiation of this spec is stored in the ``.workers``
    attribute

    >>> cluster.workers
    {'low-memory-pool-0': <Worker ...>,
    'low-memory-pool-1': <Worker ...>,
    'low-memory-pool-2': <Worker ...>,
    'high-memory-pool-0': <Worker ...>,
    'high-memory-pool-1': <Worker ...>}
    """

    def __init__(self, *args, **kwargs):
        from distributed import Worker

        super().__init__(*args, **kwargs)
        self._next_worker_id = defaultdict(int)

    def scale(self, n=0, memory=None, cores=None, pool=None):
        if not self._supports_scaling:
            raise RuntimeError("Cluster does not support scaling.")

        if pool is None:
            if len(self.new_spec) > 1:
                raise ValueError(
                    "to use scale(pool=...) you must provide a pool as "
                    "cluster contains more than one worker pool"
                )
            pool = list(self.new_spec)[0]
        if pool not in self.new_spec:
            raise ValueError(f"Unknown worker pool: {pool}")

        if memory is not None:
            n = max(
                n,
                int(math.ceil(parse_bytes(memory) / self._memory_per_worker(pool))),
            )

        if cores is not None:
            n = max(n, int(math.ceil(cores / self._threads_per_worker(pool))))

        # Get workers that belong to the given pool
        pool_spec = self._pool_spec(pool)

        if len(pool_spec) > n:
            not_yet_launched = set(pool_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while len(pool_spec) > n and not_yet_launched:
                w = not_yet_launched.pop()
                del self.worker_spec[w]
                del pool_spec[w]

        while len(pool_spec) > n:
            w, _ = pool_spec.popitem()
            self.worker_spec.pop(w)

        num_workers = len(pool_spec)
        if self.status not in (Status.closing, Status.closed):
            while num_workers < n:
                self.worker_spec.update(self.new_worker_spec(pool))
                num_workers += 1

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()

    def _threads_per_worker(self, pool: str) -> int:
        """Return the number of threads per worker for new workers"""
        if not self.new_spec:  # pragma: no cover
            raise ValueError("To scale by cores= you must specify cores per worker")

        for name in ["nthreads", "ncores", "threads", "cores"]:
            with suppress(KeyError):
                return self.new_spec[pool]["options"][name]
        raise RuntimeError("unreachable")

    def _memory_per_worker(self, pool: str) -> int:
        """Return the memory limit per worker for new workers"""
        if not self.new_spec:  # pragma: no cover
            raise ValueError(
                "to scale by memory= your worker definition must include a "
                "memory_limit definition"
            )

        for name in ["memory_limit", "memory"]:
            with suppress(KeyError):
                return parse_bytes(self.new_spec[pool]["options"][name])

        raise ValueError(
            "to use scale(memory=...) your worker definition must include a "
            "memory_limit definition"
        )

    def _pool_spec(self, pool: str) -> dict[str, dict]:
        """Return workers that belong to the given pool"""
        return {k: v for (k, v) in self.worker_spec.items() if v["pool_name"] == pool}

    def _new_worker_name(self, pool, worker_number):
        """Return new worker name.

        This can be overridden in SpecCluster derived classes to customise the
        worker names.
        """
        return f"{pool}-{worker_number}"

    def new_worker_spec(self, pool):
        """Return name and spec for the next worker

        Returns
        -------
        d: dict mapping names to worker specs

        See Also
        --------
        scale
        """
        new_worker_name = self._new_worker_name(pool, self._next_worker_id[pool])
        while new_worker_name in self.worker_spec:
            self._next_worker_id[pool] += 1
            new_worker_name = self._new_worker_name(pool, self._next_worker_id[pool])
        self._next_worker_id[pool] += 1

        worker_pool = self.new_spec[pool]
        if "pool_name" not in worker_pool:
            worker_pool["pool_name"] = pool

        return {new_worker_name: worker_pool}

    def adapt(self, **kwargs: Any) -> Adaptive:
        raise NotImplementedError("Heterogeneous cluster does not adaptivity")
