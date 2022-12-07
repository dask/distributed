from __future__ import annotations

from collections import Counter
from typing import Any, Hashable

import pytest

import dask
from dask import graph_manipulation
from dask.base import collections_to_dsk
from dask.cogroups import cogroup
from dask.core import flatten, get_dependencies
from dask.order import order
from dask.utils import stringify

from distributed.diagnostics import SchedulerPlugin
from distributed.scheduler import Scheduler, TaskState, TaskStateState
from distributed.utils_test import gen_cluster


@dask.delayed(pure=True)
def f(*args):
    return None


def tsk(name, *args):
    "Syntactic sugar for calling dummy delayed function"
    return f(*args, dask_key_name=name)


def get_cogroups(
    xs: Any,
) -> list[list[Hashable]]:
    if not isinstance(xs, list):
        xs = [xs]

    # dask.visualize(
    #     xs, color="cogroup-name", optimize_graph=False, collapse_outputs=True
    # )

    dsk = collections_to_dsk(xs, optimize_graph=False)
    dependencies = {k: get_dependencies(dsk, k) for k in dsk}

    priorities: dict[Hashable, int] = order(dsk, dependencies=dependencies)

    cogroups = list(cogroup(priorities, dependencies))

    return cogroups


class ReplicaTracker(SchedulerPlugin):
    max_replicas: dict[str, int]
    who_has_at_max: dict[str, set[str]]
    scheduler: Scheduler

    def __init__(self) -> None:
        self.max_replicas: dict[str, int] = {}
        self.who_has_at_max: dict[str, set[str]] = {}

    async def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def transition(
        self,
        key: str,
        start: TaskStateState,
        finish: TaskStateState,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if finish == "memory":
            ts: TaskState = self.scheduler.tasks[key]
            if len(ts.who_has) > self.max_replicas.get(key, 0):
                self.max_replicas[key] = len(ts.who_has)
                self.who_has_at_max[key] = {ws.address for ws in ts.who_has}


async def track_replicas(s: Scheduler) -> tuple[dict[str, int], dict[str, set[str]]]:
    plugin = ReplicaTracker()
    await s.register_scheduler_plugin(plugin)
    return plugin.max_replicas, plugin.who_has_at_max


@pytest.mark.parametrize("from_zarr", [False, True])
@gen_cluster(client=True, nthreads=[("", 2)] * 2)
async def test_co_assign_tree_reduce_multigroup(c, s, *workers, from_zarr):
    da = pytest.importorskip("dask.array")

    roots = da.ones((100,), chunks=(10,))
    arr = graph_manipulation.bind(roots, tsk("open-zarr")) if from_zarr else roots
    result = arr.sum()

    if len(get_cogroups(result)) != 3:
        pytest.fail("Test assumptions changed")

    result.visualize(color="cogroup-name", collapse_outputs=True)

    max_replicas, who_has_at_max = await track_replicas(s)
    await c.gather(c.compute(result, optimize_graph=False))

    root_keys = set(map(stringify, flatten(arr.__dask_keys__())))
    assert {k: r for k, r in max_replicas.items() if k in root_keys} == dict.fromkeys(
        root_keys, 1
    )

    worker_counts = Counter(
        addr for k, addrs in who_has_at_max.items() if k in root_keys for addr in addrs
    )
    # Two groups of 4, then one of 2.
    assert set(worker_counts.values()) == {4, 6}, worker_counts
