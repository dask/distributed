from __future__ import annotations

from ast import literal_eval
from collections import Counter
from typing import Any, Hashable, Sequence

import pytest

import dask
from dask import graph_manipulation
from dask.base import collections_to_dsk
from dask.cogroups import cogroup
from dask.core import flatten, get_dependencies
from dask.order import order
from dask.utils import key_split, stringify

from distributed.utils_test import gen_cluster
from distributed.worker import Worker
from distributed.worker_state_machine import ComputeTaskEvent


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


def get_transfers(workers: Sequence[Worker]) -> Counter[str]:
    return Counter(
        key for w in workers for l in w.transfer_incoming_log for key in l["keys"]
    )


def get_transfers_by_prefix(workers: Sequence[Worker]) -> Counter[str]:
    return Counter(
        key_split(key)
        for w in workers
        for l in w.transfer_incoming_log
        for key in l["keys"]
    )


def get_who_ran(workers: Sequence[Worker]) -> dict[str, str]:
    result: dict[str, str] = {}
    for w in workers:
        for event in w.state.stimulus_log:
            if isinstance(event, ComputeTaskEvent):
                prev = result.setdefault(event.key, w.address)
                assert (
                    prev == w.address
                ), f"Task {event.key!r} run on multiple workers: {prev}, {w.address}"
    return result


def collection_keys(*xs: Any) -> list[str]:
    return [stringify(k) for x in xs for k in flatten(x.__dask_keys__())]


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

    await c.gather(c.compute(result, optimize_graph=False))

    transfers = get_transfers_by_prefix(workers)
    assert transfers.keys() <= {"sum-partial", "checkpoint"}

    root_keys = collection_keys(arr)
    who_ran = get_who_ran(workers)
    worker_counts = Counter(who_ran[k] for k in root_keys)

    # Two groups of 4, then one of 2.
    assert set(worker_counts.values()) == {4, 6}, worker_counts


def unstringify(k):
    try:
        return literal_eval(k)
    except SyntaxError:
        return k


@gen_cluster(
    client=True,
    nthreads=[("", 2)] * 4,
    config={"distributed.scheduler.work-stealing": False},
)
async def test_double_diff(c, s, *workers):
    # Variant of https://github.com/dask/distributed/issues/6597
    da = pytest.importorskip("dask.array")
    a = da.ones((30, 30), chunks=(10, 10))
    b = da.zeros((30, 30), chunks=(10, 10))

    result = a[1:, 1:] - b[:-1, :-1]

    cogroups = get_cogroups(result)
    print([len(cg) for cg in cogroups])

    cogroups_by_key = {k: i for i, cg in enumerate(cogroups) for k in cg}
    root_keys = collection_keys(a, b)
    root_group_counts = Counter(cogroups_by_key[unstringify(k)] for k in root_keys)
    print(root_group_counts)

    await c.gather(c.compute(result, optimize_graph=False))

    who_ran = get_who_ran(workers)
    addr_i = {w.address: i for i, w in enumerate(workers)}

    result.visualize(
        # color={unstringify(k): addr_i[addr] for k, addr in who_ran.items()},
        color="cogroup",
        collapse_outputs=True,
    )

    worker_counts = Counter(who_ran[k] for k in root_keys)
    print(worker_counts)
    assert len(worker_counts) == len(workers)
