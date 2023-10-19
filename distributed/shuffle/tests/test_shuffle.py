from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import os
import random
import shutil
from collections import defaultdict
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from typing import Any, cast
from unittest import mock

import pytest

from dask.utils import key_split

from distributed.shuffle._core import ShuffleId, ShuffleRun, barrier_key
from distributed.worker import Status

dd = pytest.importorskip("dask.dataframe")

import numpy as np
import pandas as pd

import dask
from dask.dataframe._compat import PANDAS_GE_150, PANDAS_GE_200
from dask.typing import Key

from distributed import (
    Client,
    Event,
    KilledWorker,
    LocalCluster,
    Nanny,
    Scheduler,
    Worker,
)
from distributed.core import ConnectionPool
from distributed.scheduler import TaskState as SchedulerTaskState
from distributed.shuffle._arrow import (
    convert_shards,
    list_of_buffers_to_table,
    read_from_disk,
    serialize_table,
)
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._scheduler_plugin import ShuffleSchedulerPlugin
from distributed.shuffle._shuffle import (
    DataFrameShuffleRun,
    _get_worker_for_range_sharding,
    split_by_partition,
    split_by_worker,
)
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin, _ShuffleRunManager
from distributed.shuffle.tests.utils import (
    AbstractShuffleTestPool,
    invoke_annotation_chaos,
)
from distributed.utils import Deadline
from distributed.utils_test import (
    async_poll_for,
    cluster,
    gen_cluster,
    gen_test,
    raises_with_cause,
    wait_for_state,
)
from distributed.worker_state_machine import TaskState as WorkerTaskState

try:
    import pyarrow as pa
except ImportError:
    pa = None


@pytest.fixture(params=[0, 0.3, 1], ids=["none", "some", "all"])
def lose_annotations(request):
    return request.param


async def check_worker_cleanup(
    worker: Worker,
    closed: bool = False,
    interval: float = 0.01,
    timeout: int | None = 5,
) -> None:
    """Assert that the worker has no shuffle state"""
    deadline = Deadline.after(timeout)
    plugin = worker.plugins["shuffle"]
    assert isinstance(plugin, ShuffleWorkerPlugin)

    while plugin.shuffle_runs._runs and not deadline.expired:
        await asyncio.sleep(interval)
    assert not plugin.shuffle_runs._runs
    if closed:
        assert plugin.closed
    for dirpath, dirnames, filenames in os.walk(worker.local_directory):
        assert "shuffle" not in dirpath
        for fn in dirnames + filenames:
            assert "shuffle" not in fn


async def check_scheduler_cleanup(
    scheduler: Scheduler, interval: float = 0.01, timeout: int | None = 5
) -> None:
    """Assert that the scheduler has no shuffle state"""
    deadline = Deadline.after(timeout)
    plugin = scheduler.plugins["shuffle"]
    assert isinstance(plugin, ShuffleSchedulerPlugin)
    while plugin._shuffles and not deadline.expired:
        await asyncio.sleep(interval)
    assert not plugin.active_shuffles
    assert not plugin._shuffles, scheduler.tasks
    assert not plugin._archived_by_stimulus
    assert not plugin.heartbeats


@gen_cluster(client=True)
async def test_minimal_version(c, s, a, b):
    no_pyarrow_ctx = (
        mock.patch.dict("sys.modules", {"pyarrow": None})
        if pa is not None
        else contextlib.nullcontext()
    )
    with no_pyarrow_ctx:
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        with pytest.raises(ModuleNotFoundError, match="requires pyarrow"):
            await c.compute(dd.shuffle.shuffle(df, "x", shuffle="p2p"))


@pytest.mark.gpu
@pytest.mark.filterwarnings(
    "ignore:Ignoring the following arguments to `from_pyarrow_table_dispatch`."
)
@gen_cluster(client=True)
async def test_basic_cudf_support(c, s, a, b):
    cudf = pytest.importorskip("cudf")
    pytest.importorskip("dask_cudf")

    try:
        from dask.dataframe.dispatch import to_pyarrow_table_dispatch

        to_pyarrow_table_dispatch(cudf.DataFrame())
    except TypeError:
        pytest.skip(reason="Newer version of dask_cudf is required.")

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    ).to_backend("cudf")
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    assert shuffled.npartitions == df.npartitions

    result, expected = await c.compute([shuffled, df], sync=True)
    dd.assert_eq(result, expected)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


def get_active_shuffle_run(shuffle_id: ShuffleId, worker: Worker) -> ShuffleRun:
    return get_active_shuffle_runs(worker)[shuffle_id]


def get_shuffle_run_manager(worker: Worker) -> _ShuffleRunManager:
    return cast(ShuffleWorkerPlugin, worker.plugins["shuffle"]).shuffle_runs


def get_active_shuffle_runs(worker: Worker) -> dict[ShuffleId, ShuffleRun]:
    return get_shuffle_run_manager(worker)._active_runs


@pytest.mark.parametrize("npartitions", [None, 1, 20])
@pytest.mark.parametrize("disk", [True, False])
@gen_cluster(client=True)
async def test_basic_integration(c, s, a, b, lose_annotations, npartitions, disk):
    await invoke_annotation_chaos(lose_annotations, c)
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    with dask.config.set({"distributed.p2p.disk": disk}):
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p", npartitions=npartitions)
    if npartitions is None:
        assert shuffled.npartitions == df.npartitions
    else:
        assert shuffled.npartitions == npartitions
    result, expected = await c.compute([shuffled, df], sync=True)
    dd.assert_eq(result, expected)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@pytest.mark.parametrize("processes", [True, False])
@gen_test()
async def test_basic_integration_local_cluster(processes):
    async with LocalCluster(
        n_workers=2,
        processes=processes,
        asynchronous=True,
        dashboard_address=":0",
    ) as cluster:
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        c = cluster.get_client()
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        x, y = c.compute([df, out])
        x, y = await c.gather([x, y])
        dd.assert_eq(x, y)


@pytest.mark.parametrize("npartitions", [None, 1, 20])
@gen_cluster(client=True)
async def test_shuffle_with_array_conversion(c, s, a, b, lose_annotations, npartitions):
    await invoke_annotation_chaos(lose_annotations, c)
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p", npartitions=npartitions).values

    if npartitions == 1:
        # FIXME: distributed#7816
        with raises_with_cause(
            RuntimeError, "failed during transfer", RuntimeError, "Barrier task"
        ):
            await c.compute(out)
    else:
        await c.compute(out)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


def test_shuffle_before_categorize(loop_in_thread):
    """Regression test for https://github.com/dask/distributed/issues/7615"""
    with cluster() as (s, [a, b]), Client(s["address"], loop=loop_in_thread) as c:
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": str},
            freq="10 s",
        )
        df = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        df.categorize(columns=["y"])
        c.compute(df)


@gen_cluster(client=True)
async def test_concurrent(c, s, a, b, lose_annotations):
    await invoke_annotation_chaos(lose_annotations, c)
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    y = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    df, x, y = await c.compute([df, x, y], sync=True)
    dd.assert_eq(x, df, check_index=False)
    dd.assert_eq(y, df, check_index=False)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_bad_disk(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    while not get_active_shuffle_runs(a):
        await asyncio.sleep(0.01)
    shutil.rmtree(a.local_directory)

    while not get_active_shuffle_runs(b):
        await asyncio.sleep(0.01)
    shutil.rmtree(b.local_directory)
    with pytest.raises(RuntimeError, match=f"{shuffle_id} failed during transfer"):
        out = await c.compute(out)

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


async def wait_until_worker_has_tasks(
    prefix: str, worker: str, count: int, scheduler: Scheduler, interval: float = 0.01
) -> None:
    ws = scheduler.workers[worker]
    while (
        len(
            [
                key
                for key, ts in scheduler.tasks.items()
                if prefix in key_split(key)
                and ts.state == "memory"
                and {ws} == ts.who_has
            ]
        )
        < count
    ):
        await asyncio.sleep(interval)


async def wait_for_tasks_in_state(
    prefix: str,
    state: str,
    count: int,
    dask_worker: Worker | Scheduler,
    interval: float = 0.01,
) -> None:
    tasks: Mapping[Key, SchedulerTaskState | WorkerTaskState]

    if isinstance(dask_worker, Worker):
        tasks = dask_worker.state.tasks
    elif isinstance(dask_worker, Scheduler):
        tasks = dask_worker.tasks
    else:
        raise TypeError(dask_worker)

    while (
        len(
            [
                key
                for key, ts in tasks.items()
                if prefix in key_split(key) and ts.state == state
            ]
        )
        < count
    ):
        await asyncio.sleep(interval)


async def wait_until_new_shuffle_is_initialized(
    scheduler: Scheduler, interval: float = 0.01, timeout: int | None = None
) -> ShuffleId:
    deadline = Deadline.after(timeout)
    scheduler_plugin = scheduler.plugins["shuffle"]
    assert isinstance(scheduler_plugin, ShuffleSchedulerPlugin)
    while not scheduler_plugin.shuffle_ids() and not deadline.expired:
        await asyncio.sleep(interval)
    shuffle_ids = scheduler_plugin.shuffle_ids()
    assert len(shuffle_ids) == 1
    return next(iter(shuffle_ids))


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_transfer(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    fut = c.compute([shuffled, df], sync=True)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)
    await b.close()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b, closed=True)
    await check_scheduler_cleanup(s)


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.allowed-failures": 0},
)
async def test_restarting_during_transfer_raises_killed_worker(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = c.compute(out.x.size)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)
    await b.close()

    with pytest.raises(KilledWorker):
        await out

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b, closed=True)
    await check_scheduler_cleanup(s)


class BlockedGetOrCreateShuffleRunManager(_ShuffleRunManager):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.in_get_or_create = asyncio.Event()
        self.block_get_or_create = asyncio.Event()

    async def get_or_create(self, *args: Any, **kwargs: Any) -> ShuffleRun:
        self.in_get_or_create.set()
        await self.block_get_or_create.wait()
        return await super().get_or_create(*args, **kwargs)


@mock.patch(
    "distributed.shuffle._worker_plugin._ShuffleRunManager",
    BlockedGetOrCreateShuffleRunManager,
)
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.allowed-failures": 0},
)
async def test_get_or_create_from_dangling_transfer(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = c.compute(out.x.size)

    shuffle_extA = a.plugins["shuffle"]
    shuffle_extB = b.plugins["shuffle"]
    shuffle_extB.shuffle_runs.block_get_or_create.set()

    await shuffle_extA.shuffle_runs.in_get_or_create.wait()
    await b.close()
    await async_poll_for(
        lambda: not any(ws.processing for ws in s.workers.values()), timeout=5
    )

    with pytest.raises(KilledWorker):
        await out

    await async_poll_for(lambda: not s.plugins["shuffle"].active_shuffles, timeout=5)
    assert a.state.tasks
    shuffle_extA.shuffle_runs.block_get_or_create.set()
    await async_poll_for(lambda: not a.state.tasks, timeout=10)

    assert not s.plugins["shuffle"].active_shuffles
    await check_worker_cleanup(a)
    await check_worker_cleanup(b, closed=True)
    await c.close()
    await check_scheduler_cleanup(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_during_transfer(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        killed_worker_address = n.worker_address
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        fut = c.compute([shuffled, df], sync=True)
        await wait_until_worker_has_tasks(
            "shuffle-transfer", killed_worker_address, 1, s
        )
        await n.process.process.kill()

        result, expected = await fut
        dd.assert_eq(result, expected)

        await c.close()
        await check_worker_cleanup(a)
        await check_scheduler_cleanup(s)


@gen_cluster(
    client=True,
    nthreads=[],
    # Effectively disable the memory monitor to be able to manually control
    # the worker status
    config={"distributed.worker.memory.monitor-interval": "60s"},
)
async def test_restarting_does_not_deadlock(c, s):
    """Regression test for https://github.com/dask/distributed/issues/8088"""
    async with Worker(s.address) as a:
        async with Nanny(s.address) as b:
            # Ensure that a holds the input tasks to the shuffle
            with dask.annotate(workers=[a.address]):
                df = dask.datasets.timeseries(
                    start="2000-01-01",
                    end="2000-03-01",
                    dtypes={"x": float, "y": float},
                    freq="10 s",
                )
            out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
            fut = c.compute(out.x.size)
            await wait_until_worker_has_tasks(
                "shuffle-transfer", b.worker_address, 1, s
            )
            a.status = Status.paused
            await async_poll_for(lambda: len(s.running) == 1, timeout=5)
            b.close_gracefully()
            await b.process.process.kill()

            await async_poll_for(lambda: not s.running, timeout=5)

            a.status = Status.running

            await async_poll_for(lambda: s.running, timeout=5)
            await fut


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_input_only_worker_during_transfer(c, s, a, b):
    def mock_get_worker_for_range_sharding(
        output_partition: int, workers: list[str], npartitions: int
    ) -> str:
        return a.address

    with mock.patch(
        "distributed.shuffle._shuffle._get_worker_for_range_sharding",
        mock_get_worker_for_range_sharding,
    ):
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-05-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        fut = c.compute([shuffled, df], sync=True)
        await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b, 0.001)
        await b.close()

        result, expected = await fut
        dd.assert_eq(result, expected)

        await c.close()
        await check_worker_cleanup(a)
        await check_worker_cleanup(b, closed=True)
        await check_scheduler_cleanup(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)], clean_kwargs={"processes": False})
async def test_crashed_input_only_worker_during_transfer(c, s, a):
    def mock_mock_get_worker_for_range_sharding(
        output_partition: int, workers: list[str], npartitions: int
    ) -> str:
        return a.address

    with mock.patch(
        "distributed.shuffle._shuffle._get_worker_for_range_sharding",
        mock_mock_get_worker_for_range_sharding,
    ):
        async with Nanny(s.address, nthreads=1) as n:
            killed_worker_address = n.worker_address
            df = dask.datasets.timeseries(
                start="2000-01-01",
                end="2000-03-01",
                dtypes={"x": float, "y": float},
                freq="10 s",
            )
            shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
            fut = c.compute([shuffled, df], sync=True)
            await wait_until_worker_has_tasks(
                "shuffle-transfer", n.worker_address, 1, s
            )
            await n.process.process.kill()

            result, expected = await fut
            dd.assert_eq(result, expected)

            await c.close()
            await check_worker_cleanup(a)
            await check_scheduler_cleanup(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_closed_bystanding_worker_during_shuffle(c, s, w1, w2, w3):
    with dask.annotate(workers=[w1.address, w2.address], allow_other_workers=False):
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-02-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        fut = c.compute([shuffled, df], sync=True)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, w1)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, w2)
    await w3.close()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await check_worker_cleanup(w1)
    await check_worker_cleanup(w2)
    await check_worker_cleanup(w3, closed=True)
    await check_scheduler_cleanup(s)


class RaiseOnCloseShuffleRun(DataFrameShuffleRun):
    async def close(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("test-exception-on-close")


@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    RaiseOnCloseShuffleRun,
)
@gen_cluster(client=True, nthreads=[])
async def test_exception_on_close_cleans_up(c, s, caplog):
    # Ensure that everything is cleaned up and does not lock up if an exception
    # is raised during shuffle close.
    with caplog.at_level(logging.ERROR):
        async with Worker(s.address) as w:
            df = dask.datasets.timeseries(
                start="2000-01-01",
                end="2000-01-10",
                dtypes={"x": float, "y": float},
                freq="10 s",
            )
            shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
            await c.compute([shuffled, df], sync=True)

    assert any("test-exception-on-close" in record.message for record in caplog.records)
    await check_worker_cleanup(w, closed=True)


class BlockedInputsDoneShuffle(DataFrameShuffleRun):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_inputs_done = asyncio.Event()
        self.block_inputs_done = asyncio.Event()

    async def inputs_done(self) -> None:
        self.in_inputs_done.set()
        await self.block_inputs_done.wait()
        await super().inputs_done()


@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    BlockedInputsDoneShuffle,
)
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_barrier(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    fut = c.compute([shuffled, df], sync=True)
    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    key = barrier_key(shuffle_id)
    await wait_for_state(key, "processing", s)
    shuffleA = get_active_shuffle_run(shuffle_id, a)
    shuffleB = get_active_shuffle_run(shuffle_id, b)
    await shuffleA.in_inputs_done.wait()
    await shuffleB.in_inputs_done.wait()

    ts = s.tasks[key]
    processing_worker = a if ts.processing_on.address == a.address else b
    if processing_worker == a:
        close_worker, alive_worker = a, b
        alive_shuffle = shuffleB

    else:
        close_worker, alive_worker = b, a
        alive_shuffle = shuffleA
    await close_worker.close()

    alive_shuffle.block_inputs_done.set()
    alive_shuffles = get_active_shuffle_runs(alive_worker)

    def shuffle_restarted():
        try:
            return alive_shuffles[shuffle_id].run_id > alive_shuffle.run_id
        except KeyError:
            return False

    await async_poll_for(
        shuffle_restarted,
        timeout=5,
    )
    restarted_shuffle = alive_shuffles[shuffle_id]
    restarted_shuffle.block_inputs_done.set()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await c.close()
    await check_worker_cleanup(close_worker, closed=True)
    await check_worker_cleanup(alive_worker)
    await check_scheduler_cleanup(s)


@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    BlockedInputsDoneShuffle,
)
@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.allowed-failures": 0},
)
async def test_restarting_during_barrier_raises_killed_worker(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = c.compute(out.x.size)
    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    key = barrier_key(shuffle_id)
    await wait_for_state(key, "processing", s)
    shuffleA = get_active_shuffle_run(shuffle_id, a)
    shuffleB = get_active_shuffle_run(shuffle_id, b)
    await shuffleA.in_inputs_done.wait()
    await shuffleB.in_inputs_done.wait()

    ts = s.tasks[key]
    processing_worker = a if ts.processing_on.address == a.address else b
    if processing_worker == a:
        close_worker, alive_worker = a, b
        alive_shuffle = shuffleB

    else:
        close_worker, alive_worker = b, a
        alive_shuffle = shuffleA
    await close_worker.close()

    with pytest.raises(KilledWorker):
        await out

    alive_shuffle.block_inputs_done.set()

    await c.close()
    await check_worker_cleanup(close_worker, closed=True)
    await check_worker_cleanup(alive_worker)
    await check_scheduler_cleanup(s)


@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    BlockedInputsDoneShuffle,
)
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_other_worker_during_barrier(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    fut = c.compute([shuffled, df], sync=True)
    shuffle_id = await wait_until_new_shuffle_is_initialized(s)

    key = barrier_key(shuffle_id)
    await wait_for_state(key, "processing", s, interval=0)

    shuffleA = get_active_shuffle_run(shuffle_id, a)
    shuffleB = get_active_shuffle_run(shuffle_id, b)
    await shuffleA.in_inputs_done.wait()
    await shuffleB.in_inputs_done.wait()

    ts = s.tasks[key]
    processing_worker = a if ts.processing_on.address == a.address else b
    if processing_worker == a:
        close_worker, alive_worker = b, a
        alive_shuffle = shuffleA

    else:
        close_worker, alive_worker = a, b
        alive_shuffle = shuffleB
    await close_worker.close()

    alive_shuffle.block_inputs_done.set()
    alive_shuffles = get_active_shuffle_runs(alive_worker)

    def shuffle_restarted():
        try:
            return alive_shuffles[shuffle_id].run_id > alive_shuffle.run_id
        except KeyError:
            return False

    await async_poll_for(
        shuffle_restarted,
        timeout=5,
    )
    restarted_shuffle = alive_shuffles[shuffle_id]
    restarted_shuffle.block_inputs_done.set()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await c.close()
    await check_worker_cleanup(close_worker, closed=True)
    await check_worker_cleanup(alive_worker)
    await check_scheduler_cleanup(s)


@pytest.mark.slow
@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    BlockedInputsDoneShuffle,
)
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_other_worker_during_barrier(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        fut = c.compute([shuffled, df], sync=True)
        shuffle_id = await wait_until_new_shuffle_is_initialized(s)
        key = barrier_key(shuffle_id)
        # Ensure that barrier is not executed on the nanny
        s.set_restrictions({key: {a.address}})
        await wait_for_state(key, "processing", s, interval=0)
        shuffles = get_active_shuffle_runs(a)
        shuffle = get_active_shuffle_run(shuffle_id, a)
        await shuffle.in_inputs_done.wait()
        await n.process.process.kill()
        shuffle.block_inputs_done.set()

        def shuffle_restarted():
            try:
                return shuffles[shuffle_id].run_id > shuffle.run_id
            except KeyError:
                return False

        await async_poll_for(
            shuffle_restarted,
            timeout=5,
        )
        restarted_shuffle = get_active_shuffle_run(shuffle_id, a)
        restarted_shuffle.block_inputs_done.set()

        result, expected = await fut
        dd.assert_eq(result, expected)

        await c.close()
        await check_worker_cleanup(a)
        await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_closed_worker_during_unpack(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    fut = c.compute([shuffled, df], sync=True)
    await wait_for_tasks_in_state("shuffle_p2p", "memory", 1, b)
    await b.close()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b, closed=True)
    await check_scheduler_cleanup(s)


@gen_cluster(
    client=True,
    nthreads=[("", 1)] * 2,
    config={"distributed.scheduler.allowed-failures": 0},
)
async def test_restarting_during_unpack_raises_killed_worker(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = c.compute(out.x.size)
    await wait_for_tasks_in_state("shuffle_p2p", "memory", 1, b)
    await b.close()

    with pytest.raises(KilledWorker):
        await out

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b, closed=True)
    await check_scheduler_cleanup(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_during_unpack(c, s, a):
    async with Nanny(s.address, nthreads=2) as n:
        killed_worker_address = n.worker_address
        df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="10 s",
        )
        expected = await c.compute(df)
        shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        result = c.compute(shuffled)

        await wait_until_worker_has_tasks("shuffle_p2p", killed_worker_address, 1, s)
        await n.process.process.kill()

        result = await result
        dd.assert_eq(result, expected)

        await c.close()
        await check_worker_cleanup(a)
        await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_heartbeat(c, s, a, b):
    await a.heartbeat()
    await check_scheduler_cleanup(s)
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    while not s.plugins["shuffle"].heartbeats:
        await asyncio.sleep(0.001)
        await a.heartbeat()

    assert s.plugins["shuffle"].heartbeats.values()
    await out

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    del out
    await check_scheduler_cleanup(s)


def test_processing_chain(tmp_path):
    """
    This is a serial version of the entire compute chain

    In practice this takes place on many different workers.
    Here we verify its accuracy in a single threaded situation.
    """
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")

    class Stub:
        def __init__(self, value: int) -> None:
            self.value = value

    counter = count()
    workers = ["a", "b", "c"]
    npartitions = 5

    # Test the processing chain with a dataframe that contains all supported dtypes
    columns = {
        # numpy dtypes
        f"col{next(counter)}": pd.array([True, False] * 50, dtype="bool"),
        f"col{next(counter)}": pd.array(range(100), dtype="int8"),
        f"col{next(counter)}": pd.array(range(100), dtype="int16"),
        f"col{next(counter)}": pd.array(range(100), dtype="int32"),
        f"col{next(counter)}": pd.array(range(100), dtype="int64"),
        f"col{next(counter)}": pd.array(range(100), dtype="uint8"),
        f"col{next(counter)}": pd.array(range(100), dtype="uint16"),
        f"col{next(counter)}": pd.array(range(100), dtype="uint32"),
        f"col{next(counter)}": pd.array(range(100), dtype="uint64"),
        f"col{next(counter)}": pd.array(range(100), dtype="float16"),
        f"col{next(counter)}": pd.array(range(100), dtype="float32"),
        f"col{next(counter)}": pd.array(range(100), dtype="float64"),
        f"col{next(counter)}": pd.array(
            [np.datetime64("2022-01-01") + i for i in range(100)],
            dtype="datetime64[ns]",
        ),
        f"col{next(counter)}": pd.array(
            [np.timedelta64(1, "D") + i for i in range(100)],
            dtype="timedelta64[ns]",
        ),
        # FIXME PyArrow does not support complex numbers:
        #       https://issues.apache.org/jira/browse/ARROW-638
        # f"col{next(counter)}": pd.array(range(100), dtype="csingle"),
        # f"col{next(counter)}": pd.array(range(100), dtype="cdouble"),
        # f"col{next(counter)}": pd.array(range(100), dtype="clongdouble"),
        # Nullable dtypes
        f"col{next(counter)}": pd.array([True, False] * 50, dtype="boolean"),
        f"col{next(counter)}": pd.array(range(100), dtype="Int8"),
        f"col{next(counter)}": pd.array(range(100), dtype="Int16"),
        f"col{next(counter)}": pd.array(range(100), dtype="Int32"),
        f"col{next(counter)}": pd.array(range(100), dtype="Int64"),
        f"col{next(counter)}": pd.array(range(100), dtype="UInt8"),
        f"col{next(counter)}": pd.array(range(100), dtype="UInt16"),
        f"col{next(counter)}": pd.array(range(100), dtype="UInt32"),
        f"col{next(counter)}": pd.array(range(100), dtype="UInt64"),
        # pandas dtypes
        f"col{next(counter)}": pd.array(
            [np.datetime64("2022-01-01") + i for i in range(100)],
            dtype=pd.DatetimeTZDtype(tz="Europe/Berlin"),
        ),
        f"col{next(counter)}": pd.array(
            [pd.Period("2022-01-01", freq="D") + i for i in range(100)],
            dtype="period[D]",
        ),
        f"col{next(counter)}": pd.array(
            [pd.Interval(left=i, right=i + 2) for i in range(100)], dtype="Interval"
        ),
        f"col{next(counter)}": pd.array(["x", "y"] * 50, dtype="category"),
        f"col{next(counter)}": pd.array(["lorem ipsum"] * 100, dtype="string"),
        # FIXME: PyArrow does not support sparse data:
        #        https://issues.apache.org/jira/browse/ARROW-8679
        # f"col{next(counter)}": pd.array(
        #     [np.nan, np.nan, 1.0, np.nan, np.nan] * 20,
        #     dtype="Sparse[float64]",
        # ),
        # custom objects
        # FIXME: Serializing custom objects is not supported in P2P shuffling
        # f"col{next(counter)}": pd.array(
        #     [Stub(i) for i in range(100)], dtype="object"
        # ),
    }

    if PANDAS_GE_150:
        columns.update(
            {
                # PyArrow dtypes
                f"col{next(counter)}": pd.array(
                    [True, False] * 50, dtype="bool[pyarrow]"
                ),
                f"col{next(counter)}": pd.array(range(100), dtype="int8[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="int16[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="int32[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="int64[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="uint8[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="uint16[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="uint32[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="uint64[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="float32[pyarrow]"),
                f"col{next(counter)}": pd.array(range(100), dtype="float64[pyarrow]"),
                f"col{next(counter)}": pd.array(
                    [pd.Timestamp.fromtimestamp(1641034800 + i) for i in range(100)],
                    dtype=pd.ArrowDtype(pa.timestamp("ms")),
                ),
                f"col{next(counter)}": pd.array(
                    ["lorem ipsum"] * 100,
                    dtype="string[pyarrow]",
                ),
                f"col{next(counter)}": pd.array(
                    ["lorem ipsum"] * 100,
                    dtype=pd.StringDtype("pyarrow"),
                ),
                f"col{next(counter)}": pd.array(
                    ["lorem ipsum"] * 100,
                    dtype="string[python]",
                ),
            }
        )

    df = pd.DataFrame(columns)
    df["_partitions"] = df.col4 % npartitions
    worker_for = {i: random.choice(workers) for i in list(range(npartitions))}
    worker_for = pd.Series(worker_for, name="_worker").astype("category")

    meta = df.head(0)
    data = split_by_worker(df, "_partitions", worker_for=worker_for, meta=meta)
    assert set(data) == set(worker_for.cat.categories)
    assert sum(map(len, data.values())) == len(df)

    batches = {worker: [serialize_table(t)] for worker, t in data.items()}

    # Typically we communicate to different workers at this stage
    # We then receive them back and reconstute them

    by_worker = {
        worker: list_of_buffers_to_table(list_of_batches)
        for worker, list_of_batches in batches.items()
    }
    assert sum(map(len, by_worker.values())) == len(df)

    # We split them again, and then dump them down to disk

    splits_by_worker = {
        worker: split_by_partition(t, "_partitions") for worker, t in by_worker.items()
    }

    splits_by_worker = {
        worker: {partition: [t] for partition, t in d.items()}
        for worker, d in splits_by_worker.items()
    }

    # No two workers share data from any partition
    assert not any(
        set(a) & set(b)
        for w1, a in splits_by_worker.items()
        for w2, b in splits_by_worker.items()
        if w1 is not w2
    )

    for partitions in splits_by_worker.values():
        for partition, tables in partitions.items():
            for table in tables:
                with (tmp_path / str(partition)).open("ab") as f:
                    f.write(serialize_table(table))

    out = {}
    for k in range(npartitions):
        shards, _ = read_from_disk(tmp_path / str(k))
        out[k] = convert_shards(shards, meta)

    shuffled_df = pd.concat(df for df in out.values())
    pd.testing.assert_frame_equal(
        df,
        shuffled_df,
        check_like=True,
        check_exact=True,
    )


@gen_cluster(client=True)
async def test_head(c, s, a, b):
    a_files = list(os.walk(a.local_directory))
    b_files = list(os.walk(b.local_directory))

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = await out.head(compute=False).persist()  # Only ask for one key

    assert list(os.walk(a.local_directory)) == a_files  # cleaned up files?
    assert list(os.walk(b.local_directory)) == b_files

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    del out
    await check_scheduler_cleanup(s)


def test_split_by_worker():
    workers = ["a", "b", "c"]
    npartitions = 5
    df = pd.DataFrame({"x": range(100), "y": range(100)})
    df["_partitions"] = df.x % npartitions
    worker_for = {i: random.choice(workers) for i in range(npartitions)}
    s = pd.Series(worker_for, name="_worker").astype("category")


@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_clean_after_forgotten_early(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-03-01",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, a)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)
    del out
    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_tail(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="1 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    full = await x.persist()
    ntasks_full = len(s.tasks)
    del full
    while s.tasks:
        await asyncio.sleep(0)
    partial = await x.tail(compute=False).persist()  # Only ask for one key

    assert len(s.tasks) < ntasks_full
    del partial

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@pytest.mark.parametrize("wait_until_forgotten", [True, False])
@gen_cluster(client=True)
async def test_repeat_shuffle_instance(c, s, a, b, wait_until_forgotten):
    """Tests repeating the same instance of a shuffle-based task graph.

    See Also
    --------
    test_repeat_shuffle_operation
    """
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p").size
    await c.compute(out)

    if wait_until_forgotten:
        while s.tasks:
            await asyncio.sleep(0)

    await c.compute(out)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@pytest.mark.parametrize("wait_until_forgotten", [True, False])
@gen_cluster(client=True)
async def test_repeat_shuffle_operation(c, s, a, b, wait_until_forgotten):
    """Tests repeating the same shuffle operation using two distinct instances of the
    task graph.

    See Also
    --------
    test_repeat_shuffle_instance
    """
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    await c.compute(dd.shuffle.shuffle(df, "x", shuffle="p2p"))

    if wait_until_forgotten:
        while s.tasks:
            await asyncio.sleep(0)

    await c.compute(dd.shuffle.shuffle(df, "x", shuffle="p2p"))

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_after_shuffle(c, s, a):
    in_event = Event()
    block_event = Event()

    @dask.delayed
    def block(df, in_event, block_event):
        in_event.set()
        block_event.wait()
        return df

    async with Nanny(s.address, nthreads=1) as n:
        df = df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-03-01",
            dtypes={"x": float, "y": float},
            freq="100 s",
            seed=42,
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        in_event = Event()
        block_event = Event()
        with dask.annotate(workers=[n.worker_address], allow_other_workers=True):
            out = block(out, in_event, block_event)
        out = c.compute(out)

        await wait_until_worker_has_tasks("shuffle_p2p", n.worker_address, 1, s)
        await in_event.wait()
        await n.process.process.kill()
        await block_event.set()

        out = await out
        result = out.x.size
        expected = await c.compute(df.x.size)
        assert result == expected

        await c.close()
        await check_worker_cleanup(a)
        await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_crashed_worker_after_shuffle_persisted(c, s, a):
    async with Nanny(s.address, nthreads=1) as n:
        df = df = dask.datasets.timeseries(
            start="2000-01-01",
            end="2000-01-10",
            dtypes={"x": float, "y": float},
            freq="10 s",
            seed=42,
        )
        out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
        out = out.persist()

        await wait_until_worker_has_tasks("shuffle_p2p", n.worker_address, 1, s)
        await out

        await n.process.process.kill()

        result, expected = c.compute([out.x.size, df.x.size])
        result = await result
        expected = await expected
        assert result == expected

        await c.close()
        await check_worker_cleanup(a)
        await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)] * 3)
async def test_closed_worker_between_repeats(c, s, w1, w2, w3):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
        seed=42,
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    await c.compute(out.head(compute=False))

    await check_worker_cleanup(w1)
    await check_worker_cleanup(w2)
    await check_worker_cleanup(w3)
    await check_scheduler_cleanup(s)

    await w3.close()
    await c.compute(out.tail(compute=False))

    await check_worker_cleanup(w1)
    await check_worker_cleanup(w2)
    await check_worker_cleanup(w3, closed=True)
    await check_scheduler_cleanup(s)

    await w2.close()
    await c.compute(out.head(compute=False))
    await check_worker_cleanup(w1)
    await check_worker_cleanup(w2, closed=True)
    await check_worker_cleanup(w3, closed=True)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_new_worker(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-20",
        dtypes={"x": float, "y": float},
        freq="1 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    persisted = shuffled.persist()
    while not s.plugins["shuffle"].active_shuffles:
        await asyncio.sleep(0.001)

    async with Worker(s.address) as w:
        await c.compute(persisted)

        await check_worker_cleanup(a)
        await check_worker_cleanup(b)
        await check_worker_cleanup(w)
        del persisted
        await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_multi(c, s, a, b):
    left = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-20",
        freq="10s",
        dtypes={"id": float, "x": float},
    )
    right = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        freq="10s",
        dtypes={"id": float, "y": float},
    )
    left["id"] = (left["id"] * 1000000).astype(int)
    right["id"] = (right["id"] * 1000000).astype(int)

    out = left.merge(right, on="id", shuffle="p2p")
    out = await c.compute(out.size)
    assert out

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_restrictions(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    ).persist(workers=a.address)
    await df
    assert a.data
    assert not b.data

    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    x = x.persist(workers=b.address)
    y = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    y = y.persist(workers=a.address)

    await x
    assert all(key in b.data for key in x.__dask_keys__())

    await y
    assert all(key in a.data for key in y.__dask_keys__())


@gen_cluster(client=True)
async def test_delete_some_results(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p").persist()
    while not s.tasks or not any(ts.state == "memory" for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    x = x.partitions[: x.npartitions // 2]
    x = await c.compute(x.size)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    del x
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_add_some_results(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    y = x.partitions[: x.npartitions // 2].persist()

    while not s.tasks or not any(ts.state == "memory" for ts in s.tasks.values()):
        await asyncio.sleep(0.01)

    x = x.persist()

    await c.compute(x.size)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    del x
    del y
    await check_scheduler_cleanup(s)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_clean_after_close(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2001-01-01",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )

    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    await wait_for_tasks_in_state("shuffle-transfer", "executing", 1, a)
    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, b)

    await a.close()
    await check_worker_cleanup(a, closed=True)

    del out
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


class DataFrameShuffleTestPool(AbstractShuffleTestPool):
    _shuffle_run_id_iterator = itertools.count()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._executor = ThreadPoolExecutor(2)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._executor.shutdown(cancel_futures=True)
        except Exception:  # pragma: no cover
            self._executor.shutdown()

    def new_shuffle(
        self,
        name,
        meta,
        worker_for_mapping,
        directory,
        loop,
        disk,
        Shuffle=DataFrameShuffleRun,
    ):
        s = Shuffle(
            column="_partition",
            meta=meta,
            worker_for=worker_for_mapping,
            directory=directory / name,
            id=ShuffleId(name),
            run_id=next(AbstractShuffleTestPool._shuffle_run_id_iterator),
            local_address=name,
            executor=self._executor,
            rpc=self,
            scheduler=self,
            memory_limiter_disk=ResourceLimiter(10000000),
            memory_limiter_comms=ResourceLimiter(10000000),
            disk=disk,
        )
        self.shuffles[name] = s
        return s


# 36 parametrizations
# Runtime each ~0.1s
@pytest.mark.parametrize("n_workers", [1, 10])
@pytest.mark.parametrize("n_input_partitions", [1, 2, 10])
@pytest.mark.parametrize("npartitions", [1, 20])
@pytest.mark.parametrize("barrier_first_worker", [True, False])
@pytest.mark.parametrize("disk", [True, False])
@gen_test()
async def test_basic_lowlevel_shuffle(
    tmp_path,
    loop_in_thread,
    n_workers,
    n_input_partitions,
    npartitions,
    barrier_first_worker,
    disk,
):
    pa = pytest.importorskip("pyarrow")

    dfs = []
    rows_per_df = 10
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)

    workers = list("abcdefghijklmn")[:n_workers]

    worker_for_mapping = {}

    for part in range(npartitions):
        worker_for_mapping[part] = _get_worker_for_range_sharding(
            npartitions, part, workers
        )
    assert len(set(worker_for_mapping.values())) == min(n_workers, npartitions)
    meta = dfs[0].head(0)

    with DataFrameShuffleTestPool() as local_shuffle_pool:
        shuffles = []
        for ix in range(n_workers):
            shuffles.append(
                local_shuffle_pool.new_shuffle(
                    name=workers[ix],
                    meta=meta,
                    worker_for_mapping=worker_for_mapping,
                    directory=tmp_path,
                    loop=loop_in_thread,
                    disk=disk,
                )
            )
        random.seed(42)
        if barrier_first_worker:
            barrier_worker = shuffles[0]
        else:
            barrier_worker = random.sample(shuffles, k=1)[0]

        run_ids = []
        try:
            for ix, df in enumerate(dfs):
                s = shuffles[ix % len(shuffles)]
                run_ids.append(await s.add_partition(df, ix))

            await barrier_worker.barrier(run_ids=run_ids)

            total_bytes_sent = 0
            total_bytes_recvd = 0
            total_bytes_recvd_shuffle = 0
            for s in shuffles:
                metrics = s.heartbeat()
                assert metrics["comm"]["total"] == metrics["comm"]["written"]
                total_bytes_sent += metrics["comm"]["written"]
                total_bytes_recvd += metrics["disk"]["total"]
                total_bytes_recvd_shuffle += s.total_recvd

            assert total_bytes_recvd_shuffle == total_bytes_sent

            all_parts = []
            for part, worker in worker_for_mapping.items():
                s = local_shuffle_pool.shuffles[worker]
                all_parts.append(s.get_output_partition(part, f"key-{part}", meta=meta))

            all_parts = await asyncio.gather(*all_parts)

            df_after = pd.concat(all_parts)
        finally:
            await asyncio.gather(*[s.close() for s in shuffles])
        assert len(df_after) == len(pd.concat(dfs))


@gen_test()
async def test_error_offload(tmp_path, loop_in_thread):
    pa = pytest.importorskip("pyarrow")

    dfs = []
    rows_per_df = 10
    n_input_partitions = 2
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)
    meta = dfs[0].head(0)
    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = _get_worker_for_range_sharding(
            npartitions, part, workers
        )
        partitions_for_worker[w].append(part)

    class ErrorOffload(DataFrameShuffleRun):
        async def offload(self, func, *args):
            raise RuntimeError("Error during deserialization")

    with DataFrameShuffleTestPool() as local_shuffle_pool:
        sA = local_shuffle_pool.new_shuffle(
            name="A",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
            Shuffle=ErrorOffload,
        )
        sB = local_shuffle_pool.new_shuffle(
            name="B",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
        )
        try:
            await sB.add_partition(dfs[0], 0)
            with pytest.raises(RuntimeError, match="Error during deserialization"):
                await sB.add_partition(dfs[1], 1)
                await sB.barrier(run_ids=[sB.run_id, sB.run_id])
        finally:
            await asyncio.gather(*[s.close() for s in [sA, sB]])


@gen_test()
async def test_error_send(tmp_path, loop_in_thread):
    pa = pytest.importorskip("pyarrow")

    dfs = []
    rows_per_df = 10
    n_input_partitions = 1
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)
    meta = dfs[0].head(0)

    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = _get_worker_for_range_sharding(
            npartitions, part, workers
        )
        partitions_for_worker[w].append(part)

    class ErrorSend(DataFrameShuffleRun):
        async def send(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("Error during send")

    with DataFrameShuffleTestPool() as local_shuffle_pool:
        sA = local_shuffle_pool.new_shuffle(
            name="A",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
            Shuffle=ErrorSend,
        )
        sB = local_shuffle_pool.new_shuffle(
            name="B",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
        )
        try:
            await sA.add_partition(dfs[0], 0)
            with pytest.raises(RuntimeError, match="Error during send"):
                await sA.barrier(run_ids=[sA.run_id])
        finally:
            await asyncio.gather(*[s.close() for s in [sA, sB]])


@gen_test()
async def test_error_receive(tmp_path, loop_in_thread):
    pa = pytest.importorskip("pyarrow")

    dfs = []
    rows_per_df = 10
    n_input_partitions = 1
    npartitions = 2
    for ix in range(n_input_partitions):
        df = pd.DataFrame({"x": range(rows_per_df * ix, rows_per_df * (ix + 1))})
        df["_partition"] = df.x % npartitions
        dfs.append(df)
    meta = dfs[0].head(0)

    workers = ["A", "B"]

    worker_for_mapping = {}
    partitions_for_worker = defaultdict(list)

    for part in range(npartitions):
        worker_for_mapping[part] = w = _get_worker_for_range_sharding(
            npartitions, part, workers
        )
        partitions_for_worker[w].append(part)

    class ErrorReceive(DataFrameShuffleRun):
        async def receive(self, data: list[tuple[int, bytes]]) -> None:
            raise RuntimeError("Error during receive")

    with DataFrameShuffleTestPool() as local_shuffle_pool:
        sA = local_shuffle_pool.new_shuffle(
            name="A",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
            Shuffle=ErrorReceive,
        )
        sB = local_shuffle_pool.new_shuffle(
            name="B",
            meta=meta,
            worker_for_mapping=worker_for_mapping,
            directory=tmp_path,
            loop=loop_in_thread,
            disk=True,
        )
        try:
            await sB.add_partition(dfs[0], 0)
            with pytest.raises(RuntimeError, match="Error during receive"):
                await sB.barrier(run_ids=[sB.run_id])
        finally:
            await asyncio.gather(*[s.close() for s in [sA, sB]])


class BlockedShuffleReceiveShuffleWorkerPlugin(ShuffleWorkerPlugin):
    def setup(self, worker: Worker) -> None:
        super().setup(worker)
        self.in_shuffle_receive = asyncio.Event()
        self.block_shuffle_receive = asyncio.Event()

    async def shuffle_receive(self, *args: Any, **kwargs: Any) -> None:
        self.in_shuffle_receive.set()
        await self.block_shuffle_receive.wait()
        return await super().shuffle_receive(*args, **kwargs)


@pytest.mark.parametrize("wait_until_forgotten", [True, False])
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_deduplicate_stale_transfer(c, s, a, b, wait_until_forgotten):
    await c.register_plugin(BlockedShuffleReceiveShuffleWorkerPlugin(), name="shuffle")
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    shuffled = shuffled.persist()

    shuffle_extA = a.plugins["shuffle"]
    shuffle_extB = b.plugins["shuffle"]
    await asyncio.gather(
        shuffle_extA.in_shuffle_receive.wait(), shuffle_extB.in_shuffle_receive.wait()
    )
    del shuffled

    if wait_until_forgotten:
        while s.tasks or get_active_shuffle_runs(a) or get_active_shuffle_runs(b):
            await asyncio.sleep(0)

    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    result = c.compute(shuffled)
    await wait_until_new_shuffle_is_initialized(s)
    shuffle_extA.block_shuffle_receive.set()
    shuffle_extB.block_shuffle_receive.set()

    result = await result
    expected = await c.compute(df)
    dd.assert_eq(result, expected)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


class BlockedBarrierShuffleWorkerPlugin(ShuffleWorkerPlugin):
    def setup(self, worker: Worker) -> None:
        super().setup(worker)
        self.in_barrier = asyncio.Event()
        self.block_barrier = asyncio.Event()

    async def _barrier(self, *args: Any, **kwargs: Any) -> int:
        self.in_barrier.set()
        await self.block_barrier.wait()
        return await super()._barrier(*args, **kwargs)


@pytest.mark.parametrize("wait_until_forgotten", [True, False])
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_handle_stale_barrier(c, s, a, b, wait_until_forgotten):
    await c.register_plugin(BlockedBarrierShuffleWorkerPlugin(), name="shuffle")
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    shuffled = shuffled.persist()

    shuffle_extA = a.plugins["shuffle"]
    shuffle_extB = b.plugins["shuffle"]

    wait_for_barrier_on_A_task = asyncio.create_task(shuffle_extA.in_barrier.wait())
    wait_for_barrier_on_B_task = asyncio.create_task(shuffle_extB.in_barrier.wait())

    await asyncio.wait(
        [wait_for_barrier_on_A_task, wait_for_barrier_on_B_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    del shuffled

    if wait_until_forgotten:
        while s.tasks:
            await asyncio.sleep(0)

    shuffled = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    fut = c.compute([shuffled, df], sync=True)
    await wait_until_new_shuffle_is_initialized(s)
    shuffle_extA.block_barrier.set()
    shuffle_extB.block_barrier.set()

    result, expected = await fut
    dd.assert_eq(result, expected)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_shuffle_run_consistency(c, s, a):
    """This test checks the correct creation of shuffle run IDs through the scheduler
    as well as the correct handling through the workers.

    In particular, newer run IDs for the same shuffle must always be larger than
    previous ones so that we can detect stale runs.

    .. note:
        The P2P implementation relies on the correctness of this behavior,
        but it is an implementation detail that users should not rely upon.
    """
    await c.register_plugin(BlockedBarrierShuffleWorkerPlugin(), name="shuffle")
    run_manager = get_shuffle_run_manager(a)
    worker_plugin = a.plugins["shuffle"]
    scheduler_ext = s.plugins["shuffle"]

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    # Initialize first shuffle execution
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    spec = scheduler_ext.get(shuffle_id, a.worker_address).data

    # Shuffle run manager can fetch the current run
    assert await run_manager.get_with_run_id(shuffle_id, spec.run_id)

    # This should never occur, but fetching an ID larger than the ID available on
    # the scheduler should result in an error.
    with pytest.raises(RuntimeError, match="invalid"):
        await run_manager.get_with_run_id(shuffle_id, spec.run_id + 1)

    # Finish first execution
    worker_plugin.block_barrier.set()
    await out
    del out
    while s.tasks:
        await asyncio.sleep(0)
    worker_plugin.block_barrier.clear()

    # Initialize second shuffle execution
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    new_shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    assert shuffle_id == new_shuffle_id

    new_spec = scheduler_ext.get(shuffle_id, a.worker_address).data

    # Check invariant that the new run ID is larger than the previous
    assert spec.run_id < new_spec.run_id

    # Shuffle run manager can fetch the new shuffle run
    assert await run_manager.get_with_run_id(shuffle_id, new_spec.run_id)

    # Fetching a stale run from a worker aware of the new run raises an error
    with pytest.raises(RuntimeError, match="stale"):
        await run_manager.get_with_run_id(shuffle_id, spec.run_id)

    worker_plugin.block_barrier.set()
    await out
    del out
    while s.tasks:
        await asyncio.sleep(0)
    worker_plugin.block_barrier.clear()

    # Create an unrelated shuffle on a different column
    out = dd.shuffle.shuffle(df, "y", shuffle="p2p")
    out = out.persist()
    independent_shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    assert shuffle_id != independent_shuffle_id

    independent_spec = scheduler_ext.get(independent_shuffle_id, a.worker_address).data

    # Check invariant that the new run ID is larger than the previous
    # for independent shuffles
    assert new_spec.run_id < independent_spec.run_id

    worker_plugin.block_barrier.set()
    await out
    del out

    await check_worker_cleanup(a)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_fail_fetch_race(c, s, a):
    """This test manually triggers a race condition where a `shuffle_fail` arrives on
    the worker before the result of `get` or `get_or_create`.

    TODO: This assumes that there are no ordering guarantees between failing and fetching
    This test checks the correct creation of shuffle run IDs through the scheduler
    as well as the correct handling through the workers. It can be removed once ordering
    is guaranteed.
    """
    await c.register_plugin(BlockedBarrierShuffleWorkerPlugin(), name="shuffle")
    run_manager = get_shuffle_run_manager(a)
    worker_plugin = a.plugins["shuffle"]
    scheduler_ext = s.plugins["shuffle"]

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    spec = scheduler_ext.get(shuffle_id, a.worker_address).data
    await worker_plugin.in_barrier.wait()
    # Pretend that the fail from the scheduler arrives first
    run_manager.fail(shuffle_id, spec.run_id, "error")
    assert shuffle_id not in run_manager._active_runs

    with pytest.raises(RuntimeError, match="Received stale shuffle run"):
        await run_manager.get_with_run_id(shuffle_id, spec.run_id)
    assert shuffle_id not in run_manager._active_runs

    with pytest.raises(RuntimeError, match="Received stale shuffle run"):
        await run_manager.get_or_create(spec.spec, "test-key")
    assert shuffle_id not in run_manager._active_runs

    worker_plugin.block_barrier.set()
    del out

    await check_worker_cleanup(a)
    await check_scheduler_cleanup(s)


class BlockedShuffleAccessAndFailShuffleRunManager(_ShuffleRunManager):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.in_get_or_create_shuffle = asyncio.Event()
        self.block_get_or_create_shuffle = asyncio.Event()
        self.in_get_shuffle_run = asyncio.Event()
        self.block_get_shuffle_run = asyncio.Event()
        self.finished_get_shuffle_run = asyncio.Event()
        self.allow_fail = False

    async def get_or_create(self, *args: Any, **kwargs: Any) -> ShuffleRun:
        self.in_get_or_create_shuffle.set()
        await self.block_get_or_create_shuffle.wait()
        return await super().get_or_create(*args, **kwargs)

    async def get_with_run_id(self, *args: Any, **kwargs: Any) -> ShuffleRun:
        self.in_get_shuffle_run.set()
        await self.block_get_shuffle_run.wait()
        result = await super().get_with_run_id(*args, **kwargs)
        self.finished_get_shuffle_run.set()
        return result

    def fail(self, *args: Any, **kwargs: Any) -> None:
        if self.allow_fail:
            return super().fail(*args, **kwargs)


@mock.patch(
    "distributed.shuffle._worker_plugin._ShuffleRunManager",
    BlockedShuffleAccessAndFailShuffleRunManager,
)
@gen_cluster(client=True, nthreads=[("", 1)] * 2)
async def test_replace_stale_shuffle(c, s, a, b):
    run_manager_A = cast(
        BlockedShuffleAccessAndFailShuffleRunManager, get_shuffle_run_manager(a)
    )
    run_manager_B = cast(
        BlockedShuffleAccessAndFailShuffleRunManager, get_shuffle_run_manager(b)
    )

    # Let A behave normal
    run_manager_A.allow_fail = True
    run_manager_A.block_get_shuffle_run.set()
    run_manager_A.block_get_or_create_shuffle.set()

    # B can accept shuffle transfers
    run_manager_B.block_get_shuffle_run.set()

    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="100 s",
    )
    # Initialize first shuffle execution
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    shuffle_id = await wait_until_new_shuffle_is_initialized(s)

    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, a)
    await run_manager_B.finished_get_shuffle_run.wait()
    assert shuffle_id in get_active_shuffle_runs(a)
    assert shuffle_id in get_active_shuffle_runs(b)
    stale_shuffle_run = get_active_shuffle_run(shuffle_id, b)

    del out
    while s.tasks:
        await asyncio.sleep(0)

    # A is cleaned
    await check_worker_cleanup(a)

    # B is not cleaned
    assert shuffle_id in get_active_shuffle_runs(b)
    assert not stale_shuffle_run.closed
    run_manager_B.finished_get_shuffle_run.clear()
    run_manager_B.allow_fail = True

    # Initialize second shuffle execution
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = out.persist()

    await wait_for_tasks_in_state("shuffle-transfer", "memory", 1, a)
    await run_manager_B.finished_get_shuffle_run.wait()

    # Stale shuffle run has been replaced
    shuffle_run = get_active_shuffle_run(shuffle_id, b)
    assert shuffle_run != stale_shuffle_run
    assert shuffle_run.run_id > stale_shuffle_run.run_id

    # Stale shuffle gets cleaned up
    await stale_shuffle_run._closed_event.wait()

    # Finish shuffle run
    run_manager_B.block_get_shuffle_run.set()
    run_manager_B.block_get_or_create_shuffle.set()
    run_manager_B.allow_fail = True
    await out
    del out

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@pytest.mark.skipif(not PANDAS_GE_200, reason="requires pandas >=2.0")
@gen_cluster(client=True)
async def test_handle_null_partitions_p2p_shuffling(c, s, a, b):
    data = [
        {"companies": [], "id": "a", "x": None},
        {"companies": [{"id": 3}, {"id": 5}], "id": "b", "x": None},
        {"companies": [{"id": 3}, {"id": 4}, {"id": 5}], "id": "c", "x": "b"},
        {"companies": [{"id": 9}], "id": "a", "x": "a"},
    ]
    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)
    ddf = ddf.shuffle(on="id", shuffle="p2p", ignore_index=True)
    result = await c.compute(ddf)
    dd.assert_eq(result, df)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_handle_null_partitions_p2p_shuffling_2(c, s, a, b):
    def make_partition(i):
        """Return null column for every other partition"""
        if i % 2 == 1:
            return pd.DataFrame({"a": np.random.random(10), "b": [None] * 10})
        return pd.DataFrame({"a": np.random.random(10), "b": np.random.random(10)})

    ddf = dd.from_map(make_partition, range(50))
    out = ddf.shuffle(on="a", shuffle="p2p", ignore_index=True)
    result, expected = c.compute([ddf, out])
    del out
    result = await result
    expected = await expected
    dd.assert_eq(result, expected)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_handle_object_columns_p2p(c, s, a, b):
    with dask.config.set({"dataframe.convert-string": False}):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [
                    np.asarray([1, 2, 3]),
                    np.asarray([4, 5, 6]),
                    np.asarray([7, 8, 9]),
                ],
                "c": ["foo", "bar", "baz"],
            }
        )

        ddf = dd.from_pandas(
            df,
            npartitions=2,
        )
        shuffled = ddf.shuffle(on="a")

        result = await c.compute(shuffled)
    dd.assert_eq(result, df)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_reconcile_mismatching_partitions_p2p_shuffling(c, s, a, b):
    def make_partition(i):
        """Return mismatched column types for every other partition"""
        if i % 2 == 1:
            return pd.DataFrame({"a": np.random.random(10), "b": [True] * 10})
        return pd.DataFrame({"a": np.random.random(10), "b": np.random.random(10)})

    ddf = dd.from_map(make_partition, range(50))
    out = ddf.shuffle(on="a", shuffle="p2p", ignore_index=True)
    result, expected = c.compute([ddf, out])
    del out
    result = await result
    expected = await expected
    dd.assert_eq(result, expected)
    del result

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_raise_on_incompatible_partitions_p2p_shuffling(c, s, a, b):
    def make_partition(i):
        """Return incompatible column types for every other partition"""
        if i % 2 == 1:
            return pd.DataFrame({"a": np.random.random(10), "b": ["a"] * 10})
        return pd.DataFrame({"a": np.random.random(10), "b": np.random.random(10)})

    ddf = dd.from_map(make_partition, range(50))
    out = ddf.shuffle(on="a", shuffle="p2p", ignore_index=True)
    with raises_with_cause(
        RuntimeError, "failed during transfer", ValueError, "could not convert"
    ):
        await c.compute(out)

    await c.close()
    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


@gen_cluster(client=True)
async def test_set_index_p2p(c, s, *workers):
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8], "b": 1})
    ddf = dd.from_pandas(df, npartitions=3)
    ddf = ddf.set_index("a", shuffle="p2p", divisions=(1, 3, 8))
    assert ddf.npartitions == 2
    result = await c.compute(ddf)
    dd.assert_eq(result, df.set_index("a"))

    await c.close()
    await asyncio.gather(*[check_worker_cleanup(w) for w in workers])
    await check_scheduler_cleanup(s)


def test_shuffle_p2p_with_existing_index(client):
    df = pd.DataFrame({"a": np.random.randint(0, 3, 20)}, index=np.random.random(20))
    ddf = dd.from_pandas(
        df,
        npartitions=4,
    )
    ddf = ddf.shuffle("a", shuffle="p2p")
    result = client.compute(ddf, sync=True)
    dd.assert_eq(result, df)


def test_set_index_p2p_with_existing_index(client):
    df = pd.DataFrame({"a": np.random.randint(0, 3, 20)}, index=np.random.random(20))
    ddf = dd.from_pandas(
        df,
        npartitions=4,
    )
    ddf = ddf.set_index("a", shuffle="p2p")
    result = client.compute(ddf, sync=True)
    dd.assert_eq(result, df.set_index("a"))


def test_sort_values_p2p_with_existing_divisions(client):
    "Regression test for #8165"
    df = pd.DataFrame(
        {"a": np.random.randint(0, 3, 20), "b": np.random.randint(0, 3, 20)}
    )
    ddf = dd.from_pandas(
        df,
        npartitions=4,
    )
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        ddf = ddf.set_index("a").sort_values("b")
        result = client.compute(ddf, sync=True)
        dd.assert_eq(
            result,
            df.set_index("a").sort_values("b"),
            check_index=False,
            sort_results=False,
        )


class BlockedBarrierShuffleRun(DataFrameShuffleRun):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.in_barrier = asyncio.Event()
        self.block_barrier = asyncio.Event()

    async def barrier(self, *args: Any, **kwargs: Any) -> int:
        self.in_barrier.set()
        await self.block_barrier.wait()
        return await super().barrier(*args, **kwargs)


@mock.patch(
    "distributed.shuffle._shuffle.DataFrameShuffleRun",
    BlockedBarrierShuffleRun,
)
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_unpack_gets_rescheduled_from_non_participating_worker(c, s, a):
    await invoke_annotation_chaos(1.0, c)

    expected = pd.DataFrame({"a": list(range(10))})
    ddf = dd.from_pandas(expected, npartitions=2)
    ddf = ddf.shuffle("a")
    fut = c.compute(ddf)

    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    key = barrier_key(shuffle_id)
    await wait_for_state(key, "processing", s)
    shuffleA = get_active_shuffle_run(shuffle_id, a)
    await shuffleA.in_barrier.wait()

    async with Worker(s.address) as b:
        # Restrict an unpack task to B so that the previously non-participating
        # worker takes part in the unpack phase
        for key in s.tasks:
            if key_split(key) == "shuffle_p2p":
                s.set_restrictions({key: {b.address}})
                break

        shuffleA.block_barrier.set()
        result = await fut
        dd.assert_eq(result, expected)


class FlakyConnectionPool(ConnectionPool):
    def __init__(self, *args, failing_connects=0, **kwargs):
        self.attempts = 0
        self.failed_attempts = 0
        self.failing_connects = failing_connects
        super().__init__(*args, **kwargs)

    async def connect(self, *args, **kwargs):
        self.attempts += 1
        if self.attempts > self.failing_connects:
            return await super().connect(*args, **kwargs)

        with dask.config.set({"distributed.comm.timeouts.connect": "0 ms"}):
            try:
                _ = await super().connect(*args, **kwargs)
            except Exception:
                self.failed_attempts += 1
                raise


@gen_cluster(
    client=True,
    config={"distributed.comm.retry.count": 0, "distributed.p2p.comm.retry.count": 0},
)
async def test_p2p_flaky_connect_fails_without_retry(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")

    rpc = await FlakyConnectionPool(failing_connects=1)

    with mock.patch.object(a, "rpc", rpc):
        with raises_with_cause(
            expected_exception=RuntimeError,
            match="P2P shuffling.*transfer",
            expected_cause=OSError,
            match_cause=None,
        ):
            await c.compute(x)

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await c.close()
    await check_scheduler_cleanup(s)


@gen_cluster(
    client=True,
    config={"distributed.comm.retry.count": 0, "distributed.p2p.comm.retry.count": 1},
)
async def test_p2p_flaky_connect_recover_with_retry(c, s, a, b):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    x = dd.shuffle.shuffle(df, "x", shuffle="p2p")

    rpc = await FlakyConnectionPool(failing_connects=1)

    with mock.patch.object(a, "rpc", rpc):
        await c.compute(x)
    assert rpc.failed_attempts == 1

    await check_worker_cleanup(a)
    await check_worker_cleanup(b)
    await check_scheduler_cleanup(s)


class BlockedAfterGatherDep(Worker):
    def __init__(self, *args, **kwargs):
        self.after_gather_dep = asyncio.Event()
        self.block_gather_dep = asyncio.Event()
        super().__init__(*args, **kwargs)

    async def gather_dep(self, *args, **kwargs):
        result = await super().gather_dep(*args, **kwargs)
        self.after_gather_dep.set()
        await self.block_gather_dep.wait()
        return result


@gen_cluster(client=True, nthreads=[("", 1)] * 3, Worker=BlockedAfterGatherDep)
async def test_barrier_handles_stale_resumed_transfer(c, s, *workers):
    df = dask.datasets.timeseries(
        start="2000-01-01",
        end="2000-01-10",
        dtypes={"x": float, "y": float},
        freq="10 s",
    )
    out = dd.shuffle.shuffle(df, "x", shuffle="p2p")
    out = c.compute(out)
    shuffle_id = await wait_until_new_shuffle_is_initialized(s)
    key = barrier_key(shuffle_id)
    await wait_for_state(key, "processing", s)
    barrier_worker = None
    bts = s.tasks[key]
    workers = list(workers)
    for w in workers:
        if w.address == bts.processing_on.address:
            barrier_worker = w
            workers.remove(w)
            break
    assert barrier_worker
    closed_worker, remaining_worker = workers
    remaining_worker.block_gather_dep.set()
    await wait_for_tasks_in_state("shuffle-transfer", "flight", 1, barrier_worker)
    await barrier_worker.after_gather_dep.wait()
    await closed_worker.close()
    await wait_for_tasks_in_state("shuffle-transfer", "resumed", 1, barrier_worker)
    barrier_worker.block_gather_dep.set()
    await out
